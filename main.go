package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/coder/websocket"
)

type Room struct {
	mu          sync.Mutex
	connections map[*websocket.Conn]int
	seatOwner   map[int]bool
	state       []byte

	playerSeat  map[string]int
	playerConns map[string]int
}

type RoomRegistry struct {
	mu    sync.RWMutex
	cards map[string]string
	rooms map[string]*Room
}

var registry = &RoomRegistry{
	cards: make(map[string]string),
	rooms: make(map[string]*Room),
}

func (r *RoomRegistry) register(cardID, chatJID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cards[cardID] = chatJID
	if r.rooms[cardID] == nil {
		r.rooms[cardID] = &Room{
			connections: make(map[*websocket.Conn]int),
			seatOwner:   make(map[int]bool),
			playerSeat:  make(map[string]int),
			playerConns: make(map[string]int),
		}
	}
}

func (r *RoomRegistry) resolve(cardID string) (string, *Room, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	chatJID, chatOK := r.cards[cardID]
	room, roomOK := r.rooms[cardID]
	return chatJID, room, chatOK && roomOK
}

func (room *Room) join(conn *websocket.Conn, playerID string) (int, int) {
	room.mu.Lock()
	defer room.mu.Unlock()
	if playerID == "" {
		playerID = "anonymous"
	}
	seat, reused := room.playerSeat[playerID]
	if reused {
		room.connections[conn] = seat
		room.playerConns[playerID]++
		return seat, len(room.playerSeat)
	}
	seat = -1
	for i := 0; i < 4; i++ {
		if !room.seatOwner[i] {
			seat = i
			room.seatOwner[i] = true
			break
		}
	}
	room.connections[conn] = seat
	if seat >= 0 {
		room.playerSeat[playerID] = seat
		room.playerConns[playerID] = 1
	}
	return seat, len(room.playerSeat)
}

func (room *Room) leave(conn *websocket.Conn) {
	room.mu.Lock()
	defer room.mu.Unlock()
	delete(room.connections, conn)
}

func (room *Room) broadcast(payload []byte) {
	room.mu.Lock()
	defer room.mu.Unlock()
	for conn := range room.connections {
		if err := conn.Write(context.Background(), websocket.MessageText, payload); err != nil {
			log.Printf("[WS] broadcast write error: %v", err)
		}
	}
}

func (room *Room) snapshot() []byte {
	room.mu.Lock()
	defer room.mu.Unlock()
	return append([]byte(nil), room.state...)
}

func (room *Room) setState(payload []byte) {
	room.mu.Lock()
	room.state = append([]byte(nil), payload...)
	room.mu.Unlock()
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		registerToken := os.Getenv("REGISTER_TOKEN")
		if registerToken != "" && r.Header.Get("X-Register-Token") != registerToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var input struct {
			CardID string `json:"card_id"`
			Chat   string `json:"chat"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.CardID == "" || input.Chat == "" {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		registry.register(input.CardID, input.Chat)
		log.Printf("[CARD] registered card=%s chat=%s", input.CardID, input.Chat)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]bool{"registered": true})
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		cardID := r.URL.Query().Get("card")
		chatJID, room, ok := registry.resolve(cardID)
		if cardID == "" || !ok {
			log.Printf("[WS] rejected card=%q remote=%s", cardID, r.RemoteAddr)
			http.Error(w, "unknown card", http.StatusUnauthorized)
			return
		}

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			log.Printf("[WS] accept error card=%s err=%v", cardID, err)
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "closed")

		playerID := r.URL.Query().Get("player")
		seat, players := room.join(conn, playerID)
		defer room.leave(conn)
		joined, _ := json.Marshal(map[string]any{"type": "joined", "seat": seat, "players": players, "ready": players >= 2, "card": cardID, "player": playerID})
		room.broadcast(joined)
		if snapshot := room.snapshot(); len(snapshot) > 0 {
			if err := conn.Write(r.Context(), websocket.MessageText, snapshot); err != nil {
				log.Printf("[WS] snapshot write error card=%s: %v", cardID, err)
			}
		}

		log.Printf("[WS] JOIN card=%s chat=%s seat=%d players=%d remote=%s", cardID, chatJID, seat, players, r.RemoteAddr)

		for {
			messageType, payload, err := conn.Read(r.Context())
			if err != nil {
				left, _ := json.Marshal(map[string]any{"type": "left", "seat": seat, "card": cardID})
				room.broadcast(left)
				log.Printf("[WS] CLOSE card=%s chat=%s seat=%d err=%v", cardID, chatJID, seat, err)
				return
			}
			if messageType != websocket.MessageText {
				continue
			}

			var event map[string]any
			if err := json.Unmarshal(payload, &event); err != nil {
				continue
			}
			event["card"] = cardID
			event["seat"] = seat
			relay, _ := json.Marshal(event)
			if event["type"] == "state" {
				room.setState(relay)
			}
			room.broadcast(relay)
			log.Printf("[WS] ACTION card=%s chat=%s seat=%d payload=%s", cardID, chatJID, seat, relay)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	address := ":" + port
	log.Printf("[WS] listening on %s", address)
	log.Fatal(http.ListenAndServe(address, withCORS(mux)))
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Register-Token")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

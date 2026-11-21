package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/coder/websocket"
)

type Registry struct {
	mu    sync.RWMutex
	cards map[string]string
}

var registry = &Registry{cards: make(map[string]string)}

func (r *Registry) register(cardID, chatJID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cards[cardID] = chatJID
}

func (r *Registry) resolve(cardID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	chatJID, ok := r.cards[cardID]
	return chatJID, ok
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
		chatJID, ok := registry.resolve(cardID)
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

		log.Printf("[WS] OPEN card=%s chat=%s remote=%s", cardID, chatJID, r.RemoteAddr)
		for {
			messageType, payload, err := conn.Read(r.Context())
			if err != nil {
				log.Printf("[WS] CLOSE card=%s chat=%s err=%v", cardID, chatJID, err)
				return
			}
			log.Printf("[WS] MESSAGE card=%s chat=%s type=%d payload=%s", cardID, chatJID, messageType, payload)
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

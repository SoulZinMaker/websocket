# Ludo WebSocket Server

Deploy this folder as a Render Web Service.

Endpoints:
- `GET /health`
- `GET /ws?card=<card-id>`
- `POST /register` with `X-Register-Token` and JSON `{"card_id":"...","chat":"..."}`

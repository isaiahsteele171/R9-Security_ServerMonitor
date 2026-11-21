import asyncio
import time
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import uvicorn

# --- CONFIGURATION ---
HOST = "0.0.0.0"       # Listen on all interfaces so VMs/LAN nodes can reach it
PORT = 8443            # Match the port configured in agent-setup.sh & UI settings
AUTH_TOKEN = ""        # Leave empty "" to disable auth, or set a shared secret token
STALE_TIMEOUT_SEC = 60 # Mark node offline if no heartbeat for 60s
# ---------------------

app = FastAPI(title="Server Telemetry Collector")

# Allow the browser UI (even opened via file:// or another port) to fetch data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory store keyed by node_id
nodes_store: Dict[str, Dict[str, Any]] = {}
connected_clients: List[WebSocket] = []


def verify_token(authorization: Optional[str]):
    if not AUTH_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")


async def broadcast_update(node_id: str, payload: Dict[str, Any]):
    """Push incoming node telemetry immediately to any connected WebSocket UI clients."""
    if not connected_clients:
        return
    message = {"nodeId": node_id, "payload": payload}
    dead_clients = []
    for ws in connected_clients:
        try:
            await ws.send_json(message)
        except Exception:
            dead_clients.append(ws)
    for ws in dead_clients:
        if ws in connected_clients:
            connected_clients.remove(ws)


@app.get("/")
async def serve_dashboard():
    """Optional convenience route: serves index.html if placed in the same directory."""
    try:
        return FileResponse("index.html")
    except Exception:
        return {"status": "collector_online", "active_nodes": len(nodes_store)}


@app.post("/api/telemetry")
async def ingest_telemetry(request: Request, authorization: Optional[str] = Header(None)):
    """Endpoint that agent-setup.sh POSTs JSON metrics to."""
    verify_token(authorization)
    data = await request.json()

    node_id = (
        data.get("node_id")
        or data.get("nodeId")
        or data.get("hostname")
        or request.client.host
    )

    # Stamp arrival time and preserve payload fields expected by index.html
    data["node_id"] = node_id
    data["_last_seen"] = time.time()
    data["status"] = "online"

    nodes_store[node_id] = data
    print(f"[INGEST] {node_id} ({request.client.host}) -> CPU: {data.get('cpu', {}).get('usage', 'N/A')}%")

    await broadcast_update(node_id, data)
    return {"status": "ok", "node_id": node_id}


@app.get("/api/nodes")
async def get_nodes(authorization: Optional[str] = Header(None)):
    """Endpoint polled by index.html when using HTTP Polling mode."""
    verify_token(authorization)
    now = time.time()
    for node_id, payload in nodes_store.items():
        if now - payload.get("_last_seen", now) > STALE_TIMEOUT_SEC:
            payload["status"] = "offline"
    return {"nodes": nodes_store}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time WebSocket stream for index.html when using WebSocket mode."""
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        # Immediately send current snapshot of all known nodes on connect
        for node_id, payload in nodes_store.items():
            await websocket.send_json({"nodeId": node_id, "payload": payload})
        while True:
            # Keep connection alive and wait for client pings/close
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


if __name__ == "__main__":
    print(f"Starting Telemetry Collector on http://{HOST}:{PORT}")
    print(f"Agent POST endpoint : http://<YOUR-HOST-IP>:{PORT}/api/telemetry")
    print(f"UI HTTP Poll URL    : http://localhost:{PORT}/api/nodes")
    print(f"UI WebSocket URL    : ws://localhost:{PORT}/ws")
    uvicorn.run(app, host=HOST, port=PORT)
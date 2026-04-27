import threading
import time
import json
import socket
import hashlib
from typing import Dict, List, Optional

# Assuming a simple configuration module exists
from config import MESH_PORT, MESH_DISCOVERY_INTERVAL, MESH_NODE_ID_PREFIX, MESH_PEER_TIMEOUT

class NodeManager:
    """
    Manages the lifecycle and connectivity of nodes within the Flux Distributed JSON Mesh.
    Handles peer discovery, health monitoring, and maintaining the mesh topology.
    """

    def __init__(self, node_id: str, local_address: str = "0.0.0.0"):
        self.node_id = node_id
        self.local_address = local_address
        self.peers: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self._running = False
        self._discovery_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._discovery_socket: Optional[socket.socket] = None

        self.logger = self._get_logger() # Placeholder for a proper logging setup

    def _get_logger(self):
        """ Initializes a basic logger. In a production system, this would be more robust. """
        class SimpleLogger:
            def info(self, msg): print(f"[INFO] {msg}")
            def warning(self, msg): print(f"[WARN] {msg}")
            def error(self, msg): print(f"[ERROR] {msg}")
        return SimpleLogger()

    def start(self):
        """
        Initializes and starts the node manager's core services:
        peer discovery and health monitoring.
        """
        with self._lock:
            if self._running:
                self.logger.warning("NodeManager is already running.")
                return
            self._running = True

            self.logger.info(f"Starting NodeManager for node: {self.node_id}")

            try:
                self._discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self._discovery_socket.bind((self.local_address, MESH_PORT))
                self._discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                self._discovery_socket.settimeout(1.0) # Non-blocking for graceful shutdown
            except socket.error as e:
                self.logger.error(f"Failed to bind discovery socket: {e}")
                self._running = False
                return

            self._discovery_thread = threading.Thread(target=self._run_discovery_service, name="DiscoveryService")
            self._discovery_thread.daemon = True
            self._discovery_thread.start()

            self._heartbeat_thread = threading.Thread(target=self._run_heartbeat_service, name="HeartbeatService")
            self._heartbeat_thread.daemon = True
            self._heartbeat_thread.start()

            self.logger.info("NodeManager services initiated.")

    def stop(self):
        """
        Shuts down all active services and cleans up resources.
        """
        with self._lock:
            if not self._running:
                self.logger.warning("NodeManager is not running.")
                return
            self._running = False

            self.logger.info("Stopping NodeManager services...")

            if self._discovery_socket:
                self._discovery_socket.close()
                self._discovery_socket = None

            if self._discovery_thread:
                self._discovery_thread.join(timeout=5) # Wait for thread to finish
                if self._discovery_thread.is_alive():
                    self.logger.warning("Discovery thread did not terminate gracefully.")

            if self._heartbeat_thread:
                self._heartbeat_thread.join(timeout=5)
                if self._heartbeat_thread.is_alive():
                    self.logger.warning("Heartbeat thread did not terminate gracefully.")

            self.logger.info("NodeManager stopped.")

    def _run_discovery_service(self):
        """
        Continuously broadcasts node presence and listens for other nodes.
        Uses UDP broadcast for initial peer discovery.
        """
        self.logger.info("Discovery service started.")
        while self._running:
            try:
                self._broadcast_presence()
                self._listen_for_peers()
            except Exception as e:
                self.logger.error(f"Error in discovery service loop: {e}")
            time.sleep(MESH_DISCOVERY_INTERVAL)
        self.logger.info("Discovery service terminated.")

    def _broadcast_presence(self):
        """
        Sends a UDP broadcast packet announcing this node's presence.
        """
        broadcast_message = {
            "type": "discovery",
            "node_id": self.node_id,
            "address": self._get_local_ip(),
            "port": MESH_PORT
        }
        try:
            message_bytes = json.dumps(broadcast_message).encode('utf-8')
            self._discovery_socket.sendto(message_bytes, ('<broadcast>', MESH_PORT))
            # self.logger.info(f"Broadcasted presence: {broadcast_message}")
        except Exception as e:
            self.logger.error(f"Failed to broadcast presence: {e}")

    def _listen_for_peers(self):
        """
        Listens for incoming discovery messages from other nodes.
        """
        while self._running:
            try:
                data, addr = self._discovery_socket.recvfrom(4096) # Buffer size
                message = json.loads(data.decode('utf-8'))

                if message.get("type") == "discovery" and message.get("node_id") != self.node_id:
                    peer_node_id = message["node_id"]
                    peer_address = message["address"]
                    peer_port = message["port"]

                    if peer_node_id not in self.peers:
                        self.logger.info(f"Discovered new peer: {peer_node_id} at {peer_address}:{peer_port}")
                    self._update_peer_info(peer_node_id, peer_address, peer_port)
                elif message.get("type") == "heartbeat" and message.get("node_id") != self.node_id:
                    peer_node_id = message["node_id"]
                    self._update_peer_heartbeat(peer_node_id)

            except socket.timeout:
                # No data received within timeout, continue loop to check _running flag
                break
            except json.JSONDecodeError:
                self.logger.warning(f"Received malformed JSON from {addr}")
            except Exception as e:
                self.logger.error(f"Error receiving discovery message: {e}")

    def _run_heartbeat_service(self):
        """
        Periodically sends heartbeats to known peers and prunes inactive ones.
        """
        self.logger.info("Heartbeat service started.")
        while self._running:
            self._send_heartbeats()
            self._prune_inactive_peers()
            time.sleep(MESH_DISCOVERY_INTERVAL / 2) # More frequent than discovery
        self.logger.info("Heartbeat service terminated.")

    def _send_heartbeats(self):
        """
        Sends a unicast heartbeat message to each known peer.
        """
        heartbeat_message = {
            "type": "heartbeat",
            "node_id": self.node_id
        }
        message_bytes = json.dumps(heartbeat_message).encode('utf-8')

        with self._lock:
            for peer_id, peer_info in list(self.peers.items()): # Iterate over a copy
                try:
                    target_address = (peer_info["address"], peer_info["port"])
                    self._discovery_socket.sendto(message_bytes, target_address)
                    # self.logger.info(f"Sent heartbeat to {peer_id} at {target_address}")
                except Exception as e:
                    self.logger.warning(f"Failed to send heartbeat to {peer_id}: {e}")

    def _prune_inactive_peers(self):
        """
        Removes peers that have not sent a heartbeat within the timeout period.
        """
        current_time = time.time()
        inactive_peers = []
        with self._lock:
            for peer_id, peer_info in self.peers.items():
                if (current_time - peer_info["last_heartbeat"]) > MESH_PEER_TIMEOUT:
                    inactive_peers.append(peer_id)
            for peer_id in inactive_peers:
                self.peers.pop(peer_id, None)
                self.logger.info(f"Pruned inactive peer: {peer_id}")

    def _update_peer_info(self, peer_node_id: str, peer_address: str, peer_port: int):
        """
        Updates or adds information for a discovered peer.
        """
        with self._lock:
            self.peers[peer_node_id] = {
                "address": peer_address,
                "port": peer_port,
                "last_heartbeat": time.time()
            }

    def _update_peer_heartbeat(self, peer_node_id: str):
        """
        Updates the last heartbeat timestamp for a known peer.
        """
        with self._lock:
            if peer_node_id in self.peers:
                self.peers[peer_node_id]["last_heartbeat"] = time.time()
            # else: # Peer might have been pruned due to race condition, or not fully discovered yet
            #     self.logger.warning(f"Received heartbeat from unknown peer {peer_node_id}")

    def get_active_peers(self) -> Dict[str, Dict]:
        """
        Returns a copy of the currently active peers.
        """
        with self._lock:
            return dict(self.peers)

    def _get_local_ip(self):
        """
        Attempts to determine the local IP address for broadcasting.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Doesn't actually connect, just used to determine the best local interface IP
            s.connect(('192.255.255.255', 1))
            IP = s.getsockname()[0]
        except Exception:
            IP = '127.0.0.1'
        finally:
            s.close()
        return IP

# --- Configuration Placeholder (would typically be in config.py) ---
class Config:
    MESH_PORT = 5000
    MESH_DISCOVERY_INTERVAL = 10 # seconds
    MESH_NODE_ID_PREFIX = "flux_node_"
    MESH_PEER_TIMEOUT = 30 # seconds (3 discovery intervals)

# Make placeholder config available as if imported
config = Config()
MESH_PORT = config.MESH_PORT
MESH_DISCOVERY_INTERVAL = config.MESH_DISCOVERY_INTERVAL
MESH_NODE_ID_PREFIX = config.MESH_NODE_ID_PREFIX
MESH_PEER_TIMEOUT = config.MESH_PEER_TIMEOUT

if __name__ == "__main__":
    # Simple demonstration of NodeManager lifecycle
    import uuid
    node_id = f"{MESH_NODE_ID_PREFIX}{str(uuid.uuid4())[:8]}"
    manager = NodeManager(node_id)

    try:
        manager.start()
        print(f"\nNodeManager {node_id} started. Press Ctrl+C to stop.")
        while True:
            time.sleep(5)
            peers = manager.get_active_peers()
            print(f"\n[{time.strftime('%H:%M:%S')}] Active Peers ({len(peers)}):")
            for pid, pinfo in peers.items():
                print(f"  - {pid} at {pinfo['address']}:{pinfo['port']} (Last seen: {time.time() - pinfo['last_heartbeat']:.2f}s ago)")
    except KeyboardInterrupt:
        print("\nStopping NodeManager...")
    finally:
        manager.stop()
        print("NodeManager stopped successfully.")

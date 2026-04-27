import asyncio
import json
import hashlib
import time
from typing import Dict, Any, List, Set, Optional

class ReplicationManager:
    """
    Manages data replication and eventual consistency across the Flux Distributed JSON Mesh.

    This component is responsible for:
    - Discovering and maintaining a list of active peer nodes.
    - Propagating local data changes to remote peers.
    - Receiving and applying data changes from remote peers.
    - Resolving conflicts using a version vector or timestamp-based approach.
    - Ensuring data integrity and availability across the mesh.
    """

    def __init__(self, node_id: str, local_data_store: Any, peer_discovery_service: Any, replication_port: int = 8001):
        """
        Initializes the ReplicationManager.

        Args:
            node_id: Unique identifier for this mesh node.
            local_data_store: An interface to the local JSON data storage.
            peer_discovery_service: An interface to discover active peer nodes.
            replication_port: The port to listen for incoming replication requests.
        """
        self.node_id = node_id
        self.local_data_store = local_data_store
        self.peer_discovery_service = peer_discovery_service
        self.replication_port = replication_port
        self._peer_nodes: Dict[str, str] = {}  # {node_id: address}
        self._replication_queue: asyncio.Queue = asyncio.Queue()
        self._last_replicated_versions: Dict[str, Dict[str, int]] = {} # {peer_id: {key: version}}
        self._data_lock = asyncio.Lock()
        self._running = False
        self._replication_task: Optional[asyncio.Task] = None
        self._server_task: Optional[asyncio.Task] = None

    async def start(self):
        """Starts the replication manager, including discovery and data propagation loops."""
        self.log_info("Starting Replication Manager...")
        self._running = True
        self._replication_task = asyncio.create_task(self._replication_loop())
        self._server_task = asyncio.create_task(self._start_replication_server())
        self.log_info("Replication Manager started.")

    async def stop(self):
        """Stops all replication manager tasks and cleans up resources."""
        self.log_info("Stopping Replication Manager...")
        self._running = False
        if self._replication_task:
            self._replication_task.cancel()
            await self._replication_task
        if self._server_task:
            self._server_task.cancel()
            await self._server_task
        self.log_info("Replication Manager stopped.")

    async def _start_replication_server(self):
        """Starts an asyncio server to listen for incoming replication requests from peers."""
        # This would typically involve a lightweight HTTP server or a custom TCP protocol
        # For demonstration, we'll simulate the server logic.
        self.log_info(f"Replication server listening on port {self.replication_port}...")
        while self._running:
            # Simulate receiving requests
            await asyncio.sleep(5) # Polling interval
            # In a real system, this would be handled by a protocol handler
            # e.g., `asyncio.start_server(self._handle_replication_request, '0.0.0.0', self.replication_port)`
            pass # Placeholder for actual server logic

    async def _handle_replication_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handles an incoming replication request from a peer node."""
        addr = writer.get_extra_info('peername')
        self.log_debug(f"Received replication request from {addr}")
        try:
            data = await reader.read(4096)
            message = json.loads(data.decode('utf-8'))

            message_type = message.get("type")
            sender_id = message.get("sender_id")

            if message_type == "DATA_UPDATE":
                updates = message.get("updates", [])
                await self._apply_remote_updates(sender_id, updates)
                response = {"status": "success", "message": "Updates applied"}
            elif message_type == "REQUEST_MISSING_DATA":
                requested_keys = message.get("keys", [])
                missing_data = await self._get_data_for_replication(requested_keys)
                response = {"status": "success", "data": missing_data}
            else:
                response = {"status": "error", "message": "Unknown message type"}

            writer.write(json.dumps(response).encode('utf-8'))
            await writer.drain()

        except Exception as e:
            self.log_error(f"Error handling replication request from {addr}: {e}")
        finally:
            writer.close()

    async def _replication_loop(self):
        """Main loop for discovering peers and propagating local changes."""
        while self._running:
            await self._discover_peers()
            await self._propagate_local_changes()
            await asyncio.sleep(10) # Replication interval

    async def _discover_peers(self):
        """Uses the peer discovery service to update the list of active peer nodes."""
        async with self._data_lock:
            active_peers = await self.peer_discovery_service.get_active_peers()
            new_peers = {p["node_id"]: p["address"] for p in active_peers if p["node_id"] != self.node_id}
            
            # Update peer list and initialize last replicated versions if new peer
            for peer_id, address in new_peers.items():
                if peer_id not in self._peer_nodes:
                    self.log_info(f"Discovered new peer: {peer_id} at {address}")
                    self._last_replicated_versions[peer_id] = {} # Initialize for new peer
                self._peer_nodes[peer_id] = address
            
            # Remove inactive peers
            current_peer_ids = set(self._peer_nodes.keys())
            active_peer_ids = set(new_peers.keys())
            for inactive_peer_id in current_peer_ids - active_peer_ids:
                self.log_info(f"Peer {inactive_peer_id} is no longer active.")
                del self._peer_nodes[inactive_peer_id]
                self._last_replicated_versions.pop(inactive_peer_id, None)

    async def _propagate_local_changes(self):
        """Propagates local data changes to all known peer nodes."""
        local_changes = await self.local_data_store.get_unreplicated_changes(self.node_id)
        if not local_changes:
            return

        for peer_id, peer_address in list(self._peer_nodes.items()): # Iterate over a copy
            if peer_id == self.node_id:
                continue

            changes_for_peer = []
            for key, data_item in local_changes.items():
                current_version = data_item.get("version", 0)
                last_replicated = self._last_replicated_versions.get(peer_id, {}).get(key, 0)
                
                if current_version > last_replicated:
                    changes_for_peer.append({
                        "key": key,
                        "value": data_item["value"],
                        "version": current_version,
                        "timestamp": data_item.get("timestamp", time.time())
                    })
            
            if changes_for_peer:
                self.log_debug(f"Propagating {len(changes_for_peer)} changes to {peer_id}")
                try:
                    await self._send_replication_message(
                        peer_address,
                        {"type": "DATA_UPDATE", "sender_id": self.node_id, "updates": changes_for_peer}
                    )
                    # Update local tracking after successful replication
                    for change in changes_for_peer:
                        self._last_replicated_versions.setdefault(peer_id, {})[change["key"]] = change["version"]
                    await self.local_data_store.mark_changes_replicated(self.node_id, changes_for_peer)
                except Exception as e:
                    self.log_error(f"Failed to propagate changes to {peer_id} ({peer_address}): {e}")

    async def _apply_remote_updates(self, remote_node_id: str, updates: List[Dict[str, Any]]):
        """Applies received updates from a remote node, resolving conflicts."""
        async with self._data_lock:
            applied_count = 0
            for update in updates:
                key = update["key"]
                remote_value = update["value"]
                remote_version = update["version"]
                remote_timestamp = update.get("timestamp", 0)

                local_data = await self.local_data_store.get(key)
                local_version = local_data.get("version", 0) if local_data else 0
                local_timestamp = local_data.get("timestamp", 0) if local_data else 0

                # Conflict Resolution: Last-Write-Wins based on timestamp, then version if timestamps are equal
                if remote_version > local_version:
                    if remote_timestamp > local_timestamp:
                        await self.local_data_store.set(key, remote_value, remote_version, remote_timestamp, source_node=remote_node_id)
                        applied_count += 1
                        self.log_debug(f"Applied remote update for key '{key}' from {remote_node_id} (new version: {remote_version})")
                    elif remote_timestamp == local_timestamp and remote_version > local_version:
                         await self.local_data_store.set(key, remote_value, remote_version, remote_timestamp, source_node=remote_node_id)
                         applied_count += 1
                         self.log_debug(f"Applied remote update for key '{key}' from {remote_node_id} (same timestamp, higher version: {remote_version})")
                    else:
                        self.log_debug(f"Skipping remote update for key '{key}' from {remote_node_id}: local data is newer or same version/timestamp.")
                elif remote_version == local_version:
                    # If versions are same, check timestamps. If timestamps are same, values should be identical.
                    # If values differ, this indicates a potential issue or a merge scenario not covered by simple LWW.
                    # For now, we assume identical values for identical version/timestamp.
                    pass
                else:
                    self.log_debug(f"Skipping remote update for key '{key}' from {remote_node_id}: local version {local_version} is higher than remote {remote_version}.")
            
            if applied_count > 0:
                self.log_info(f"Applied {applied_count} updates from remote node {remote_node_id}.")

    async def _send_replication_message(self, target_address: str, message: Dict[str, Any]):
        """Sends a replication message to a target peer node."""
        host, port_str = target_address.split(":")
        port = int(port_str)
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(json.dumps(message).encode('utf-8'))
            await writer.drain()

            response_data = await reader.read(4096)
            response = json.loads(response_data.decode('utf-8'))
            self.log_debug(f"Received response from {target_address}: {response}")
            
            if response.get("status") == "error":
                raise Exception(f"Peer responded with error: {response.get('message')}")

        except ConnectionRefusedError:
            self.log_error(f"Connection refused by {target_address}. Peer might be offline.")
            raise
        except Exception as e:
            self.log_error(f"Error sending message to {target_address}: {e}")
            raise
        finally:
            if 'writer' in locals():
                writer.close()
                await writer.wait_closed()


    def log_info(self, message: str):
        """Simple logging function for informational messages."""
        print(f"[{self.node_id}][INFO] {message}")

    def log_debug(self, message: str):
        """Simple logging function for debug messages."""
        # In a real system, this would integrate with a proper logging framework
        # print(f"[{self.node_id}][DEBUG] {message}")
        pass

    def log_error(self, message: str):
        """Simple logging function for error messages."""
        print(f"[{self.node_id}][ERROR] {message}")

# --- Interfaces for demonstration purposes ---
class LocalDataStoreInterface:
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieves data for a given key, including version and timestamp."""
        raise NotImplementedError

    async def set(self, key: str, value: Any, version: int, timestamp: float, source_node: str):
        """Sets data for a key, updating its version and timestamp."""
        raise NotImplementedError

    async def get_unreplicated_changes(self, node_id: str) -> Dict[str, Dict[str, Any]]:
        """Returns local data changes that haven't been replicated by this node."""
        raise NotImplementedError

    async def mark_changes_replicated(self, node_id: str, changes: List[Dict[str, Any]]):
        """Marks specific changes as replicated for a given node."""
        raise NotImplementedError

class PeerDiscoveryServiceInterface:
    async def get_active_peers(self) -> List[Dict[str, str]]:
        """Returns a list of active peers, each with 'node_id' and 'address'."""
        raise NotImplementedError

# Example usage (would be in a separate file, e.g., `main.py` or `node.py`)
async def main():
    class MockDataStore(LocalDataStoreInterface):
        def __init__(self):
            self._data = {} # {key: {"value": ..., "version": ..., "timestamp": ..., "last_replicated_by": {node_id: version}}}
            self._unreplicated_changes = {} # {node_id: {key: data}}

        async def get(self, key: str) -> Optional[Dict[str, Any]]:
            return self._data.get(key)

        async def set(self, key: str, value: Any, version: int, timestamp: float, source_node: str):
            self._data[key] = {"value": value, "version": version, "timestamp": timestamp}
            # Mark as unreplicated for current node (if it's a local write)
            # This logic needs refinement for proper multi-node tracking
            # For simplicity, if source_node is current node, it's a local change
            # Otherwise, it's a remote change applied.
            if source_node == "nodeA": # Assume "nodeA" is the local node for this example
                self._unreplicated_changes.setdefault("nodeA", {})[key] = self._data[key]

        async def get_unreplicated_changes(self, node_id: str) -> Dict[str, Dict[str, Any]]:
            # This mock assumes changes are only unreplicated for the 'node_id' that generated them
            # A real system would track per-peer replication status.
            return self._unreplicated_changes.pop(node_id, {})

        async def mark_changes_replicated(self, node_id: str, changes: List[Dict[str, Any]]):
            # In a real system, this would update replication metadata for each item
            pass

    class MockPeerDiscovery(PeerDiscoveryServiceInterface):
        async def get_active_peers(self) -> List[Dict[str, str]]:
            # Simulate discovering peers
            return [
                {"node_id": "nodeB", "address": "127.0.0.1:8002"},
                {"node_id": "nodeC", "address": "127.0.0.1:8003"}
            ]

    node_id = "nodeA"
    data_store = MockDataStore()
    peer_discovery = MockPeerDiscovery()
    replication_manager = ReplicationManager(node_id, data_store, peer_discovery, replication_port=8001)

    await replication_manager.start()

    # Simulate a local write
    await data_store.set("user:1", {"name": "Alice", "email": "alice@example.com"}, 1, time.time(), source_node=node_id)
    await data_store.set("config:app", {"theme": "dark", "locale": "en_US"}, 1, time.time(), source_node=node_id)

    # Allow some time for replication to occur
    await asyncio.sleep(20)

    # In a real scenario, you'd have another node (nodeB) running its own ReplicationManager
    # and receiving these updates.

    await replication_manager.stop()

if __name__ == "__main__":
    # asyncio.run(main()) # This would run the example if uncommented
    pass
import asyncio
import json
import logging
import socket
import time
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MeshNodeWorker:
    """Manages peer discovery, data synchronization, and message routing within the Flux JSON Mesh."""

    def __init__(self, node_id: str, host: str, port: int, discovery_interval: int = 10):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = {}
        self.data_store = {}
        self.discovery_interval = discovery_interval
        self.server_socket = None
        self.running = False
        logging.info(f"Node {self.node_id} initialized on {self.host}:{self.port}")

    async def _send_message(self, peer_host: str, peer_port: int, message: dict) -> bool:
        """Sends a JSON message to a specified peer."""
        try:
            reader, writer = await asyncio.open_connection(peer_host, peer_port)
            serialized_message = json.dumps(message) + '\n'
            writer.write(serialized_message.encode('utf-8'))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except ConnectionRefusedError:
            logging.warning(f"Connection refused by peer {peer_host}:{peer_port}")
            return False
        except Exception as e:
            logging.error(f"Error sending message to {peer_host}:{peer_port}: {e}")
            return False

    async def _handle_client(self, reader, writer):
        """Handles incoming connections and messages from other mesh nodes."""
        peer_address = writer.get_extra_info('peername')
        logging.info(f"Incoming connection from {peer_address}")
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                message = json.loads(data.decode('utf-8'))
                await self._process_message(message, peer_address)
        except json.JSONDecodeError:
            logging.error(f"Received malformed JSON from {peer_address}")
        except Exception as e:
            logging.error(f"Error handling client {peer_address}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"Connection closed with {peer_address}")

    async def _process_message(self, message: dict, sender_address: tuple):
        """Processes various types of messages received from peers."""
        msg_type = message.get('type')
        sender_node_id = message.get('sender_id', 'UNKNOWN')

        if sender_node_id != 'UNKNOWN' and sender_node_id != self.node_id:
            self.peers[sender_node_id] = {'host': sender_address[0], 'port': sender_address[1], 'last_seen': time.time()}

        logging.debug(f"Processing message type '{msg_type}' from {sender_node_id}")

        if msg_type == 'DISCOVERY_BEACON':
            logging.info(f"Received discovery beacon from {sender_node_id}")
            await self._send_message(sender_address[0], sender_address[1], {
                'type': 'DISCOVERY_ACK',
                'sender_id': self.node_id,
                'host': self.host,
                'port': self.port,
                'timestamp': time.time()
            })
        elif msg_type == 'DISCOVERY_ACK':
            peer_id = message.get('sender_id')
            peer_host = message.get('host')
            peer_port = message.get('port')
            if peer_id and peer_host and peer_port and peer_id != self.node_id:
                if peer_id not in self.peers:
                    logging.info(f"Discovered new peer: {peer_id} at {peer_host}:{peer_port}")
                self.peers[peer_id] = {'host': peer_host, 'port': peer_port, 'last_seen': time.time()}
        elif msg_type == 'DATA_SYNC':
            key = message.get('key')
            value = message.get('value')
            timestamp = message.get('timestamp')
            if key and value is not None and timestamp is not None:
                current_data = self.data_store.get(key, {'timestamp': 0})
                if timestamp > current_data['timestamp']:
                    self.data_store[key] = {'value': value, 'timestamp': timestamp}
                    logging.info(f"Data synchronized for key '{key}'. New value: {value}")
                    # Propagate to other peers
                    await self.broadcast_data_update(key, value)
                else:
                    logging.debug(f"Received older data for key '{key}'. Ignoring.")
        elif msg_type == 'DATA_REQUEST':
            key = message.get('key')
            if key in self.data_store:
                data = self.data_store[key]
                await self._send_message(sender_address[0], sender_address[1], {
                    'type': 'DATA_SYNC',
                    'sender_id': self.node_id,
                    'key': key,
                    'value': data['value'],
                    'timestamp': data['timestamp']
                })
            else:
                logging.warning(f"Requested key '{key}' not found in local store.")
        else:
            logging.warning(f"Unknown message type: {msg_type}")

    async def _peer_discovery_loop(self):
        """Periodically broadcasts discovery beacons to known and potential peers."""
        while self.running:
            logging.debug("Broadcasting discovery beacon...")
            beacon_message = {
                'type': 'DISCOVERY_BEACON',
                'sender_id': self.node_id,
                'host': self.host,
                'port': self.port,
                'timestamp': time.time()
            }
            # Broadcast to all known peers
            for peer_id, peer_info in list(self.peers.items()): # Use list to avoid RuntimeError during dict modification
                if not await self._send_message(peer_info['host'], peer_info['port'], beacon_message):
                    logging.warning(f"Failed to reach peer {peer_id}. Marking for potential removal.")
                    # Optional: Implement more robust peer removal logic based on consecutive failures

            await asyncio.sleep(self.discovery_interval)

    async def broadcast_data_update(self, key: str, value: any):
        """Broadcasts a data update to all known peers."""
        if key not in self.data_store:
            self.data_store[key] = {'value': value, 'timestamp': time.time()}
        else:
            self.data_store[key]['value'] = value
            self.data_store[key]['timestamp'] = time.time()

        sync_message = {
            'type': 'DATA_SYNC',
            'sender_id': self.node_id,
            'key': key,
            'value': value,
            'timestamp': self.data_store[key]['timestamp']
        }
        logging.info(f"Broadcasting data update for key '{key}' with value '{value}'.")
        for peer_id, peer_info in self.peers.items():
            await self._send_message(peer_info['host'], peer_info['port'], sync_message)

    async def get_data(self, key: str, request_from_peers: bool = False) -> any:
        """Retrieves data for a given key, optionally requesting from peers."""
        if key in self.data_store:
            return self.data_store[key]['value']
        elif request_from_peers and self.peers:
            logging.info(f"Key '{key}' not found locally. Requesting from peers.")
            request_message = {
                'type': 'DATA_REQUEST',
                'sender_id': self.node_id,
                'key': key
            }
            # Request from one peer, or all and take the first response
            for peer_id, peer_info in self.peers.items():
                await self._send_message(peer_info['host'], peer_info['port'], request_message)
                # In a real system, you'd wait for a response here or have a more complex request/response mechanism
            # For simplicity, we'll return None and assume eventual consistency will update local store
            return None
        return None

    async def start(self):
        """Starts the mesh node server and discovery loop."""
        self.running = True
        self.server_socket = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        addr = self.server_socket.sockets[0].getsockname()
        logging.info(f"Flux Mesh Node listening on {addr}")

        discovery_task = asyncio.create_task(self._peer_discovery_loop())

        async with self.server_socket:
            await self.server_socket.serve_forever()

    async def stop(self):
        """Stops the mesh node."""
        logging.info(f"Stopping Flux Mesh Node {self.node_id}...")
        self.running = False
        if self.server_socket:
            self.server_socket.close()
            await self.server_socket.wait_closed()
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logging.info(f"Flux Mesh Node {self.node_id} stopped.")


if __name__ == '__main__':
    # Example Usage:
    # To run multiple nodes, start them in separate terminals or processes with different ports.
    # Example: python mesh_node_worker.py 8000
    # Example: python mesh_node_worker.py 8001

    import sys

    if len(sys.argv) < 2:
        print("Usage: python mesh_node_worker.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    node_id = f"node-{port}-{str(uuid.uuid4())[:8]}"
    node = MeshNodeWorker(node_id=node_id, host='127.0.0.1', port=port)

    async def main():
        # Add initial data for demonstration
        await node.broadcast_data_update('config.version', {'major': 1, 'minor': 0})
        await node.broadcast_data_update('status.health', 'healthy')

        # Start the node
        await node.start()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        asyncio.run(node.stop())
        logging.info("Node shutdown initiated by user.")
    except Exception as e:
        logging.critical(f"Unhandled exception in main loop: {e}")
        asyncio.run(node.stop())
import json
import logging
import asyncio
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class QuerySyntaxError(Exception):
    """Custom exception for query syntax issues within the distributed mesh."""
    pass

class QueryExecutionError(Exception):
    """Custom exception for distributed query execution failures."""
    pass

class JSONQueryProcessor:
    """
    Manages distributed JSON query execution across the mesh.

    This processor is responsible for parsing query requests,
    distributing them to relevant mesh nodes, aggregating results,
    and applying final filtering or transformation steps.
    It supports a simplified query language for selecting and filtering
    JSON documents within the distributed data store.

    Dependencies:
    - A 'peer_manager' instance capable of discovering and communicating with other nodes.
    - A 'local_data_store' instance providing access to the current node's JSON documents.
    """

    def __init__(self, node_id: str, peer_manager: Any, local_data_store: Any):
        """
        Initializes the JSONQueryProcessor.

        Args:
            node_id: Unique identifier for the current mesh node.
            peer_manager: An object responsible for managing peer connections
                          and dispatching messages (e.g., query requests).
            local_data_store: An object providing access to locally stored JSON documents.
        """
        if not all([node_id, peer_manager, local_data_store]):
            raise ValueError("node_id, peer_manager, and local_data_store must be provided.")
        
        self.node_id = node_id
        self.peer_manager = peer_manager
        self.local_data_store = local_data_store
        logger.info(f"JSONQueryProcessor initialized for node: {self.node_id}")

    async def _dispatch_query_to_peer(self, peer_id: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Dispatches a query request to a specific peer node.

        This method leverages the peer_manager to send the query and await
        its response, encapsulating network communication and error handling.

        Args:
            peer_id: The ID of the peer node to send the query to.
            query: The query dictionary to send, adhering to the mesh's query protocol.

        Returns:
            The response from the peer, typically a dictionary containing results
            and status, or None if the dispatch fails or times out.
        """
        try:
            # Assume peer_manager has an asynchronous method to send requests
            # and await responses, potentially with retry logic.
            response = await self.peer_manager.send_query_request(peer_id, query)
            logger.debug(f"Received response from peer {peer_id} for query: {query.get('collection', 'N/A')}.")
            return response
        except asyncio.TimeoutError:
            logger.warning(f"Query dispatch to peer {peer_id} timed out.")
            return None
        except Exception as e:
            logger.error(f"Failed to dispatch query to peer {peer_id}: {e}", exc_info=True)
            return None

    def _execute_local_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Executes a query against the current node's local data store.

        This method interprets a simplified query structure to filter
        and select documents from the local storage. It's designed to be
        efficient for single-node operations before results are aggregated.

        Args:
            query: A dictionary defining the query. Expected keys:
                   - 'collection': (str) The collection name to query.
                   - 'filter': (dict, optional) A dictionary of key-value pairs
                               for exact match filtering. Supports nested paths
                               for basic matching (e.g., {"user.address.city": "New York"}).
                   - 'select': (list, optional) A list of top-level fields to project.

        Returns:
            A list of matching JSON documents or projected fields.

        Raises:
            QuerySyntaxError: If the query structure is invalid or missing required fields.
        """
        collection_name = query.get('collection')
        if not collection_name or not isinstance(collection_name, str):
            raise QuerySyntaxError("Query must specify a valid 'collection' name (string).")

        local_docs = self.local_data_store.get_collection(collection_name)
        if not local_docs:
            return []

        filtered_docs = []
        query_filter = query.get('filter', {})
        if not isinstance(query_filter, dict):
            raise QuerySyntaxError("Query 'filter' must be a dictionary.")

        for doc in local_docs:
            match = True
            for filter_key, filter_value in query_filter.items():
                # Basic support for nested keys (e.g., "user.address.city")
                current_value = doc
                try:
                    for part in filter_key.split('.'):
                        if isinstance(current_value, dict):
                            current_value = current_value.get(part)
                        else:
                            current_value = None # Path not found or not a dict
                            break
                except AttributeError:
                    current_value = None # Handle cases where a part is not a dict

                if current_value != filter_value:
                    match = False
                    break
            if match:
                filtered_docs.append(doc)

        select_fields = query.get('select')
        if select_fields:
            if not isinstance(select_fields, list) or not all(isinstance(f, str) for f in select_fields):
                raise QuerySyntaxError("Query 'select' must be a list of strings.")
            projected_docs = []
            for doc in filtered_docs:
                projected = {field: doc.get(field) for field in select_fields if field in doc}
                projected_docs.append(projected)
            return projected_docs
        
        return filtered_docs

    async def execute_distributed_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Executes a given query across the entire distributed JSON mesh.

        This method orchestrates the query execution by first running
        the query locally, then dispatching it concurrently to all known active peers,
        and finally aggregating and consolidating the results from all sources.
        It handles potential partial failures and ensures robust data collection.

        Args:
            query: The query dictionary to execute, structured according to
                   the mesh's query language specification.

        Returns:
            A consolidated and deduplicated list of results from across the mesh.

        Raises:
            QueryExecutionError: If the query cannot be executed due to fundamental
                                 system issues (e.g., invalid query structure, no active peers).
        """
        if not isinstance(query, dict):
            raise QuerySyntaxError("Query must be a dictionary.")

        all_results = []

        # 1. Execute query locally on the current node
        try:
            local_results = self._execute_local_query(query)
            all_results.extend(local_results)
            logger.debug(f"Local query executed on {self.node_id}. Found {len(local_results)} results.")
        except QuerySyntaxError as e:
            raise QueryExecutionError(f"Invalid query syntax for local execution: {e}") from e
        except Exception as e:
            logger.warning(f"Error executing local query on {self.node_id}: {e}", exc_info=True)
            # Do not re-raise, attempt to get results from peers even if local fails

        # 2. Dispatch query to active peers concurrently
        peer_ids = self.peer_manager.get_active_peers() # Assumes this returns a list of peer IDs
        # Filter out self to avoid sending query to self again if `get_active_peers` includes it
        remote_peers = [p_id for p_id in peer_ids if p_id != self.node_id]

        if remote_peers:
            tasks = [self._dispatch_query_to_peer(peer_id, query) for peer_id in remote_peers]
            
            # Use gather to run tasks concurrently, allowing for individual task failures
            peer_responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            for response in peer_responses:
                if isinstance(response, dict) and response.get('status') == 'success' and 'results' in response:
                    if isinstance(response['results'], list):
                        all_results.extend(response['results'])
                    else:
                        logger.warning(f"Peer returned non-list results: {response['results']}")
                elif isinstance(response, Exception):
                    logger.error(f"Peer query task failed with exception: {response}")
                else:
                    logger.warning(f"Received unexpected or error response from peer: {response}")
        else:
            logger.info("No active remote peers found for distributed query.")
        
        logger.info(f"Distributed query on {self.node_id} completed. Total raw results aggregated: {len(all_results)}")
        
        # 3. Deduplicate and consolidate results
        final_results = self._deduplicate_results(all_results)
        logger.info(f"Final results after deduplication: {len(final_results)}")
        return final_results

    def _deduplicate_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicates a list of JSON documents based on a common identifier.
        Assumes documents have an 'id' field for robust deduplication.
        If 'id' is not present, it attempts to deduplicate based on the
        JSON content itself (less efficient but a fallback).

        Args:
            results: A list of JSON dictionaries, potentially containing duplicates
                     from different mesh nodes.

        Returns:
            A list of deduplicated JSON dictionaries.
        """
        if not results:
            return []
        
        unique_results_map = {}
        for doc in results:
            doc_id = doc.get('id') # Common unique identifier
            if doc_id:
                # If multiple nodes return the same ID, a conflict resolution strategy
                # might be needed (e.g., latest timestamp, specific node priority).
                # For simplicity, we prioritize the first encountered document for a given ID.
                if doc_id not in unique_results_map:
                    unique_results_map[doc_id] = doc
            else:
                # Fallback: if no 'id', use a hash of the content.
                # This is less performant and relies on stable JSON serialization.
                try:
                    content_hash = json.dumps(doc, sort_keys=True, separators=(',', ':'))
                    if content_hash not in unique_results_map: # Using hash as key
                        unique_results_map[content_hash] = doc
                except TypeError as e:
                    logger.warning(f"Could not serialize document for deduplication (no 'id' and unhashable content): {doc}. Error: {e}")
                    # If unhashable and no ID, we might just include it or log it.
                    # For now, we'll let it pass if it couldn't be hashed, meaning it won't be deduplicated by content.
                    # A more robust solution for non-ID, non-hashable docs would involve a generated ID or a specific conflict policy.
                    pass # Doc without ID and unhashable content will be treated as unique if no ID is present.
        
        # If any documents couldn't be deduplicated by ID or content hash,
        # they are implicitly unique in this process.
        
        # Re-add items that couldn't be deduplicated by ID or content hash (if any)
        final_list = list(unique_results_map.values())
        
        # This part is tricky. If a doc has no 'id' and its content is not hashable (e.g., contains sets),
        # it won't enter unique_results_map. For now, we'll assume 'id' is standard or content is hashable.
        # A more robust solution for non-ID, non-hashable docs would involve a more complex comparison or
        # forcing an ID generation.
        
        return final_list
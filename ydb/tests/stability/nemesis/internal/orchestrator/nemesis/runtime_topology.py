"""Fresh runtime node membership for recovery; independent of cluster deployment."""

import logging

import requests

logger = logging.getLogger(__name__)


class RuntimeNodePiles:
    def __init__(self, endpoints, pile_names, *, get=requests.get):
        self._endpoints = tuple(endpoints)
        self._pile_ids = {name: pile for pile, name in pile_names.items()}
        self._get = get

    def __call__(self):
        # Do not cache dynamic IDs across restarts or guess membership from a host:
        # multiple piles can share localhost. Missing membership suppresses recovery.
        for endpoint in self._endpoints:
            try:
                response = self._get(endpoint + "/viewer/json/nodes",
                    params={"fields_required": "SystemState", "timeout": 2000}, timeout=3)
                response.raise_for_status()
                result = {}
                for node in response.json().get("Nodes", []):
                    name = node.get("SystemState", {}).get("Location", {}).get("BridgePileName")
                    if name in self._pile_ids:
                        node_id = int(node["NodeId"])
                        pile = self._pile_ids[name]
                        if node_id in result and result[node_id] != pile:
                            raise ValueError("Conflicting runtime pile membership")
                        result[node_id] = pile
                if result:
                    return result
            except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
                logger.warning("Cannot read runtime node piles from %s", endpoint, exc_info=True)
        return {}

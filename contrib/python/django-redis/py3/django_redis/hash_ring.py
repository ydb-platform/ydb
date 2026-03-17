import bisect
import hashlib
from collections.abc import Iterable, Iterator
from typing import Optional


class HashRing:
    nodes: list[str] = []

    def __init__(self, nodes: Iterable[str] = (), replicas: int = 128) -> None:
        self.replicas: int = replicas
        self.ring: dict[str, str] = {}
        self.sorted_keys: list[str] = []

        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str) -> None:
        self.nodes.append(node)

        for x in range(self.replicas):
            _key = f"{node}:{x}"
            _hash = hashlib.sha256(_key.encode()).hexdigest()

            self.ring[_hash] = node
            self.sorted_keys.append(_hash)

        self.sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        self.nodes.remove(node)
        for x in range(self.replicas):
            _hash = hashlib.sha256(f"{node}:{x}".encode()).hexdigest()
            del self.ring[_hash]
            self.sorted_keys.remove(_hash)

    def get_node(self, key: str) -> Optional[str]:
        n, i = self.get_node_pos(key)
        return n

    def get_node_pos(self, key: str) -> tuple[Optional[str], Optional[int]]:
        if len(self.ring) == 0:
            return None, None

        _hash = hashlib.sha256(key.encode()).hexdigest()
        idx = bisect.bisect(self.sorted_keys, _hash)
        idx = min(idx - 1, (self.replicas * len(self.nodes)) - 1)
        return self.ring[self.sorted_keys[idx]], idx

    def iter_nodes(self, key: str) -> Iterator[tuple[Optional[str], Optional[str]]]:
        if len(self.ring) == 0:
            yield None, None

        node, pos = self.get_node_pos(key)
        for k in self.sorted_keys[pos:]:
            yield k, self.ring[k]

    def __call__(self, key: str) -> Optional[str]:
        return self.get_node(key)

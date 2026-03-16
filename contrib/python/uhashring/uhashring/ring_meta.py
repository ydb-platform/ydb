from collections import Counter
from hashlib import md5


class MetaRing:
    """Implement a tunable consistent hashing ring."""

    def __init__(self, hash_fn):
        """Create a new HashRing.

        :param hash_fn: use this callable function to hash keys.
        """
        self._distribution = Counter()
        self._keys = []
        self._nodes = {}
        self._ring = {}

        if hash_fn and not hasattr(hash_fn, "__call__"):
            raise TypeError("hash_fn should be a callable function")
        self._hash_fn = hash_fn or (lambda key: int(md5(str(key).encode("utf-8")).hexdigest(), 16))

    def hashi(self, key):
        """Returns an integer derived from the md5 hash of the given key."""
        return self._hash_fn(key)

    def _create_ring(self, nodes):
        """Generate a ketama compatible continuum/ring."""
        for node_name, node_conf in nodes:
            for w in range(0, node_conf["vnodes"] * node_conf["weight"]):
                self._distribution[node_name] += 1
                self._ring[self.hashi(f"{node_name}-{w}")] = node_name
        self._keys = sorted(self._ring.keys())

    def _remove_node(self, node_name):
        """Remove the given node from the continuum/ring.

        :param node_name: the node name.
        """
        try:
            node_conf = self._nodes.pop(node_name)
        except Exception:
            raise KeyError(
                "node '{}' not found, available nodes: {}".format(node_name, self._nodes.keys())
            )
        else:
            self._distribution.pop(node_name)
            for w in range(0, node_conf["vnodes"] * node_conf["weight"]):
                del self._ring[self.hashi(f"{node_name}-{w}")]
            self._keys = sorted(self._ring.keys())

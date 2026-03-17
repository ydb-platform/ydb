from bisect import insort
from collections import Counter
from hashlib import md5


class KetamaRing:
    """Implement a ketama compatible consistent hashing ring."""

    def __init__(self, replicas=4):
        """Create a new HashRing."""
        self._distribution = Counter()
        self._keys = []
        self._nodes = {}
        self._replicas = replicas
        self._ring = {}

        self._listbytes = lambda x: x

    def hashi(self, key, replica=0):
        """Returns a ketama compatible hash from the given key."""
        dh = self._listbytes(md5(str(key).encode("utf-8")).digest())
        rd = replica * 4
        return (dh[3 + rd] << 24) | (dh[2 + rd] << 16) | (dh[1 + rd] << 8) | dh[0 + rd]

    def _hashi_weight_generator(self, node_name, node_conf):
        """Calculate the weight factor of the given node and
        yield its hash key for every configured replica.

        :param node_name: the node name.
        """
        ks = (node_conf["vnodes"] * len(self._nodes) * node_conf["weight"]) // self._weight_sum
        for w in range(0, ks):
            w_node_name = f"{node_name}-{w}"
            for i in range(0, self._replicas):
                yield self.hashi(w_node_name, replica=i)

    @staticmethod
    def _listbytes(data):
        """Python 2 compatible int iterator from str.

        :param data: the string to int iterate upon.
        """
        return map(ord, data)

    def _create_ring(self, nodes):
        """Generate a ketama compatible continuum/ring."""
        _weight_sum = 0
        for node_conf in self._nodes.values():
            _weight_sum += node_conf["weight"]
        self._weight_sum = _weight_sum

        _distribution = Counter()
        _keys = []
        _ring = {}
        for node_name, node_conf in self._nodes.items():
            for h in self._hashi_weight_generator(node_name, node_conf):
                _ring[h] = node_name
                insort(_keys, h)
                _distribution[node_name] += 1
        self._distribution = _distribution
        self._keys = _keys
        self._ring = _ring

    def _remove_node(self, node_name):
        """Remove the given node from the continuum/ring.

        :param node_name: the node name.
        """
        try:
            self._nodes.pop(node_name)
        except Exception:
            raise KeyError(
                "node '{}' not found, available nodes: {}".format(node_name, self._nodes.keys())
            )
        else:
            self._create_ring(self._nodes)

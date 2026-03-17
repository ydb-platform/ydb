from bisect import bisect

from uhashring.ring_ketama import KetamaRing
from uhashring.ring_meta import MetaRing


class HashRing:
    """Implement a consistent hashing ring."""

    def __init__(self, nodes=[], **kwargs):
        """Create a new HashRing given the implementation.

        :param nodes: nodes used to create the continuum (see doc for format).
        :param hash_fn: use this callable function to hash keys, can be set to
                        'ketama' to use the ketama compatible implementation.
        :param vnodes: default number of vnodes per node.
        :param weight_fn: use this function to calculate the node's weight.
        """
        hash_fn = kwargs.get("hash_fn", None)
        vnodes = kwargs.get("vnodes", None)
        weight_fn = kwargs.get("weight_fn", None)

        if hash_fn == "ketama":
            ketama_args = {k: v for k, v in kwargs.items() if k in ("replicas",)}
            if vnodes is None:
                vnodes = 40
            self.runtime = KetamaRing(**ketama_args)
        else:
            if vnodes is None:
                vnodes = 160
            self.runtime = MetaRing(hash_fn)

        self._default_vnodes = vnodes
        self.hashi = self.runtime.hashi

        if weight_fn and not hasattr(weight_fn, "__call__"):
            raise TypeError("weight_fn should be a callable function")
        self._weight_fn = weight_fn

        if self._configure_nodes(nodes):
            self.runtime._create_ring(self.runtime._nodes.items())

    def _configure_nodes(self, nodes):
        """Parse and set up the given nodes.

        :param nodes: nodes used to create the continuum (see doc for format).
        """
        if isinstance(nodes, str):
            nodes = [nodes]
        elif not isinstance(nodes, (dict, list)):
            raise ValueError(
                "nodes configuration should be a list or a dict," " got {}".format(type(nodes))
            )

        conf_changed = False
        for node in nodes:
            conf = {
                "hostname": node,
                "instance": None,
                "nodename": node,
                "port": None,
                "vnodes": self._default_vnodes,
                "weight": 1,
            }
            current_conf = self.runtime._nodes.get(node, {})
            nodename = node
            # new node, trigger a ring update
            if not current_conf:
                conf_changed = True
            # complex config
            if isinstance(nodes, dict):
                node_conf = nodes[node]
                if isinstance(node_conf, int):
                    conf["weight"] = node_conf
                elif isinstance(node_conf, dict):
                    for k, v in node_conf.items():
                        if k in conf:
                            conf[k] = v
                            # changing those config trigger a ring update
                            if k in ["nodename", "vnodes", "weight"]:
                                if current_conf.get(k) != v:
                                    conf_changed = True
                else:
                    raise ValueError(
                        "node configuration should be a dict or an int,"
                        " got {}".format(type(node_conf))
                    )
            if self._weight_fn:
                conf["weight"] = self._weight_fn(**conf)
            # changing the weight of a node trigger a ring update
            if current_conf.get("weight") != conf["weight"]:
                conf_changed = True
            self.runtime._nodes[nodename] = conf
        return conf_changed

    def __delitem__(self, nodename):
        """Remove the given node.

        :param nodename: the node name.
        """
        self.runtime._remove_node(nodename)

    remove_node = __delitem__

    def __getitem__(self, key):
        """Returns the instance of the node matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "instance")

    get_node_instance = __getitem__

    def __setitem__(self, nodename, conf={"weight": 1}):
        """Add the given node with its associated configuration.

        :param nodename: the node name.
        :param conf: the node configuration.
        """
        if self._configure_nodes({nodename: conf}):
            self.runtime._create_ring([(nodename, self._nodes[nodename])])

    add_node = __setitem__

    def _get_pos(self, key):
        """Get the index of the given key in the sorted key list.

        We return the position with the nearest hash based on
        the provided key unless we reach the end of the continuum/ring
        in which case we return the 0 (beginning) index position.

        :param key: the key to hash and look for.
        """
        p = bisect(self.runtime._keys, self.hashi(key))
        if p == len(self.runtime._keys):
            return 0
        else:
            return p

    def _get(self, key, what):
        """Generic getter magic method.

        The node with the nearest but not less hash value is returned.

        :param key: the key to look for.
        :param what: the information to look for in, allowed values:
            - instance (default): associated node instance
            - nodename: node name
            - pos: index of the given key in the ring
            - tuple: ketama compatible (pos, name) tuple
            - weight: node weight
        """
        if not self.runtime._ring:
            return None

        pos = self._get_pos(key)
        if what == "pos":
            return pos

        nodename = self.runtime._ring[self.runtime._keys[pos]]
        if what in ["hostname", "instance", "port", "weight"]:
            return self.runtime._nodes[nodename][what]
        elif what == "dict":
            return self.runtime._nodes[nodename]
        elif what == "nodename":
            return nodename
        elif what == "tuple":
            return (self.runtime._keys[pos], nodename)

    def get(self, key):
        """Returns the node object dict matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "dict")

    def get_instances(self):
        """Returns a list of the instances of all the configured nodes."""
        return [c.get("instance") for c in self.runtime._nodes.values() if c.get("instance")]

    def get_key(self, key):
        """Alias of ketama hashi method, returns the hash of the given key.

        This method is present for hash_ring compatibility.

        :param key: the key to look for.
        """
        return self.hashi(key)

    def get_node(self, key):
        """Returns the node name of the node matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "nodename")

    def get_node_hostname(self, key):
        """Returns the hostname of the node matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "hostname")

    def get_node_port(self, key):
        """Returns the port of the node matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "port")

    def get_node_pos(self, key):
        """Returns the index position of the node matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "pos")

    def get_node_weight(self, key):
        """Returns the weight of the node matching the hashed key.

        :param key: the key to look for.
        """
        return self._get(key, "weight")

    def get_nodes(self):
        """Returns a list of the names of all the configured nodes."""
        return self.runtime._nodes.keys()

    def get_points(self):
        """Returns a ketama compatible list of (position, nodename) tuples."""
        return [(k, self.runtime._ring[k]) for k in self.runtime._keys]

    def get_server(self, key):
        """Returns a ketama compatible (position, nodename) tuple.

        :param key: the key to look for.
        """
        return self._get(key, "tuple")

    def iterate_nodes(self, key, distinct=True):
        """hash_ring compatibility implementation.

        Given a string key it returns the nodes as a generator that
        can hold the key.
        The generator iterates one time through the ring
        starting at the correct position.
        if `distinct` is set, then the nodes returned will be unique,
        i.e. no virtual copies will be returned.
        """
        if not self.runtime._ring:
            yield None
        else:
            for node in self.range(key, unique=distinct):
                yield node["nodename"]

    def print_continuum(self):
        """Prints a ketama compatible continuum report."""
        numpoints = len(self.runtime._keys)
        if numpoints:
            print(f"Numpoints in continuum: {numpoints}")
        else:
            print("Continuum empty")
        for p in self.get_points():
            point, node = p
            print(f"{node} ({point})")

    def range(self, key, size=None, unique=True):
        """Returns a generator of nodes' configuration available
        in the continuum/ring.

        :param key: the key to look for.
        :param size: limit the list to at most this number of nodes.
        :param unique: a node may only appear once in the list (default True).
        """
        all_nodes = set()
        if unique:
            size = size or len(self.runtime._nodes)
        else:
            all_nodes = []

        pos = self._get_pos(key)
        for key in self.runtime._keys[pos:]:
            nodename = self.runtime._ring[key]
            if unique:
                if nodename in all_nodes:
                    continue
                all_nodes.add(nodename)
            else:
                all_nodes.append(nodename)
            yield self.runtime._nodes[nodename]
            if len(all_nodes) == size:
                break
        else:
            for i, key in enumerate(self.runtime._keys):
                if i < pos:
                    nodename = self.runtime._ring[key]
                    if unique:
                        if nodename in all_nodes:
                            continue
                        all_nodes.add(nodename)
                    else:
                        all_nodes.append(nodename)
                    yield self.runtime._nodes[nodename]
                    if len(all_nodes) == size:
                        break

    def regenerate(self):
        self.runtime._create_ring(self.runtime._nodes.items())

    @property
    def conf(self):
        return self.runtime._nodes

    nodes = conf

    @property
    def distribution(self):
        return self.runtime._distribution

    @property
    def ring(self):
        return self.runtime._ring

    continuum = ring

    @property
    def size(self):
        return len(self.runtime._ring)

    @property
    def _ring(self):
        return self.runtime._ring

    @property
    def _nodes(self):
        return self.runtime._nodes

    @property
    def _keys(self):
        return self.runtime._keys

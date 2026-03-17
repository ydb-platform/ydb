from socket import (getaddrinfo, gaierror,
                    inet_pton, inet_ntop, AF_INET, AF_INET6, SOCK_RAW,
                    AI_NUMERICHOST)


class RadixPrefix(object):
    family = None
    bitlen = 0
    addr = None

    def __init__(self, network=None, masklen=None, packed=None):
        if network and packed:
            raise ValueError('Two address types specified. Please pick one.')
        if network is None and packed is None:
            raise TypeError('No address specified (use `address` or `packed`)')

        if network:
            self._from_network(network, masklen)
        elif packed:
            self._from_packed(packed, masklen)

    def __str__(self):
        return '{0}/{1}'.format(self.network, self.bitlen)

    @property
    def packed(self):
        return bytes(self.addr)

    @property
    def network(self):
        if not self.addr:
            return None
        return inet_ntop(self.family, bytes(self.addr))

    def _inet_pton(self, family, sockaddr, masklen):
        addr = bytearray(inet_pton(family, sockaddr))
        if family == AF_INET:
            max_masklen = 32
        elif family == AF_INET6:
            max_masklen = 128
        quotient, remainder = divmod(masklen, 8)
        if remainder != 0:
            addr[quotient] = addr[quotient] & ((~0) << (8 - remainder))
            quotient += 1
        while quotient < max_masklen / 8:
            addr[quotient] = 0
            quotient += 1
        return addr

    def _from_network(self, network, masklen):
        split = network.split('/')
        if len(split) > 1:
            # network has prefix in it
            if masklen:
                raise ValueError('masklen specified twice')
            network = split[0]
            masklen = int(split[1])
        else:
            network = split[0]
        try:
            family, _, _, _, sockaddr = getaddrinfo(
                network, None, 0, SOCK_RAW, 6, AI_NUMERICHOST)[0]
        except gaierror as e:
            raise ValueError(e)
        if family == AF_INET:
            if masklen is None:
                masklen = 32
            if not (0 <= masklen <= 32):
                raise ValueError('invalid prefix length')
        elif family == AF_INET6:
            if masklen is None:
                masklen = 128
            if not (0 <= masklen <= 128):
                raise ValueError('invalid prefix length')
        else:
            return
        self.addr = self._inet_pton(family, sockaddr[0], masklen)
        self.bitlen = masklen
        self.family = family

    def _from_packed(self, packed, masklen):
        packed_len = len(packed)
        if packed_len == 4:
            family = AF_INET
            if masklen is None:
                masklen = 32
            if not (0 <= masklen <= 32):
                raise ValueError('invalid prefix length')
        elif packed_len == 16:
            family = AF_INET6
            if masklen is None:
                masklen = 128
            if not (0 <= masklen <= 128):
                raise ValueError('invalid prefix length')
        else:
            return
        self.addr = packed
        self.bitlen = masklen
        self.family = family


class RadixTree(object):
    def __init__(self):
        self.maxbits = 128
        self.head = None
        self.active_nodes = 0

    def _addr_test(self, addr, bitlen):
        left = addr[bitlen >> 3]
        right = 0x80 >> (bitlen & 0x07)
        return left & right

    def add(self, prefix):
        if self.head is None:
            # easy case
            node = RadixNode(prefix)
            self.head = node
            self.active_nodes += 1
            return node
        addr = prefix.addr
        bitlen = prefix.bitlen
        node = self.head
        # find the best place for the node
        while node.bitlen < bitlen or node._prefix.addr is None:
            if (node.bitlen < self.maxbits and
                    self._addr_test(addr, node.bitlen)):
                if node.right is None:
                    break
                node = node.right
            else:
                if node.left is None:
                    break
                node = node.left
        # find the first differing bit
        test_addr = node._prefix.addr
        check_bit = node.bitlen if node.bitlen < bitlen else bitlen
        differ_bit = 0
        i = 0
        while i * 8 < check_bit:
            r = addr[i] ^ test_addr[i]
            if r == 0:
                differ_bit = (i + 1) * 8
                i += 1
                continue
            # bitwise check
            for j in range(8):
                if r & (0x80 >> j):
                    break
            differ_bit = i * 8 + j
            break
        if differ_bit > check_bit:
            differ_bit = check_bit
        # now figure where to insert
        parent = node.parent
        while parent and parent.bitlen >= differ_bit:
            node, parent = parent, node.parent
        # found a match
        if differ_bit == bitlen and node.bitlen == bitlen:
            if isinstance(node._prefix, RadixGlue):
                node._prefix = prefix
            return node
        # no match, new node
        new_node = RadixNode(prefix)
        self.active_nodes += 1
        # fix it up
        if node.bitlen == differ_bit:
            new_node.parent = node
            if (node.bitlen < self.maxbits and
                    self._addr_test(addr, node.bitlen)):
                node.right = new_node
            else:
                node.left = new_node
            return new_node
        if bitlen == differ_bit:
            if bitlen < self.maxbits and self._addr_test(test_addr, bitlen):
                new_node.right = node
            else:
                new_node.left = node
            new_node.parent = node.parent
            if node.parent is None:
                self.head = new_node
            elif node.parent.right == node:
                node.parent.right = new_node
            else:
                node.parent.left = new_node
            node.parent = new_node
        else:
            glue_node = RadixNode(prefix_size=differ_bit, parent=node.parent)
            self.active_nodes += 1
            if differ_bit < self.maxbits and self._addr_test(addr, differ_bit):
                glue_node.right = new_node
                glue_node.left = node
            else:
                glue_node.right = node
                glue_node.left = new_node
            new_node.parent = glue_node
            if node.parent is None:
                self.head = glue_node
            elif node.parent.right == node:
                node.parent.right = glue_node
            else:
                node.parent.left = glue_node
            node.parent = glue_node
        return new_node

    def remove(self, node):
        if node.right and node.left:
            node._prefix.addr = None
            node.data = None
            node.bitlen = 0
            return
        if node.right is None and node.left is None:
            parent = node.parent
            self.active_nodes -= 1
            if parent is None:
                self.head = None
                return
            if parent.right == node:
                parent.right = None
                child = parent.left
            else:
                parent.left = None
                child = parent.right
            if parent._prefix.addr:
                return
            # remove the parent too
            if parent.parent is None:
                self.head = child
            elif parent.parent.right == parent:
                parent.parent.right = child
            else:
                parent.parent.left = child
            child.parent = parent.parent
            self.active_nodes -= 1
            return
        if node.right:
            child = node.right
        else:
            child = node.left
        parent = node.parent
        child.parent = parent
        self.active_nodes -= 1

        if parent is None:
            self.head = child
            return
        if parent.right == node:
            parent.right = child
        else:
            parent.left = child
        return

    def search_best(self, prefix):
        if self.head is None:
            return None
        node = self.head
        addr = prefix.addr
        bitlen = prefix.bitlen

        stack = []
        while node.bitlen < bitlen:
            if node._prefix.addr:
                stack.append(node)
            if self._addr_test(addr, node.bitlen):
                node = node.right
            else:
                node = node.left
            if node is None:
                break
        if node and node._prefix.addr:
            stack.append(node)
        if len(stack) <= 0:
            return None
        for node in stack[::-1]:
            if (self._prefix_match(node._prefix, prefix, node.bitlen) and
                    node.bitlen <= bitlen):
                return node
        return None

    def search_exact(self, prefix):
        if self.head is None:
            return None
        node = self.head
        addr = prefix.addr
        bitlen = prefix.bitlen

        while node.bitlen < bitlen:
            if self._addr_test(addr, node.bitlen):
                node = node.right
            else:
                node = node.left
            if node is None:
                return None

        if node.bitlen > bitlen or node._prefix.addr is None:
            return None

        if self._prefix_match(node._prefix, prefix, bitlen):
            return node
        return None

    def search_worst(self, prefix):
        if self.head is None:
            return None
        node = self.head
        addr = prefix.addr
        bitlen = prefix.bitlen

        stack = []
        while node.bitlen < bitlen:
            if node._prefix.addr:
                stack.append(node)
            if self._addr_test(addr, node.bitlen):
                node = node.right
            else:
                node = node.left
            if node is None:
                break
        if node and node._prefix.addr:
            stack.append(node)
        if len(stack) <= 0:
            return None
        for node in stack:
            if self._prefix_match(node._prefix, prefix, node.bitlen):
                return node
        return None

    def search_covered(self, prefix):
        results = []
        if self.head is None:
            return results
        node = self.head
        addr = prefix.addr
        bitlen = prefix.bitlen

        while node.bitlen < bitlen:
            if self._addr_test(addr, node.bitlen):
                node = node.right
            else:
                node = node.left
            if node is None:
                return results

        stack = [node]
        while stack:
            node = stack.pop()
            if self._prefix_match(node._prefix, prefix, prefix.bitlen):
                results.append(node)
            if node.right:
                stack.append(node.right)
            if node.left:
                stack.append(node.left)

        return results

    def _prefix_match(self, left, right, bitlen):
        l = left.addr
        r = right.addr
        if l is None or r is None:
            return False
        quotient, remainder = divmod(bitlen, 8)
        if l[:quotient] != r[:quotient]:
            return False
        if remainder == 0:
            return True
        mask = (~0) << (8 - remainder)
        if (l[quotient] & mask) == (r[quotient] & mask):
            return True
        return False


class RadixGlue(RadixPrefix):
    def __init__(self, bitlen=None):
        self.bitlen = bitlen


class RadixNode(object):
    def __init__(self, prefix=None, prefix_size=None, data=None,
                 parent=None, left=None, right=None):
        if prefix:
            self._prefix = prefix
        else:
            self._prefix = RadixGlue(bitlen=prefix_size)
        self._parent = parent
        self.bitlen = self._prefix.bitlen
        self.left = left
        self.right = right
        self.data = data
        self._cache = {}

    def __str__(self):
        return self.prefix

    def __repr__(self):
        return '<{0}>'.format(self.prefix)

    @property
    def network(self):
        return self._prefix.network

    @property
    def prefix(self):
        return str(self._prefix)

    @property
    def prefixlen(self):
        return self.bitlen

    @property
    def family(self):
        return self._prefix.family

    @property
    def packed(self):
        return self._prefix.packed

    def __set_parent(self, parent):
        self._parent = parent

    def __get_parent(self):
        return self._parent

    parent = property(__get_parent, __set_parent, None, "parent of node")


class Radix(object):
    def __init__(self):
        self._tree4 = RadixTree()
        self._tree6 = RadixTree()
        self.gen_id = 0            # detection of modifiction during iteration

    def add(self, network=None, masklen=None, packed=None):
        prefix = RadixPrefix(network, masklen, packed)
        if prefix.family == AF_INET:
            node = self._tree4.add(prefix)
        else:
            node = self._tree6.add(prefix)
        if node.data is None:
            node.data = {}
        self.gen_id += 1
        return node

    def delete(self, network=None, masklen=None, packed=None):
        node = self.search_exact(network, masklen, packed)
        if not node:
            raise KeyError('match not found')
        if node.family == AF_INET:
            self._tree4.remove(node)
        else:
            self._tree6.remove(node)
        self.gen_id += 1

    def search_exact(self, network=None, masklen=None, packed=None):
        prefix = RadixPrefix(network, masklen, packed)
        if prefix.family == AF_INET:
            node = self._tree4.search_exact(prefix)
        else:
            node = self._tree6.search_exact(prefix)
        if node and node.data is not None:
            return node
        else:
            return None

    def search_best(self, network=None, masklen=None, packed=None):
        prefix = RadixPrefix(network, masklen, packed)
        if prefix.family == AF_INET:
            node = self._tree4.search_best(prefix)
        else:
            node = self._tree6.search_best(prefix)
        if node and node.data is not None:
            return node
        else:
            return None

    def search_worst(self, network=None, masklen=None, packed=None):
        prefix = RadixPrefix(network, masklen, packed)
        if prefix.family == AF_INET:
            node = self._tree4.search_worst(prefix)
        else:
            node = self._tree6.search_worst(prefix)
        if node and node.data is not None:
            return node
        else:
            return None

    def search_covered(self, network=None, masklen=None, packed=None):
        prefix = RadixPrefix(network, masklen, packed)
        if prefix.family == AF_INET:
            return self._tree4.search_covered(prefix)
        else:
            return self._tree6.search_covered(prefix)

    def search_covering(self, network=None, masklen=None, packed=None):
        node = self.search_best(network=network, masklen=masklen,
                                packed=packed)
        stack = []
        while node is not None:
            if node._prefix.addr and node.data is not None:
                stack.append(node)
            node = node.parent
        return stack

    def _iter(self, node):
        stack = []
        while node is not None:
            if node._prefix.addr and node.data is not None:
                yield node
            if node.left:
                if node.right:
                    # we'll come back to it
                    stack.append(node.right)
                node = node.left
            elif node.right:
                node = node.right
            elif len(stack) != 0:
                node = stack.pop()
            else:
                break
        return

    def nodes(self):
        return [elt for elt in self]

    def prefixes(self):
        return [str(elt._prefix) for elt in self]

    def __iter__(self):
        init_id = self.gen_id
        for elt in self._iter(self._tree4.head):
            if init_id != self.gen_id:
                raise RuntimeWarning('detected modification during iteration')
            yield elt
        for elt in self._iter(self._tree6.head):
            if init_id != self.gen_id:
                raise RuntimeWarning('detected modification during iteration')
            yield elt

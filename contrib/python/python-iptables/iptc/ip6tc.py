# -*- coding: utf-8 -*-

import ctypes as ct
import socket

from .ip4tc import Rule, Table, IPTCError
from .util import find_library, load_kernel
from .xtables import (XT_INV_PROTO, NFPROTO_IPV6, xt_align, xt_counters)

__all__ = ["Table6", "Rule6"]

try:
    load_kernel("ip6_tables")
except:
    pass

_IFNAMSIZ = 16


def is_table6_available(name):
    try:
        if name in Table6.existing_table_names:
            return Table6.existing_table_names[name]
        Table6(name)
        Table6.existing_table_names[name] = True
        return True
    except IPTCError:
        pass
    Table6.existing_table_names[name] = False
    return False


class in6_addr(ct.Structure):
    """This class is a representation of the C struct in6_addr."""
    _fields_ = [("s6_addr", ct.c_uint8 * 16)]  # IPv6 address


class ip6t_ip6(ct.Structure):
    """This class is a representation of the C struct ip6t_ip6."""
    _fields_ = [("src", in6_addr),  # Source and destination IP6 addr
                ("dst", in6_addr),  # Mask for src and dest IP6 addr
                ("smsk", in6_addr),
                ("dmsk", in6_addr),
                ("iniface", ct.c_char * _IFNAMSIZ),
                ("outiface", ct.c_char * _IFNAMSIZ),
                ("iniface_mask", ct.c_char * _IFNAMSIZ),
                ("outiface_mask", ct.c_char * _IFNAMSIZ),
                ("proto", ct.c_uint16),    # Upper protocol number
                ("tos", ct.c_uint8),       # TOS, match iff flags & IP6T_F_TOS
                ("flags", ct.c_uint8),     # Flags word
                ("invflags", ct.c_uint8)]  # Inverse flags

    # flags
    IP6T_F_PROTO = 0x01    # Set if rule cares about upper protocols
    IP6T_F_TOS = 0x02    # Match the TOS
    IP6T_F_GOTO = 0x04    # Set if jump is a goto
    IP6T_F_MASK = 0x07    # All possible flag bits mask

    # invflags
    IP6T_INV_VIA_IN = 0x01    # Invert the sense of IN IFACE
    IP6T_INV_VIA_OUT = 0x02    # Invert the sense of OUT IFACE
    IP6T_INV_TOS = 0x04    # Invert the sense of TOS
    IP6T_INV_SRCIP = 0x08    # Invert the sense of SRC IP
    IP6T_INV_DSTIP = 0x10    # Invert the sense of DST OP
    IP6T_INV_FRAG = 0x20    # Invert the sense of FRAG
    IP6T_INV_PROTO = XT_INV_PROTO
    IP6T_INV_MASK = 0x7F    # All possible flag bits mask

    def __init__(self):
        # default: full netmask
        self.smsk.s6_addr = self.dmsk.s6_addr = 0xff * 16


class ip6t_entry(ct.Structure):
    """This class is a representation of the C struct ip6t_entry."""
    _fields_ = [("ipv6", ip6t_ip6),
                ("nfcache", ct.c_uint),          # fields that we care about
                ("target_offset", ct.c_uint16),  # size of ip6t_entry + matches
                ("next_offset", ct.c_uint16),    # size of e + matches + target
                ("comefrom", ct.c_uint),         # back pointer
                ("counters", xt_counters),       # packet and byte counters
                ("elems", ct.c_ubyte * 0)]       # the matches then the target


_libiptc, _ = find_library("ip6tc", "iptc")  # old iptables versions use iptc


class ip6tc(object):
    """This class contains all libip6tc API calls."""
    iptc_init = _libiptc.ip6tc_init
    iptc_init.restype = ct.POINTER(ct.c_int)
    iptc_init.argstype = [ct.c_char_p]

    iptc_free = _libiptc.ip6tc_free
    iptc_free.restype = None
    iptc_free.argstype = [ct.c_void_p]

    iptc_commit = _libiptc.ip6tc_commit
    iptc_commit.restype = ct.c_int
    iptc_commit.argstype = [ct.c_void_p]

    iptc_builtin = _libiptc.ip6tc_builtin
    iptc_builtin.restype = ct.c_int
    iptc_builtin.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_first_chain = _libiptc.ip6tc_first_chain
    iptc_first_chain.restype = ct.c_char_p
    iptc_first_chain.argstype = [ct.c_void_p]

    iptc_next_chain = _libiptc.ip6tc_next_chain
    iptc_next_chain.restype = ct.c_char_p
    iptc_next_chain.argstype = [ct.c_void_p]

    iptc_is_chain = _libiptc.ip6tc_is_chain
    iptc_is_chain.restype = ct.c_int
    iptc_is_chain.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_create_chain = _libiptc.ip6tc_create_chain
    iptc_create_chain.restype = ct.c_int
    iptc_create_chain.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_delete_chain = _libiptc.ip6tc_delete_chain
    iptc_delete_chain.restype = ct.c_int
    iptc_delete_chain.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_rename_chain = _libiptc.ip6tc_rename_chain
    iptc_rename_chain.restype = ct.c_int
    iptc_rename_chain.argstype = [ct.c_char_p, ct.c_char_p, ct.c_void_p]

    iptc_flush_entries = _libiptc.ip6tc_flush_entries
    iptc_flush_entries.restype = ct.c_int
    iptc_flush_entries.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_zero_entries = _libiptc.ip6tc_zero_entries
    iptc_zero_entries.restype = ct.c_int
    iptc_zero_entries.argstype = [ct.c_char_p, ct.c_void_p]

    # Get the policy of a given built-in chain
    iptc_get_policy = _libiptc.ip6tc_get_policy
    iptc_get_policy.restype = ct.c_char_p
    iptc_get_policy.argstype = [ct.c_char_p, ct.POINTER(xt_counters),
                                ct.c_void_p]

    # Set the policy of a chain
    iptc_set_policy = _libiptc.ip6tc_set_policy
    iptc_set_policy.restype = ct.c_int
    iptc_set_policy.argstype = [ct.c_char_p, ct.c_char_p,
                                ct.POINTER(xt_counters), ct.c_void_p]

    # Get first rule in the given chain: NULL for empty chain.
    iptc_first_rule = _libiptc.ip6tc_first_rule
    iptc_first_rule.restype = ct.POINTER(ip6t_entry)
    iptc_first_rule.argstype = [ct.c_char_p, ct.c_void_p]

    # Returns NULL when rules run out.
    iptc_next_rule = _libiptc.ip6tc_next_rule
    iptc_next_rule.restype = ct.POINTER(ip6t_entry)
    iptc_next_rule.argstype = [ct.POINTER(ip6t_entry), ct.c_void_p]

    # Returns a pointer to the target name of this entry.
    iptc_get_target = _libiptc.ip6tc_get_target
    iptc_get_target.restype = ct.c_char_p
    iptc_get_target.argstype = [ct.POINTER(ip6t_entry), ct.c_void_p]

    # These functions return TRUE for OK or 0 and set errno.  If errno ==
    # 0, it means there was a version error (ie. upgrade libiptc).
    # Rule numbers start at 1 for the first rule.

    # Insert the entry `e' in chain `chain' into position `rulenum'.
    iptc_insert_entry = _libiptc.ip6tc_insert_entry
    iptc_insert_entry.restype = ct.c_int
    iptc_insert_entry.argstype = [ct.c_char_p, ct.POINTER(ip6t_entry),
                                  ct.c_int, ct.c_void_p]

    # Atomically replace rule `rulenum' in `chain' with `e'.
    iptc_replace_entry = _libiptc.ip6tc_replace_entry
    iptc_replace_entry.restype = ct.c_int
    iptc_replace_entry.argstype = [ct.c_char_p, ct.POINTER(ip6t_entry),
                                   ct.c_int, ct.c_void_p]

    # Append entry `e' to chain `chain'.  Equivalent to insert with
    #   rulenum = length of chain.
    iptc_append_entry = _libiptc.ip6tc_append_entry
    iptc_append_entry.restype = ct.c_int
    iptc_append_entry.argstype = [ct.c_char_p, ct.POINTER(ip6t_entry),
                                  ct.c_void_p]

    # Delete the first rule in `chain' which matches `e', subject to
    #   matchmask (array of length == origfw)
    iptc_delete_entry = _libiptc.ip6tc_delete_entry
    iptc_delete_entry.restype = ct.c_int
    iptc_delete_entry.argstype = [ct.c_char_p, ct.POINTER(ip6t_entry),
                                  ct.POINTER(ct.c_ubyte), ct.c_void_p]

    # Delete the rule in position `rulenum' in `chain'.
    iptc_delete_num_entry = _libiptc.ip6tc_delete_num_entry
    iptc_delete_num_entry.restype = ct.c_int
    iptc_delete_num_entry.argstype = [ct.c_char_p, ct.c_uint, ct.c_void_p]

    # Check the packet `e' on chain `chain'.  Returns the verdict, or
    #   NULL and sets errno.
    # iptc_check_packet = _libiptc.ip6tc_check_packet
    # iptc_check_packet.restype = ct.c_char_p
    # iptc_check_packet.argstype = [ct.c_char_p, ct.POINTER(ipt), ct.c_void_p]

    # Get the number of references to this chain
    iptc_get_references = _libiptc.ip6tc_get_references
    iptc_get_references.restype = ct.c_int
    iptc_get_references.argstype = [ct.c_uint, ct.c_char_p, ct.c_void_p]

    # read packet and byte counters for a specific rule
    iptc_read_counter = _libiptc.ip6tc_read_counter
    iptc_read_counter.restype = ct.POINTER(xt_counters)
    iptc_read_counter.argstype = [ct.c_char_p, ct.c_uint, ct.c_void_p]

    # zero packet and byte counters for a specific rule
    iptc_zero_counter = _libiptc.ip6tc_zero_counter
    iptc_zero_counter.restype = ct.c_int
    iptc_zero_counter.argstype = [ct.c_char_p, ct.c_uint, ct.c_void_p]

    # set packet and byte counters for a specific rule
    iptc_set_counter = _libiptc.ip6tc_set_counter
    iptc_set_counter.restype = ct.c_int
    iptc_set_counter.argstype = [ct.c_char_p, ct.c_uint,
                                 ct.POINTER(xt_counters), ct.c_void_p]

    # Translates errno numbers into more human-readable form than strerror.
    iptc_strerror = _libiptc.ip6tc_strerror
    iptc_strerror.restype = ct.c_char_p
    iptc_strerror.argstype = [ct.c_int]


class Rule6(Rule):
    """This is an IPv6 rule."""

    def __init__(self, entry=None, chain=None):
        self.nfproto = NFPROTO_IPV6
        self._matches = []
        self._target = None
        self.chain = chain
        self.rule = entry

    def __eq__(self, rule):
        if self._target != rule._target:
            return False
        if len(self._matches) != len(rule._matches):
            return False
        if set(rule._matches) != set([x for x in rule._matches
                                      if x in self._matches]):
            return False
        if (self.src == rule.src and self.dst == rule.dst and
                self.protocol == rule.protocol and
                self.in_interface == rule.in_interface and
                self.out_interface == rule.out_interface):
            return True
        return False

    def save(self, name):
        return self._save(name, self.entry.ipv6)

    def _get_tables(self):
        return [Table6(t) for t in Table6.ALL if is_table6_available(t)]
    tables = property(_get_tables)
    """This is the list of tables for our protocol."""

    def _count_bits(self, n):
        bits = 0
        while n > 0:
            if n & 1:
                bits += 1
            n = n >> 1
        return bits

    def _create_mask(self, plen):
        mask = []
        for i in range(16):
            if plen >= 8:
                mask.append(0xff)
            elif plen > 0:
                mask.append(0xff>>(8-plen)<<(8-plen))
            else:
                mask.append(0x00)
            plen -= 8
        return mask

    def get_src(self):
        src = ""
        if self.entry.ipv6.invflags & ip6t_ip6.IP6T_INV_SRCIP:
            src = "".join([src, "!"])
        try:
            addr = socket.inet_ntop(socket.AF_INET6,
                                    self.entry.ipv6.src.s6_addr)
        except socket.error:
            raise IPTCError("error in internal state: invalid address")
        src = "".join([src, addr, "/"])

        # create prefix length from mask in smsk
        plen = 0
        for x in self.entry.ipv6.smsk.s6_addr:
            if x == 0xff:
                plen += 8
            else:
                plen += self._count_bits(x)
                break
        src = "".join([src, str(plen)])
        return src

    def _get_address_netmask(self, a):
        slash = a.find("/")
        if slash == -1:
            addr = a
            netm = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
        else:
            addr = a[:slash]
            netm = a[slash + 1:]
        return addr, netm

    def _addr2in6addr(self, addr):
        arr = ct.c_uint8 * 16
        ina = in6_addr()
        try:
            ina.s6_addr = arr.from_buffer_copy(
                socket.inet_pton(socket.AF_INET6, addr))
        except socket.error:
            raise ValueError("invalid address %s" % (addr))
        return arr, ina

    def set_src(self, src):
        if src[0] == "!":
            self.entry.ipv6.invflags |= ip6t_ip6.IP6T_INV_SRCIP
            src = src[1:]
        else:
            self.entry.ipv6.invflags &= (~ip6t_ip6.IP6T_INV_SRCIP &
                                         ip6t_ip6.IP6T_INV_MASK)

        addr, netm = self._get_address_netmask(src)

        arr, self.entry.ipv6.src = self._addr2in6addr(addr)

        # if we got a numeric prefix length
        if netm.isdigit():
            plen = int(netm)
            if plen < 0 or plen > 128:
                raise ValueError("invalid prefix length %d" % (plen))
            self.entry.ipv6.smsk.s6_addr = arr(*self._create_mask(plen))
            return

        # nope, we got an IPv6 address-style prefix
        neta = in6_addr()
        try:
            neta.s6_addr = arr.from_buffer_copy(
                socket.inet_pton(socket.AF_INET6, netm))
        except socket.error:
            raise ValueError("invalid netmask %s" % (netm))
        self.entry.ipv6.smsk = neta

    src = property(get_src, set_src)
    """This is the source network address with an optional prefix length in
    string form."""

    def get_dst(self):
        dst = ""
        if self.entry.ipv6.invflags & ip6t_ip6.IP6T_INV_DSTIP:
            dst = "".join([dst, "!"])
        try:
            addr = socket.inet_ntop(socket.AF_INET6,
                                    self.entry.ipv6.dst.s6_addr)
        except socket.error:
            raise IPTCError("error in internal state: invalid address")
        dst = "".join([dst, addr, "/"])

        # create prefix length from mask in dmsk
        plen = 0
        for x in self.entry.ipv6.dmsk.s6_addr:
            if x & 0xff == 0xff:
                plen += 8
            else:
                plen += self._count_bits(x)
                break
        dst = "".join([dst, str(plen)])
        return dst

    def set_dst(self, dst):
        if dst[0] == "!":
            self.entry.ipv6.invflags |= ip6t_ip6.IP6T_INV_DSTIP
            dst = dst[1:]
        else:
            self.entry.ipv6.invflags &= (~ip6t_ip6.IP6T_INV_DSTIP &
                                         ip6t_ip6.IP6T_INV_MASK)

        addr, netm = self._get_address_netmask(dst)

        arr, self.entry.ipv6.dst = self._addr2in6addr(addr)

        # if we got a numeric prefix length
        if netm.isdigit():
            plen = int(netm)
            if plen < 0 or plen > 128:
                raise ValueError("invalid prefix length %d" % (plen))
            self.entry.ipv6.dmsk.s6_addr = arr(*self._create_mask(plen))
            return

        # nope, we got an IPv6 address-style prefix
        neta = in6_addr()
        try:
            neta.s6_addr = arr.from_buffer_copy(
                socket.inet_pton(socket.AF_INET6, netm))
        except socket.error:
            raise ValueError("invalid netmask %s" % (netm))
        self.entry.ipv6.dmsk = neta

    dst = property(get_dst, set_dst)
    """This is the destination network address with an optional network mask
    in string form."""

    def get_in_interface(self):
        intf = ""
        if self.entry.ipv6.invflags & ip6t_ip6.IP6T_INV_VIA_IN:
            intf = "!"

        iface = self.entry.ipv6.iniface.decode()
        mask = self.entry.ipv6.iniface_mask

        if len(mask) == 0:
            return None

        intf += iface
        if len(iface) == len(mask):
            intf += '+'
        intf = intf[:_IFNAMSIZ]

        return intf

    def set_in_interface(self, intf):
        if intf[0] == "!":
            self.entry.ipv6.invflags |= ip6t_ip6.IP6T_INV_VIA_IN
            intf = intf[1:]
        else:
            self.entry.ipv6.invflags &= (~ip6t_ip6.IP6T_INV_VIA_IN &
                                         ip6t_ip6.IP6T_INV_MASK)
        if len(intf) >= _IFNAMSIZ:
            raise ValueError("interface name %s too long" % (intf))
        masklen = len(intf) + 1
        if intf[len(intf) - 1] == "+":
            intf = intf[:-1]
            masklen -= 2

        self.entry.ipv6.iniface = ("".join(
            [intf, '\x00' * (_IFNAMSIZ - len(intf))])).encode()
        self.entry.ipv6.iniface_mask = ("".join(
            ['\x01' * masklen, '\x00' * (_IFNAMSIZ - masklen)])).encode()

    in_interface = property(get_in_interface, set_in_interface)
    """This is the input network interface e.g. *eth0*.  A wildcard match can
    be achieved via *+* e.g. *ppp+* matches any *ppp* interface."""

    def get_out_interface(self):
        intf = ""
        if self.entry.ipv6.invflags & ip6t_ip6.IP6T_INV_VIA_OUT:
            intf = "!"

        iface = self.entry.ipv6.outiface.decode()
        mask = self.entry.ipv6.outiface_mask

        if len(mask) == 0:
            return None

        intf += iface
        if len(iface) == len(mask):
            intf += '+'
        intf = intf[:_IFNAMSIZ]

        return intf

    def set_out_interface(self, intf):
        if intf[0] == "!":
            self.entry.ipv6.invflags |= ip6t_ip6.IP6T_INV_VIA_OUT
            intf = intf[1:]
        else:
            self.entry.ipv6.invflags &= (~ip6t_ip6.IP6T_INV_VIA_OUT &
                                         ip6t_ip6.IP6T_INV_MASK)
        if len(intf) >= _IFNAMSIZ:
            raise ValueError("interface name %s too long" % (intf))
        masklen = len(intf) + 1
        if intf[len(intf) - 1] == "+":
            intf = intf[:-1]
            masklen -= 2

        self.entry.ipv6.outiface = ("".join(
            [intf, '\x00' * (_IFNAMSIZ - len(intf))])).encode()
        self.entry.ipv6.outiface_mask = ("".join(
            ['\x01' * masklen, '\x00' * (_IFNAMSIZ - masklen)])).encode()

    out_interface = property(get_out_interface, set_out_interface)
    """This is the output network interface e.g. *eth0*.  A wildcard match can
    be achieved via *+* e.g. *ppp+* matches any *ppp* interface."""

    def get_protocol(self):
        if self.entry.ipv6.invflags & ip6t_ip6.IP6T_INV_PROTO:
            proto = "!"
        else:
            proto = ""
        proto = "".join([proto, self.protocols.get(self.entry.ipv6.proto, str(self.entry.ipv6.proto))])
        return proto

    def set_protocol(self, proto):
        proto = str(proto)
        if proto[0] == "!":
            self.entry.ipv6.invflags |= ip6t_ip6.IP6T_INV_PROTO
            self.entry.ipv6.flags &= (~ip6t_ip6.IP6T_F_PROTO &
                                      ip6t_ip6.IP6T_F_MASK)
            proto = proto[1:]
        else:
            self.entry.ipv6.invflags &= (~ip6t_ip6.IP6T_INV_PROTO &
                                         ip6t_ip6.IP6T_INV_MASK)
            self.entry.ipv6.flags |= ip6t_ip6.IP6T_F_PROTO
        if proto.isdigit():
            self.entry.ipv6.proto = int(proto)
            return
        for p in self.protocols.items():
            if proto.lower() == p[1]:
                self.entry.ipv6.proto = p[0]
                return
        raise ValueError("invalid protocol %s" % (proto))

    protocol = property(get_protocol, set_protocol)
    """This is the transport layer protocol."""

    def get_ip(self):
        return self.entry.ipv6

    def _entry_size(self):
        return xt_align(ct.sizeof(ip6t_entry))

    def _entry_type(self):
        return ip6t_entry

    def _new_entry(self):
        return ip6t_entry()


class Table6(Table):
    """The IPv6 version of Table.

    There are four fixed tables:
        * **Table.FILTER**, the filter table,
        * **Table.MANGLE**, the mangle table,
        * **Table.RAW**, the raw table and
        * **Table.SECURITY**, the security table.

    The four tables are cached, so if you create a new Table, and it has been
    instantiated before, then it will be reused. To get access to e.g. the
    filter table:

    >>> import iptc
    >>> table = iptc.Table6(iptc.Table6.FILTER)

    The interface provided by *Table* is rather low-level, in fact it maps to
    *libiptc* API calls one by one, and take low-level iptables structs as
    parameters.  It is encouraged to, when possible, use Chain, Rule, Match
    and Target to achieve what is wanted instead, since they hide the
    low-level details from the user.
    """

    FILTER = "filter"
    """This is the constant for the filter table."""
    MANGLE = "mangle"
    """This is the constant for the mangle table."""
    RAW = "raw"
    """This is the constant for the raw table."""
    NAT = "nat"
    """This is the constant for the nat table."""
    SECURITY = "security"
    """This is the constant for the security table."""
    ALL = ["filter", "mangle", "raw", "nat", "security"]
    """This is the constant for all tables."""

    _cache = dict()
    existing_table_names = dict()
    """Dictionary to check faster if a table is available."""

    def __new__(cls, name, autocommit=None):
        obj = Table6._cache.get(name, None)
        if not obj:
            obj = object.__new__(cls)
            if autocommit is None:
                autocommit = True
            obj._init(name, autocommit)
            Table6._cache[name] = obj
        elif autocommit is not None:
            obj.autocommit = autocommit
        return obj

    def _init(self, name, autocommit):
        """
        Here *name* is the name of the table to instantiate, if it has already
        been instantiated the existing cached object is returned.
        *Autocommit* specifies that any low-level iptables operation should be
        committed immediately, making changes visible in the kernel.
        """
        self._iptc = ip6tc()  # to keep references to functions
        self._handle = None
        self.name = name
        self.autocommit = autocommit
        self.refresh()

    def create_rule(self, entry=None, chain=None):
        return Rule6(entry, chain)

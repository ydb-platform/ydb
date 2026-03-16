# -*- coding: utf-8 -*-

import os
import re
import shlex
import sys
import ctypes as ct
import socket
import struct
import weakref

from .util import find_library, load_kernel, find_libc
from .xtables import (XT_INV_PROTO, NFPROTO_IPV4, XTablesError, xtables,
                      xt_align, xt_counters, xt_entry_target, xt_entry_match)

__all__ = ["Table", "Chain", "Rule", "Match", "Target", "Policy", "IPTCError"]

try:
    load_kernel("ip_tables")
except:
    pass

# Add IPPROTO_SCTP to socket module if not available
if not hasattr(socket, 'IPPROTO_SCTP'):
    setattr(socket, 'IPPROTO_SCTP', 132)

_IFNAMSIZ = 16

_libc = find_libc()
_get_errno_loc = _libc.__errno_location
_get_errno_loc.restype = ct.POINTER(ct.c_int)
_malloc = _libc.malloc
_malloc.restype = ct.POINTER(ct.c_ubyte)
_malloc.argtypes = [ct.c_size_t]
_free = _libc.free
_free.restype = None
_free.argtypes = [ct.POINTER(ct.c_ubyte)]

# Make sure xt_params is set up.
xtables(NFPROTO_IPV4)


def is_table_available(name):
    try:
        if name in Table.existing_table_names:
            return Table.existing_table_names[name]
        Table(name)
        Table.existing_table_names[name] = True
        return True
    except IPTCError:
        pass
    Table.existing_table_names[name] = False
    return False


class in_addr(ct.Structure):
    """This class is a representation of the C struct in_addr."""
    _fields_ = [("s_addr", ct.c_uint32)]


class ipt_ip(ct.Structure):
    """This class is a representation of the C struct ipt_ip."""
    _fields_ = [("src", in_addr),
                ("dst", in_addr),
                ("smsk", in_addr),
                ("dmsk", in_addr),
                ("iniface", ct.c_char * _IFNAMSIZ),
                ("outiface", ct.c_char * _IFNAMSIZ),
                ("iniface_mask", ct.c_char * _IFNAMSIZ),
                ("outiface_mask", ct.c_char * _IFNAMSIZ),
                ("proto", ct.c_uint16),
                ("flags", ct.c_uint8),
                ("invflags", ct.c_uint8)]

    # flags
    IPT_F_FRAG = 0x01    # set if rule is a fragment rule
    IPT_F_GOTO = 0x02    # set if jump is a goto
    IPT_F_MASK = 0x03    # all possible flag bits mask

    # invflags
    IPT_INV_VIA_IN = 0x01  # invert the sense of IN IFACE
    IPT_INV_VIA_OUT = 0x02  # invert the sense of OUT IFACE
    IPT_INV_TOS = 0x04  # invert the sense of TOS
    IPT_INV_SRCIP = 0x08  # invert the sense of SRC IP
    IPT_INV_DSTIP = 0x10  # invert the sense of DST OP
    IPT_INV_FRAG = 0x20  # invert the sense of FRAG
    IPT_INV_PROTO = XT_INV_PROTO  # invert the sense of PROTO (XT_INV_PROTO)
    IPT_INV_MASK = 0x7F  # all possible flag bits mask

    def __init__(self):
        # default: full netmask
        self.smsk.s_addr = self.dmsk.s_addr = 0xffffffff


class ipt_entry(ct.Structure):
    """This class is a representation of the C struct ipt_entry."""
    _fields_ = [("ip", ipt_ip),
                ("nfcache", ct.c_uint),  # mark with fields that we care about
                ("target_offset", ct.c_uint16),  # size of ipt_entry + matches
                ("next_offset", ct.c_uint16),  # size of e + matches + target
                ("comefrom", ct.c_uint),  # back pointer
                ("counters", xt_counters),  # packet and byte counters
                ("elems", ct.c_ubyte * 0)]  # any matches then the target


class IPTCError(Exception):
    """This exception is raised when a low-level libiptc error occurs.

    It contains a short description about the error that occurred while
    executing an iptables operation.
    """


_libiptc, _ = find_library("ip4tc", "iptc")  # old iptables versions use iptc


class iptc(object):
    """This class contains all libiptc API calls."""
    iptc_init = _libiptc.iptc_init
    iptc_init.restype = ct.POINTER(ct.c_int)
    iptc_init.argstype = [ct.c_char_p]

    iptc_free = _libiptc.iptc_free
    iptc_free.restype = None
    iptc_free.argstype = [ct.c_void_p]

    iptc_commit = _libiptc.iptc_commit
    iptc_commit.restype = ct.c_int
    iptc_commit.argstype = [ct.c_void_p]

    iptc_builtin = _libiptc.iptc_builtin
    iptc_builtin.restype = ct.c_int
    iptc_builtin.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_first_chain = _libiptc.iptc_first_chain
    iptc_first_chain.restype = ct.c_char_p
    iptc_first_chain.argstype = [ct.c_void_p]

    iptc_next_chain = _libiptc.iptc_next_chain
    iptc_next_chain.restype = ct.c_char_p
    iptc_next_chain.argstype = [ct.c_void_p]

    iptc_is_chain = _libiptc.iptc_is_chain
    iptc_is_chain.restype = ct.c_int
    iptc_is_chain.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_create_chain = _libiptc.iptc_create_chain
    iptc_create_chain.restype = ct.c_int
    iptc_create_chain.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_delete_chain = _libiptc.iptc_delete_chain
    iptc_delete_chain.restype = ct.c_int
    iptc_delete_chain.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_rename_chain = _libiptc.iptc_rename_chain
    iptc_rename_chain.restype = ct.c_int
    iptc_rename_chain.argstype = [ct.c_char_p, ct.c_char_p, ct.c_void_p]

    iptc_flush_entries = _libiptc.iptc_flush_entries
    iptc_flush_entries.restype = ct.c_int
    iptc_flush_entries.argstype = [ct.c_char_p, ct.c_void_p]

    iptc_zero_entries = _libiptc.iptc_zero_entries
    iptc_zero_entries.restype = ct.c_int
    iptc_zero_entries.argstype = [ct.c_char_p, ct.c_void_p]

    # get the policy of a given built-in chain
    iptc_get_policy = _libiptc.iptc_get_policy
    iptc_get_policy.restype = ct.c_char_p
    iptc_get_policy.argstype = [ct.c_char_p, ct.POINTER(xt_counters),
                                ct.c_void_p]

    # Set the policy of a chain
    iptc_set_policy = _libiptc.iptc_set_policy
    iptc_set_policy.restype = ct.c_int
    iptc_set_policy.argstype = [ct.c_char_p, ct.c_char_p,
                                ct.POINTER(xt_counters), ct.c_void_p]

    # Get first rule in the given chain: NULL for empty chain.
    iptc_first_rule = _libiptc.iptc_first_rule
    iptc_first_rule.restype = ct.POINTER(ipt_entry)
    iptc_first_rule.argstype = [ct.c_char_p, ct.c_void_p]

    # Returns NULL when rules run out.
    iptc_next_rule = _libiptc.iptc_next_rule
    iptc_next_rule.restype = ct.POINTER(ipt_entry)
    iptc_next_rule.argstype = [ct.POINTER(ipt_entry), ct.c_void_p]

    # Returns a pointer to the target name of this entry.
    iptc_get_target = _libiptc.iptc_get_target
    iptc_get_target.restype = ct.c_char_p
    iptc_get_target.argstype = [ct.POINTER(ipt_entry), ct.c_void_p]

    # These functions return TRUE for OK or 0 and set errno.  If errno ==
    # 0, it means there was a version error (ie. upgrade libiptc).
    # Rule numbers start at 1 for the first rule.

    # Insert the entry `e' in chain `chain' into position `rulenum'.
    iptc_insert_entry = _libiptc.iptc_insert_entry
    iptc_insert_entry.restype = ct.c_int
    iptc_insert_entry.argstype = [ct.c_char_p, ct.POINTER(ipt_entry),
                                  ct.c_int, ct.c_void_p]

    # Atomically replace rule `rulenum' in `chain' with `e'.
    iptc_replace_entry = _libiptc.iptc_replace_entry
    iptc_replace_entry.restype = ct.c_int
    iptc_replace_entry.argstype = [ct.c_char_p, ct.POINTER(ipt_entry),
                                   ct.c_int, ct.c_void_p]

    # Append entry `e' to chain `chain'.  Equivalent to insert with
    #   rulenum = length of chain.
    iptc_append_entry = _libiptc.iptc_append_entry
    iptc_append_entry.restype = ct.c_int
    iptc_append_entry.argstype = [ct.c_char_p, ct.POINTER(ipt_entry),
                                  ct.c_void_p]

    # Delete the first rule in `chain' which matches `e', subject to
    #   matchmask (array of length == origfw)
    iptc_delete_entry = _libiptc.iptc_delete_entry
    iptc_delete_entry.restype = ct.c_int
    iptc_delete_entry.argstype = [ct.c_char_p, ct.POINTER(ipt_entry),
                                  ct.POINTER(ct.c_ubyte), ct.c_void_p]

    # Delete the rule in position `rulenum' in `chain'.
    iptc_delete_num_entry = _libiptc.iptc_delete_num_entry
    iptc_delete_num_entry.restype = ct.c_int
    iptc_delete_num_entry.argstype = [ct.c_char_p, ct.c_uint, ct.c_void_p]

    # Check the packet `e' on chain `chain'.  Returns the verdict, or
    #   NULL and sets errno.
    # iptc_check_packet = _libiptc.iptc_check_packet
    # iptc_check_packet.restype = ct.c_char_p
    # iptc_check_packet.argstype = [ct.c_char_p, ct.POINTER(ipt), ct.c_void_p]

    # Get the number of references to this chain
    iptc_get_references = _libiptc.iptc_get_references
    iptc_get_references.restype = ct.c_int
    iptc_get_references.argstype = [ct.c_uint, ct.c_char_p, ct.c_void_p]

    # read packet and byte counters for a specific rule
    iptc_read_counter = _libiptc.iptc_read_counter
    iptc_read_counter.restype = ct.POINTER(xt_counters)
    iptc_read_counter.argstype = [ct.c_char_p, ct.c_uint, ct.c_void_p]

    # zero packet and byte counters for a specific rule
    iptc_zero_counter = _libiptc.iptc_zero_counter
    iptc_zero_counter.restype = ct.c_int
    iptc_zero_counter.argstype = [ct.c_char_p, ct.c_uint, ct.c_void_p]

    # set packet and byte counters for a specific rule
    iptc_set_counter = _libiptc.iptc_set_counter
    iptc_set_counter.restype = ct.c_int
    iptc_set_counter.argstype = [ct.c_char_p, ct.c_uint,
                                 ct.POINTER(xt_counters), ct.c_void_p]

    # Translates errno numbers into more human-readable form than strerror.
    iptc_strerror = _libiptc.iptc_strerror
    iptc_strerror.restype = ct.c_char_p
    iptc_strerror.argstype = [ct.c_int]


class IPTCModule(object):
    """Superclass for Match and Target."""
    pattern = re.compile(
        '\s*(!)?\s*--([-\w]+)\s+(!)?\s*"?([^"]*?)"?(?=\s*(?:!?\s*--|$))')

    def __init__(self):
        self._name = None
        self._rule = None
        self._module = None
        self._revision = None
        self._ptr = None
        self._ptrptr = None
        raise NotImplementedError()

    def set_parameter(self, parameter, value=None):
        """
        Set a parameter for target or match extension, with an optional value.

        @param parameter: name of the parameter to set
        @type parameter: C{str}

        @param value: optional value of the parameter, defaults to C{None}
        @type value: C{str} or a C{list} of C{str}
        """
        if value is None:
            value = ""

        return self.parse(parameter.replace("_", "-"), value)

    def parse(self, parameter, value):
        # Parameter name must always be a string.
        parameter = parameter.encode()

        # Check if we are dealing with an inverted parameter value.
        inv = ct.c_int(0)
        if len(value) > 0 and value[0] == "!":
            inv = ct.c_int(1)
            value = value[1:]

        # Value can be either a string, or a list of strings, e.g. "8888",
        # "!0:65535" or ["!", "example_set", "dst"].
        args = []

        is_str = isinstance(value, str)
        try:
            if not is_str:
                is_str = isinstance(value, unicode)
        except:
            pass

        if is_str:
            args = [value.encode()]
        else:
            try:
                args = [val.encode() for val in value]
            except:
                raise TypeError("Invalid parameter value: "
                                "must be string or list of strings")

        if not self._module.extra_opts and not self._module.x6_options:
            raise AttributeError("%s: invalid parameter %s" %
                                 (self._module.name, parameter))

        parameter = parameter.strip()

        N = len(args)

        argv = (ct.c_char_p * (N + 1))()
        argv[0] = parameter
        for i in range(N):
            argv[i + 1] = args[i]

        entry = self._rule.entry and ct.pointer(self._rule.entry) or None

        self._parse(argv, inv, entry)

    def _parse(self, argv, inv, entry):
        raise NotImplementedError()

    def final_check(self):
        if self._module:
            self._update_parameters()
            self._final_check()  # subclasses override this

    def _final_check(self):
        raise NotImplementedError()

    def _get_saved_buf(self, ip):
        if not self._module or not self._module.save:
            return None

        # redirect C stdout to a pipe and read back the output of m->save

        # Flush stdout to avoid getting buffered results
        sys.stdout.flush()
        # Save the current C stdout.
        stdout = os.dup(1)
        try:
            # Create a pipe and use the write end to replace the original C
            # stdout.
            pipes = os.pipe()
            os.dup2(pipes[1], 1)
            self._xt.save(self._module, ip, self._ptr)

            # Use the read end to read whatever was written.
            buf = os.read(pipes[0], 1024)

            # Clean up the pipe.
            os.close(pipes[0])
            os.close(pipes[1])
            return buf
        finally:
            # Put the original C stdout back in place.
            os.dup2(stdout, 1)

            # Clean up the copy we made.
            os.close(stdout)

    def save(self, name):
        return self._save(name, self.rule.get_ip())

    def _save(self, name, ip):
        buf = self._get_saved_buf(ip).decode()
        if buf is None:
            return None
        if not self._module or not self._module.save:
            return None
        if name:
            return self._get_value(buf, name)
        else:
            return self._get_all_values(buf)

    def _get_all_values(self, buf):
        table = {}  # variable -> (value, inverted)
        res = re.findall(IPTCModule.pattern, buf)
        for x in res:
            value, invert = (x[3], x[0] or x[2])
            table[x[1].replace("-", "_")] = "%s%s" % (invert and "!" or "",
                                                      value)
        return table

    def _get_value(self, buf, name):
        table = {}  # variable -> (value, inverted)
        res = re.findall(IPTCModule.pattern, buf)
        for x in res:
            table[x[1]] = (x[3], x[0] or x[2])
        try:
            value, invert = table[name]
            return "%s%s" % (invert and "!" or "", value)
        except KeyError:
            return None

    def get_all_parameters(self):
        params = {}
        ip = self.rule.get_ip()
        buf = self._get_saved_buf(ip)
        if buf is None:
            return params
        if type(buf) != str:
            # In Python3, string and bytes are different types.
            buf = buf.decode()
        res = shlex.split(buf)
        res.reverse()
        inv = False
        key = None
        while len(res) > 0:
            x = res.pop()
            if x == '!':
                # Next parameter is negated.
                inv = True
                continue
            if x.startswith('--'):  # This is a parameter name.
                key = x[2:]
                if inv:
                    params[key] = ['!']
                else:
                    params[key] = []
                inv = False
                continue
            # At this point key should be set, unless the output from save is
            # not formatted right. Let's be defensive, since some users
            # reported that problem.
            if key is not None:
                params[key].append(x)  # This is a parameter value.
        return params

    def _update_parameters(self):
        params = self.get_all_parameters().items()
        self.reset()
        for k, v in params:
            self.set_parameter(k, v)

    def _get_alias_name(self):
        if not self._module or not self._ptr:
            return None
        alias = getattr(self._module, 'alias', None)
        if not alias:
            return None
        return self._module.alias(self._ptr).decode()

    def __setattr__(self, name, value):
        if not name.startswith('_') and name not in dir(self):
            self.parse(name.replace("_", "-"), value)
        else:
            object.__setattr__(self, name, value)

    def __getattr__(self, name):
        if not name.startswith('_'):
            return self.save(name.replace("_", "-"))

    def _get_parameters(self):
        return self.save(None)
    parameters = property(_get_parameters)
    """Dictionary with all parameters in the form of name -> value. A match or
    target might have default parameters as well, so this dictionary will
    contain those set by the module by default too."""

    def _get_name(self):
        alias = self._get_alias_name()
        return alias and alias or self._name
    name = property(_get_name)
    """Name of this target or match."""

    def _get_rule(self):
        return self._rule

    def _set_rule(self, rule):
        self._rule = rule
    rule = property(_get_rule, _set_rule)
    """The rule this target or match belong to."""


class _Buffer(object):
    def __init__(self, size=0):
        if size > 0:
            self.buffer = _malloc(size)
            if self.buffer is None:
                raise Exception("Can't allocate buffer")
        else:
            self.buffer = None

    def __del__(self):
        if self.buffer is not None:
            _free(self.buffer)


class Match(IPTCModule):
    """Matches are extensions which can match for special header fields or
    other attributes of a packet.

    Target and match extensions in iptables have parameters.  These parameters
    are implemented as instance attributes in python.  However, to make the
    names of parameters legal attribute names they have to be converted.  The
    rule is to cut the leading double dash from the name, and replace
    dashes in parameter names with underscores so they are accepted by
    python as attribute names.  E.g. the *TOS* target has parameters
    *--set-tos*, *--and-tos*, *--or-tos* and *--xor-tos*; they become
    *target.set_tos*, *target.and_tos*, *target.or_tos* and *target.xor_tos*,
    respectively.  The value of a parameter is always a string, if a parameter
    does not take any value in the iptables extension, an empty string *""*
    should be used.

    """
    def __init__(self, rule, name=None, match=None, revision=None):
        """
        *rule* is the Rule object this match belongs to; it can be changed
        later via *set_rule()*.  *name* is the name of the iptables match
        extension (in lower case), *match* is the raw buffer of the match
        structure if the caller has it.  Either *name* or *match* must be
        provided.  *revision* is the revision number of the extension that
        should be used; different revisions use different structures in C and
        they usually only work with certain kernel versions. Python-iptables
        by default will use the latest revision available.
        """
        if not name and not match:
            raise ValueError("can't create match based on nothing")
        if not name:
            name = match.u.user.name.decode()
        self._name = name
        self._rule = rule
        self._orig_parse = None
        self._orig_options = None

        self._xt = xtables(rule.nfproto)

        module = self._xt.find_match(name)
        real_name = module and getattr(module[0], 'real_name', None) or None
        if real_name:
            # Alias name, look up real module.
            self._name = real_name.decode()
            self._orig_parse = getattr(module[0], 'x6_parse', None)
            self._orig_options = getattr(module[0], 'x6_options', None)
            module = self._xt.find_match(real_name)
        if not module:
            raise XTablesError("can't find match %s" % (name))

        self._module = module[0]
        self._module.mflags = 0
        if revision is not None:
            self._revision = revision
        else:
            self._revision = self._module.revision

        self._match_buf = (ct.c_ubyte * self.size)()
        if match:
            ct.memmove(ct.byref(self._match_buf), ct.byref(match), self.size)
            self._update_pointers()
            self._check_alias()
        else:
            self.reset()

    def _check_alias(self):
        name = self._get_alias_name()
        if name is None:
            return
        alias_module = self._xt.find_match(name)
        if alias_module is None:
            return
        self._alias_module = alias_module[0]
        self._orig_parse = getattr(self._alias_module, 'x6_parse', None)
        self._orig_options = getattr(self._alias_module, 'x6_options', None)

    def __eq__(self, match):
        basesz = ct.sizeof(xt_entry_match)
        if (self.name == match.name and
            self.match_buf[basesz:self.usersize] ==
                match.match_buf[basesz:match.usersize]):
            return True
        return False

    def __hash__(self):
        return (hash(self.match.u.match_size) ^
                hash(self.match.u.user.name) ^
                hash(self.match.u.user.revision) ^
                hash(bytes(self.match_buf)))

    def __ne__(self, match):
        return not self.__eq__(match)

    def _final_check(self):
        self._xt.final_check_match(self._module)

    def _parse(self, argv, inv, entry):
        self._xt.parse_match(argv, inv, self._module, entry,
                             ct.cast(self._ptrptr, ct.POINTER(ct.c_void_p)),
                             self._orig_parse, self._orig_options)

    def _get_size(self):
        return xt_align(self._module.size + ct.sizeof(xt_entry_match))
    size = property(_get_size)
    """This is the full size of the underlying C structure."""

    def _get_user_size(self):
        return self._module.userspacesize + ct.sizeof(xt_entry_match)
    usersize = property(_get_user_size)
    """This is the size of the part of the underlying C structure that is used
    in userspace."""

    def _update_pointers(self):
        self._ptr = ct.cast(ct.byref(self._match_buf),
                            ct.POINTER(xt_entry_match))
        self._ptrptr = ct.cast(ct.pointer(self._ptr),
                               ct.POINTER(ct.POINTER(xt_entry_match)))
        self._module.m = self._ptr
        self._update_name()

    def _update_name(self):
        m = self._ptr[0]
        m.u.user.name = self.name.encode()

    def reset(self):
        """Reset the match.

        Parameters are set to their default values, any flags are cleared."""
        ct.memset(ct.byref(self._match_buf), 0, self.size)
        self._update_pointers()
        m = self._ptr[0]
        m.u.match_size = self.size
        m.u.user.revision = self._revision
        if self._module.init:
            self._module.init(self._ptr)
        self._module.mflags = 0
        udata_size = getattr(self._module, 'udata_size', 0)
        if udata_size > 0:
            udata_buf = (ct.c_ubyte * udata_size)()
            self._module.udata = ct.cast(ct.byref(udata_buf), ct.c_void_p)

    def _get_match(self):
        return ct.cast(ct.byref(self.match_buf), ct.POINTER(xt_entry_match))[0]
    match = property(_get_match)
    """This is the C structure used by the extension."""

    def _get_match_buf(self):
        return self._match_buf
    match_buf = property(_get_match_buf)
    """This is the buffer holding the C structure used by the extension."""


class Target(IPTCModule):
    """Targets specify what to do with a packet when a match is found while
    traversing the list of rule entries in a chain.

    Target and match extensions in iptables have parameters.  These parameters
    are implemented as instance attributes in python.  However, to make the
    names of parameters legal attribute names they have to be converted.  The
    rule is to cut the leading double dash from the name, and replace
    dashes in parameter names with underscores so they are accepted by
    python as attribute names.  E.g. the *TOS* target has parameters
    *--set-tos*, *--and-tos*, *--or-tos* and *--xor-tos*; they become
    *target.set_tos*, *target.and_tos*, *target.or_tos* and *target.xor_tos*,
    respectively.  The value of a parameter is always a string, if a parameter
    does not take any value in the iptables extension, an empty string i.e. ""
    should be used.
    """

    STANDARD_TARGETS = ["", "ACCEPT", "DROP", "REJECT", "RETURN", "REDIRECT", "SNAT", "DNAT", \
        "MASQUERADE", "MIRROR", "TOS", "MARK", "QUEUE", "LOG"]
    """This is the constant for all standard targets."""

    def __init__(self, rule, name=None, target=None, revision=None, goto=None):
        """
        *rule* is the Rule object this match belongs to; it can be changed
        later via *set_rule()*.  *name* is the name of the iptables target
        extension (in upper case), *target* is the raw buffer of the target
        structure if the caller has it.  Either *name* or *target* must be
        provided.  *revision* is the revision number of the extension that
        should be used; different revisions use different structures in C and
        they usually only work with certain kernel versions. Python-iptables
        by default will use the latest revision available.
        If goto is True, then it converts '-j' to '-g'.
        """
        if name is None and target is None:
            raise ValueError("can't create target based on nothing")
        if name is None:
            name = target.u.user.name.decode()
        self._name = name
        self._rule = rule
        self._orig_parse = None
        self._orig_options = None

        # NOTE:
        # get_ip() returns the 'ip' structure that contains (1)the 'flags' field, and
        # (2)the value for the GOTO flag.
        # We *must* use get_ip() because the actual name of the field containing the
        # structure apparently differs between implementation
        ipstruct = rule.get_ip()
        f_goto_attrs = [a for a in dir(ipstruct) if a.endswith('_F_GOTO')]
        if len(f_goto_attrs) == 0:
            raise RuntimeError('What kind of struct is this? It does not have "*_F_GOTO" constant!')
        _F_GOTO = getattr(ipstruct, f_goto_attrs[0])

        if target is not None or goto is None:
            # We are 'decoding' existing Target
            self._goto = bool(ipstruct.flags & _F_GOTO)
        if goto is not None:
            assert isinstance(goto, bool)
            self._goto = goto
            if goto:
                ipstruct.flags |= _F_GOTO
            else:
                ipstruct.flags &= ~_F_GOTO

        self._xt = xtables(rule.nfproto)

        module = (self._is_standard_target() and
                  self._xt.find_target('') or
                  self._xt.find_target(name))
        real_name = module and getattr(module[0], 'real_name', None) or None
        if real_name:
            # Alias name, look up real module.
            self._name = real_name.decode()
            self._orig_parse = getattr(module[0], 'x6_parse', None)
            self._orig_options = getattr(module[0], 'x6_options', None)
            module = self._xt.find_target(real_name)
        if not module:
            raise XTablesError("can't find target %s" % (name))

        self._module = module[0]
        self._module.tflags = 0
        if revision is not None:
            self._revision = revision
        else:
            self._revision = self._module.revision

        self._create_buffer(target)

        if self._is_standard_target():
            self.standard_target = name
        elif target:
            self._check_alias()

    def _check_alias(self):
        name = self._get_alias_name()
        if name is None:
            return
        alias_module = self._xt.find_target(name)
        if alias_module is None:
            return
        self._alias_module = alias_module[0]
        self._orig_parse = getattr(self._alias_module, 'x6_parse', None)
        self._orig_options = getattr(self._alias_module, 'x6_options', None)

    def __eq__(self, targ):
        basesz = ct.sizeof(xt_entry_target)
        if (self.target.u.target_size != targ.target.u.target_size or
                self.target.u.user.name != targ.target.u.user.name or
                self.target.u.user.revision != targ.target.u.user.revision):
            return False
        if (self.target.u.user.name == b"" or
                self.target.u.user.name == b"standard" or
                self.target.u.user.name == b"ACCEPT" or
                self.target.u.user.name == b"DROP" or
                self.target.u.user.name == b"RETURN" or
                self.target.u.user.name == b"ERROR" or
                self._is_standard_target()):
            return True
        if (self._target_buf[basesz:self.usersize] ==
                targ._target_buf[basesz:targ.usersize]):
            return True
        return False

    def __ne__(self, target):
        return not self.__eq__(target)

    def _create_buffer(self, target):
        self._buffer = _Buffer(self.size)
        self._target_buf = self._buffer.buffer
        if target:
            ct.memmove(self._target_buf, ct.byref(target), self.size)
            self._update_pointers()
        else:
            self.reset()

    def _is_standard_target(self):
        if self._name in Target.STANDARD_TARGETS:
            return False
        for t in self._rule.tables:
            if t.is_chain(self._name):
                return True
        return False

    def _final_check(self):
        self._xt.final_check_target(self._module)

    def _parse(self, argv, inv, entry):
        self._xt.parse_target(argv, inv, self._module, entry,
                              ct.cast(self._ptrptr, ct.POINTER(ct.c_void_p)),
                              self._orig_parse, self._orig_options)
        self._target_buf = ct.cast(self._module.t, ct.POINTER(ct.c_ubyte))
        if self._buffer.buffer != self._target_buf:
            self._buffer.buffer = self._target_buf
        self._update_pointers()

    def _get_size(self):
        return xt_align(self._module.size + ct.sizeof(xt_entry_target))
    size = property(_get_size)
    """This is the full size of the underlying C structure."""

    def _get_user_size(self):
        return self._module.userspacesize + ct.sizeof(xt_entry_target)
    usersize = property(_get_user_size)
    """This is the size of the part of the underlying C structure that is used
    in userspace."""

    def _get_standard_target(self):
        t = self._ptr[0]
        return t.u.user.name.decode()

    def _set_standard_target(self, name):
        t = self._ptr[0]
        if isinstance(name, str):
            name = name.encode()
        t.u.user.name = name
        if isinstance(name, bytes):
            name = name.decode()
        self._name = name
    standard_target = property(_get_standard_target, _set_standard_target)
    """This attribute is used for standard targets.  It can be set to
    *ACCEPT*, *DROP*, *RETURN* or to a name of a chain the rule should jump
    into."""

    def _update_pointers(self):
        self._ptr = ct.cast(self._target_buf, ct.POINTER(xt_entry_target))
        self._ptrptr = ct.cast(ct.pointer(self._ptr),
                               ct.POINTER(ct.POINTER(xt_entry_target)))
        self._module.t = self._ptr
        self._update_name()

    def _update_name(self):
        m = self._ptr[0]
        m.u.user.name = self.name.encode()

    def reset(self):
        """Reset the target.  Parameters are set to their default values, any
        flags are cleared."""
        ct.memset(self._target_buf, 0, self.size)
        self._update_pointers()
        t = self._ptr[0]
        t.u.target_size = self.size
        t.u.user.revision = self._revision
        if self._module.init:
            self._module.init(self._ptr)
        self._module.tflags = 0
        udata_size = getattr(self._module, 'udata_size', 0)
        if udata_size > 0:
            udata_buf = (ct.c_ubyte * udata_size)()
            self._module.udata = ct.cast(ct.byref(udata_buf), ct.c_void_p)

    def _get_target(self):
        return self._ptr[0]
    target = property(_get_target)
    """This is the C structure used by the extension."""

    def _get_goto(self):
        return self._goto
    goto = property(_get_goto)


class Policy(object):
    """
    If the end of a built-in chain is reached or a rule in a built-in chain
    with target RETURN is matched, the target specified by the chain policy
    determines the fate of the packet.
    """

    ACCEPT = "ACCEPT"
    """If no matching rule has been found so far then accept the packet."""
    DROP = "DROP"
    """If no matching rule has been found so far then drop the packet."""
    QUEUE = "QUEUE"
    """If no matching rule has been found so far then queue the packet to
    userspace."""
    RETURN = "RETURN"
    """Return to calling chain."""

    _cache = weakref.WeakValueDictionary()

    def __new__(cls, name):
        obj = Policy._cache.get(name, None)
        if not obj:
            obj = object.__new__(cls)
            Policy._cache[name] = obj
        return obj

    def __init__(self, name):
        self.name = name


def _a_to_i(addr):
    return struct.unpack("I", addr)[0]


def _i_to_a(ip):
    return struct.pack("I", int(ip.s_addr))


class Rule(object):
    """Rules are entries in chains.

    Each rule has three parts:
        * An entry with protocol family attributes like source and destination
          address, transport protocol, etc.  If the packet does not match the
          attributes set here, then processing continues with the next rule or
          the chain policy is applied at the end of the chain.
        * Any number of matches.  They are optional, and make it possible to
          match for further packet attributes.
        * One target.  This determines what happens with the packet if it is
          matched.
    """

    protocols = {0: "all",
                 socket.IPPROTO_AH: "ah",
                 socket.IPPROTO_DSTOPTS: "dstopts",
                 socket.IPPROTO_EGP: "egp",
                 socket.IPPROTO_ESP: "esp",
                 socket.IPPROTO_FRAGMENT: "fragment",
                 socket.IPPROTO_GRE: "gre",
                 socket.IPPROTO_HOPOPTS: "hopopts",
                 socket.IPPROTO_ICMP: "icmp",
                 socket.IPPROTO_ICMPV6: "icmpv6",
                 socket.IPPROTO_IDP: "idp",
                 socket.IPPROTO_IGMP: "igmp",
                 socket.IPPROTO_IP: "ip",
                 socket.IPPROTO_IPIP: "ipip",
                 socket.IPPROTO_IPV6: "ipv6",
                 socket.IPPROTO_NONE: "none",
                 socket.IPPROTO_PIM: "pim",
                 socket.IPPROTO_PUP: "pup",
                 socket.IPPROTO_RAW: "raw",
                 socket.IPPROTO_ROUTING: "routing",
                 socket.IPPROTO_RSVP: "rsvp",
                 socket.IPPROTO_SCTP: "sctp",
                 socket.IPPROTO_TCP: "tcp",
                 socket.IPPROTO_TP: "tp",
                 socket.IPPROTO_UDP: "udp",
                 }

    def __init__(self, entry=None, chain=None):
        """
        *entry* is the ipt_entry buffer or None if the caller does not have
        it.  *chain* is the chain object this rule belongs to.
        """
        self.nfproto = NFPROTO_IPV4
        self._matches = []
        self._target = None
        self.chain = chain
        self.rule = entry

    def __eq__(self, rule):
        if self._target != rule._target:
            return False
        if len(self._matches) != len(rule._matches):
            return False
        if set(rule._matches) != set([x for x in rule._matches if x in
                                      self._matches]):
            return False
        if (self.src == rule.src and self.dst == rule.dst and
                self.protocol == rule.protocol and
                self.fragment == rule.fragment and
                self.in_interface == rule.in_interface and
                self.out_interface == rule.out_interface):
            return True
        return False

    def __ne__(self, rule):
        return not self.__eq__(rule)

    def _get_tables(self):
        return [Table(t) for t in Table.ALL if is_table_available(t)]
    tables = property(_get_tables)
    """This is the list of tables for our protocol."""

    def final_check(self):
        """Do a final check on the target and the matches."""
        if self.target:
            self.target.final_check()
        for match in self.matches:
            match.final_check()

    def create_match(self, name, revision=None):
        """Create a *match*, and add it to the list of matches in this rule.
        *name* is the name of the match extension, *revision* is the revision
        to use."""
        match = Match(self, name=name, revision=revision)
        self.add_match(match)
        return match

    def create_target(self, name, revision=None, goto=False):
        """Create a new *target*, and set it as this rule's target. *name* is
        the name of the target extension, *revision* is the revision to
        use. *goto* determines if target uses '-j' (default) or '-g'."""
        target = Target(self, name=name, revision=revision, goto=goto)
        self.target = target
        return target

    def add_match(self, match):
        """Adds a match to the rule.  One can add any number of matches."""
        match.rule = self
        self._matches.append(match)

    def remove_match(self, match):
        """Removes *match* from the list of matches."""
        self._matches.remove(match)

    def get_ip(self):
        return self.entry.ip

    def _get_matches(self):
        return self._matches[:]  # return a copy
    matches = property(_get_matches)
    """This is the list of matches held in this rule."""

    def _get_target(self):
        return self._target

    def _set_target(self, target):
        target.rule = self
        self._target = target
    target = property(_get_target, _set_target)
    """This is the target of the rule."""

    def get_src(self):
        src = ""
        if self.entry.ip.invflags & ipt_ip.IPT_INV_SRCIP:
            src = "".join([src, "!"])
        paddr = _i_to_a(self.entry.ip.src)
        try:
            addr = socket.inet_ntop(socket.AF_INET, paddr)
        except socket.error:
            raise IPTCError("error in internal state: invalid address")
        src = "".join([src, addr, "/"])
        paddr = _i_to_a(self.entry.ip.smsk)
        try:
            netmask = socket.inet_ntop(socket.AF_INET, paddr)
        except socket.error:
            raise IPTCError("error in internal state: invalid netmask")
        src = "".join([src, netmask])
        return src

    def set_src(self, src):
        if src[0] == "!":
            self.entry.ip.invflags |= ipt_ip.IPT_INV_SRCIP
            src = src[1:]
        else:
            self.entry.ip.invflags &= (~ipt_ip.IPT_INV_SRCIP &
                                       ipt_ip.IPT_INV_MASK)

        slash = src.find("/")
        if slash == -1:
            addr = src
            netm = "255.255.255.255"
        else:
            addr = src[:slash]
            netm = src[slash + 1:]

        try:
            saddr = _a_to_i(socket.inet_pton(socket.AF_INET, addr))
        except socket.error:
            raise ValueError("invalid address %s" % (addr))

        if not netm.isdigit():
            try:
                nmask = _a_to_i(socket.inet_pton(socket.AF_INET, netm))
            except socket.error:
                raise ValueError("invalid netmask %s" % (netm))
        else:
            imask = int(netm)
            if imask > 32 or imask < 0:
                raise ValueError("invalid netmask %s" % (netm))
            nmask = socket.htonl((2 ** imask - 1) << (32 - imask))
        neta = in_addr()
        neta.s_addr = ct.c_uint32(nmask)
        self.entry.ip.smsk = neta
        # Apply subnet mask to IP address
        ina = in_addr()
        ina.s_addr = ct.c_uint32(saddr & nmask)
        self.entry.ip.src = ina

    src = property(get_src, set_src)
    """This is the source network address with an optional network mask in
    string form."""

    def get_dst(self):
        dst = ""
        if self.entry.ip.invflags & ipt_ip.IPT_INV_DSTIP:
            dst = "".join([dst, "!"])
        paddr = _i_to_a(self.entry.ip.dst)
        try:
            addr = socket.inet_ntop(socket.AF_INET, paddr)
        except socket.error:
            raise IPTCError("error in internal state: invalid address")
        dst = "".join([dst, addr, "/"])
        paddr = _i_to_a(self.entry.ip.dmsk)
        try:
            netmask = socket.inet_ntop(socket.AF_INET, paddr)
        except socket.error:
            raise IPTCError("error in internal state: invalid netmask")
        dst = "".join([dst, netmask])
        return dst

    def set_dst(self, dst):
        if dst[0] == "!":
            self.entry.ip.invflags |= ipt_ip.IPT_INV_DSTIP
            dst = dst[1:]
        else:
            self.entry.ip.invflags &= (~ipt_ip.IPT_INV_DSTIP &
                                       ipt_ip.IPT_INV_MASK)

        slash = dst.find("/")
        if slash == -1:
            addr = dst
            netm = "255.255.255.255"
        else:
            addr = dst[:slash]
            netm = dst[slash + 1:]

        try:
            daddr = _a_to_i(socket.inet_pton(socket.AF_INET, addr))
        except socket.error:
            raise ValueError("invalid address %s" % (addr))

        if not netm.isdigit():
            try:
                nmask = _a_to_i(socket.inet_pton(socket.AF_INET, netm))
            except socket.error:
                raise ValueError("invalid netmask %s" % (netm))
        else:
            imask = int(netm)
            if imask > 32 or imask < 0:
                raise ValueError("invalid netmask %s" % (netm))
            nmask = socket.htonl((2 ** imask - 1) << (32 - imask))
        neta = in_addr()
        neta.s_addr = ct.c_uint32(nmask)
        self.entry.ip.dmsk = neta
        # Apply subnet mask to IP address
        ina = in_addr()
        ina.s_addr = ct.c_uint32(daddr & nmask)
        self.entry.ip.dst = ina

    dst = property(get_dst, set_dst)
    """This is the destination network address with an optional network mask
    in string form."""

    def get_in_interface(self):
        intf = ""
        if self.entry.ip.invflags & ipt_ip.IPT_INV_VIA_IN:
            intf = "!"

        iface = self.entry.ip.iniface.decode()
        mask = self.entry.ip.iniface_mask

        if len(mask) == 0:
            return None

        intf += iface
        if len(iface) == len(mask):
            intf += '+'
        intf = intf[:_IFNAMSIZ]

        return intf

    def set_in_interface(self, intf):
        if intf[0] == "!":
            self.entry.ip.invflags |= ipt_ip.IPT_INV_VIA_IN
            intf = intf[1:]
        else:
            self.entry.ip.invflags &= (~ipt_ip.IPT_INV_VIA_IN &
                                       ipt_ip.IPT_INV_MASK)
        if len(intf) >= _IFNAMSIZ:
            raise ValueError("interface name %s too long" % (intf))
        masklen = len(intf) + 1
        if intf[len(intf) - 1] == "+":
            intf = intf[:-1]
            masklen -= 2

        self.entry.ip.iniface = b"".join([intf.encode(),
                                          b'\x00' * (_IFNAMSIZ - len(intf))])
        self.entry.ip.iniface_mask = b"".join([b'\xff' * masklen,
                                               b'\x00' * (_IFNAMSIZ -
                                                          masklen)])

    in_interface = property(get_in_interface, set_in_interface)
    """This is the input network interface e.g. *eth0*.  A wildcard match can
    be achieved via *+* e.g. *ppp+* matches any *ppp* interface."""

    def get_out_interface(self):
        intf = ""
        if self.entry.ip.invflags & ipt_ip.IPT_INV_VIA_OUT:
            intf = "!"

        iface = self.entry.ip.outiface.decode()
        mask = self.entry.ip.outiface_mask

        if len(mask) == 0:
            return None

        intf += iface
        if len(iface) == len(mask):
            intf += '+'
        intf = intf[:_IFNAMSIZ]

        return intf

    def set_out_interface(self, intf):
        if intf[0] == "!":
            self.entry.ip.invflags |= ipt_ip.IPT_INV_VIA_OUT
            intf = intf[1:]
        else:
            self.entry.ip.invflags &= (~ipt_ip.IPT_INV_VIA_OUT &
                                       ipt_ip.IPT_INV_MASK)
        if len(intf) >= _IFNAMSIZ:
            raise ValueError("interface name %s too long" % (intf))
        masklen = len(intf) + 1
        if intf[len(intf) - 1] == "+":
            intf = intf[:-1]
            masklen -= 2

        self.entry.ip.outiface = b"".join([intf.encode(),
                                           b'\x00' * (_IFNAMSIZ - len(intf))])
        self.entry.ip.outiface_mask = b"".join([b'\xff' * masklen,
                                                b'\x00' * (_IFNAMSIZ -
                                                           masklen)])

    out_interface = property(get_out_interface, set_out_interface)
    """This is the output network interface e.g. *eth0*.  A wildcard match can
    be achieved via *+* e.g. *ppp+* matches any *ppp* interface."""

    def get_fragment(self):
        frag = bool(self.entry.ip.flags & ipt_ip.IPT_F_FRAG)
        if self.entry.ip.invflags & ipt_ip.IPT_INV_FRAG:
            frag = not frag
        return frag

    def set_fragment(self, frag):
        self.entry.ip.invflags &= ~ipt_ip.IPT_INV_FRAG & ipt_ip.IPT_INV_MASK
        if frag:
            self.entry.ip.flags |= ipt_ip.IPT_F_FRAG
        else:
            self.entry.ip.flags &= ~ipt_ip.IPT_F_FRAG

    fragment = property(get_fragment, set_fragment)
    """This means that the rule refers to the second and further fragments of
    fragmented packets.  It can be *True* or *False*."""

    def get_protocol(self):
        if self.entry.ip.invflags & ipt_ip.IPT_INV_PROTO:
            proto = "!"
        else:
            proto = ""
        proto = "".join([proto, self.protocols.get(self.entry.ip.proto, str(self.entry.ip.proto))])
        return proto

    def set_protocol(self, proto):
        proto = str(proto)
        if proto[0] == "!":
            self.entry.ip.invflags |= ipt_ip.IPT_INV_PROTO
            proto = proto[1:]
        else:
            self.entry.ip.invflags &= (~ipt_ip.IPT_INV_PROTO &
                                       ipt_ip.IPT_INV_MASK)
        if proto.isdigit():
            self.entry.ip.proto = int(proto)
            return
        for p in self.protocols.items():
            if proto.lower() == p[1]:
                self.entry.ip.proto = p[0]
                return
        raise ValueError("invalid protocol %s" % (proto))

    protocol = property(get_protocol, set_protocol)
    """This is the transport layer protocol."""

    def get_counters(self):
        """This method returns a tuple pair of the packet and byte counters of
        the rule."""
        counters = self.entry.counters
        return counters.pcnt, counters.bcnt

    def set_counters(self, counters):
        """This method set a tuple pair of the packet and byte counters of
        the rule."""
        self.entry.counters.pcnt = counters[0]
        self.entry.counters.bcnt = counters[1]

    counters = property(get_counters, set_counters)
    """This is the packet and byte counters of the rule."""

    # override the following three for the IPv6 subclass
    def _entry_size(self):
        return xt_align(ct.sizeof(ipt_entry))

    def _entry_type(self):
        return ipt_entry

    def _new_entry(self):
        return ipt_entry()

    def _get_rule(self):
        if not self.entry or not self._target or not self._target.target:
            return None

        entrysz = self._entry_size()
        matchsz = 0
        for m in self._matches:
            matchsz += xt_align(m.size)
        targetsz = xt_align(self._target.size)

        self.entry.target_offset = entrysz + matchsz
        self.entry.next_offset = entrysz + matchsz + targetsz

        # allocate array of full length (entry + matches + target)
        buf = (ct.c_ubyte * (entrysz + matchsz + targetsz))()

        # copy entry to buf
        ptr = ct.cast(ct.pointer(self.entry), ct.POINTER(ct.c_ubyte))
        buf[:entrysz] = ptr[:entrysz]

        # copy matches to buf at offset of entrysz + match size
        offset = 0
        for m in self._matches:
            sz = xt_align(m.size)
            buf[entrysz + offset:entrysz + offset + sz] = m.match_buf[:sz]
            offset += sz

        # copy target to buf at offset of entrysz + matchsz
        ptr = ct.cast(ct.pointer(self._target.target), ct.POINTER(ct.c_ubyte))
        buf[entrysz + matchsz:entrysz + matchsz + targetsz] = ptr[:targetsz]

        return buf

    def _set_rule(self, entry):
        if not entry:
            self.entry = self._new_entry()
            return
        else:
            self.entry = ct.cast(ct.pointer(entry),
                                 ct.POINTER(self._entry_type()))[0]

        if not isinstance(entry, self._entry_type()):
            raise TypeError("Invalid rule type %s; expected %s" %
                            (entry, self._entry_type()))

        entrysz = self._entry_size()
        matchsz = entry.target_offset - entrysz
        # targetsz = entry.next_offset - entry.target_offset

        # iterate over matches to create blob
        if matchsz:
            off = 0
            while entrysz + off < entry.target_offset:
                match = ct.cast(ct.byref(entry.elems, off),
                                ct.POINTER(xt_entry_match))[0]
                m = Match(self, match=match)
                self.add_match(m)
                off += m.size

        target = ct.cast(ct.byref(entry, entry.target_offset),
                         ct.POINTER(xt_entry_target))[0]
        self.target = Target(self, target=target)
        jump = self.chain.table.get_target(entry)  # standard target is special
        if jump:
            self._target.standard_target = jump

    rule = property(_get_rule, _set_rule)
    """This is the raw rule buffer as iptables expects and returns it."""

    def _get_mask(self):
        if not self.entry:
            return None

        entrysz = self._entry_size()
        matchsz = self.entry.target_offset - entrysz
        targetsz = self.entry.next_offset - self.entry.target_offset

        # allocate array for mask
        mask = (ct.c_ubyte * (entrysz + matchsz + targetsz))()

        # fill it out
        pos = 0
        for i in range(pos, pos + entrysz):
            mask[i] = 0xff
        pos += entrysz
        for m in self._matches:
            for i in range(pos, pos + m.usersize):
                mask[i] = 0xff
            pos += m.size
        for i in range(pos, pos + self._target.usersize):
            mask[i] = 0xff

        return mask

    mask = property(_get_mask)
    """This is the raw mask buffer as iptables uses it when removing rules."""


class Chain(object):
    """Rules are contained by chains.

    *iptables* has built-in chains for every table, and users can also create
    additional chains.  Rule targets can specify to jump into another chain
    and continue processing its rules, or return to the caller chain.
    """
    _cache = weakref.WeakValueDictionary()

    def __new__(cls, table, name):
        table_name = type(table).__name__ + "." + table.name
        obj = Chain._cache.get(table_name + "." + name, None)
        if not obj:
            obj = object.__new__(cls)
            Chain._cache[table_name + "." + name] = obj
        return obj

    def __init__(self, table, name):
        """*table* is the table this chain belongs to, *name* is the chain's
        name.

        If a chain already exists with *name* in *table* it is returned.
        """
        self.name = name
        self.table = table

    def delete(self):
        """Delete chain from its table."""
        self.table.delete_chain(self.name)

    def rename(self, new_name):
        """Rename chain to *new_name*."""
        self.table.rename_chain(self.name, new_name)

    def flush(self):
        """Flush all rules from the chain."""
        self.table.flush_entries(self.name)

    def get_counters(self):
        """This method returns a tuple pair of the packet and byte counters of
        the chain."""
        policy, counters = self.table.get_policy(self.name)
        return counters

    def zero_counters(self):
        """This method zeroes the packet and byte counters of the chain."""
        self.table.zero_entries(self.name)

    def set_policy(self, policy, counters=None):
        """Set the chain policy to *policy*, which should either be a string
        or a Policy object.  If *counters* is not *None*, the chain counters
        are also adjusted. *Counters* is a list or tuple with two elements."""
        if isinstance(policy, Policy):
            policy = policy.name
        self.table.set_policy(self.name, policy, counters)

    def get_policy(self):
        """Returns the policy of the chain as a Policy object."""
        policy, counters = self.table.get_policy(self.name)
        return policy

    def is_builtin(self):
        """Returns whether the chain is a built-in one."""
        return self.table.builtin_chain(self.name)

    def append_rule(self, rule):
        """Append *rule* to the end of the chain."""
        rule.final_check()
        rbuf = rule.rule
        if not rbuf:
            raise ValueError("invalid rule")
        self.table.append_entry(self.name, rbuf)

    def insert_rule(self, rule, position=0):
        """Insert *rule* as the first entry in the chain if *position* is 0 or
        not specified, else *rule* is inserted in the given position."""
        rule.final_check()
        rbuf = rule.rule
        if not rbuf:
            raise ValueError("invalid rule")
        self.table.insert_entry(self.name, rbuf, position)

    def replace_rule(self, rule, position=0):
        """Replace existing rule in the chain at *position* with given
        *rule*"""
        rbuf = rule.rule
        if not rbuf:
            raise ValueError("invalid rule")
        self.table.replace_entry(self.name, rbuf, position)

    def delete_rule(self, rule):
        """Removes *rule* from the chain."""
        rule.final_check()
        rbuf = rule.rule
        if not rbuf:
            raise ValueError("invalid rule")
        self.table.delete_entry(self.name, rbuf, rule.mask)

    def get_target(self, rule):
        """This method returns the target of *rule* if it is a standard
        target, or *None* if it is not."""
        rbuf = rule.rule
        if not rbuf:
            raise ValueError("invalid rule")
        return self.table.get_target(rbuf)

    def _get_rules(self):
        entries = []
        entry = self.table.first_rule(self.name)
        while entry:
            entries.append(entry)
            entry = self.table.next_rule(entry)
        return [self.table.create_rule(e, self) for e in entries]

    rules = property(_get_rules)
    """This is the list of rules currently in the chain.

    The indexes of the Rule items produced from this list *should* correspond
    to the IPTables --line-numbers value minus one.  Keeping in mind that
    iptables rules are 1-indexed whereas the Python list is 0-indexed
    """


def autocommit(fn):
    def new(*args):
        obj = args[0]
        ret = fn(*args)
        if obj.autocommit:
            obj.refresh()
        return ret
    return new


class Table(object):
    """A table is the most basic building block in iptables.

    There are four fixed tables:
        * **Table.FILTER**, the filter table,
        * **Table.NAT**, the NAT table,
        * **Table.MANGLE**, the mangle table and
        * **Table.RAW**, the raw table.

    The four tables are cached, so if you create a new Table, and it has been
    instantiated before, then it will be reused. To get access to e.g. the
    filter table:

    >>> table = iptc.Table(iptc.Table.FILTER)

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
        obj = Table._cache.get(name, None)
        if not obj:
            obj = object.__new__(cls)
            if autocommit is None:
                autocommit = True
            obj._init(name, autocommit)
            Table._cache[name] = obj
        elif autocommit is not None:
            obj.autocommit = autocommit
        return obj

    def _init(self, name, autocommit):
        """
        *name* is the name of the table, if it already exists it is returned.
        *autocommit* specifies that any iptables operation that changes a
        rule, chain or table should be committed immediately.
        """
        self.name = name
        self.autocommit = autocommit
        self._iptc = iptc()  # to keep references to functions
        self._handle = None
        self.refresh()

    def __del__(self):
        self.close()

    def close(self):
        """Close the underlying connection handle to iptables."""
        if self._handle:
            self._free()

    def commit(self):
        """Commit any pending operation."""
        rv = self._iptc.iptc_commit(self._handle)
        if rv != 1:
            raise IPTCError("can't commit: %s" % (self.strerror()))

    def _free(self, ignore_exc=True):
        if self._handle is None:
            raise IPTCError("table is not initialized")
        try:
            if self.autocommit:
                self.commit()
        except IPTCError as e:
            if not ignore_exc:
                raise e
        finally:
            self._iptc.iptc_free(self._handle)
            self._handle = None

    def refresh(self):
        """Commit any pending operation and refresh the status of iptables."""
        if self._handle:
            self._free()

        handle = self._iptc.iptc_init(self.name.encode())
        if not handle:
            raise IPTCError("can't initialize %s: %s" % (self.name,
                                                         self.strerror()))
        self._handle = handle

    def is_chain(self, chain):
        """Returns *True* if *chain* exists as a chain."""
        if isinstance(chain, Chain):
            chain = chain.name
        if self._iptc.iptc_is_chain(chain.encode(), self._handle):
            return True
        else:
            return False

    def builtin_chain(self, chain):
        """Returns *True* if *chain* is a built-in chain."""
        if isinstance(chain, Chain):
            chain = chain.name
        if self._iptc.iptc_builtin(chain.encode(), self._handle):
            return True
        else:
            return False

    def strerror(self):
        """Returns any pending iptables error from the previous operation."""
        errno = _get_errno_loc()[0]
        if errno == 0:
            return "libiptc version error"
        return self._iptc.iptc_strerror(errno)

    @autocommit
    def create_chain(self, chain):
        """Create a new chain *chain*."""
        if isinstance(chain, Chain):
            chain = chain.name
        rv = self._iptc.iptc_create_chain(chain.encode(), self._handle)
        if rv != 1:
            raise IPTCError("can't create chain %s: %s" % (chain,
                                                           self.strerror()))
        return Chain(self, chain)

    @autocommit
    def delete_chain(self, chain):
        """Delete chain *chain* from the table."""
        if isinstance(chain, Chain):
            chain = chain.name
        rv = self._iptc.iptc_delete_chain(chain.encode(), self._handle)
        if rv != 1:
            raise IPTCError("can't delete chain %s: %s" % (chain,
                                                           self.strerror()))

    @autocommit
    def rename_chain(self, chain, new_name):
        """Rename chain *chain* to *new_name*."""
        if isinstance(chain, Chain):
            chain = chain.name
        rv = self._iptc.iptc_rename_chain(chain.encode(), new_name.encode(),
                                          self._handle)
        if rv != 1:
            raise IPTCError("can't rename chain %s: %s" % (chain,
                                                           self.strerror()))

    @autocommit
    def flush_entries(self, chain):
        """Flush all rules from *chain*."""
        if isinstance(chain, Chain):
            chain = chain.name
        rv = self._iptc.iptc_flush_entries(chain.encode(), self._handle)
        if rv != 1:
            raise IPTCError("can't flush chain %s: %s" % (chain,
                                                          self.strerror()))

    @autocommit
    def zero_entries(self, chain):
        """Zero the packet and byte counters of *chain*."""
        if isinstance(chain, Chain):
            chain = chain.name
        rv = self._iptc.iptc_zero_entries(chain.encode(), self._handle)
        if rv != 1:
            raise IPTCError("can't zero chain %s counters: %s" %
                            (chain, self.strerror()))

    @autocommit
    def set_policy(self, chain, policy, counters=None):
        """Set the policy of *chain* to *policy*, and also update chain
        counters if *counters* is specified."""
        if isinstance(chain, Chain):
            chain = chain.name
        if isinstance(policy, Policy):
            policy = policy.name
        if counters:
            cntrs = xt_counters()
            cntrs.pcnt = counters[0]
            cntrs.bcnt = counters[1]
            cntrs = ct.pointer(cntrs)
        else:
            cntrs = None
        rv = self._iptc.iptc_set_policy(chain.encode(), policy.encode(),
                                        cntrs, self._handle)
        if rv != 1:
            raise IPTCError("can't set policy %s on chain %s: %s" %
                            (policy, chain, self.strerror()))

    @autocommit
    def get_policy(self, chain):
        """Returns the policy of *chain* as a string."""
        if isinstance(chain, Chain):
            chain = chain.name
        if not self.builtin_chain(chain):
            return None, None
        cntrs = xt_counters()
        pol = self._iptc.iptc_get_policy(chain.encode(), ct.pointer(cntrs),
                                         self._handle).decode()
        if not pol:
            raise IPTCError("can't get policy on chain %s: %s" %
                            (chain, self.strerror()))
        return Policy(pol), (cntrs.pcnt, cntrs.bcnt)

    @autocommit
    def append_entry(self, chain, entry):
        """Appends rule *entry* to *chain*."""
        rv = self._iptc.iptc_append_entry(chain.encode(),
                                          ct.cast(entry, ct.c_void_p),
                                          self._handle)
        if rv != 1:
            raise IPTCError("can't append entry to chain %s: %s" %
                            (chain, self.strerror()))

    @autocommit
    def insert_entry(self, chain, entry, position):
        """Inserts rule *entry* into *chain* at position *position*."""
        rv = self._iptc.iptc_insert_entry(chain.encode(),
                                          ct.cast(entry, ct.c_void_p),
                                          position, self._handle)
        if rv != 1:
            raise IPTCError("can't insert entry into chain %s: %s" %
                            (chain, self.strerror()))

    @autocommit
    def replace_entry(self, chain, entry, position):
        """Replace existing rule in *chain* at *position* with given *rule*."""
        rv = self._iptc.iptc_replace_entry(chain.encode(),
                                           ct.cast(entry, ct.c_void_p),
                                           position, self._handle)
        if rv != 1:
            raise IPTCError("can't replace entry in chain %s: %s" %
                            (chain, self.strerror()))

    @autocommit
    def delete_entry(self, chain, entry, mask):
        """Removes rule *entry* with *mask* from *chain*."""
        rv = self._iptc.iptc_delete_entry(chain.encode(),
                                          ct.cast(entry, ct.c_void_p),
                                          mask, self._handle)
        if rv != 1:
            raise IPTCError("can't delete entry from chain %s: %s" %
                            (chain, self.strerror()))

    def first_rule(self, chain):
        """Returns the first rule in *chain* or *None* if it is empty."""
        rule = self._iptc.iptc_first_rule(chain.encode(), self._handle)
        if rule:
            return rule[0]
        else:
            return rule

    def next_rule(self, prev_rule):
        """Returns the next rule after *prev_rule*."""
        rule = self._iptc.iptc_next_rule(ct.pointer(prev_rule), self._handle)
        if rule:
            return rule[0]
        else:
            return rule

    def get_target(self, entry):
        """Returns the standard target in *entry*."""
        t = self._iptc.iptc_get_target(ct.pointer(entry), self._handle)
        # t can be NULL if standard target has a "simple" verdict e.g. ACCEPT
        return t

    def _get_chains(self):
        chains = []
        chain = self._iptc.iptc_first_chain(self._handle)
        while chain:
            chain = chain.decode()
            chains.append(Chain(self, chain))
            chain = self._iptc.iptc_next_chain(self._handle)
        return chains

    chains = property(_get_chains)
    """List of chains in the table."""

    def flush(self):
        """Flush and delete all non-builtin chains the table."""
        for chain in self.chains:
            chain.flush()
        for chain in self.chains:
            if not self.builtin_chain(chain):
                self.delete_chain(chain)

    def create_rule(self, entry=None, chain=None):
        return Rule(entry, chain)

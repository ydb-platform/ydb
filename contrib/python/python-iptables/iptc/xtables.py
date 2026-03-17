# -*- coding: utf-8 -*-

import ctypes as ct
import os
import sys
import weakref

from . import version
from .util import find_library, find_libc
from .errors import *

XT_INV_PROTO = 0x40  # invert the sense of PROTO

NFPROTO_UNSPEC = 0
NFPROTO_IPV4 = 2
NFPROTO_ARP = 3
NFPROTO_BRIDGE = 7
NFPROTO_IPV6 = 10
NFPROTO_DECNET = 12
NFPROTO_NUMPROTO = 6

XTF_DONT_LOAD = 0x00
XTF_DURING_LOAD = 0x01
XTF_TRY_LOAD = 0x02
XTF_LOAD_MUST_SUCCEED = 0x03


XTOPT_INVERT = 1 << 0
XTOPT_MAND = 1 << 1
XTOPT_MULTI = 1 << 2
XTOPT_PUT = 1 << 3
XTOPT_NBO = 1 << 4

_WORDLEN = ct.sizeof(ct.c_long)
_XT_FUNCTION_MAXNAMELEN = 30


def xt_align(sz):
    return ((sz + (_WORDLEN - 1)) & ~(_WORDLEN - 1))


class xt_counters(ct.Structure):
    """This class is a representation of the C struct xt_counters."""
    _fields_ = [("pcnt", ct.c_uint64),  # packet counter
                ("bcnt", ct.c_uint64)]  # byte counter


class xt_entry_target_user(ct.Structure):
    _fields_ = [("target_size", ct.c_uint16),
                ("name", ct.c_char * (_XT_FUNCTION_MAXNAMELEN - 1)),
                ("revision", ct.c_uint8)]


class xt_entry_target_u(ct.Union):
    _fields_ = [("user", xt_entry_target_user),
                ("target_size", ct.c_uint16)]  # full length


class xt_entry_target(ct.Structure):
    """This class is a representation of the C struct xt_entry_target."""
    _fields_ = [("u", xt_entry_target_u),
                ("data", ct.c_ubyte * 0)]


class xt_entry_match_user(ct.Structure):
    _fields_ = [("match_size", ct.c_uint16),
                ("name", ct.c_char * (_XT_FUNCTION_MAXNAMELEN - 1)),
                ("revision", ct.c_uint8)]


class xt_entry_match_u(ct.Union):
    _fields_ = [("user", xt_entry_match_user),
                ("match_size", ct.c_uint16)]  # full length


class xt_entry_match(ct.Structure):
    """This class is a representation of the C struct xt_entry_match."""
    _fields_ = [("u", xt_entry_match_u),
                ("data", ct.c_ubyte * 0)]


class xtables_globals(ct.Structure):
    _fields_ = [("option_offset", ct.c_uint),
                ("program_name", ct.c_char_p),
                ("program_version", ct.c_char_p),
                ("orig_opts", ct.c_void_p),
                ("opts", ct.c_void_p),
                ("exit_err", ct.CFUNCTYPE(None, ct.c_int, ct.c_char_p)),
                ("compat_rev", ct.CFUNCTYPE(ct.c_int, ct.c_char_p, ct.c_uint8,
                                            ct.c_int))]


# struct used by getopt()
class option(ct.Structure):
    _fields_ = [("name", ct.c_char_p),
                ("has_arg", ct.c_int),
                ("flag", ct.POINTER(ct.c_int)),
                ("val", ct.c_int)]


class xt_option_entry(ct.Structure):
    _fields_ = [("name", ct.c_char_p),
                ("type", ct.c_int),
                ("id", ct.c_uint),
                ("excl", ct.c_uint),
                ("also", ct.c_uint),
                ("flags", ct.c_uint),
                ("ptroff", ct.c_uint),
                ("size", ct.c_size_t),
                ("min", ct.c_uint),
                ("max", ct.c_uint)]


class _U1(ct.Union):
    _fields_ = [("match", ct.POINTER(ct.POINTER(xt_entry_match))),
                ("target", ct.POINTER(ct.POINTER(xt_entry_target)))]


class nf_inet_addr(ct.Union):
    _fields_ = [("all", ct.c_uint32 * 4),
                ("ip", ct.c_uint32),
                ("ip6", ct.c_uint32 * 4),
                ("in", ct.c_uint32),
                ("in6", ct.c_uint8 * 16)]


class _S1(ct.Structure):
    _fields_ = [("haddr", nf_inet_addr),
                ("hmask", nf_inet_addr),
                ("hlen", ct.c_uint8)]


class _S2(ct.Structure):
    _fields_ = [("tos_value", ct.c_uint8),
                ("tos_mask", ct.c_uint8)]


class _S3(ct.Structure):
    _fields_ = [("mark", ct.c_uint32),
                ("mask", ct.c_uint32)]


class _U_val(ct.Union):
    _anonymous_ = ("s1", "s2", "s3")
    _fields_ = [("u8", ct.c_uint8),
                ("u8_range", ct.c_uint8 * 2),
                ("syslog_level", ct.c_uint8),
                ("protocol", ct.c_uint8),
                ("u16", ct.c_uint16),
                ("u16_range", ct.c_uint16 * 2),
                ("port", ct.c_uint16),
                ("port_range", ct.c_uint16 * 2),
                ("u32", ct.c_uint32),
                ("u32_range", ct.c_uint32 * 2),
                ("u64", ct.c_uint64),
                ("u64_range", ct.c_uint64 * 2),
                ("double", ct.c_double),
                ("s1", _S1),
                ("s2", _S2),
                ("s3", _S3),
                ("ethermac", ct.c_uint8 * 6)]


class xt_option_call(ct.Structure):
    _anonymous_ = ("u",)
    _fields_ = [("arg", ct.c_char_p),
                ("ext_name", ct.c_char_p),
                ("entry", ct.POINTER(xt_option_entry)),
                ("data", ct.c_void_p),
                ("xflags", ct.c_uint),
                ("invert", ct.c_uint8),
                ("nvals", ct.c_uint8),
                ("val", _U_val),
                ("u", _U1),
                ("xt_entry", ct.c_void_p),
                ("udata", ct.c_void_p)]


class xt_fcheck_call(ct.Structure):
    _fields_ = [("ext_name", ct.c_char_p),
                ("data", ct.c_void_p),
                ("udata", ct.c_void_p),
                ("xflags", ct.c_uint)]


class _xtables_match_v1(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_match))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert, unsigned int
                # *flags, const void *entry, struct xt_entry_match **match)
                ("parse", ct.CFUNCTYPE(ct.c_int, ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_match)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the match iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_match), ct.c_int)),
                # saves the match info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_match))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                ("option_offset", ct.c_uint),
                ("m", ct.POINTER(xt_entry_match)),
                ("mflags", ct.c_uint),
                ("loaded", ct.c_uint)]
    x6_parse = None
    x6_fcheck = None
    x6_options = None

_xtables_match_v2 = _xtables_match_v1
_xtables_match_v4 = _xtables_match_v1
_xtables_match_v5 = _xtables_match_v1


class _xtables_match_v6(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_match))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert, unsigned int
                # *flags, const void *entry, struct xt_entry_match **match)
                ("parse", ct.CFUNCTYPE(ct.c_int, ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_match)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the match iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_match), ct.c_int)),
                # saves the match info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_match))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                ("option_offset", ct.c_uint),
                ("m", ct.POINTER(xt_entry_match)),
                ("mflags", ct.c_uint),
                ("loaded", ct.c_uint)]


class _xtables_match_v7(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_match))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert, unsigned int
                # *flags, const void *entry, struct xt_entry_match **match)
                ("parse", ct.CFUNCTYPE(ct.c_int, ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_match)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the match iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_match), ct.c_int)),
                # saves the match info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_match))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("m", ct.POINTER(xt_entry_match)),
                ("mflags", ct.c_uint),
                ("loaded", ct.c_uint)]


class _xtables_match_v9(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("real_name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_match))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert, unsigned int
                # *flags, const void *entry, struct xt_entry_match **match)
                ("parse", ct.CFUNCTYPE(ct.c_int, ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_match)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the match iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_match), ct.c_int)),
                # saves the match info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_match))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("m", ct.POINTER(xt_entry_match)),
                ("mflags", ct.c_uint),
                ("loaded", ct.c_uint)]


class _xtables_match_v10(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("real_name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("ext_flags", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_match))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert, unsigned int
                # *flags, const void *entry, struct xt_entry_match **match)
                ("parse", ct.CFUNCTYPE(ct.c_int, ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_match)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the match iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_match), ct.c_int)),
                # saves the match info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_match))),
                # Print match name or alias
                ("alias", ct.CFUNCTYPE(ct.c_char_p,
                                       ct.POINTER(xt_entry_match))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("m", ct.POINTER(xt_entry_match)),
                ("mflags", ct.c_uint),
                ("loaded", ct.c_uint)]


_xtables_match_v11 = _xtables_match_v10


class _xtables_match_v12(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("real_name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("ext_flags", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_match))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert, unsigned int
                # *flags, const void *entry, struct xt_entry_match **match)
                ("parse", ct.CFUNCTYPE(ct.c_int, ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_match)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the match iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_match), ct.c_int)),
                # saves the match info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_match))),
                # Print match name or alias
                ("alias", ct.CFUNCTYPE(ct.c_char_p,
                                       ct.POINTER(xt_entry_match))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                ('xt_xlate', ct.c_int),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("m", ct.POINTER(xt_entry_match)),
                ("mflags", ct.c_uint),
                ("loaded", ct.c_uint)]


class xtables_match(ct.Union):
    _fields_ = [("v1", _xtables_match_v1),
                ("v2", _xtables_match_v2),
                # Apparently v3 was skipped
                ("v4", _xtables_match_v4),
                ("v5", _xtables_match_v5),
                ("v6", _xtables_match_v6),
                ("v7", _xtables_match_v7),
                # Apparently v8 was skipped
                ("v9", _xtables_match_v9),
                ("v10", _xtables_match_v10),
                ("v11", _xtables_match_v11),
                ("v12", _xtables_match_v12)]


class _xtables_target_v1(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_target))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert,
                #              unsigned int *flags, const void *entry,
                #              struct xt_entry_target **target)
                ("parse", ct.CFUNCTYPE(ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_target)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the target iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_target), ct.c_int)),
                # saves the target info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_target))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                ("option_offset", ct.c_uint),
                ("t", ct.POINTER(xt_entry_target)),
                ("tflags", ct.c_uint),
                ("used", ct.c_uint),
                ("loaded", ct.c_uint)]
    x6_parse = None
    x6_fcheck = None
    x6_options = None

_xtables_target_v2 = _xtables_target_v1
_xtables_target_v4 = _xtables_target_v1
_xtables_target_v5 = _xtables_target_v1


class _xtables_target_v6(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_target))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert,
                #              unsigned int *flags, const void *entry,
                #              struct xt_entry_target **target)
                ("parse", ct.CFUNCTYPE(ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_target)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the target iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_target), ct.c_int)),
                # saves the target info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_target))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                ("option_offset", ct.c_uint),
                ("t", ct.POINTER(xt_entry_target)),
                ("tflags", ct.c_uint),
                ("used", ct.c_uint),
                ("loaded", ct.c_uint)]


class _xtables_target_v7(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_target))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert,
                #              unsigned int *flags, const void *entry,
                #              struct xt_entry_target **target)
                ("parse", ct.CFUNCTYPE(ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_target)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the target iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_target), ct.c_int)),
                # saves the target info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_target))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("t", ct.POINTER(xt_entry_target)),
                ("tflags", ct.c_uint),
                ("used", ct.c_uint),
                ("loaded", ct.c_uint)]


class _xtables_target_v9(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("real_name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_target))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert,
                #              unsigned int *flags, const void *entry,
                #              struct xt_entry_target **target)
                ("parse", ct.CFUNCTYPE(ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_target)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the target iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_target), ct.c_int)),
                # saves the target info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_target))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("t", ct.POINTER(xt_entry_target)),
                ("tflags", ct.c_uint),
                ("used", ct.c_uint),
                ("loaded", ct.c_uint)]


class _xtables_target_v10(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("real_name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("ext_flags", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_target))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert,
                #              unsigned int *flags, const void *entry,
                #              struct xt_entry_target **target)
                ("parse", ct.CFUNCTYPE(ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_target)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the target iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_target), ct.c_int)),
                # saves the target info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_target))),
                # Print target name or alias
                ("alias", ct.CFUNCTYPE(ct.c_char_p,
                                       ct.POINTER(xt_entry_target))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("t", ct.POINTER(xt_entry_target)),
                ("tflags", ct.c_uint),
                ("used", ct.c_uint),
                ("loaded", ct.c_uint)]


_xtables_target_v11 = _xtables_target_v10

class _xtables_target_v12(ct.Structure):
    _fields_ = [("version", ct.c_char_p),
                ("next", ct.c_void_p),
                ("name", ct.c_char_p),
                ("real_name", ct.c_char_p),
                ("revision", ct.c_uint8),
                ("ext_flags", ct.c_uint8),
                ("family", ct.c_uint16),
                ("size", ct.c_size_t),
                ("userspacesize", ct.c_size_t),
                ("help", ct.CFUNCTYPE(None)),
                ("init", ct.CFUNCTYPE(None, ct.POINTER(xt_entry_target))),
                # fourth parameter entry is struct ipt_entry for example
                # int (*parse)(int c, char **argv, int invert,
                #              unsigned int *flags, const void *entry,
                #              struct xt_entry_target **target)
                ("parse", ct.CFUNCTYPE(ct.c_int,
                                       ct.POINTER(ct.c_char_p), ct.c_int,
                                       ct.POINTER(ct.c_uint), ct.c_void_p,
                                       ct.POINTER(ct.POINTER(
                                           xt_entry_target)))),
                ("final_check", ct.CFUNCTYPE(None, ct.c_uint)),
                # prints out the target iff non-NULL: put space at end
                # first parameter ip is struct ipt_ip * for example
                ("print", ct.CFUNCTYPE(None, ct.c_void_p,
                                       ct.POINTER(xt_entry_target), ct.c_int)),
                # saves the target info in parsable form to stdout.
                # first parameter ip is struct ipt_ip * for example
                ("save", ct.CFUNCTYPE(None, ct.c_void_p,
                                      ct.POINTER(xt_entry_target))),
                # Print target name or alias
                ("alias", ct.CFUNCTYPE(ct.c_char_p,
                                       ct.POINTER(xt_entry_target))),
                # pointer to list of extra command-line options
                ("extra_opts", ct.POINTER(option)),

                # introduced with the new iptables API
                ("x6_parse", ct.CFUNCTYPE(None, ct.POINTER(xt_option_call))),
                ("x6_fcheck", ct.CFUNCTYPE(None, ct.POINTER(xt_fcheck_call))),
                ("x6_options", ct.POINTER(xt_option_entry)),

                ('xt_xlate', ct.c_int),

                # size of per-extension instance extra "global" scratch space
                ("udata_size", ct.c_size_t),

                # ignore these men behind the curtain:
                ("udata", ct.c_void_p),
                ("option_offset", ct.c_uint),
                ("t", ct.POINTER(xt_entry_target)),
                ("tflags", ct.c_uint),
                ("used", ct.c_uint),
                ("loaded", ct.c_uint)]


class xtables_target(ct.Union):
    _fields_ = [("v1", _xtables_target_v1),
                ("v2", _xtables_target_v2),
                # Apparently v3 was skipped
                ("v4", _xtables_target_v4),
                ("v5", _xtables_target_v5),
                ("v6", _xtables_target_v6),
                ("v7", _xtables_target_v7),
                # Apparently v8 was skipped
                ("v9", _xtables_target_v9),
                ("v10", _xtables_target_v10),
                ("v11", _xtables_target_v11),
                ("v12", _xtables_target_v12)]


_libc = find_libc()
_optind = ct.c_long.in_dll(_libc, "optind")
_optarg = ct.c_char_p.in_dll(_libc, "optarg")

xtables_version = os.getenv("PYTHON_IPTABLES_XTABLES_VERSION")
if xtables_version:
    _searchlib = "libxtables.so.%s" % (xtables_version,)
else:
    _searchlib = "xtables"
_lib_xtables, xtables_version = find_library(_searchlib)
_xtables_libdir = os.getenv("XTABLES_LIBDIR")
if _xtables_libdir is None:
    import re
    ldconfig_path_regex = re.compile('^(/.*):($| \(.*\)$)')
    import subprocess
    ldconfig = subprocess.Popen(
        ('/sbin/ldconfig', '-N', '-v'),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    ldconfig_out, ldconfig_err = ldconfig.communicate()
    if ldconfig.returncode != 0:
        raise XTablesError("ldconfig failed, please set XTABLES_LIBDIR")
    for ldconfig_out_line in ldconfig_out.splitlines():
        ldconfig_path_regex_match = ldconfig_path_regex.match(ldconfig_out_line)
        if ldconfig_path_regex_match is not None:
            ldconfig_path = os.path.join(ldconfig_path_regex_match.group(1), 'xtables')
            if os.path.isdir(ldconfig_path):
                _xtables_libdir = ldconfig_path
                break
if _xtables_libdir is None:
    raise XTablesError("can't find directory with extensions; "
                       "please set XTABLES_LIBDIR")

_lib_xtwrapper, _ = find_library("xtwrapper")

_throw = _lib_xtwrapper.throw_exception

_wrap_parse = _lib_xtwrapper.wrap_parse
_wrap_parse.restype = ct.c_int
_wrap_parse.argtypes = [ct.c_void_p, ct.c_int, ct.POINTER(ct.c_char_p),
                        ct.c_int, ct.POINTER(ct.c_uint), ct.c_void_p,
                        ct.POINTER(ct.c_void_p)]

_wrap_save = _lib_xtwrapper.wrap_save
_wrap_save.restype = ct.c_void_p
_wrap_save.argtypes = [ct.c_void_p, ct.c_void_p, ct.c_void_p]

_wrap_uintfn = _lib_xtwrapper.wrap_uintfn
_wrap_uintfn.restype = ct.c_int
_wrap_uintfn.argtypes = [ct.c_void_p, ct.c_uint]

_wrap_voidfn = _lib_xtwrapper.wrap_voidfn
_wrap_voidfn.restype = ct.c_int
_wrap_voidfn.argtypes = [ct.c_void_p]

_wrap_x6fn = _lib_xtwrapper.wrap_x6fn
_wrap_x6fn.restype = ct.c_int
_wrap_x6fn.argtypes = [ct.c_void_p, ct.c_void_p]

_kernel_version = ct.c_int.in_dll(_lib_xtwrapper, 'kernel_version')
_get_kernel_version = _lib_xtwrapper.get_kernel_version
_get_kernel_version()


def _xt_exit(status, *args):
    _throw(status)
_EXIT_FN = ct.CFUNCTYPE(None, ct.c_int, ct.c_char_p)
_xt_exit = _EXIT_FN(_xt_exit)


def set_nfproto(fn):
    def new(*args):
        xtobj = args[0]
        xtables._xtables_set_nfproto(xtobj.proto)
        return fn(*args)
    return new


_xt_globals = xtables_globals()
_xt_globals.option_offset = 0
_xt_globals.program_name = version.__pkgname__.encode()
_xt_globals.program_version = version.__version__.encode()
_xt_globals.orig_opts = None
_xt_globals.opts = None
_xt_globals.exit_err = _xt_exit

if xtables_version > 10:
    _COMPAT_REV_FN = ct.CFUNCTYPE(ct.c_int, ct.c_char_p, ct.c_uint8, ct.c_int)
    _xt_compat_rev = _COMPAT_REV_FN(_lib_xtables.xtables_compatible_revision)
    _xt_globals.compat_rev = _xt_compat_rev


_loaded_exts = {}


class xtables(object):
    _xtables_init_all = _lib_xtables.xtables_init_all
    _xtables_init_all.restype = ct.c_int
    _xtables_init_all.argtypes = [ct.POINTER(xtables_globals), ct.c_uint8]

    _xtables_find_match = _lib_xtables.xtables_find_match
    _xtables_find_match.restype = ct.POINTER(xtables_match)
    _xtables_find_match.argtypes = [ct.c_char_p, ct.c_int, ct.c_void_p]

    _xtables_find_target = _lib_xtables.xtables_find_target
    _xtables_find_target.restype = ct.POINTER(xtables_target)
    _xtables_find_target.argtypes = [ct.c_char_p, ct.c_int]

    _xtables_set_nfproto = _lib_xtables.xtables_set_nfproto
    _xtables_set_nfproto.restype = None
    _xtables_set_nfproto.argtypes = [ct.c_uint8]

    _xtables_xt_params = ct.c_void_p.in_dll(_lib_xtables, "xt_params")
    _xtables_matches = (ct.c_void_p.in_dll(_lib_xtables, "xtables_matches"))
    try:
        _xtables_pending_matches = (ct.c_void_p.in_dll(
            _lib_xtables, "xtables_pending_matches"))
    except ValueError:
        _xtables_pending_matches = ct.POINTER(None)
    _xtables_targets = (ct.c_void_p.in_dll(_lib_xtables, "xtables_targets"))
    try:
        _xtables_pending_targets = (ct.c_void_p.in_dll(
            _lib_xtables, "xtables_pending_targets"))
    except ValueError:
        _xtables_pending_targets = ct.POINTER(None)

    _real_name = {
        'state': 'conntrack',
        'NOTRACK': 'CT'
    }

    _cache = weakref.WeakValueDictionary()

    def __new__(cls, proto):
        obj = xtables._cache.get(proto, None)
        if not obj:
            obj = object.__new__(cls)
            xtables._cache[proto] = obj
            obj._xtinit(proto)
        return obj

    def _xtinit(self, proto, no_alias_check=False):
        self.proto = proto
        self.no_alias_check = no_alias_check

        thismodule = sys.modules[__name__]
        matchname = "_xtables_match_v%d" % (xtables_version)
        targetname = "_xtables_target_v%d" % (xtables_version)
        try:
            self._match_struct = getattr(thismodule, matchname)
            self._target_struct = getattr(thismodule, targetname)
        except:
            raise XTablesError("unknown xtables version %d" %
                               (xtables_version))

        rv = xtables._xtables_init_all(ct.pointer(_xt_globals), proto)
        if rv:
            raise XTablesError("xtables_init_all() failed: %d" % (rv))

    def __repr__(self):
        return "XTables for protocol %d" % (self.proto)

    def _check_extname(self, name):
        if name in [b"", b"ACCEPT", b"DROP", b"QUEUE", b"RETURN"]:
            name = b"standard"
        return name

    def _loaded(self, name, ext):
        _loaded_exts['%s___%s' % (self.proto, name)] = ext

    def _get_initfn_from_lib(self, name, lib):
        try:
            initfn = getattr(lib, "libxt_%s_init" % (name))
        except AttributeError:
            prefix = self._get_prefix()
            initfn = getattr(lib, "%s%s_init" % (prefix, name), None)
            if initfn is None and not self.no_alias_check:
                if name in xtables._real_name:
                    name = xtables._real_name[name]
                    initfn = self._get_initfn_from_lib(name, lib)
        return initfn

    def _try_extinit(self, name, lib):
        try:
            if type(lib) != ct.CDLL:
                lib = ct.CDLL(lib)
            fn = self._get_initfn_from_lib(name, lib)
            if fn:
                _wrap_voidfn(fn)
                return True
        except:
            pass
        return False

    def _get_prefix(self):
        if self.proto == NFPROTO_IPV4:
            return "libipt_"
        elif self.proto == NFPROTO_IPV6:
            return "libip6t_"
        else:
            raise XTablesError("Unknown protocol %d" % (self.proto))

    def _try_register(self, name):
        if isinstance(name, bytes):
            name = name.decode()
        if self._try_extinit(name, _lib_xtables):
            return
        prefix = self._get_prefix()
        libs = [os.path.join(_xtables_libdir, "libxt_" + name + ".so"),
                os.path.join(_xtables_libdir, prefix + name + ".so")]
        for lib in libs:
            if self._try_extinit(name, lib):
                return

    def _get_loaded_ext(self, name):
        ext = _loaded_exts.get('%s___%s' % (self.proto, name), None)
        return ext

    @set_nfproto
    def find_match(self, name):
        if isinstance(name, str):
            name = name.encode()
        name = self._check_extname(name)

        ext = self._get_loaded_ext(name)
        if ext is not None:
            return ext

        match = xtables._xtables_find_match(name, XTF_TRY_LOAD, None)
        if not match:
            self._try_register(name)
            match = xtables._xtables_find_match(name, XTF_DONT_LOAD, None)
            if not match:
                return match

        m = ct.cast(match, ct.POINTER(self._match_struct))
        self._loaded(m[0].name, m)
        return m

    @set_nfproto
    def find_target(self, name):
        if isinstance(name, str):
            name = name.encode()
        name = self._check_extname(name)

        ext = self._get_loaded_ext(name)
        if ext is not None:
            return ext

        target = xtables._xtables_find_target(name, XTF_TRY_LOAD)
        if not target:
            self._try_register(name)
            target = xtables._xtables_find_target(name, XTF_DONT_LOAD)
            if not target:
                return target

        t = ct.cast(target, ct.POINTER(self._target_struct))
        self._loaded(t[0].name, t)
        return t

    @set_nfproto
    def save(self, module, ip, ptr):
        _wrap_save(module.save, ct.cast(ct.pointer(ip), ct.c_void_p), ptr)

    def _option_lookup(self, entries, name):
        for e in entries:
            if not e.name:
                break
            if e.name == name:
                return e
        return None

    def _parse(self, module, argv, inv, flags, entry, ptr):
        for opt in module.extra_opts:
            if opt.name == argv[0]:
                rv = _wrap_parse(module.parse, opt.val, argv, inv, flags,
                                 entry, ptr)
                if rv != 1:
                    raise ValueError("invalid value %s" % (argv[1]))
                return
            elif not opt.name:
                break
        raise AttributeError("invalid parameter %s" % (argv[0]))

    # Dispatch arguments to the appropriate parse function, based upon the
    # extension's choice of API.
    @set_nfproto
    def parse_target(self, argv, invert, t, fw, ptr, x6_parse, x6_options):
        _optarg.value = len(argv) > 1 and argv[1] or None
        _optind.value = len(argv) - 1

        try:
            # new API?
            if x6_options is None:
                x6_options = t.x6_options
            if x6_parse is None:
                x6_parse = t.x6_parse
        except AttributeError:
            pass

        if x6_options and x6_parse:
            # new API
            entry = self._option_lookup(x6_options, argv[0])
            if not entry:
                raise XTablesError("%s: no such parameter %s" % (t.name,
                                                                 argv[0]))

            cb = xt_option_call()
            cb.entry = ct.pointer(entry)
            cb.arg = _optarg
            cb.invert = ct.c_uint8(invert.value)
            cb.ext_name = t.name
            cb.data = ct.cast(t.t[0].data, ct.c_void_p)
            cb.xflags = 0
            cb.target = ct.pointer(t.t)
            cb.xt_entry = ct.cast(fw, ct.c_void_p)
            cb.udata = t.udata
            rv = _wrap_x6fn(x6_parse, ct.pointer(cb))
            if rv != 0:
                raise XTablesError("%s: parameter error %d (%s)" % (t.name, rv,
                                                                    argv[1]))
            t.tflags |= cb.xflags
            return

        # old API
        flags = ct.pointer(ct.c_uint(0))
        self._parse(t, argv, invert, flags, fw, ptr)
        t.tflags |= flags[0]

    # Dispatch arguments to the appropriate parse function, based upon the
    # extension's choice of API.
    @set_nfproto
    def parse_match(self, argv, invert, m, fw, ptr, x6_parse, x6_options):
        _optarg.value = len(argv) > 1 and argv[1] or None
        _optind.value = len(argv) - 1

        try:
            # new API?
            if x6_options is None:
                x6_options = m.x6_options
            if x6_parse is None:
                x6_parse = m.x6_parse
        except AttributeError:
            pass

        if x6_options and x6_parse:
            # new API
            entry = self._option_lookup(x6_options, argv[0])
            if not entry:
                raise XTablesError("%s: no such parameter %s" % (m.name,
                                                                 argv[0]))

            cb = xt_option_call()
            cb.entry = ct.pointer(entry)
            cb.arg = _optarg
            cb.invert = ct.c_uint8(invert.value)
            cb.ext_name = m.name
            cb.data = ct.cast(m.m[0].data, ct.c_void_p)
            cb.xflags = 0
            cb.match = ct.pointer(m.m)
            cb.xt_entry = ct.cast(fw, ct.c_void_p)
            cb.udata = m.udata
            rv = _wrap_x6fn(x6_parse, ct.pointer(cb))
            if rv != 0:
                raise XTablesError("%s: parameter '%s' error %d" % (
                    m.name, len(argv) > 1 and argv[1] or "", rv))
            m.mflags |= cb.xflags
            return

        # old API
        flags = ct.pointer(ct.c_uint(0))
        self._parse(m, argv, invert, flags, fw, ptr)
        m.mflags |= flags[0]

    # Check that all option constraints have been met. This effectively
    # replaces ->final_check of the older API.
    def _options_fcheck(self, name, xflags, table):
        for entry in table:
            if entry.name is None:
                break
            if entry.flags & XTOPT_MAND and not xflags & (1 << entry.id):
                raise XTablesError("%s: --%s must be specified" % (name,
                                                                   entry.name))
                if not xflags & (1 << entry.id):
                    continue
            # XXX: check for conflicting options

    def _fcheck_target_old(self, target):
        # old API
        if not target.final_check:
            return
        rv = _wrap_uintfn(target.final_check, target.tflags)
        if rv:
            raise XTablesError("%s.final_check() has failed" %
                               (target.name))

    def _fcheck_target_new(self, target):
        # new API
        cb = xt_fcheck_call()
        cb.ext_name = target.name
        cb.data = ct.cast(target.t[0].data, ct.c_void_p)
        cb.xflags = target.tflags
        cb.udata = target.udata
        rv = _wrap_x6fn(target.x6_fcheck, ct.pointer(cb))
        if rv:
            raise XTablesError("%s.x6_fcheck has failed" % (target.name))
        if target.x6_options:
            self._options_fcheck(target.name, target.tflags,
                                 target.x6_options)

    # Dispatch arguments to the appropriate final_check function, based upon
    # the extension's choice of API.
    @set_nfproto
    def final_check_target(self, target):
        x6_fcheck = None
        try:
            # new API?
            x6_fcheck = target.x6_fcheck
        except AttributeError:
            # old API
            pass

        if x6_fcheck:
            self._fcheck_target_new(target)
        else:
            self._fcheck_target_old(target)

    def _fcheck_match_old(self, match):
        # old API
        if not match.final_check:
            return
        rv = _wrap_uintfn(match.final_check, match.mflags)
        if rv:
            raise XTablesError("%s.final_check() has failed" %
                               (match.name))

    def _fcheck_match_new(self, match):
        # new API
        cb = xt_fcheck_call()
        cb.ext_name = match.name
        cb.data = ct.cast(match.m[0].data, ct.c_void_p)
        cb.xflags = match.mflags
        cb.udata = match.udata
        rv = _wrap_x6fn(match.x6_fcheck, ct.pointer(cb))
        if rv:
            raise XTablesError("%s.x6_fcheck has failed" % (match.name))
        if match.x6_options:
            self._options_fcheck(match.name, match.mflags,
                                 match.x6_options)

    # Dispatch arguments to the appropriate final_check function, based upon
    # the extension's choice of API.
    @set_nfproto
    def final_check_match(self, match):
        x6_fcheck = None
        try:
            # new API?
            x6_fcheck = match.x6_fcheck
        except AttributeError:
            # old API
            pass

        if x6_fcheck:
            self._fcheck_match_new(match)
        else:
            self._fcheck_match_old(match)

"""Structs and definitions used serialize/deserialize Atop statistics directly from log files.

Structs are declared in a way that will help provide as close to a 1 to 1 match as possible for debuggability
and maintenance. The _fields_ of every struct match their original name, however the struct names have been updated
to match python CamelCase standards. Each struct includes the following to help identify the original source:
    C Name: utsname
    C Location: sys/utsname.h

Struct ordering matches the C source to help with comparisons.

See https://github.com/Atoptool/atop for more information and full details about each field.
Using schemas and structs from Atop 1.26.
"""

import ctypes

from atoparser.structs.shared import HeaderMixin
from atoparser.structs.shared import UTSName
from atoparser.structs.shared import count_t
from atoparser.structs.shared import time_t

# Disable the following pylint warnings to allow the variables and classes to match the style from the C.
# This helps with maintainability and cross-referencing.
# pylint: disable=invalid-name,too-few-public-methods

# Definitions from atop.h
ACCTACTIVE = 0x00000001
PATCHSTAT = 0x00000002
IOSTAT = 0x00000004
PATCHACCT = 0x00000008

# Definitions from photoproc.h
PNAMLEN = 15
CMDLEN = 150

# Definitions from photosyst.h
MAXCPU = 64
MAXDSK = 256
MAXLVM = 256
MAXMDD = 128
MAXDKNAM = 32
MAXINTF = 32


class Record(ctypes.Structure):
    """Top level struct to describe basic process information, and the following SStat and PStat structs.

    C Name: rawrecord
    C Location: rawlog.c
    """

    _fields_ = [
        ("curtime", time_t),
        ("flags", ctypes.c_ushort),
        ("sfuture", ctypes.c_ushort * 3),
        ("scomplen", ctypes.c_uint),
        ("pcomplen", ctypes.c_uint),
        ("interval", ctypes.c_uint),
        ("nlist", ctypes.c_uint),
        ("npresent", ctypes.c_uint),
        ("nexit", ctypes.c_uint),
        ("ntrun", ctypes.c_uint),
        ("ntslpi", ctypes.c_uint),
        ("ntslpu", ctypes.c_uint),
        ("nzombie", ctypes.c_uint),
        ("ifuture", ctypes.c_uint * 6),
    ]


class MemStat(ctypes.Structure):
    """Embedded struct to describe basic memory information.

    C Name: memstat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("physmem", count_t),
        ("freemem", count_t),
        ("buffermem", count_t),
        ("slabmem", count_t),
        ("cachemem", count_t),
        ("cachedrt", count_t),
        ("totswap", count_t),
        ("freeswap", count_t),
        ("pgscans", count_t),
        ("allocstall", count_t),
        ("swouts", count_t),
        ("swins", count_t),
        ("commitlim", count_t),
        ("committed", count_t),
        ("cfuture", count_t * 4),
    ]


class FreqCnt(ctypes.Structure):
    """Embedded struct to describe basic processor frequency information.

    C Name: freqcnt
    C Location: photosyst.h
    C Parent: percpu
    """

    _fields_ = [
        ("maxfreq", count_t),
        ("cnt", count_t),
        ("ticks", count_t),
    ]


class PerCPU(ctypes.Structure):
    """Embedded struct to describe per processor usage information.

    C Name: percpu
    C Location: photosyst.h
    C Parent: cpustat
    """

    _fields_ = [
        ("cpunr", ctypes.c_int),
        ("stime", count_t),
        ("utime", count_t),
        ("ntime", count_t),
        ("itime", count_t),
        ("wtime", count_t),
        ("Itime", count_t),
        ("Stime", count_t),
        ("steal", count_t),
        ("guest", count_t),
        ("freqcnt", FreqCnt),
        ("cfuture", count_t * 1),
    ]


class CPUStat(ctypes.Structure):
    """Embedded struct to describe basic overall processor information.

    C Name: cpustat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("nrcpu", count_t),
        ("devint", count_t),
        ("csw", count_t),
        ("nprocs", count_t),
        ("lavg1", ctypes.c_float),
        ("lavg5", ctypes.c_float),
        ("lavg15", ctypes.c_float),
        ("cfuture", count_t * 4),
        ("all", PerCPU),
        ("cpu", PerCPU * MAXCPU),
    ]
    fields_limiters = {"cpu": "nrcpu"}


class IPv4Stats(ctypes.Structure):
    """Embedded struct to describe overall IPv4 statistics.

    C Name: ipv4_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("Forwarding", count_t),
        ("DefaultTTL", count_t),
        ("InReceives", count_t),
        ("InHdrErrors", count_t),
        ("InAddrErrors", count_t),
        ("ForwDatagrams", count_t),
        ("InUnknownProtos", count_t),
        ("InDiscards", count_t),
        ("InDelivers", count_t),
        ("OutRequests", count_t),
        ("OutDiscards", count_t),
        ("OutNoRoutes", count_t),
        ("ReasmTimeout", count_t),
        ("ReasmReqds", count_t),
        ("ReasmOKs", count_t),
        ("ReasmFails", count_t),
        ("FragOKs", count_t),
        ("FragFails", count_t),
        ("FragCreates", count_t),
    ]


class ICMPv4Stats(ctypes.Structure):
    """Embedded struct to describe overall ICMPv4 statistics.

    C Name: icmpv4_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("InMsgs", count_t),
        ("InErrors", count_t),
        ("InDestUnreachs", count_t),
        ("InTimeExcds", count_t),
        ("InParmProbs", count_t),
        ("InSrcQuenchs", count_t),
        ("InRedirects", count_t),
        ("InEchos", count_t),
        ("InEchoReps", count_t),
        ("InTimestamps", count_t),
        ("InTimestampReps", count_t),
        ("InAddrMasks", count_t),
        ("InAddrMaskReps", count_t),
        ("OutMsgs", count_t),
        ("OutErrors", count_t),
        ("OutDestUnreachs", count_t),
        ("OutTimeExcds", count_t),
        ("OutParmProbs", count_t),
        ("OutSrcQuenchs", count_t),
        ("OutRedirects", count_t),
        ("OutEchos", count_t),
        ("OutEchoReps", count_t),
        ("OutTimestamps", count_t),
        ("OutTimestampReps", count_t),
        ("OutAddrMasks", count_t),
        ("OutAddrMaskReps", count_t),
    ]


class UDPv4Stats(ctypes.Structure):
    """Embedded struct to describe overall UDPv4 statistics.

    C Name: udpv4_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("InDatagrams", count_t),
        ("NoPorts", count_t),
        ("InErrors", count_t),
        ("OutDatagrams", count_t),
    ]


class TCPStats(ctypes.Structure):
    """Embedded struct to describe overall TCP statistics.

    C Name: tcp_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("RtoAlgorithm", count_t),
        ("RtoMin", count_t),
        ("RtoMax", count_t),
        ("MaxConn", count_t),
        ("ActiveOpens", count_t),
        ("PassiveOpens", count_t),
        ("AttemptFails", count_t),
        ("EstabResets", count_t),
        ("CurrEstab", count_t),
        ("InSegs", count_t),
        ("OutSegs", count_t),
        ("RetransSegs", count_t),
        ("InErrs", count_t),
        ("OutRsts", count_t),
    ]


class IPv6Stats(ctypes.Structure):
    """Embedded struct to describe overall IPv6 statistics.

    C Name: ipv6_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("Ip6InReceives", count_t),
        ("Ip6InHdrErrors", count_t),
        ("Ip6InTooBigErrors", count_t),
        ("Ip6InNoRoutes", count_t),
        ("Ip6InAddrErrors", count_t),
        ("Ip6InUnknownProtos", count_t),
        ("Ip6InTruncatedPkts", count_t),
        ("Ip6InDiscards", count_t),
        ("Ip6InDelivers", count_t),
        ("Ip6OutForwDatagrams", count_t),
        ("Ip6OutRequests", count_t),
        ("Ip6OutDiscards", count_t),
        ("Ip6OutNoRoutes", count_t),
        ("Ip6ReasmTimeout", count_t),
        ("Ip6ReasmReqds", count_t),
        ("Ip6ReasmOKs", count_t),
        ("Ip6ReasmFails", count_t),
        ("Ip6FragOKs", count_t),
        ("Ip6FragFails", count_t),
        ("Ip6FragCreates", count_t),
        ("Ip6InMcastPkts", count_t),
        ("Ip6OutMcastPkts", count_t),
    ]


class ICMPv6Stats(ctypes.Structure):
    """Embedded struct to describe overall ICMPv6 statistics.

    C Name: icmpv6_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("Icmp6InMsgs", count_t),
        ("Icmp6InErrors", count_t),
        ("Icmp6InDestUnreachs", count_t),
        ("Icmp6InPktTooBigs", count_t),
        ("Icmp6InTimeExcds", count_t),
        ("Icmp6InParmProblems", count_t),
        ("Icmp6InEchos", count_t),
        ("Icmp6InEchoReplies", count_t),
        ("Icmp6InGroupMembQueries", count_t),
        ("Icmp6InGroupMembResponses", count_t),
        ("Icmp6InGroupMembReductions", count_t),
        ("Icmp6InRouterSolicits", count_t),
        ("Icmp6InRouterAdvertisements", count_t),
        ("Icmp6InNeighborSolicits", count_t),
        ("Icmp6InNeighborAdvertisements", count_t),
        ("Icmp6InRedirects", count_t),
        ("Icmp6OutMsgs", count_t),
        ("Icmp6OutDestUnreachs", count_t),
        ("Icmp6OutPktTooBigs", count_t),
        ("Icmp6OutTimeExcds", count_t),
        ("Icmp6OutParmProblems", count_t),
        ("Icmp6OutEchoReplies", count_t),
        ("Icmp6OutRouterSolicits", count_t),
        ("Icmp6OutNeighborSolicits", count_t),
        ("Icmp6OutNeighborAdvertisements", count_t),
        ("Icmp6OutRedirects", count_t),
        ("Icmp6OutGroupMembResponses", count_t),
        ("Icmp6OutGroupMembReductions", count_t),
    ]


class UDPv6Stats(ctypes.Structure):
    """Embedded struct to describe overall UDPv6 statistics.

    C Name: udpv6_stats
    C Location: netstats.h
    C Parent: netstat
    """

    _fields_ = [
        ("Udp6InDatagrams", count_t),
        ("Udp6NoPorts", count_t),
        ("Udp6InErrors", count_t),
        ("Udp6OutDatagrams", count_t),
    ]


class NETStat(ctypes.Structure):
    """Embedded struct to describe overall network statistics.

    C Name: netstat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("ipv4", IPv4Stats),
        ("icmpv4", ICMPv4Stats),
        ("udpv4", UDPv4Stats),
        ("ipv6", IPv6Stats),
        ("icmpv6", ICMPv6Stats),
        ("udpv6", UDPv6Stats),
        ("tcp", TCPStats),
    ]


class PerDSK(ctypes.Structure):
    """Embedded struct to describe per disk information.

    C Name: perdsk
    C Location: photosyst.h
    C Parent: dskstat
    """

    _fields_ = [
        ("name", ctypes.c_char * MAXDKNAM),
        ("nread", count_t),
        ("nrsect", count_t),
        ("nwrite", count_t),
        ("nwsect", count_t),
        ("io_ms", count_t),
        ("avque", count_t),
        ("cfuture", count_t * 4),
    ]


class DSKStat(ctypes.Structure):
    """Embedded struct to describe overall disk information.

    C Name: dskstat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("ndsk", ctypes.c_int),
        ("nmdd", ctypes.c_int),
        ("nlvm", ctypes.c_int),
        ("dsk", PerDSK * MAXDSK),
        ("mdd", PerDSK * MAXMDD),
        ("lvm", PerDSK * MAXLVM),
    ]
    fields_limiters = {
        "dsk": "ndsk",
        "mdd": "nmdd",
        "lvm": "nlvm",
    }


class PerIntf(ctypes.Structure):
    """Embedded struct to describe per interface statistics.

    C Name: perintf
    C Location: photosyst.h
    C Parent: intfstat
    """

    _fields_ = [
        ("name", ctypes.c_char * 16),
        ("rbyte", count_t),
        ("rpack", count_t),
        ("rerrs", count_t),
        ("rdrop", count_t),
        ("rfifo", count_t),
        ("rframe", count_t),
        ("rcompr", count_t),
        ("rmultic", count_t),
        ("rfuture", count_t * 4),
        ("sbyte", count_t),
        ("spack", count_t),
        ("serrs", count_t),
        ("sdrop", count_t),
        ("sfifo", count_t),
        ("scollis", count_t),
        ("scarrier", count_t),
        ("scompr", count_t),
        ("sfuture", count_t * 4),
        ("speed", ctypes.c_long),
        ("duplex", ctypes.c_char),
        ("cfuture", count_t * 4),
    ]


class IntfStat(ctypes.Structure):
    """Embedded struct to describe overall interface statistics.

    C Name: intfstat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("nrintf", ctypes.c_int),
        ("intf", PerIntf * MAXINTF),
    ]
    fields_limiters = {
        "intf": "nrintf",
    }


class WWWStat(ctypes.Structure):
    """Embedded struct to describe experimental statistics for local HTTP daemons.

    C Name: wwwstat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("accesses", count_t),
        ("totkbytes", count_t),
        ("uptime", count_t),
        ("bworkers", ctypes.c_int),
        ("iworkers", ctypes.c_int),
    ]


class SStat(ctypes.Structure):
    """Top level struct to describe various subsystems.

    C Name: sstat
    C Location: photosyst.h
    """

    _fields_ = [
        ("cpu", CPUStat),
        ("mem", MemStat),
        ("net", NETStat),
        ("intf", IntfStat),
        ("dsk", DSKStat),
        ("www", WWWStat),
    ]


class GEN(ctypes.Structure):
    """Embedded struct to describe a single process' general information.

    C Name: gen
    C Location: photoproc.h
    C Parent: pstat
    """

    _fields_ = [
        ("pid", ctypes.c_int),
        ("ppid", ctypes.c_int),
        ("ruid", ctypes.c_int),
        ("euid", ctypes.c_int),
        ("suid", ctypes.c_int),
        ("fsuid", ctypes.c_int),
        ("rgid", ctypes.c_int),
        ("egid", ctypes.c_int),
        ("sgid", ctypes.c_int),
        ("fsgid", ctypes.c_int),
        ("nthr", ctypes.c_int),
        ("name", ctypes.c_char * (PNAMLEN + 1)),
        ("state", ctypes.c_char),
        ("excode", ctypes.c_int),
        ("btime", time_t),
        ("elaps", time_t),
        ("cmdline", ctypes.c_char * (CMDLEN + 1)),
        ("nthrslpi", ctypes.c_int),
        ("nthrslpu", ctypes.c_int),
        ("nthrrun", ctypes.c_int),
        ("ifuture", ctypes.c_int * 1),
    ]


class CPU(ctypes.Structure):
    """Embedded struct to describe a single process' processor usage.

    C Name: cpu
    C Location: photoproc.h
    C Parent: pstat
    """

    _fields_ = [
        ("utime", count_t),
        ("stime", count_t),
        ("nice", ctypes.c_int),
        ("prio", ctypes.c_int),
        ("rtprio", ctypes.c_int),
        ("policy", ctypes.c_int),
        ("curcpu", ctypes.c_int),
        ("sleepavg", ctypes.c_int),
        ("ifuture", ctypes.c_int * 4),
        ("cfuture", count_t * 4),
    ]


class DSK(ctypes.Structure):
    """Embedded struct to describe a single process' disk usage.

    C Name: dsk
    C Location: photoproc.h
    C Parent: pstat
    """

    _fields_ = [
        ("rio", count_t),
        ("rsz", count_t),
        ("wio", count_t),
        ("wsz", count_t),
        ("cwsz", count_t),
        ("cfuture", count_t * 4),
    ]


class MEM(ctypes.Structure):
    """Embedded struct to describe a single process' memory usage.

    C Name: mem
    C Location: photoproc.h
    C Parent: pstat
    """

    _fields_ = [
        ("minflt", count_t),
        ("majflt", count_t),
        ("shtext", count_t),
        ("vmem", count_t),
        ("rmem", count_t),
        ("vgrow", count_t),
        ("rgrow", count_t),
        ("cfuture", count_t * 4),
    ]


class NET(ctypes.Structure):
    """Embedded struct to describe a single process' network usage.

    C Name: net
    C Location: photoproc.h
    C Parent: pstat
    """

    _fields_ = [
        ("tcpsnd", count_t),
        ("tcpssz", count_t),
        ("tcprcv", count_t),
        ("tcprsz", count_t),
        ("udpsnd", count_t),
        ("udpssz", count_t),
        ("udprcv", count_t),
        ("udprsz", count_t),
        ("rawsnd", count_t),
        ("rawrcv", count_t),
        ("cfuture", count_t * 4),
    ]


class PStat(ctypes.Structure):
    """Top level struct to describe multiple statistic categories per process.

    C Name: pstat
    C Location: photoproc.h
    """

    _fields_ = [
        ("gen", GEN),
        ("cpu", CPU),
        ("dsk", DSK),
        ("mem", MEM),
        ("net", NET),
    ]


# Add TStat alias for forwards compatibility. TStat and PStat are the same base stats, but renamed in later versions.
TStat = PStat


class Header(ctypes.Structure, HeaderMixin):
    """Top level struct to describe information about the system running Atop and the log file itself.

    C Name: rawheader
    C Location: rawlog.c
    """

    _fields_ = [
        ("magic", ctypes.c_uint),
        ("aversion", ctypes.c_ushort),
        ("future1", ctypes.c_ushort),
        ("future2", ctypes.c_ushort),
        ("rawheadlen", ctypes.c_ushort),
        ("rawreclen", ctypes.c_ushort),
        ("hertz", ctypes.c_ushort),
        ("sfuture", ctypes.c_ushort * 6),
        ("sstatlen", ctypes.c_uint),
        ("pstatlen", ctypes.c_uint),
        ("utsname", UTSName),
        ("cfuture", ctypes.c_char * 8),
        ("pagesize", ctypes.c_uint),
        ("supportflags", ctypes.c_int),
        ("osrel", ctypes.c_int),
        ("osvers", ctypes.c_int),
        ("ossub", ctypes.c_int),
        ("ifuture", ctypes.c_int * 6),
    ]
    supported_version = "1.26"
    Record = Record
    SStat = SStat
    PStat = PStat
    # Add TStat alias for forwards compatibility. TStat and PStat are the same base stats, but renamed in later versions.
    TStat = PStat
    CStat = None
    CGChainer = None

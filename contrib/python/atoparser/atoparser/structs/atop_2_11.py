"""Structs and definitions used serialize/deserialize Atop statistics directly from log files.

Structs are declared in a way that will help provide as close to a 1 to 1 match as possible for debuggability
and maintenance. The _fields_ of every struct match their original name, however the struct names have been updated
to match python CamelCase standards. Each struct includes the following to help identify the original source:
    C Name: utsname
    C Location: sys/utsname.h

Struct ordering matches the C source to help with comparisons.
If structs match exactly from a previous version, they are reused via aliasing.

See https://github.com/Atoptool/atop for more information and full details about each field.
Using schemas and structs from Atop 2.11.0.
"""

import ctypes

from atoparser.structs import atop_1_26
from atoparser.structs import atop_2_3
from atoparser.structs import atop_2_4
from atoparser.structs import atop_2_7
from atoparser.structs import atop_2_8
from atoparser.structs import atop_2_10
from atoparser.structs.shared import HeaderMixin
from atoparser.structs.shared import UTSName
from atoparser.structs.shared import count_t
from atoparser.structs.shared import pid_t
from atoparser.structs.shared import time_t

# Disable the following pylint warnings to allow the variables and classes to match the style from the C.
# This helps with maintainability and cross-referencing.
# pylint: disable=invalid-name,too-few-public-methods

# Definitions from atop.h
ACCTACTIVE = 0x00000001
IOSTAT = 0x00000004
NETATOP = 0x00000010
NETATOPD = 0x00000020
CONTAINERSTAT = 0x00000040
GPUSTAT = 0x00000080
CGROUPV2 = 0x00000100
NETATOPBPF = 0x00001000

# Definitions from photoproc.h
PNAMLEN = 15
CMDLEN = 255
CGRLEN = 64
UTSLEN = 15

# Definitions from photosyst.h
MAXCPU = 2048
MAXDSK = 1024
MAXNUMA = 1024
MAXLVM = 2048
MAXMDD = 256
MAXINTF = 128
MAXCONTAINER = 128
MAXNFSMOUNT = 64
MAXIBPORT = 32
MAXGPU = 32
MAXGPUBUS = 12
MAXGPUTYPE = 12
MAXLLC = 256
MAXDKNAM = 32
MAXIBNAME = 12


class Record(ctypes.Structure):
    """Top level struct to describe basic process information, and the following SStat and TStat structs.

    C Name: rawrecord
    C Location: rawlog.h
    """

    _fields_ = [
        ("curtime", time_t),
        ("flags", ctypes.c_ushort),
        ("ncgroups", ctypes.c_ushort),
        ("sfuture", ctypes.c_ushort * 2),
        ("scomplen", ctypes.c_uint),
        ("pcomplen", ctypes.c_uint),
        ("interval", ctypes.c_uint),
        ("ndeviat", ctypes.c_uint),
        ("nactproc", ctypes.c_uint),
        ("ntask", ctypes.c_uint),
        ("totproc", ctypes.c_uint),
        ("totrun", ctypes.c_uint),
        ("totslpi", ctypes.c_uint),
        ("totslpu", ctypes.c_uint),
        ("totzomb", ctypes.c_uint),
        ("nexit", ctypes.c_uint),
        ("noverflow", ctypes.c_uint),
        ("totidle", ctypes.c_uint),
        ("ccomplen", ctypes.c_uint),
        ("coriglen", ctypes.c_uint),
        ("ncgpids", ctypes.c_uint),
        ("icomplen", ctypes.c_uint),
        ("ifuture", ctypes.c_uint),
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
        ("pgsteal", count_t),
        ("allocstall", count_t),
        ("swouts", count_t),
        ("swins", count_t),
        ("tcpsock", count_t),
        ("udpsock", count_t),
        ("commitlim", count_t),
        ("committed", count_t),
        ("shmem", count_t),
        ("shmrss", count_t),
        ("shmswp", count_t),
        ("slabreclaim", count_t),
        ("stothugepage", count_t),
        ("sfreehugepage", count_t),
        ("shugepagesz", count_t),
        ("vmwballoon", count_t),
        ("zfsarcsize", count_t),
        ("swapcached", count_t),
        ("ksmsharing", count_t),
        ("ksmshared", count_t),
        ("zswapped", count_t),
        ("zswap", count_t),
        ("oomkills", count_t),
        ("compactstall", count_t),
        ("pgmigrate", count_t),
        ("numamigrate", count_t),
        ("pgouts", count_t),
        ("pgins", count_t),
        ("pagetables", count_t),
        ("zswouts", count_t),
        ("zswins", count_t),
        ("ltothugepage", count_t),
        ("lfreehugepage", count_t),
        ("lhugepagesz", count_t),
        ("availablemem", count_t),
        ("anonhugepage", count_t),
        ("cfuture", count_t * 5),
    ]


class MemPerNUMA(ctypes.Structure):
    """Embedded struct to describe basic memory information per NUMA node.

    C Name: mempernuma
    C Location: photosyst.h
    C Parent: memnuma
    """

    _fields_ = [
        ("numanr", ctypes.c_int),
        ("frag", ctypes.c_float),
        ("totmem", count_t),
        ("freemem", count_t),
        ("filepage", count_t),
        ("dirtymem", count_t),
        ("slabmem", count_t),
        ("slabreclaim", count_t),
        ("active", count_t),
        ("inactive", count_t),
        ("shmem", count_t),
        ("tothp", count_t),
        ("freehp", count_t),
        ("cfuture", count_t * 2),
    ]


class MemNUMA(ctypes.Structure):
    """Embedded struct to describe memory usage across all NUMA nodes.

    C Name: memnuma
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("nrnuma", count_t),
        ("numa", MemPerNUMA * MAXNUMA),
    ]
    fields_limiters = {"numa": "nrnuma"}


class CPUPerNUMA(ctypes.Structure):
    """Embedded struct to describe basic CPU information per NUMA node.

    C Name: cpupernuma
    C Location: photosyst.h
    C Parent: cpunuma
    """

    _fields_ = [
        ("numanr", ctypes.c_int),
        ("nrcpu", count_t),
        ("stime", count_t),
        ("utime", count_t),
        ("ntime", count_t),
        ("itime", count_t),
        ("wtime", count_t),
        ("Itime", count_t),
        ("Stime", count_t),
        ("steal", count_t),
        ("guest", count_t),
        ("cfuture", count_t * 2),
    ]


class CPUNUMA(ctypes.Structure):
    """Embedded struct to describe CPU usage across all NUMA nodes.

    C Name: cpunuma
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("nrnuma", count_t),
        ("numa", CPUPerNUMA * MAXNUMA),
    ]
    fields_limiters = {"numa": "nrnuma"}


FreqCnt = atop_1_26.FreqCnt


PerCPU = atop_2_7.PerCPU


CPUStat = atop_2_7.CPUStat


PerDSK = atop_2_8.PerDSK


DSKStat = atop_2_8.DSKStat


PerIntf = atop_2_3.PerIntf


IntfStat = atop_2_3.IntfStat


PerNFSMount = atop_2_3.PerNFSMount


Server = atop_2_3.Server


Client = atop_2_3.Client


NFSMounts = atop_2_3.NFSMounts


NFSStat = atop_2_3.NFSStat


PSI = atop_2_4.PSI


Pressure = atop_2_4.Pressure


PerContainer = atop_2_3.PerContainer


ContStat = atop_2_3.ContStat


WWWStat = atop_1_26.WWWStat


PerGPU = atop_2_4.PerGPU


GPUStat = atop_2_4.GPUStat


class PerIFB(ctypes.Structure):
    """Embedded struct to describe per InfiniBand statistics.

    C Name: perifb
    C Location: photosyst.h
    C Parent: ifbstat
    """

    _fields_ = [
        ("ibname", ctypes.c_char * MAXIBNAME),
        ("portnr", ctypes.c_short),
        ("lanes", ctypes.c_short),
        ("rate", count_t),
        ("rcvb", count_t),
        ("sndb", count_t),
        ("rcvp", count_t),
        ("sndp", count_t),
        ("cfuture", count_t * 4),
    ]


class IFBStat(ctypes.Structure):
    """Embedded struct to describe overall InfiniBand statistics.

    C Name: ifbstat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("nrports", ctypes.c_int),
        ("ifb", PerIFB * MAXIBPORT),
    ]
    fields_limiters = {
        "ifb": "nrports",
    }


PerLLC = atop_2_8.PerLLC


LLCStat = atop_2_8.LLCStat


IPv4Stats = atop_1_26.IPv4Stats


ICMPv4Stats = atop_2_8.ICMPv4Stats


UDPv4Stats = atop_1_26.UDPv4Stats


TCPStats = atop_2_10.TCPStats


IPv6Stats = atop_1_26.IPv6Stats


ICMPv6Stats = atop_1_26.ICMPv6Stats


UDPv6Stats = atop_1_26.UDPv6Stats


NETStat = atop_2_10.NETStat


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
        ("memnuma", MemNUMA),
        ("cpunuma", CPUNUMA),
        ("dsk", DSKStat),
        ("nfs", NFSStat),
        ("cfs", ContStat),
        ("psi", Pressure),
        ("gpu", GPUStat),
        ("ifb", IFBStat),
        ("llc", LLCStat),
        ("www", WWWStat),
    ]


class GEN(ctypes.Structure):
    """Embedded struct to describe a single process' general information.

    C Name: gen
    C Location: photoproc.h
    C Parent: tstat
    """

    _fields_ = [
        ("tgid", ctypes.c_int),
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
        ("isproc", ctypes.c_char),
        ("state", ctypes.c_char),
        ("excode", ctypes.c_int),
        ("btime", time_t),
        ("elaps", time_t),
        ("cmdline", ctypes.c_char * (CMDLEN + 1)),
        ("nthrslpi", ctypes.c_int),
        ("nthrslpu", ctypes.c_int),
        ("nthrrun", ctypes.c_int),
        ("nthridle", ctypes.c_int),
        ("ctid", ctypes.c_int),
        ("vpid", ctypes.c_int),
        ("wasinactive", ctypes.c_int),
        ("utsname", ctypes.c_char * (UTSLEN + 1)),
        ("cgroupix", ctypes.c_int),
        ("ifuture", ctypes.c_int * 4),
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
        ("ifuture", ctypes.c_int * 6),
        ("wchan", ctypes.c_char * 16),
        ("rundelay", count_t),
        ("blkdelay", count_t),
        ("nvcsw", count_t),
        ("nivcsw", count_t),
        ("cfuture", count_t * 3),
    ]


DSK = atop_1_26.DSK


class MEM(ctypes.Structure):
    """Embedded struct to describe a single process' memory usage.

    C Name: mem
    C Location: photoproc.h
    C Parent: pstat
    """

    _fields_ = [
        ("minflt", count_t),
        ("majflt", count_t),
        ("vexec", count_t),
        ("vmem", count_t),
        ("rmem", count_t),
        ("pmem", count_t),
        ("vgrow", count_t),
        ("rgrow", count_t),
        ("vdata", count_t),
        ("vstack", count_t),
        ("vlibs", count_t),
        ("vswap", count_t),
        ("vlock", count_t),
        ("cfuture", count_t * 7),
    ]


NET = atop_2_3.NET


class GPU(ctypes.Structure):
    """Embedded struct to describe a single process' GPU usage.

    C Name: gpu
    C Location: photoproc.h
    C Parent: tstat
    """

    _fields_ = [
        ("state", ctypes.c_char),
        ("bfuture", ctypes.c_char * 3),
        ("nrgpus", ctypes.c_short),
        ("gpulist", ctypes.c_int32),
        ("gpubusy", ctypes.c_int),
        ("membusy", ctypes.c_int),
        ("timems", count_t),
        ("memnow", count_t),
        ("memcum", count_t),
        ("sample", count_t),
        ("cfuture", count_t * 3),
    ]


class TStat(ctypes.Structure):
    """Top level struct to describe multiple statistic categories per task/process.

    C Name: tstat
    C Location: photoproc.h
    """

    _fields_ = [
        ("gen", GEN),
        ("cpu", CPU),
        ("dsk", DSK),
        ("mem", MEM),
        ("net", NET),
        ("gpu", GPU),
    ]


class CGGen(ctypes.Structure):
    """Embedded struct to describe a single cgroup's general information.

    C Name: cggen
    C Location: cgroups.h
    C Parent: cstat
    """

    _fields_ = [
        ("structlen", ctypes.c_int),
        ("sequence", ctypes.c_int),
        ("parentseq", ctypes.c_int),
        ("depth", ctypes.c_int),
        ("nprocs", ctypes.c_int),
        ("procsbelow", ctypes.c_int),
        ("namelen", ctypes.c_int),
        ("fullnamelen", ctypes.c_int),
        ("ifuture", ctypes.c_int * 4),
        ("namehash", ctypes.c_long),
        ("lfuture", ctypes.c_long * 4),
    ]


class CGConf(ctypes.Structure):
    """Embedded struct to describe a single cgroup's configuration.

    C Name: cgconfg
    C Location: cgroups.h
    C Parent: cstat
    """

    _fields_ = [
        ("cpuweight", ctypes.c_int),
        ("cpumax", ctypes.c_int),
        ("memmax", count_t),
        ("swpmax", count_t),
        ("dskweight", ctypes.c_int),
        ("ifuture", ctypes.c_int * 5),
        ("cfuture", count_t * 5),
    ]


class CGCPU(ctypes.Structure):
    """Embedded struct to describe a single cgroup's processor usage.

    C Name: cgcpu
    C Location: cgroups.h
    C Parent: cstat
    """

    _fields_ = [
        ("utime", count_t),
        ("stime", count_t),
        ("somepres", count_t),
        ("fullpres", count_t),
        ("cfuture", count_t * 5),
    ]


class CGMem(ctypes.Structure):
    """Embedded struct to describe a single cgroup's memory usage.

    C Name: cgmem
    C Location: cgroups.h
    C Parent: cstat
    """

    _fields_ = [
        ("current", count_t),
        ("anon", count_t),
        ("file", count_t),
        ("kernel", count_t),
        ("shmem", count_t),
        ("somepres", count_t),
        ("fullpres", count_t),
        ("cfuture", count_t * 5),
    ]


class CGDSK(ctypes.Structure):
    """Embedded struct to describe a single cgroup's disk usage.

    C Name: cgdsk
    C Location: cgroups.h
    C Parent: cstat
    """

    _fields_ = [
        ("rbytes", count_t),
        ("wbytes", count_t),
        ("rios", count_t),
        ("wios", count_t),
        ("somepres", count_t),
        ("fullpres", count_t),
        ("cfuture", count_t * 5),
    ]


class CStat(ctypes.Structure):
    """Top level struct to describe general info and metrics per cgroup.

    C Name: cstat
    C Location: cgroups.h
    """

    _fields_ = [
        ("gen", CGGen),
        ("conf", CGConf),
        ("cpu", CGCPU),
        ("mem", CGMem),
        ("dsk", CGDSK),
        ("cgname", ctypes.c_char * 0),  # Variable length character array. N.B. Currently unsupported.
    ]


class CGChainer:
    """Structure used to track a pidlist with a cgroup.

    Due to the variable length of cgchainer values, this is a custom C struct like object, rather than a true C struct.
    It provides the same basic attributes as a C struct to allow conversion to JSON.

    C Name: cgchainer
    C Location: cgroups.h
    """

    _fields_ = [
        ("cstat", CStat),
        ("proclist", ctypes.Array[pid_t]),
    ]

    def __init__(self, cstat: CStat, proclist: ctypes.Array[pid_t]) -> None:
        """Initialize the starting CGroup chain values.

        Args:
            cstat: The primary cstat information applicable to the processes.
            proclist: Process IDs contained in the cgroup.
        """
        self.cstat = cstat
        self.proclist = proclist


class Header(ctypes.Structure, HeaderMixin):
    """Top level struct to describe information about the system running Atop and the log file itself.

    C Name: rawheader
    C Location: rawlog.h
    """

    _fields_ = [
        ("magic", ctypes.c_uint),
        ("aversion", ctypes.c_ushort),
        ("future1", ctypes.c_ushort),
        ("future2", ctypes.c_ushort),
        ("rawheadlen", ctypes.c_ushort),
        ("rawreclen", ctypes.c_ushort),
        ("hertz", ctypes.c_ushort),
        ("pidwidth", ctypes.c_ushort),
        ("sfuture", ctypes.c_ushort * 5),
        ("sstatlen", ctypes.c_uint),
        ("tstatlen", ctypes.c_uint),
        ("utsname", UTSName),
        ("cfuture", ctypes.c_char * 8),
        ("pagesize", ctypes.c_uint),
        ("supportflags", ctypes.c_int),
        ("osrel", ctypes.c_int),
        ("osvers", ctypes.c_int),
        ("ossub", ctypes.c_int),
        ("cstatlen", ctypes.c_int),
        ("ifuture", ctypes.c_int * 5),
    ]
    supported_version = "2.11"
    Record = Record
    SStat = SStat
    TStat = TStat
    CStat = CStat
    CGChainer = CGChainer

"""Structs and definitions used serialize/deserialize Atop statistics directly from log files.

Structs are declared in a way that will help provide as close to a 1 to 1 match as possible for debuggability
and maintenance. The _fields_ of every struct match their original name, however the struct names have been updated
to match python CamelCase standards. Each struct includes the following to help identify the original source:
    C Name: utsname
    C Location: sys/utsname.h

Struct ordering matches the C source to help with comparisons.
If structs match exactly from a previous version, they are reused via aliasing.

See https://github.com/Atoptool/atop for more information and full details about each field.
Using schemas and structs from Atop 2.6.0.
"""

import ctypes

from atoparser.structs import atop_1_26
from atoparser.structs import atop_2_3
from atoparser.structs import atop_2_4
from atoparser.structs.shared import count_t

# Disable the following pylint warnings to allow the variables and classes to match the style from the C.
# This helps with maintainability and cross-referencing.
# pylint: disable=invalid-name,too-few-public-methods

# Definitions from atop.h
ACCTACTIVE = 0x00000001
IOSTAT = 0x00000004
NETATOP = 0x00000010
NETATOPD = 0x00000020
DOCKSTAT = 0x00000040
GPUSTAT = 0x00000080

# Definitions from photoproc.h
PNAMLEN = 15
CMDLEN = 255

# Definitions from photosyst.h
MAXCPU = 2048
MAXDSK = 1024
MAXLVM = 2048
MAXMDD = 256
MAXINTF = 128
MAXCONTAINER = 128
MAXNFSMOUNT = 64
MAXIBPORT = 32
MAXGPU = 32
MAXGPUBUS = 12
MAXGPUTYPE = 12
MAXDKNAM = 32
MAXIBNAME = 12


Record = atop_2_3.Record


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
        ("commitlim", count_t),
        ("committed", count_t),
        ("shmem", count_t),
        ("shmrss", count_t),
        ("shmswp", count_t),
        ("slabreclaim", count_t),
        ("tothugepage", count_t),
        ("freehugepage", count_t),
        ("hugepagesz", count_t),
        ("vmwballoon", count_t),
        ("zfsarcsize", count_t),
        ("swapcached", count_t),
        ("cfuture", count_t * 6),
    ]


FreqCnt = atop_1_26.FreqCnt


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
        ("instr", count_t),
        ("cycle", count_t),
        ("cfuture", count_t * 2),
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


PerDSK = atop_2_3.PerDSK


DSKStat = atop_2_3.DSKStat


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


PerIFB = atop_2_4.PerIFB


IFBStat = atop_2_4.IFBStat


IPv4Stats = atop_1_26.IPv4Stats


ICMPv4Stats = atop_1_26.ICMPv4Stats


UDPv4Stats = atop_1_26.UDPv4Stats


TCPStats = atop_1_26.TCPStats


IPv6Stats = atop_1_26.IPv6Stats


ICMPv6Stats = atop_1_26.ICMPv6Stats


UDPv6Stats = atop_1_26.UDPv6Stats


NETStat = atop_1_26.NETStat


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
        ("nfs", NFSStat),
        ("cfs", ContStat),
        ("psi", Pressure),
        ("gpu", GPUStat),
        ("ifb", IFBStat),
        ("www", WWWStat),
    ]


GEN = atop_2_3.GEN


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
        ("wchan", ctypes.c_char * 16),
        ("rundelay", count_t),
        ("cfuture", count_t),
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
        ("cfuture", count_t * 3),
    ]


NET = atop_2_3.NET


GPU = atop_2_4.GPU


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


class Header(atop_2_3.Header):
    """Top level struct to describe information about the system running Atop and the log file itself."""

    supported_version = "2.6"
    Record = Record
    SStat = SStat
    TStat = TStat
    CStat = None
    CGChainer = None

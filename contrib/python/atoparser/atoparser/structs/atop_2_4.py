"""Structs and definitions used serialize/deserialize Atop statistics directly from log files.

Structs are declared in a way that will help provide as close to a 1 to 1 match as possible for debuggability
and maintenance. The _fields_ of every struct match their original name, however the struct names have been updated
to match python CamelCase standards. Each struct includes the following to help identify the original source:
    C Name: utsname
    C Location: sys/utsname.h

Struct ordering matches the C source to help with comparisons.
If structs match exactly from a previous version, they are reused via aliasing.

See https://github.com/Atoptool/atop for more information and full details about each field.
Using schemas and structs from Atop 2.4.0.
"""

import ctypes

from atoparser.structs import atop_1_26
from atoparser.structs import atop_2_3
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


MemStat = atop_2_3.MemStat


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


class PSI(ctypes.Structure):
    """Embedded struct to describe average pressure.

    C Name: psi
    C Location: photosyst.h
    C Parent: pressure
    """

    _fields_ = [
        ("avg10", ctypes.c_float),
        ("avg60", ctypes.c_float),
        ("avg300", ctypes.c_float),
        ("total", count_t),
    ]


class Pressure(ctypes.Structure):
    """Embedded struct to describe pressure stats.

    C Name: pressure
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("present", ctypes.c_char),
        ("future", ctypes.c_char * 3),
        ("cpusome", PSI),
        ("memsome", PSI),
        ("memfull", PSI),
        ("iosome", PSI),
        ("iofull", PSI),
    ]


PerContainer = atop_2_3.PerContainer


ContStat = atop_2_3.ContStat


WWWStat = atop_1_26.WWWStat


class PerGPU(ctypes.Structure):
    """Embedded struct to describe per GPU statistics.

    C Name: pergpu
    C Location: photosyst.h
    C Parent: gpustat
    """

    _fields_ = [
        ("taskstats", ctypes.c_char),
        ("nrprocs", ctypes.c_uint8),
        ("type", ctypes.c_char * (MAXGPUTYPE + 1)),
        ("busid", ctypes.c_char * (MAXGPUBUS + 1)),
        ("gpunr", ctypes.c_int),
        ("gpupercnow", ctypes.c_int),
        ("mempercnow", ctypes.c_int),
        ("memtotnow", count_t),
        ("memusenow", count_t),
        ("samples", count_t),
        ("gpuperccum", count_t),
        ("memperccum", count_t),
        ("memusecum", count_t),
    ]


class GPUStat(ctypes.Structure):
    """Embedded struct to describe overall GPU statistics.

    C Name: gpustat
    C Location: photosyst.h
    C Parent: sstat
    """

    _fields_ = [
        ("nrgpus", ctypes.c_int),
        ("gpu", PerGPU * MAXGPU),
    ]
    fields_limiters = {
        "gpu": "nrgpus",
    }


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


CPU = atop_1_26.CPU


DSK = atop_1_26.DSK


MEM = atop_2_3.MEM


NET = atop_2_3.NET


class GPU(ctypes.Structure):
    """Embedded struct to describe a single process' GPU usage.

    C Name: gpu
    C Location: photoproc.h
    C Parent: tstat
    """

    _fields_ = [
        ("state", ctypes.c_char),
        ("cfuture", ctypes.c_char * 3),
        ("nrgpus", ctypes.c_short),
        ("gpulist", ctypes.c_int32),
        ("gpubusy", ctypes.c_int),
        ("membusy", ctypes.c_int),
        ("timems", count_t),
        ("memnow", count_t),
        ("memcum", count_t),
        ("sample", count_t),
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


class Header(atop_2_3.Header):
    """Top level struct to describe information about the system running Atop and the log file itself."""

    supported_version = "2.4"
    Record = Record
    SStat = SStat
    TStat = TStat
    CStat = None
    CGChainer = None

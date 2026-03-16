"""Structs and definitions used serialize/deserialize Atop statistics directly from log files.

Structs are declared in a way that will help provide as close to a 1 to 1 match as possible for debuggability
and maintenance. The _fields_ of every struct match their original name, however the struct names have been updated
to match python CamelCase standards. Each struct includes the following to help identify the original source:
    C Name: utsname
    C Location: sys/utsname.h

Struct ordering matches the C source to help with comparisons.
If structs match exactly from a previous version, they are reused via aliasing.

See https://github.com/Atoptool/atop for more information and full details about each field.
Using schemas and structs from Atop 2.9.0.
"""

import ctypes

from atoparser.structs import atop_1_26
from atoparser.structs import atop_2_3
from atoparser.structs import atop_2_4
from atoparser.structs import atop_2_7
from atoparser.structs import atop_2_8
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
CGROUPV2 = 0x00000100

# Definitions from photoproc.h
PNAMLEN = 15
CMDLEN = 255
CGRLEN = 64

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


Record = atop_2_8.Record


MemStat = atop_2_8.MemStat


MemPerNUMA = atop_2_8.MemPerNUMA


MemNUMA = atop_2_8.MemNUMA


CPUPerNUMA = atop_2_8.CPUPerNUMA


CPUNUMA = atop_2_8.CPUNUMA


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


PerIFB = atop_2_4.PerIFB


IFBStat = atop_2_4.IFBStat


PerLLC = atop_2_8.PerLLC


LLCStat = atop_2_8.LLCStat


IPv4Stats = atop_1_26.IPv4Stats


ICMPv4Stats = atop_2_8.ICMPv4Stats


UDPv4Stats = atop_1_26.UDPv4Stats


TCPStats = atop_1_26.TCPStats


IPv6Stats = atop_1_26.IPv6Stats


ICMPv6Stats = atop_1_26.ICMPv6Stats


UDPv6Stats = atop_1_26.UDPv6Stats


NETStat = atop_1_26.NETStat


SStat = atop_2_8.SStat


GEN = atop_2_8.GEN


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
        ("cgcpuweight", ctypes.c_int),
        ("cgcpumax", ctypes.c_int),
        ("cgcpumaxr", ctypes.c_int),
        ("ifuture", ctypes.c_int * 3),
        ("wchan", ctypes.c_char * 16),
        ("rundelay", count_t),
        ("blkdelay", count_t),
        ("nvcsw", count_t),
        ("nivcsw", count_t),
        ("cfuture", count_t * 1),
    ]


DSK = atop_1_26.DSK


MEM = atop_2_8.MEM


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


class Header(atop_2_8.Header):
    """Top level struct to describe information about the system running Atop and the log file itself."""

    supported_version = "2.9"
    Record = Record
    SStat = SStat
    TStat = TStat
    CStat = None
    CGChainer = None

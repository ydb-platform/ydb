# -*- coding: utf-8 -*-
"""VISA VPP-4.3 data types (VPP-4.3.2 spec, section 3) using ctypes constants.

This file is part of PyVISA.

All data types that are defined by VPP-4.3.2.

The module exports all data types including the pointer and array types.  This
means "ViUInt32" and such.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import ctypes as _ctypes
import sys

from .cthelper import FUNCTYPE

# Part One: Type Assignments for VISA and Instrument Drivers, see spec table
# 3.1.1.
#
# Remark: The pointer and probably also the array variants are of no
# significance in Python because there is no native call-by-reference.
# However, as long as I'm not fully sure about this, they won't hurt.


def _type_pair(ctypes_type):
    return ctypes_type, _ctypes.POINTER(ctypes_type)


def _type_triplet(ctypes_type):
    return (*_type_pair(ctypes_type), _ctypes.POINTER(ctypes_type))


ViUInt64, ViPUInt64, ViAUInt64 = _type_triplet(_ctypes.c_uint64)
ViInt64, ViPInt64, ViAInt64 = _type_triplet(_ctypes.c_int64)
ViUInt32, ViPUInt32, ViAUInt32 = _type_triplet(_ctypes.c_uint32)
ViInt32, ViPInt32, ViAInt32 = _type_triplet(_ctypes.c_int32)
ViUInt16, ViPUInt16, ViAUInt16 = _type_triplet(_ctypes.c_ushort)
ViInt16, ViPInt16, ViAInt16 = _type_triplet(_ctypes.c_short)
ViUInt8, ViPUInt8, ViAUInt8 = _type_triplet(_ctypes.c_ubyte)
ViInt8, ViPInt8, ViAInt8 = _type_triplet(_ctypes.c_byte)
ViAddr, ViPAddr, ViAAddr = _type_triplet(_ctypes.c_void_p)
ViChar, ViPChar, ViAChar = _type_triplet(_ctypes.c_char)
ViByte, ViPByte, ViAByte = _type_triplet(_ctypes.c_ubyte)
ViBoolean, ViPBoolean, ViABoolean = _type_triplet(ViUInt16)
ViReal32, ViPReal32, ViAReal32 = _type_triplet(_ctypes.c_float)
ViReal64, ViPReal64, ViAReal64 = _type_triplet(_ctypes.c_double)


class ViString(object):
    @classmethod
    def from_param(cls, obj):
        if isinstance(obj, str):
            return bytes(obj, "ascii")
        return obj


class ViAString(object):
    @classmethod
    def from_param(cls, obj):
        return _ctypes.POINTER(obj)


ViPString = ViString

# This follows visa.h definition, but involves a lot of manual conversion.
# ViBuf, ViPBuf, ViABuf = ViPByte, ViPByte, _ctypes.POINTER(ViPByte)

ViBuf, ViPBuf, ViABuf = ViPString, ViPString, ViAString


def buffer_to_text(buf) -> str:
    return buf.value.decode("ascii")


ViRsrc = ViString
ViPRsrc = ViString
ViARsrc = ViAString

ViKeyId, ViPKeyId = ViString, ViPString

ViStatus, ViPStatus, ViAStatus = _type_triplet(ViInt32)
ViVersion, ViPVersion, ViAVersion = _type_triplet(ViUInt32)
_ViObject, ViPObject, ViAObject = _type_triplet(ViUInt32)
_ViSession, ViPSession, ViASession = _type_triplet(ViUInt32)


class ViObject(_ViObject):  # type: ignore
    @classmethod
    def from_param(cls, obj):
        if obj is None:
            raise ValueError("Session cannot be None. The resource might be closed.")
        return _ViObject.from_param(obj)


ViSession = ViObject


ViAttr = ViUInt32
ViConstString = _ctypes.POINTER(ViChar)


# Part Two: Type Assignments for VISA only, see spec table 3.1.2.  The
# difference to the above is of no significance in Python, so I use it here
# only for easier synchronisation with the spec.
is_64bit = sys.maxsize > 2**32

ViAccessMode, ViPAccessMode = _type_pair(ViUInt32)
ViBusAddress, ViPBusAddress = _type_pair(ViUInt64) if is_64bit else _type_pair(ViUInt32)
ViBusAddress64, ViPBusAddress64 = _type_pair(ViUInt64)

ViBusSize = ViUInt64 if is_64bit else ViUInt32
ViBusSize64 = ViUInt64

ViAttrState, ViPAttrState = _type_pair(ViUInt64) if is_64bit else _type_pair(ViUInt32)

# The following is weird, taken from news:zn2ek2w2.fsf@python.net
ViVAList = _ctypes.POINTER(_ctypes.c_char)

ViEventType, ViPEventType, ViAEventType = _type_triplet(ViUInt32)

ViPAttr = _ctypes.POINTER(ViAttr)
ViAAttr = ViPAttr

ViEventFilter = ViUInt32

ViFindList, ViPFindList = _type_pair(ViObject)
ViEvent, ViPEvent = _type_pair(ViObject)
ViJobId, ViPJobId = _type_pair(ViUInt32)

# Class of callback functions for event handling, first type is result type
ViHndlr = FUNCTYPE(ViStatus, ViSession, ViEventType, ViEvent, ViAddr)

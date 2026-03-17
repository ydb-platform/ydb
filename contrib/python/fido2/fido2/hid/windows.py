# Original work Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modified work Copyright 2020 Yubico AB. All Rights Reserved.
# This file, with modifications, is licensed under the above Apache License.

from __future__ import annotations

from .base import HidDescriptor, CtapHidConnection, FIDO_USAGE_PAGE, FIDO_USAGE

from ctypes import wintypes, LibraryLoader
from typing import Dict, cast

import ctypes
import platform
import logging
import sys

# Only typecheck this file on Windows
assert sys.platform == "win32"  # nosec
from ctypes import WinDLL, WinError  # noqa: E402

logger = logging.getLogger(__name__)


# Load relevant DLLs
windll = LibraryLoader(WinDLL)
hid = windll.Hid
setupapi = windll.SetupAPI
kernel32 = windll.Kernel32


# Various structs that are used in the Windows APIs we call
class GUID(ctypes.Structure):
    _fields_ = [
        ("Data1", ctypes.c_ulong),
        ("Data2", ctypes.c_ushort),
        ("Data3", ctypes.c_ushort),
        ("Data4", ctypes.c_ubyte * 8),
    ]


# On Windows, SetupAPI.h packs structures differently in 64bit and
# 32bit mode.  In 64-bit mode, the structures are packed on 8 byte
# boundaries, while in 32-bit mode, they are packed on 1 byte boundaries.
# This is important to get right for some API calls that fill out these
# structures.
if platform.architecture()[0] == "64bit":
    SETUPAPI_PACK = 8
elif platform.architecture()[0] == "32bit":
    SETUPAPI_PACK = 1
else:
    raise OSError(f"Unknown architecture: {platform.architecture()[0]}")


class DeviceInterfaceData(ctypes.Structure):
    _fields_ = [
        ("cbSize", wintypes.DWORD),
        ("InterfaceClassGuid", GUID),
        ("Flags", wintypes.DWORD),
        ("Reserved", ctypes.POINTER(ctypes.c_ulong)),
    ]
    _pack_ = SETUPAPI_PACK


class DeviceInterfaceDetailData(ctypes.Structure):
    _fields_ = [("cbSize", wintypes.DWORD), ("DevicePath", ctypes.c_byte * 1)]
    _pack_ = SETUPAPI_PACK


class HidAttributes(ctypes.Structure):
    _fields_ = [
        ("Size", ctypes.c_ulong),
        ("VendorID", ctypes.c_ushort),
        ("ProductID", ctypes.c_ushort),
        ("VersionNumber", ctypes.c_ushort),
    ]


class HidCapabilities(ctypes.Structure):
    _fields_ = [
        ("Usage", ctypes.c_ushort),
        ("UsagePage", ctypes.c_ushort),
        ("InputReportByteLength", ctypes.c_ushort),
        ("OutputReportByteLength", ctypes.c_ushort),
        ("FeatureReportByteLength", ctypes.c_ushort),
        ("Reserved", ctypes.c_ushort * 17),
        ("NotUsed", ctypes.c_ushort * 10),
    ]


# Various void* aliases for readability.
HDEVINFO = ctypes.c_void_p
HANDLE = ctypes.c_void_p
PHIDP_PREPARSED_DATA = ctypes.c_void_p  # pylint: disable=invalid-name

# This is a HANDLE.
# INVALID_HANDLE_VALUE = 0xFFFFFFFF
INVALID_HANDLE_VALUE = (1 << 8 * ctypes.sizeof(ctypes.c_void_p)) - 1

# Status codes
FILE_SHARE_READ = 0x00000001
FILE_SHARE_WRITE = 0x00000002
OPEN_EXISTING = 0x03
NTSTATUS = ctypes.c_long
HIDP_STATUS_SUCCESS = 0x00110000

# CreateFile Flags
GENERIC_WRITE = 0x40000000
GENERIC_READ = 0x80000000

DIGCF_DEVICEINTERFACE = 0x10
DIGCF_PRESENT = 0x02

# Function signatures
hid.HidD_GetHidGuid.restype = None
hid.HidD_GetHidGuid.argtypes = [ctypes.POINTER(GUID)]
hid.HidD_GetAttributes.restype = wintypes.BOOLEAN
hid.HidD_GetAttributes.argtypes = [HANDLE, ctypes.POINTER(HidAttributes)]
hid.HidD_GetPreparsedData.restype = wintypes.BOOLEAN
hid.HidD_GetPreparsedData.argtypes = [HANDLE, ctypes.POINTER(PHIDP_PREPARSED_DATA)]
hid.HidD_FreePreparsedData.restype = wintypes.BOOLEAN
hid.HidD_FreePreparsedData.argtypes = [PHIDP_PREPARSED_DATA]
hid.HidD_GetProductString.restype = wintypes.BOOLEAN
hid.HidD_GetProductString.argtypes = [HANDLE, ctypes.c_void_p, ctypes.c_ulong]
hid.HidD_GetSerialNumberString.restype = wintypes.BOOLEAN
hid.HidD_GetSerialNumberString.argtypes = [HANDLE, ctypes.c_void_p, ctypes.c_ulong]
hid.HidP_GetCaps.restype = NTSTATUS
hid.HidP_GetCaps.argtypes = [PHIDP_PREPARSED_DATA, ctypes.POINTER(HidCapabilities)]


hid.HidD_GetFeature.restype = wintypes.BOOL
hid.HidD_GetFeature.argtypes = [HANDLE, ctypes.c_void_p, ctypes.c_ulong]
hid.HidD_SetFeature.restype = wintypes.BOOL
hid.HidD_SetFeature.argtypes = [HANDLE, ctypes.c_void_p, ctypes.c_ulong]

setupapi.SetupDiGetClassDevsA.argtypes = [
    ctypes.POINTER(GUID),
    ctypes.c_char_p,
    wintypes.HWND,
    wintypes.DWORD,
]
setupapi.SetupDiGetClassDevsA.restype = HDEVINFO
setupapi.SetupDiEnumDeviceInterfaces.restype = wintypes.BOOL
setupapi.SetupDiEnumDeviceInterfaces.argtypes = [
    HDEVINFO,
    ctypes.c_void_p,
    ctypes.POINTER(GUID),
    wintypes.DWORD,
    ctypes.POINTER(DeviceInterfaceData),
]
setupapi.SetupDiGetDeviceInterfaceDetailA.restype = wintypes.BOOL
setupapi.SetupDiGetDeviceInterfaceDetailA.argtypes = [
    HDEVINFO,
    ctypes.POINTER(DeviceInterfaceData),
    ctypes.POINTER(DeviceInterfaceDetailData),
    wintypes.DWORD,
    ctypes.POINTER(wintypes.DWORD),
    ctypes.c_void_p,
]
setupapi.SetupDiDestroyDeviceInfoList.restype = wintypes.BOOL
setupapi.SetupDiDestroyDeviceInfoList.argtypes = [
    HDEVINFO,
]

kernel32.CreateFileA.restype = HANDLE
kernel32.CreateFileA.argtypes = [
    ctypes.c_char_p,
    wintypes.DWORD,
    wintypes.DWORD,
    ctypes.c_void_p,
    wintypes.DWORD,
    wintypes.DWORD,
    HANDLE,
]
kernel32.CloseHandle.restype = wintypes.BOOL
kernel32.CloseHandle.argtypes = [HANDLE]


class WinCtapHidConnection(CtapHidConnection):
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.handle = kernel32.CreateFileA(
            descriptor.path,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            0,
            None,
        )
        if self.handle == INVALID_HANDLE_VALUE:
            raise WinError()

    def close(self):
        kernel32.CloseHandle(self.handle)

    def write_packet(self, packet):
        out = b"\0" + packet  # Prepend report ID
        num_written = wintypes.DWORD()
        ret = kernel32.WriteFile(
            self.handle, out, len(out), ctypes.byref(num_written), None
        )
        if not ret:
            raise WinError()
        if num_written.value != len(out):
            raise OSError(
                "Failed to write complete packet.  "
                + "Expected %d, but got %d" % (len(out), num_written.value)
            )

    def read_packet(self):
        buf = ctypes.create_string_buffer(self.descriptor.report_size_in + 1)
        num_read = wintypes.DWORD()
        ret = kernel32.ReadFile(
            self.handle, buf, len(buf), ctypes.byref(num_read), None
        )
        if not ret:
            raise WinError()

        if num_read.value != self.descriptor.report_size_in + 1:
            raise OSError("Failed to read full length report from device.")

        return buf.raw[1:]  # Strip report ID


def get_vid_pid(device):
    attributes = HidAttributes()
    result = hid.HidD_GetAttributes(device, ctypes.byref(attributes))
    if not result:
        raise WinError()

    return attributes.VendorID, attributes.ProductID


def get_product_name(device):
    buf = ctypes.create_unicode_buffer(128)

    result = hid.HidD_GetProductString(device, buf, ctypes.c_ulong(ctypes.sizeof(buf)))
    if not result:
        return None

    return buf.value


def get_serial(device):
    buf = ctypes.create_unicode_buffer(128)

    result = hid.HidD_GetSerialNumberString(
        device, buf, ctypes.c_ulong(ctypes.sizeof(buf))
    )
    if not result:
        return None

    return buf.value


def get_descriptor(path):
    device = kernel32.CreateFileA(
        path,
        0,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_EXISTING,
        0,
        None,
    )
    if device == INVALID_HANDLE_VALUE:
        raise WinError()
    try:
        preparsed_data = PHIDP_PREPARSED_DATA(0)
        ret = hid.HidD_GetPreparsedData(device, ctypes.byref(preparsed_data))
        if not ret:
            raise WinError()

        try:
            caps = HidCapabilities()
            ret = hid.HidP_GetCaps(preparsed_data, ctypes.byref(caps))

            if ret != HIDP_STATUS_SUCCESS:
                raise WinError()

            if caps.UsagePage == FIDO_USAGE_PAGE and caps.Usage == FIDO_USAGE:
                vid, pid = get_vid_pid(device)
                product_name = get_product_name(device)
                serial = get_serial(device)
                # Sizes here include 1-byte report ID, which we need to remove.
                size_in = caps.InputReportByteLength - 1
                size_out = caps.OutputReportByteLength - 1
                return HidDescriptor(
                    path, vid, pid, size_in, size_out, product_name, serial
                )
            raise ValueError("Not a CTAP device")

        finally:
            hid.HidD_FreePreparsedData(preparsed_data)
    finally:
        kernel32.CloseHandle(device)


def open_connection(descriptor):
    return WinCtapHidConnection(descriptor)


_SKIP = cast(HidDescriptor, object())
_descriptor_cache: Dict[bytes, HidDescriptor] = {}


def list_descriptors():
    stale = set(_descriptor_cache)
    descriptors = []

    hid_guid = GUID()
    hid.HidD_GetHidGuid(ctypes.byref(hid_guid))

    collection = setupapi.SetupDiGetClassDevsA(
        ctypes.byref(hid_guid), None, None, DIGCF_DEVICEINTERFACE | DIGCF_PRESENT
    )
    try:
        index = 0
        interface_info = DeviceInterfaceData()
        interface_info.cbSize = ctypes.sizeof(DeviceInterfaceData)

        while True:
            result = setupapi.SetupDiEnumDeviceInterfaces(
                collection,
                0,
                ctypes.byref(hid_guid),
                index,
                ctypes.byref(interface_info),
            )
            index += 1
            if not result:
                break

            dw_detail_len = wintypes.DWORD()
            result = setupapi.SetupDiGetDeviceInterfaceDetailA(
                collection,
                ctypes.byref(interface_info),
                None,
                0,
                ctypes.byref(dw_detail_len),
                None,
            )
            if result:
                raise WinError()

            detail_len = dw_detail_len.value
            if detail_len == 0:
                # skip this device, some kind of error
                continue

            buf = ctypes.create_string_buffer(detail_len)
            interface_detail = DeviceInterfaceDetailData.from_buffer(buf)
            interface_detail.cbSize = ctypes.sizeof(DeviceInterfaceDetailData)

            result = setupapi.SetupDiGetDeviceInterfaceDetailA(
                collection,
                ctypes.byref(interface_info),
                ctypes.byref(interface_detail),
                detail_len,
                None,
                None,
            )
            if not result:
                raise WinError()

            path = ctypes.string_at(interface_detail.DevicePath)
            stale.discard(path)

            # Check if path already cached
            desc = _descriptor_cache.get(path)
            if desc:
                if desc is not _SKIP:
                    descriptors.append(desc)
                continue

            try:
                descriptor = get_descriptor(path)
                _descriptor_cache[path] = descriptor
                descriptors.append(descriptor)
                continue
            except ValueError:
                pass  # Not a CTAP device
            except Exception:
                logger.debug(
                    "Failed reading HID descriptor for %s", path, exc_info=True
                )
            _descriptor_cache[path] = _SKIP
    finally:
        setupapi.SetupDiDestroyDeviceInfoList(collection)

    # Remove entries from the cache that were not seen
    for path in stale:
        del _descriptor_cache[path]

    return descriptors

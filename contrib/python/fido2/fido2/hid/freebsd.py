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

# FreeBSD HID driver.
#
# There are two options to access UHID on FreeBSD:
#
# hidraw(4) - New method, not enabled by default
#             on FreeBSD 13.x and earlier
# uhid(4) - Classic method, default option on
#           FreeBSD 13.x and earlier
#
# hidraw is available since FreeBSD 13 and can be activated by adding
# `hw.usb.usbhid.enable="1"` to `/boot/loader.conf`. The actual kernel
# module is loaded with `kldload hidraw`.

from __future__ import annotations

from ctypes.util import find_library
import ctypes
import fcntl
import glob
import re
import struct
import os
from array import array

from .base import HidDescriptor, parse_report_descriptor, FileCtapHidConnection

import logging
import sys
from typing import Dict, Optional, Set, Union

# Don't typecheck this file on Windows
assert sys.platform != "win32"  # nosec

logger = logging.getLogger(__name__)


devdir = "/dev/"

vendor_re = re.compile("vendor=(0x[0-9a-fA-F]+)")
product_re = re.compile("product=(0x[0-9a-fA-F]+)")
sernum_re = re.compile('sernum="([^"]+)')

libc = ctypes.CDLL(find_library("c"))

# /usr/include/dev/usb/usb_ioctl.h
USB_GET_REPORT_DESC = 0xC0205515

# /usr/include/dev/hid/hidraw.h>
HIDIOCGRAWINFO = 0x40085520
HIDIOCGRDESC = 0x2000551F
HIDIOCGRDESCSIZE = 0x4004551E
HIDIOCGRAWNAME_128 = 0x40805521
HIDIOCGRAWUNIQ_64 = 0x40405525


class usb_gen_descriptor(ctypes.Structure):
    _fields_ = [
        (
            "ugd_data",
            ctypes.c_void_p,
        ),  # TODO: check what COMPAT_32BIT in C header means
        ("ugd_lang_id", ctypes.c_uint16),
        ("ugd_maxlen", ctypes.c_uint16),
        ("ugd_actlen", ctypes.c_uint16),
        ("ugd_offset", ctypes.c_uint16),
        ("ugd_config_index", ctypes.c_uint8),
        ("ugd_string_index", ctypes.c_uint8),
        ("ugd_iface_index", ctypes.c_uint8),
        ("ugd_altif_index", ctypes.c_uint8),
        ("ugd_endpt_index", ctypes.c_uint8),
        ("ugd_report_type", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 8),
    ]


class HidrawCtapHidConnection(FileCtapHidConnection):
    def write_packet(self, packet):
        # Prepend the report ID
        super(HidrawCtapHidConnection, self).write_packet(b"\0" + packet)


def open_connection(descriptor):
    if descriptor.path.find(devdir + "hidraw") == 0:
        return HidrawCtapHidConnection(descriptor)
    else:
        return FileCtapHidConnection(descriptor)


def _get_report_data(fd, report_type):
    data = ctypes.create_string_buffer(4096)
    desc = usb_gen_descriptor(
        ugd_data=ctypes.addressof(data),
        ugd_maxlen=ctypes.sizeof(data),
        ugd_report_type=report_type,
    )
    ret = libc.ioctl(fd, USB_GET_REPORT_DESC, ctypes.byref(desc))
    if ret != 0:
        raise ValueError("ioctl failed")
    return data.raw[: desc.ugd_actlen]


def _read_descriptor(vid, pid, name, serial, path):
    fd = os.open(path, os.O_RDONLY)
    data = _get_report_data(fd, 3)
    os.close(fd)
    max_in_size, max_out_size = parse_report_descriptor(data)
    return HidDescriptor(path, vid, pid, max_in_size, max_out_size, name, serial)


def _enumerate():
    for uhid in glob.glob(devdir + "uhid?*"):
        index = uhid[len(devdir) + len("uhid") :]
        if not index.isdigit():
            continue

        pnpinfo = ("dev.uhid." + index + ".%pnpinfo").encode()
        desc = ("dev.uhid." + index + ".%desc").encode()

        ovalue = ctypes.create_string_buffer(1024)
        olen = ctypes.c_size_t(ctypes.sizeof(ovalue))
        key = ctypes.c_char_p(pnpinfo)
        retval = libc.sysctlbyname(key, ovalue, ctypes.byref(olen), None, None)
        if retval != 0:
            continue

        dev: Dict[str, Optional[Union[str, int]]] = {}
        dev["name"] = uhid[len(devdir) :]
        dev["path"] = uhid

        value = ovalue.value[: olen.value].decode()
        m = vendor_re.search(value)
        dev["vendor_id"] = int(m.group(1), 16) if m else None

        m = product_re.search(value)
        dev["product_id"] = int(m.group(1), 16) if m else None

        m = sernum_re.search(value)
        dev["serial_number"] = m.group(1) if m else None

        key = ctypes.c_char_p(desc)
        retval = libc.sysctlbyname(key, ovalue, ctypes.byref(olen), None, None)
        if retval == 0:
            dev["product_desc"] = ovalue.value[: olen.value].decode() or None

        yield dev


def get_hidraw_descriptor(path):
    with open(path, "rb") as f:
        # Read VID, PID
        buf = array("B", [0] * (4 + 2 + 2))
        fcntl.ioctl(f, HIDIOCGRAWINFO, buf, True)
        _, vid, pid = struct.unpack("<IHH", buf)

        # FreeBSD's hidraw(4) does not return string length for
        # HIDIOCGRAWNAME and HIDIOCGRAWUNIQ, see https://reviews.freebsd.org/D35233

        # Read product
        buf = array("B", [0] * 129)
        fcntl.ioctl(f, HIDIOCGRAWNAME_128, buf, True)
        length = buf.index(0) + 1  # emulate ioctl return value
        name = bytearray(buf[: (length - 1)]).decode("utf-8") if length > 1 else None

        # Read unique ID
        try:
            buf = array("B", [0] * 65)
            fcntl.ioctl(f, HIDIOCGRAWUNIQ_64, buf, True)
            length = buf.index(0) + 1  # emulate ioctl return value
            serial = (
                bytearray(buf[: (length - 1)]).decode("utf-8") if length > 1 else None
            )
        except OSError:
            serial = None

        # Read report descriptor
        buf = array("B", [0] * 4)
        fcntl.ioctl(f, HIDIOCGRDESCSIZE, buf, True)
        size = struct.unpack("<I", buf)[0]
        buf += array("B", [0] * size)
        fcntl.ioctl(f, HIDIOCGRDESC, buf, True)

    data = bytearray(buf[4:])
    max_in_size, max_out_size = parse_report_descriptor(data)
    return HidDescriptor(path, vid, pid, max_in_size, max_out_size, name, serial)


def get_descriptor(path):
    if path.find(devdir + "hidraw") == 0:
        return get_hidraw_descriptor(path)

    for dev in _enumerate():
        if dev["path"] == path:
            vid = dev["vendor_id"]
            pid = dev["product_id"]
            name = dev["product_desc"] or None
            serial = (dev["serial_number"] if "serial_number" in dev else None) or None
            return _read_descriptor(vid, pid, name, serial, path)
    raise ValueError("Device not found")


# Cache for continuously failing devices
_failed_cache: Set[str] = set()


def list_descriptors():
    stale = set(_failed_cache)
    descriptors = []
    for hidraw in glob.glob(devdir + "hidraw?*"):
        stale.discard(hidraw)
        try:
            descriptors.append(get_descriptor(hidraw))
        except ValueError:
            pass  # Not a CTAP device, ignore
        except Exception:
            if hidraw not in _failed_cache:
                logger.debug("Failed opening device %s", hidraw, exc_info=True)
                _failed_cache.add(hidraw)

    if not descriptors:
        for dev in _enumerate():
            path = dev["path"]
            stale.discard(path)
            try:
                name = dev["product_desc"] or None
                serial = (
                    dev["serial_number"] if "serial_number" in dev else None
                ) or None
                descriptors.append(
                    _read_descriptor(
                        dev["vendor_id"],
                        dev["product_id"],
                        name,
                        serial,
                        path,
                    )
                )
            except ValueError:
                pass  # Not a CTAP device, ignore
            except Exception:
                if path not in _failed_cache:
                    logger.debug("Failed opening HID device %s", path, exc_info=True)
                    _failed_cache.add(path)

    # Remove entries from the cache that were not seen
    _failed_cache.difference_update(stale)

    return descriptors

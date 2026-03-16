# Copyright 2016 Google Inc. All Rights Reserved.
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

"""Implements raw HID interface on NetBSD."""

from __future__ import absolute_import

import errno
import logging
import os
import select
import struct
import sys

from ctypes import (
    Structure,
    c_char,
    c_int,
    c_ubyte,
    c_uint16,
    c_uint32,
    c_uint8,
)
from typing import Set

from . import base

# Don't typecheck this file on Windows
assert sys.platform != "win32"  # nosec

from fcntl import ioctl  # noqa: E402

logger = logging.getLogger(__name__)


USB_MAX_DEVNAMELEN = 16
USB_MAX_DEVNAMES = 4
USB_MAX_STRING_LEN = 128
USB_MAX_ENCODED_STRING_LEN = USB_MAX_STRING_LEN * 3


class usb_ctl_report_desc(Structure):
    _fields_ = [
        ("ucrd_size", c_int),
        ("ucrd_data", c_ubyte * 1024),
    ]


class usb_device_info(Structure):
    _fields_ = [
        ("udi_bus", c_uint8),
        ("udi_addr", c_uint8),
        ("udi_pad0", c_uint8 * 2),
        ("udi_cookie", c_uint32),
        ("udi_product", c_char * USB_MAX_ENCODED_STRING_LEN),
        ("udi_vendor", c_char * USB_MAX_ENCODED_STRING_LEN),
        ("udi_release", c_char * 8),
        ("udi_serial", c_char * USB_MAX_ENCODED_STRING_LEN),
        ("udi_productNo", c_uint16),
        ("udi_vendorNo", c_uint16),
        ("udi_releaseNo", c_uint16),
        ("udi_class", c_uint8),
        ("udi_subclass", c_uint8),
        ("udi_protocol", c_uint8),
        ("udi_config", c_uint8),
        ("udi_speed", c_uint8),
        ("udi_pad1", c_uint8),
        ("udi_power", c_int),
        ("udi_nports", c_int),
        ("udi_devnames", c_char * USB_MAX_DEVNAMES * USB_MAX_DEVNAMELEN),
        ("udi_ports", c_uint8 * 16),
    ]


USB_GET_DEVICE_INFO = 0x44F45570  # _IOR('U', 112, struct usb_device_info)
USB_GET_REPORT_DESC = 0x44045515  # _IOR('U', 21, struct usb_ctl_report_desc)
USB_HID_SET_RAW = 0x80046802  # _IOW('h', 2, int)


# Cache for continuously failing devices
# XXX not thread-safe
_failed_cache: Set[str] = set()


def list_descriptors():
    stale = set(_failed_cache)
    descriptors = []

    for i in range(100):
        path = "/dev/uhid%d" % (i,)
        stale.discard(path)
        try:
            desc = get_descriptor(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                break
            if path not in _failed_cache:
                logger.debug("Failed opening FIDO device %s", path, exc_info=True)
                _failed_cache.add(path)
            continue
        except Exception:
            if path not in _failed_cache:
                logger.debug("Failed opening FIDO device %s", path, exc_info=True)
                _failed_cache.add(path)
            continue
        descriptors.append(desc)

    _failed_cache.difference_update(stale)
    return descriptors


def get_descriptor(path):
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
        devinfo = usb_device_info()
        ioctl(fd, USB_GET_DEVICE_INFO, devinfo)
        ucrd = usb_ctl_report_desc()
        ioctl(fd, USB_GET_REPORT_DESC, ucrd)
        report_desc = bytearray(ucrd.ucrd_data[: ucrd.ucrd_size])
        maxin, maxout = base.parse_report_descriptor(report_desc)
        vid = devinfo.udi_vendorNo
        pid = devinfo.udi_productNo
        try:
            name = devinfo.udi_product.decode("utf-8")
        except UnicodeDecodeError:
            name = None
        try:
            serial = devinfo.udi_serial.decode("utf-8")
        except UnicodeDecodeError:
            serial = None
        return base.HidDescriptor(path, vid, pid, maxin, maxout, name, serial)
    finally:
        if fd is not None:
            os.close(fd)


def open_connection(descriptor):
    return NetBSDCtapHidConnection(descriptor)


class NetBSDCtapHidConnection(base.FileCtapHidConnection):
    def __init__(self, descriptor):
        # XXX racy -- device can change identity now that it has been
        # closed
        super().__init__(descriptor)
        try:
            ioctl(self.handle, USB_HID_SET_RAW, struct.pack("@i", 1))
            ping = bytearray(64)
            ping[0:7] = bytearray([0xFF, 0xFF, 0xFF, 0xFF, 0x81, 0, 1])
            for i in range(10):
                self.write_packet(ping)
                poll = select.poll()
                poll.register(self.handle, select.POLLIN)
                if poll.poll(100):
                    self.read_packet()
                    break
            else:
                raise Exception("u2f ping timeout")
        except Exception:
            self.close()
            raise

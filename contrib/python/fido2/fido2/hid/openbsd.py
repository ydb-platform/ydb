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

import fcntl
import select
import os
import os.path
import sys

from ctypes import Structure, c_char, c_int, c_uint8, c_uint16, c_uint32

from .base import HidDescriptor, FileCtapHidConnection

import logging
from typing import Set

# Don't typecheck this file on Windows
assert sys.platform != "win32"  # nosec

logger = logging.getLogger(__name__)

# /usr/include/dev/usb/usb.h
USB_GET_DEVICEINFO = 0x421C5570
USB_MAX_STRING_LEN = 127
USB_MAX_DEVNAMES = 4
USB_MAX_DEVNAMELEN = 16

FIDO_DEVS = "/dev/fido"
MAX_U2F_HIDLEN = 64


class UsbDeviceInfo(Structure):
    _fields_ = [
        ("udi_bus", c_uint8),
        ("udi_addr", c_uint8),
        ("udi_product", c_char * USB_MAX_STRING_LEN),
        ("udi_vendor", c_char * USB_MAX_STRING_LEN),
        ("udi_release", c_char * 8),
        ("udi_productNo", c_uint16),
        ("udi_vendorNo", c_uint16),
        ("udi_releaseNo", c_uint16),
        ("udi_class", c_uint8),
        ("udi_subclass", c_uint8),
        ("udi_protocol", c_uint8),
        ("udi_config", c_uint8),
        ("udi_speed", c_uint8),
        ("udi_power", c_int),
        ("udi_nports", c_int),
        ("udi_devnames", c_char * USB_MAX_DEVNAMELEN * USB_MAX_DEVNAMES),
        ("udi_ports", c_uint32 * 16),
        ("udi_serial", c_char * USB_MAX_STRING_LEN),
    ]


class OpenBsdCtapHidConnection(FileCtapHidConnection):
    def __init__(self, descriptor):
        super().__init__(descriptor)
        try:
            self._terrible_ping_kludge()
        except Exception:
            self.close()
            raise

    def _terrible_ping_kludge(self):
        # This is pulled from
        # https://github.com/Yubico/libfido2/blob/da24193aa901086960f8d31b60d930ebef21f7a2/src/hid_openbsd.c#L128
        for _ in range(4):
            # 1 byte ping
            data = b"\xff\xff\xff\xff\x81\0\1".ljust(
                self.descriptor.report_size_out, b"\0"
            )

            poll = select.poll()
            poll.register(self.handle, select.POLLIN)

            self.write_packet(data)

            poll.poll(100)
            data = self.read_packet()


def open_connection(descriptor):
    return OpenBsdCtapHidConnection(descriptor)


def get_descriptor(path):
    f = os.open(path, os.O_RDONLY)

    dev_info = UsbDeviceInfo()

    try:
        fcntl.ioctl(f, USB_GET_DEVICEINFO, dev_info)  # type: ignore
    finally:
        os.close(f)

    vid = int(dev_info.udi_vendorNo)
    pid = int(dev_info.udi_productNo)
    name = dev_info.udi_product.decode("utf-8") or None
    serial = dev_info.udi_serial.decode("utf-8") or None

    return HidDescriptor(path, vid, pid, MAX_U2F_HIDLEN, MAX_U2F_HIDLEN, name, serial)


# Cache for continuously failing devices
_failed_cache: Set[str] = set()


def list_descriptors():
    stale = set(_failed_cache)
    descriptors = []
    for dev in os.listdir(FIDO_DEVS):
        path = os.path.join(FIDO_DEVS, dev)
        stale.discard(path)
        try:
            descriptors.append(get_descriptor(path))
        except Exception:
            if path not in _failed_cache:
                logger.debug("Failed opening FIDO device %s", path, exc_info=True)
                _failed_cache.add(path)
    return descriptors

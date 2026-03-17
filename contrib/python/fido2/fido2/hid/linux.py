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

from .base import HidDescriptor, FileCtapHidConnection, parse_report_descriptor

import glob
import fcntl
import struct
from array import array
from typing import Set

import logging
import sys

# Don't typecheck this file on Windows
assert sys.platform != "win32"  # nosec

logger = logging.getLogger(__name__)

# hidraw.h
HIDIOCGRAWINFO = 0x80084803
HIDIOCGRDESCSIZE = 0x80044801
HIDIOCGRDESC = 0x90044802
HIDIOCGRAWNAME = 0x90044804
HIDIOCGRAWUNIQ = 0x90044808


class LinuxCtapHidConnection(FileCtapHidConnection):
    def write_packet(self, packet):
        # Prepend the report ID
        super().write_packet(b"\0" + packet)


def open_connection(descriptor):
    return LinuxCtapHidConnection(descriptor)


def get_descriptor(path):
    with open(path, "rb") as f:
        # Read VID, PID
        buf = array("B", [0] * (4 + 2 + 2))
        fcntl.ioctl(f, HIDIOCGRAWINFO, buf, True)
        _, vid, pid = struct.unpack("<IHH", buf)

        # Read product
        buf = array("B", [0] * 128)
        length = fcntl.ioctl(f, HIDIOCGRAWNAME, buf, True)
        name = bytearray(buf[: (length - 1)]).decode("utf-8") if length > 1 else None

        # Read unique ID
        try:
            buf = array("B", [0] * 64)
            length = fcntl.ioctl(f, HIDIOCGRAWUNIQ, buf, True)
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


# Cache for continuously failing devices
_failed_cache: Set[str] = set()


def list_descriptors():
    stale = set(_failed_cache)
    devices = []
    for hidraw in glob.glob("/dev/hidraw*"):
        stale.discard(hidraw)
        try:
            devices.append(get_descriptor(hidraw))
        except ValueError:
            pass  # Not a CTAP device, ignore.
        except Exception:
            if hidraw not in _failed_cache:
                logger.debug("Failed opening device %s", hidraw, exc_info=True)
                _failed_cache.add(hidraw)

    # Remove entries from the cache that were not seen
    _failed_cache.difference_update(stale)

    return devices

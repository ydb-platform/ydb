# Copyright (c) 2020 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Union, Optional
import struct
import abc
import os

FIDO_USAGE_PAGE = 0xF1D0
FIDO_USAGE = 0x1


@dataclass
class HidDescriptor:
    path: Union[str, bytes]
    vid: int
    pid: int
    report_size_in: int
    report_size_out: int
    product_name: Optional[str]
    serial_number: Optional[str]


class CtapHidConnection(abc.ABC):
    @abc.abstractmethod
    def read_packet(self) -> bytes:
        """Reads a CTAP HID packet"""

    @abc.abstractmethod
    def write_packet(self, data: bytes) -> None:
        """Writes a CTAP HID packet"""

    @abc.abstractmethod
    def close(self) -> None:
        """Closes the connection"""


class FileCtapHidConnection(CtapHidConnection):
    """Basic CtapHidConnection implementation which uses a path to a file descriptor"""

    def __init__(self, descriptor):
        self.handle = os.open(descriptor.path, os.O_RDWR)
        self.descriptor = descriptor

    def close(self):
        os.close(self.handle)

    def write_packet(self, packet):
        if os.write(self.handle, packet) != len(packet):
            raise OSError("failed to write entire packet")

    def read_packet(self):
        return os.read(self.handle, self.descriptor.report_size_in)


REPORT_DESCRIPTOR_KEY_MASK = 0xFC
SIZE_MASK = ~REPORT_DESCRIPTOR_KEY_MASK
OUTPUT_ITEM = 0x90
INPUT_ITEM = 0x80
COLLECTION_ITEM = 0xA0
REPORT_COUNT = 0x94
REPORT_SIZE = 0x74
USAGE_PAGE = 0x04
USAGE = 0x08


def parse_report_descriptor(data: bytes) -> Tuple[int, int]:
    # Parse report descriptor data
    usage, usage_page = None, None
    max_input_size, max_output_size = None, None
    report_count, report_size = None, None
    remaining = 4
    while data and remaining:
        head, data = struct.unpack_from(">B", data)[0], data[1:]
        key, size = REPORT_DESCRIPTOR_KEY_MASK & head, SIZE_MASK & head
        value = struct.unpack_from("<I", data[:size].ljust(4, b"\0"))[0]
        data = data[size:]

        if report_count is not None and report_size is not None:
            if key == INPUT_ITEM:
                if max_input_size is None:
                    max_input_size = report_count * report_size // 8
                    report_count, report_size = None, None
                    remaining -= 1
            elif key == OUTPUT_ITEM:
                if max_output_size is None:
                    max_output_size = report_count * report_size // 8
                    report_count, report_size = None, None
                    remaining -= 1
        if key == USAGE_PAGE:
            if not usage_page:
                usage_page = value
                remaining -= 1
        elif key == USAGE:
            if not usage:
                usage = value
                remaining -= 1
        elif key == REPORT_COUNT:
            if not report_count:
                report_count = value
        elif key == REPORT_SIZE:
            if not report_size:
                report_size = value

    if not remaining and usage_page == FIDO_USAGE_PAGE and usage == FIDO_USAGE:
        return max_input_size, max_output_size  # type: ignore

    raise ValueError("Not a FIDO device")

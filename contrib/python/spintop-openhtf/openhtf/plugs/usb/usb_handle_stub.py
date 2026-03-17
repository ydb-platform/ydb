# Copyright 2014 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Stub USB handle implementation for testing."""

import binascii
import string

from openhtf.plugs.usb import usb_handle


class StubUsbHandle(usb_handle.UsbHandle):
  """Stub handle used for testing."""
  PRINTABLE_DATA = set(string.printable) - set(string.whitespace)

  def __init__(self, ignore_writes=False):
    super(StubUsbHandle, self).__init__('StubSerial', 'StubHandle',
                                        default_timeout_ms=0)
    self.expected_write_data = None if ignore_writes else []
    self.expected_read_data = []
    self.closed = False

  @classmethod
  def _dotify(cls, data):
    """Add dots."""
    return ''.join(char if char in cls.PRINTABLE_DATA else '.' for char in data)

  def write(self, data, dummy=None):
    """Stub Write method."""
    assert not self.closed
    if self.expected_write_data is None:
      return

    expected_data = self.expected_write_data.pop(0)
    if expected_data != data:
      raise ValueError('Expected %s, got %s (%s)' % (
          self._dotify(expected_data), binascii.hexlify(data),
          self._dotify(data)))

  def read(self, length, dummy=None):
    """Stub Read method."""
    assert not self.closed
    data = self.expected_read_data.pop(0)
    if length < len(data):
      raise ValueError(
          'Overflow packet length. Read %d bytes, got %d bytes: %s',
          length, len(data), self._dotify(data))
    return data

  def close(self):
    """Stub Close method."""
    self.closed = True

  def is_closed(self):
    """Stub is_closed method."""
    return self.closed

  def expect_write(self, data):
    """Stub expect_write method."""
    assert self.expected_write_data is not None, \
        'expect_write would be ignored!'
    self.expected_write_data.append(data)

  def expect_read(self, data):
    """Stub expect_read method."""
    self.expected_read_data.append(data)

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


"""Base interface for communicating with USB devices.

This module provides the base classes required to support interfacing with USB
devices through a few different types of USB handles.  All handles implement the
UsbHandle interface, which contains Open, Close, Read, and Write functionality.
A UsbHandle object represents a single USB Interface, *not* an entire device.
"""

from future.utils import with_metaclass
import abc
import functools
import logging

from openhtf.plugs.usb import usb_exceptions

DEFAULT_TIMEOUT_MS = 5000
FLUSH_READ_SIZE = 1024 * 64
_LOG = logging.getLogger(__name__)


def requires_open_handle(method):  # pylint: disable=invalid-name
  """Decorator to ensure a handle is open for certain methods.

  Subclasses should decorate their Read() and Write() with this rather than
  checking their own internal state, keeping all "is this handle open" logic
  in is_closed().

  Args:
    method: A class method on a subclass of UsbHandle

  Raises:
    HandleClosedError: If this handle has been closed.

  Returns:
    A wrapper around method that ensures the handle is open before calling through
  to the wrapped method.
  """
  @functools.wraps(method)
  def wrapper_requiring_open_handle(self, *args, **kwargs):
    """The wrapper to be returned."""
    if self.is_closed():
      raise usb_exceptions.HandleClosedError()
    return method(self, *args, **kwargs)
  return wrapper_requiring_open_handle


class UsbHandle(with_metaclass(abc.ABCMeta, object)):
  """UsbHandle objects provide read/write access to USB Interfaces.

  Subclasses must implement this interface to provide actual Read/Write/Close
  functionality.  Applications using UsbHandle objects should be agnostic as to
  the specific implementation used (ie, should only use methods exposed in this
  base class), and should use the subclasses only to determine the mechanism by
  which the handle is opened.

  Read and Write perform a Bulk or Interrupt transfer, depending on the type of
  the endpoints opened, so that applications need only open the correct
  Interface and call Read or Write, without having to worry about whether it is
  a Bulk or Interrupt transfer.

  It is recommended that subclasses implement the following Open() and
  iter_open() classmethods:
    Open():
      Minimum Args:
        interface_class: USB interface_class to match
        interface_subclass: USB interface_subclass to match
        interface_protocol: USB interface_protocol to match
        serial_number: USB serial number to match
        default_timeout_ms: Default timeout to use for the returned Handle.

      Returns:
        Instance of UsbHandle for communicating with the matched device, or
      None if no matching device was found.

      Raises:
        MultipleInterfacesFoundError: If multiple interfaces were found that
      match the given criteria.

    iter_open():
      Same Args as Open(), but instead of returning a UsbHandle, yields
    UsbHandle instances for all matching connected devices.

  Attributes:
    name: Name assigned to this handle, used for logging purposes.
    serial_number: Serial number of the device this handle refers to.
  """

  def __init__(self, serial_number, name=None, default_timeout_ms=None):
    """Create a UsbHandle to refer to a particular USB Interface.

    Note that this base class implementation doesn't actually track much, that's
    up to subclasses.  We track a name that can optionally be used for more
    useful log messages, a default timeout for read/write operations, and serial
    number, as it is universally available for all USB devices.

    Args:
      serial_number: Serial number of this USB device.
      name: A name for this UsbHandle, only used for log messages.
      default_timeout_ms: Default timeout for reads/writes, in milliseconds.
    """
    self.serial_number = serial_number
    self.name = name or ''
    self._default_timeout_ms = default_timeout_ms or DEFAULT_TIMEOUT_MS

  def __del__(self):  # pylint: disable=invalid-name
    if not self.is_closed():
      _LOG.error('!!!!!USB!!!!! %s not closed!', type(self).__name__)

  def __str__(self):
    return '<%s: (%s %s)>' % (type(self).__name__, self.name,
                              self.serial_number)
  __repr__ = __str__

  def _timeout_or_default(self, timeout_ms):
    """Specify a timeout or take the default."""
    return int(timeout_ms if timeout_ms is not None
               else self._default_timeout_ms)

  def flush_buffers(self):
    """Default implementation, calls Read() until it blocks."""
    while True:
      try:
        self.read(FLUSH_READ_SIZE, timeout_ms=10)
      except usb_exceptions.LibusbWrappingError as exception:
        if exception.is_timeout():
          break
        raise

  @abc.abstractmethod
  def read(self, length, timeout_ms=None):
    """Perform a USB Read on this interface, return the result.

    Implementors should call self._EnsureOpen() first, then perform any
    implementation-specific steps necessary to execute the read.

    Args:
      length: Number of bytes to attempt to read.
      timeout_ms: Timeout for this read (in millis), if None, use default.

    Returns:
      Data read by performing a Read operation.

    Raises:
      HandleClosedError: If this handle has been closed.
      UsbReadFailedError: If there was an IO error during the read.
    """

  @abc.abstractmethod
  def write(self, data, timeout_ms=None):
    """Perform a USB Write on this interface.

    Implementors should call self._EnsureOpen() first, then perform any
    implementation-specific steps necessary to execute the write.

    Args:
      data: The data to write.
      timeout_ms: Timeout for this write (in millis), if None, use default.

    Raises:
      HandleClosedError: If this handle has been closed.
      UsbWriteFailedError: If there was an IO error during the write.
    """

  @abc.abstractmethod
  def close(self):
    """Close this handle and release any associated resources.

    Other methods must not be called after calling Close().  This method must
    not raise on error.
    """

  @abc.abstractmethod
  def is_closed(self):
    """Returns True if this handle has been closed, False otherwise."""

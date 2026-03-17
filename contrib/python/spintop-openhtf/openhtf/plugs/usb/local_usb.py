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


"""UsbHandle implementation using libusb to communicate with local devices.

This implementation of UsbHandle uses python libusb1 bindings to communicate
with locally attached USB devices.  In order to fit the UsbHandle interface,
much of the complexity of the libusb1 interface is abstracted away.  This class
should never raise libusb1 errors directly - if it does it is a bug, please fix
it and wrap the error in an appropriate exception from usb_exceptions.py.
"""

import logging

try:
  import libusb1
  import usb1
except ImportError:
  logging.error('Failed to import libusb, did you pip install '
                'openhtf[usb_plugs]?')
  raise

from openhtf.plugs.usb import usb_exceptions
from openhtf.plugs.usb import usb_handle
import six

_LOG = logging.getLogger(__name__)


class LibUsbHandle(usb_handle.UsbHandle):
  """Subclass of UsbHandle that opens locally connected devices with libusb."""

  def __init__(self, device, setting, name=None, default_timeout_ms=None):
    """Initialize a local libusb-based USB Handle.

    Arguments:
      device: libusb device to connect to.
      setting: libusb setting with the endpoints to use for data reads/writes.
      name: Name for the device, used for log messages only.
      default_timeout_ms: Default timeout, in milliseconds, for reads/writes.

    Raises:
      InvalidEndpointsError: If the setting provided does not have exactly one
        in endpoint and one out endpoint.  Currently, this is the only type of
        interface supported.
      IOError: If the device has been disconnected.
    """
    super(LibUsbHandle, self).__init__(device.getSerialNumber(), name=name,
                                       default_timeout_ms=default_timeout_ms)
    self._setting = setting
    self._device = device

    self._read_endpoint = None
    self._write_endpoint = None
    self._handle = None

    for endpoint in self._setting.iterEndpoints():
      address = endpoint.getAddress()
      if address & libusb1.USB_ENDPOINT_DIR_MASK == libusb1.LIBUSB_ENDPOINT_IN:
        if self._read_endpoint is not None:
          raise usb_exceptions.InvalidEndpointsError(
              'Multiple in endpoints found')
        self._read_endpoint = address
      else:
        if self._write_endpoint is not None:
          raise usb_exceptions.InvalidEndpointsError(
              'Multiple out endpoints found')
        self._write_endpoint = address

    if self._read_endpoint is None:
      raise usb_exceptions.InvalidEndpointsError('Missing in endpoint')
    if self._write_endpoint is None:
      raise usb_exceptions.InvalidEndpointsError('Missing out endpoint')

    handle = self._device.open()
    iface_number = self._setting.getNumber()
    try:
      if handle.kernelDriverActive(iface_number):
        handle.detachKernelDriver(iface_number)
    except libusb1.USBError as exception:
      if exception.value == libusb1.LIBUSB_ERROR_NOT_FOUND:
        _LOG.warning('Kernel driver not found for interface: %s.', iface_number)
      else:
        raise usb_exceptions.LibusbWrappingError(
            exception, '%s failed to detach kernel driver', self)
    # Try to claim the interface, if the device is gone, raise an IOError
    try:
      handle.claimInterface(iface_number)
    except libusb1.USBError as exception:
      raise usb_exceptions.LibusbWrappingError(
          exception, '%s failed to claim interface: %d', self, iface_number)

    self._handle = handle
    self._interface_number = iface_number

  def is_closed(self):
    return self._handle is None

  @staticmethod
  def _device_to_sysfs_path(device):
    """Convert device to corresponding sysfs path."""
    return '%s-%s' % (
        device.getBusNumber(),
        '.'.join([str(item) for item in device.GetPortNumberList()]))

  @property
  def port_path(self):
    """A string of the physical port of this device, like 'X-X.X.X'."""
    return self._device_to_sysfs_path(self._device)

  @usb_handle.requires_open_handle
  def read(self, length, timeout_ms=None):
    try:
      return self._handle.bulkRead(
          self._read_endpoint, length,
          timeout=self._timeout_or_default(timeout_ms))
    except libusb1.USBError as exception:
      raise usb_exceptions.UsbReadFailedError(
          exception, '%s failed read (timeout %sms)', self,
          self._timeout_or_default(timeout_ms))

  @usb_handle.requires_open_handle
  def write(self, data, timeout_ms=None):
    try:
      return self._handle.bulkWrite(
          self._write_endpoint, data,
          timeout=self._timeout_or_default(timeout_ms))
    except libusb1.USBError as exception:
      raise usb_exceptions.UsbWriteFailedError(
          exception, '%s failed write (timeout %sms)', self,
          self._timeout_or_default(timeout_ms))

  def close(self):
    if self.is_closed():
      return

    try:
      self._handle.releaseInterface(self._interface_number)
      self._handle.close()
    except libusb1.USBError:
      _LOG.exception('USBError while closing handle %s:', self)
    finally:
      self._handle = None

  @classmethod
  def open(cls, **kwargs):
    """See iter_open, but raises if multiple or no matches found."""
    handle_iter = cls.iter_open(**kwargs)

    try:
      handle = six.next(handle_iter)
    except StopIteration:
      # No matching interface, raise.
      raise usb_exceptions.DeviceNotFoundError(
          'Open failed with args: %s', kwargs)

    try:
      multiple_handle = six.next(handle_iter)
    except StopIteration:
      # Exactly one matching device, return it.
      return handle

    # We have more than one device, close the ones we opened and bail.
    handle.close()
    multiple_handle.close()
    raise usb_exceptions.MultipleInterfacesFoundError(kwargs)

  # pylint: disable=too-many-arguments
  @classmethod
  def iter_open(cls, name=None, interface_class=None, interface_subclass=None,
                interface_protocol=None, serial_number=None, port_path=None,
                default_timeout_ms=None):
    """Find and yield locally connected devices that match.

    Note that devices are opened (and interfaces claimd) as they are yielded.
    Any devices yielded must be Close()'d.

    Args:
      name: Name to give *all* returned handles, used for logging only.
      interface_class: USB interface_class to match.
      interface_subclass: USB interface_subclass to match.
      interface_protocol: USB interface_protocol to match.
      serial_number: USB serial_number to match.
      port_path: USB Port path to match, like X-X.X.X
      default_timeout_ms: Default timeout in milliseconds of reads/writes on
        the handles yielded.

    Yields:
      UsbHandle instances that match any non-None args given.

    Raises:
      LibusbWrappingError: When a libusb call errors during open.
    """
    ctx = usb1.USBContext()
    try:
      devices = ctx.getDeviceList(skip_on_error=True)
    except libusb1.USBError as exception:
      raise usb_exceptions.LibusbWrappingError(
          exception, 'Open(name=%s, class=%s, subclass=%s, protocol=%s, '
          'serial=%s, port=%s) failed', name, interface_class,
          interface_subclass, interface_protocol, serial_number, port_path)

    for device in devices:
      try:
        if (serial_number is not None and
            device.getSerialNumber() != serial_number):
          continue

        if (port_path is not None and
            cls._device_to_sysfs_path(device) != port_path):
          continue

        for setting in device.iterSettings():
          if (interface_class is not None and
              setting.getClass() != interface_class):
            continue
          if (interface_subclass is not None and
              setting.getSubClass() != interface_subclass):
            continue
          if (interface_protocol is not None and
              setting.getProtocol() != interface_protocol):
            continue

          yield cls(device, setting, name=name,
                    default_timeout_ms=default_timeout_ms)
      except libusb1.USBError as exception:
        if (exception.value !=
            libusb1.libusb_error.forward_dict['LIBUSB_ERROR_ACCESS']):
          raise
  # pylint: disable=too-many-arguments

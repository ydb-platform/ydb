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


"""Fastboot device."""

import logging
import time

from openhtf.plugs.usb import fastboot_protocol
from openhtf.plugs.usb import usb_exceptions
from openhtf.util import timeouts

# From fastboot.c
VENDORS = {0x18D1, 0x0451, 0x0502, 0x0FCE, 0x05C6, 0x22B8, 0x0955,
           0x413C, 0x2314, 0x0BB4, 0x8087}
CLASS = 0xFF
SUBCLASS = 0x42
PROTOCOL = 0x03

_LOG = logging.getLogger(__name__)
class FastbootDevice(object):
  """Libusb fastboot wrapper with retries."""

  def __init__(self, _, num_retries=3):
    self._num_retries = num_retries
    self._protocol = None

  @property
  def usb_handle(self):
    """Return our USB handle."""
    return self._protocol.usb_handle

  def set_boot_config(self, name, value):
    """Set the boot configuration."""
    self.oem('bootconfig %s %s' % (name, value))

  def get_boot_config(self, name, info_cb=None):
    """Get bootconfig, either as full dict or specific value for key."""
    result = {}
    def default_info_cb(msg):
      """Default Info CB."""
      if not msg.message:
        return
      key, value = msg.message.split(':', 1)
      result[key.strip()] = value.strip()
    info_cb = info_cb or default_info_cb
    final_result = self.oem('bootconfig %s' % name, info_cb=info_cb)
    # Return INFO messages before the final OKAY message.
    if name in result:
      return result[name]
    return final_result

  def lock(self):
    """Lock the device."""
    self.oem('lock', timeout_ms=1000)

  def close(self):
    """Close the device."""
    if self._protocol:
      self.__getattr__('Close')()
      self._protocol = None

  def __getattr__(self, attr):  # pylint: disable=invalid-name
    """Fallthrough to underlying FastbootProtocol handler.

    Args:
      attr: Attribute to get.
    Returns:
      Either the attribute from the device or a retrying function-wrapper
      if attr is a method on the device.
    """
    if not self._protocol:
      raise usb_exceptions.HandleClosedError()

    val = getattr(self._protocol, attr)
    if callable(val):
      def _retry_wrapper(*args, **kwargs):
        """Wrap the retry function."""
        result = _retry_usb_function(self._num_retries, val, *args, **kwargs)
        _LOG.debug('LIBUSB FASTBOOT: %s(*%s, **%s) -> %s',
                   attr, args, kwargs, result)
        return result
      return _retry_wrapper
    return val

  @classmethod
  def connect(cls, usb_handle, **kwargs):
    """Connect to the device.

    Args:
      usb_handle: UsbHandle instance to use for communication to the device.
      **kwargs: Additional args to pass to the class constructor (currently
          only num_retries).

    Returns:
      An instance of this class if the device connected successfully.
    """
    return cls(fastboot_protocol.FastbootCommands(usb_handle), **kwargs)

def _retry_usb_function(count, func, *args, **kwargs):
  """Helper function to retry USB."""
  helper = timeouts.RetryHelper(count)
  while True:
    try:
      return func(*args, **kwargs)
    except usb_exceptions.CommonUsbError:
      if not helper.retry_if_possible():
        raise
      time.sleep(0.1)
    else:
      break

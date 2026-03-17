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


"""Common exceptions for USB, ADB and Fastboot."""

import logging

try:
  import libusb1
except ImportError:
  logging.error('Failed to import libusb, did you pip install '
                'openhtf[usb_plugs]?')
  raise

_LOG = logging.getLogger(__name__)


class CommonUsbError(Exception):
  """Exception that both looks good and is functional.

  Okay, not that kind of functional, it's still a class.

  This interpolates the message with the given arguments to make it
  human-readable, but keeps the arguments in case other code try-excepts it.
  """

  def __init__(self, message=None, *args):
    if message is not None:
      if '%' in message:
        try:
          message %= args
        except TypeError:
          # This is a fairly obscure failure, so we intercept it and emit a
          # more useful error message.
          _LOG.error('USB Exceptions expect a format-string, do not include '
                     'percent symbols to disable this functionality: %s',
                     message)
          raise
      super(CommonUsbError, self).__init__(message, *args)
    else:
      super(CommonUsbError, self).__init__(*args)


# USB exceptions, these are not specific to any particular protocol.
class LibusbWrappingError(CommonUsbError):
  """Wraps libusb1 errors while keeping its original usefulness.

  Attributes:
    usb_error: Instance of libusb1.USBError
  """

  def __init__(self, usb_error, *args):
    super(LibusbWrappingError, self).__init__(*args)
    self.usb_error = usb_error

  def __str__(self):
    return '<%s: %s>' % (type(self).__name__, str(self.usb_error))

  def is_timeout(self):
    """Returns True if the USBError we are wrapping is a timeout error."""
    return self.usb_error.value == libusb1.LIBUSB_ERROR_TIMEOUT


class InvalidArgumentError(CommonUsbError):
  """Raised when an invalid argument is passed to a USB function."""


class DeviceNotFoundError(CommonUsbError):
  """Device isn't on USB."""


class HandleClosedError(CommonUsbError):
  """Raised when an operation is requested on an already closed handle."""


class InvalidEndpointsError(CommonUsbError):
  """Raised when a USB interface does not have supported endpoints."""


class MultipleInterfacesFoundError(CommonUsbError):
  """Raised when multiple matching devices are found for an Open call."""


class UsbServiceCommunicationError(CommonUsbError):
  """Could not connect to usb service."""


class UnclosedHandleError(CommonUsbError):
  """Raised when a UsbHandle goes out of scope but hasn't been Close()'d."""


class UsbWriteFailedError(LibusbWrappingError):
  """Raised when a USB write fails."""


class UsbReadFailedError(LibusbWrappingError):
  """Raised when a USB read fails."""


# ADB specific exceptions
class DeviceAuthError(CommonUsbError):
  """Device authentication failed."""


class AdbOperationException(Exception):
  """Failed to communicate over adb with device after multiple retries."""


class AdbDataIntegrityError(CommonUsbError):
  """Raised when an AdbMessage has corrupted data."""


class AdbProtocolError(CommonUsbError):
  """Raised when we don't get the type of ADB packet we expect."""


class AdbStreamClosedError(CommonUsbError):
  """Raised when an attempt to read/write happens on a closed AdbStream."""


class AdbStreamUnavailableError(CommonUsbError):
  """Raised when a stream cannot be opened."""


class AdbTransportClosedError(CommonUsbError):
  """Raised when a transport (a layer above a Stream) is closed."""


class AdbRemoteError(CommonUsbError):
  """Raised when there's an error on the remote (device) side."""


class AdbTimeoutError(CommonUsbError):
  """Raised when an ADB operation times out."""


# Fastboot specific exceptions
class FastbootTransferError(CommonUsbError):
  """Transfer error."""


class FastbootRemoteFailure(CommonUsbError):
  """Remote error."""


class FastbootStateMismatch(CommonUsbError):
  """Fastboot and uboot's state machines are arguing. You Lose."""


class FastbootInvalidResponse(CommonUsbError):
  """Fastboot responded with a header we didn't expect."""

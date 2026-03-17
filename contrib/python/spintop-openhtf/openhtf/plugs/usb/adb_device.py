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


"""User-facing interface to an ADB device.

This module provides the user-facing interface to controlling ADB devices with
this library.  See adb_protocol.py for a breakdown of the various layers.
Essentially, this module provides the AdbDevice class, which takes a UsbHandle
and uses the layers in between to provide a high level interface that is
designed to be similar to the 'adb' commandline interface.

Some additional useful mechanisms are provided for more common programmatic
needs, see shell_service.py for a description of how to use asynchronous shell
commands.

Note we don't provide a device listing facility, as this is not actually an ADB
function, but rather a USB function - listing devices with a specific interface
class, subclass, and protocol.
"""

import logging
import os.path

try:
  from M2Crypto import RSA
except ImportError:
  logging.error('Failed to import M2Crypto, did you pip install '
                'openhtf[usb_plugs]?')
  raise

from openhtf.plugs.usb import adb_protocol
from openhtf.plugs.usb import filesync_service
from openhtf.plugs.usb import shell_service
from openhtf.plugs.usb import usb_exceptions

from openhtf.util import timeouts
import six

# USB interface class, subclass, and protocol for matching against.
CLASS = 0xFF
SUBCLASS = 0x42
PROTOCOL = 0x01


class M2CryptoSigner(adb_protocol.AuthSigner):
  """AuthSigner using M2Crypto."""

  def __init__(self, rsa_key_path):
    with open(rsa_key_path + '.pub') as rsa_pub_file:
      self.public_key = rsa_pub_file.read()

    self.rsa_key = RSA.load_key(rsa_key_path)

  def sign(self, data):
    return self.rsa_key.sign(data, 'sha1')

  def get_public_key(self):
    """Return the public key."""
    return self.public_key


class AdbDevice(object):
  """Exposes adb-like methods for use.

  Some methods are more-pythonic and/or have more options.

  Attributes:
    filesync_service: Direct access to this device's FilesyncService.
    shell_service: Direct access to this device's ShellService.
  """

  def __init__(self, adb_connection):
    self._adb_connection = adb_connection
    self.filesync_service = filesync_service.FilesyncService.using_connection(
        adb_connection)
    self.shell_service = shell_service.ShellService.using_connection(
        adb_connection)

  def __str__(self):
    return '<%s: %s(%s) @%s>' % (type(self).__name__,
                                 self._adb_connection.serial,
                                 self._adb_connection.systemtype,
                                 self._adb_connection.transport)
  __repr__ = __str__

  def get_system_type(self):
    """Return the system type."""
    return self._adb_connection.systemtype

  def get_serial(self):
    """Return the device serial."""
    return self._adb_connection.serial

  def close(self):
    """Close the ADB connection."""
    self.filesync_service.close()
    self._adb_connection.close()

  def install(self, apk_path, destination_dir=None, timeout_ms=None):
    """Install apk to device.

    Doesn't support verifier file, instead allows destination directory to be
    overridden.

    Arguments:
      apk_path: Local path to apk to install.
      destination_dir: Optional destination directory. Use /system/app/ for
        persistent applications.
      timeout_ms: Expected timeout for pushing and installing.

    Returns:
      The pm install output.
    """
    if not destination_dir:
      destination_dir = '/data/local/tmp/'
    basename = os.path.basename(apk_path)
    destination_path = destination_dir + basename
    self.push(apk_path, destination_path, timeout_ms=timeout_ms)
    return self.Shell('pm install -r "%s"' % destination_path,
                      timeout_ms=timeout_ms)

  def push(self, source_file, device_filename, timeout_ms=None):
    """Push source_file to file on device.

    Arguments:
      source_file: Either a filename or file-like object to push to the device.
        If a filename, will set the remote mtime to match the local mtime,
        otherwise will use the current time.
      device_filename: The filename on the device to write to.
      timeout_ms: Expected timeout for any part of the push.
    """
    mtime = 0
    if isinstance(source_file, six.string_types):
      mtime = os.path.getmtime(source_file)
      source_file = open(source_file)

    self.filesync_service.send(
        source_file, device_filename, mtime=mtime,
        timeout=timeouts.PolledTimeout.from_millis(timeout_ms))

  def pull(self, device_filename, dest_file=None, timeout_ms=None):
    """Pull file from device.

    Arguments:
      device_filename: The filename on the device to pull.
      dest_file: If set, a filename or writable file-like object.
      timeout_ms: Expected timeout for the pull.

    Returns:
      The file data if dest_file is not set, None otherwise.
    """
    should_return_data = dest_file is None
    if isinstance(dest_file, six.string_types):
      dest_file = open(dest_file, 'w')
    elif dest_file is None:
      dest_file = six.StringIO()
    self.filesync_service.recv(device_filename, dest_file,
                               timeouts.PolledTimeout.from_millis(timeout_ms))
    if should_return_data:
      return dest_file.getvalue()

  def list(self, device_path, timeout_ms=None):
    """Yield filesync_service.DeviceFileStat objects for directory contents."""
    return self.filesync_service.list(
        device_path, timeouts.PolledTimeout.from_millis(timeout_ms))

  def command(self, command, raw=False, timeout_ms=None):
    """Run command on the device, returning the output."""
    return self.shell_service.command(
        str(command), raw=raw, timeout_ms=timeout_ms)

  Shell = command  #pylint: disable=invalid-name

  def async_command(self, command, raw=False, timeout_ms=None):
    """See shell_service.ShellService.async_command()."""
    return self.shell_service.async_command(
        str(command), raw=raw, timeout_ms=timeout_ms)

  def _check_remote_command(self, destination, timeout_ms, success_msgs=None):
    """Open a stream to destination, check for remote errors.

    Used for reboot, remount, and root services.  If this method returns, the
    command was successful, otherwise an appropriate error will have been
    raised.

    Args:
      destination: Stream destination to open.
      timeout_ms: Timeout in milliseconds for the operation.
      success_msgs: If provided, a list of messages that, if returned from the
        device, indicate success, so don't treat them as errors.

    Raises:
      AdbRemoteError: If the remote command fails, will contain any message we
        got back from the device.
      AdbStreamUnavailableError: The service requested isn't supported.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)
    stream = self._adb_connection.open_stream(destination, timeout)
    if not stream:
      raise usb_exceptions.AdbStreamUnavailableError(
          'Service %s not supported', destination)
    try:
      message = stream.read(timeout_ms=timeout)
      # Some commands report success messages, ignore them.
      if any([m in message for m in success_msgs]):
        return
    except usb_exceptions.CommonUsbError:
      if destination.startswith('reboot:'):
        # We expect this if the device is rebooting.
        return
      raise
    raise usb_exceptions.AdbRemoteError('Device message: %s', message)

  def reboot(self, destination='', timeout_ms=None):
    """Reboot device, specify 'bootloader' for fastboot."""
    self._check_remote_command('reboot:%s' % destination, timeout_ms)

  def remount(self, timeout_ms=None):
    """Remount / as read-write."""
    self._check_remote_command('remount:', timeout_ms, ['remount succeeded'])

  def root(self, timeout_ms=None):
    """Restart adbd as root on device."""
    self._check_remote_command('root:', timeout_ms,
                               ['already running as root',
                                'restarting adbd as root'])

  @classmethod
  def connect(cls, usb_handle, **kwargs):
    """Connect to the device.

    Args:
      usb_handle: UsbHandle instance to use.
      **kwargs: See AdbConnection.connect for kwargs. Includes rsa_keys, and
        auth_timeout_ms.

    Returns:
      An instance of this class if the device connected successfully.
    """
    adb_connection = adb_protocol.AdbConnection.connect(usb_handle, **kwargs)
    return cls(adb_connection)

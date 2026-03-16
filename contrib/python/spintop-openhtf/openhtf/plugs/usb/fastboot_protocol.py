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


"""A libusb1-based fastboot implementation."""

import binascii
import collections
import logging
import os
import struct

from . import usb_exceptions
from openhtf.util import argv
import six


FASTBOOT_DOWNLOAD_CHUNK_SIZE_KB = 1024

ARG_PARSER = argv.ModuleParser()
ARG_PARSER.add_argument(
    '--fastboot_download_chunk_size_kb',
    default=FASTBOOT_DOWNLOAD_CHUNK_SIZE_KB,
    action=argv.StoreInModule,
    type=int,
    target='%s.FASTBOOT_DOWNLOAD_CHUNK_SIZE_KB' % __name__,
    help='Size of chunks to send when downloading fastboot images')

_LOG = logging.getLogger(__name__)

DEFAULT_MESSAGE_CALLBACK = lambda m: _LOG.info('Got %s from device', m)
FastbootMessage = collections.namedtuple(  # pylint: disable=invalid-name
    'FastbootMessage', ['message', 'header'])


class FastbootProtocol(object):
  """Encapsulates the fastboot protocol."""
  FINAL_HEADERS = {'OKAY', 'DATA'}

  def __init__(self, usb):
    """Constructs a FastbootProtocol instance.

    Arguments:
      usb: UsbHandle instance.
    """
    self.usb = usb

  @property
  def usb_handle(self):
    """This instance's USB handle."""
    return self.usb

  def send_command(self, command, arg=None):
    """Sends a command to the device.

    Args:
      command: The command to send.
      arg: Optional argument to the command.
    """
    if arg is not None:
      command = '%s:%s' % (command, arg)
    self._write(six.StringIO(command), len(command))

  def handle_simple_responses(
      self, timeout_ms=None, info_cb=DEFAULT_MESSAGE_CALLBACK):
    """Accepts normal responses from the device.

    Args:
      timeout_ms: Timeout in milliseconds to wait for each response.
      info_cb: Optional callback for text sent from the bootloader.

    Returns:
      OKAY packet's message.
    """
    return self._accept_responses('OKAY', info_cb, timeout_ms=timeout_ms)

  # pylint: disable=too-many-arguments
  def handle_data_sending(self, source_file, source_len,
                          info_cb=DEFAULT_MESSAGE_CALLBACK,
                          progress_callback=None, timeout_ms=None):
    """Handles the protocol for sending data to the device.

    Arguments:
      source_file: File-object to read from for the device.
      source_len: Amount of data, in bytes, to send to the device.
      info_cb: Optional callback for text sent from the bootloader.
      progress_callback: Callback that takes the current and the total progress
        of the current file.
      timeout_ms: Timeout in milliseconds to wait for each response.

    Raises:
      FastbootTransferError: When fastboot can't handle this amount of data.
      FastbootStateMismatch: Fastboot responded with the wrong packet type.
      FastbootRemoteFailure: Fastboot reported failure.
      FastbootInvalidResponse: Fastboot responded with an unknown packet type.

    Returns:
      OKAY packet's message.
    """
    accepted_size = self._accept_responses(
        'DATA', info_cb, timeout_ms=timeout_ms)

    accepted_size = binascii.unhexlify(accepted_size[:8])
    accepted_size, = struct.unpack('>I', accepted_size)
    if accepted_size != source_len:
      raise usb_exceptions.FastbootTransferError(
          'Device refused to download %s bytes of data (accepts %s bytes)',
          source_len, accepted_size)
    self._write(source_file, accepted_size, progress_callback)
    return self._accept_responses('OKAY', info_cb, timeout_ms=timeout_ms)

  # pylint: enable=too-many-arguments

  def _accept_responses(self, expected_header, info_cb, timeout_ms=None):
    """Accepts responses until the expected header or a FAIL.

    Arguments:
      expected_header: OKAY or DATA
      info_cb: Optional callback for text sent from the bootloader.
      timeout_ms: Timeout in milliseconds to wait for each response.

    Raises:
      FastbootStateMismatch: Fastboot responded with the wrong packet type.
      FastbootRemoteFailure: Fastboot reported failure.
      FastbootInvalidResponse: Fastboot responded with an unknown packet type.

    Returns:
      OKAY packet's message.
    """
    while True:
      response = self.usb.read(64, timeout_ms=timeout_ms)
      header = response[:4]
      remaining = response[4:]

      if header == 'INFO':
        info_cb(FastbootMessage(remaining, header))
      elif header in self.FINAL_HEADERS:
        if header != expected_header:
          raise usb_exceptions.FastbootStateMismatch(
              'Expected %s, got %s', expected_header, header)
        if header == 'OKAY':
          info_cb(FastbootMessage(remaining, header))
        return remaining
      elif header == 'FAIL':
        info_cb(FastbootMessage(remaining, header))
        raise usb_exceptions.FastbootRemoteFailure('FAIL: %s', remaining)
      else:
        raise usb_exceptions.FastbootInvalidResponse(
            'Got unknown header %s and response %s', header, remaining)

  def _handle_progress(self, total, progress_callback):  # pylint: disable=no-self-use
    """Calls the callback with the current progress and total ."""
    current = 0
    while True:
      current += yield
      try:
        progress_callback(current, total)
      except Exception:  # pylint: disable=broad-except
        _LOG.exception('Progress callback raised an exception. %s',
                       progress_callback)
        continue

  def _write(self, data, length, progress_callback=None):
    """Sends the data to the device, tracking progress with the callback."""
    if progress_callback:
      progress = self._handle_progress(length, progress_callback)
      six.next(progress)
    while length:
      tmp = data.read(FASTBOOT_DOWNLOAD_CHUNK_SIZE_KB * 1024)
      length -= len(tmp)
      self.usb.write(tmp)

      if progress_callback:
        progress.send(len(tmp))


class FastbootCommands(object):
  """Encapsulates the fastboot commands."""
  protocol_handler = FastbootProtocol

  def __init__(self, usb):
    """Constructs a FastbootCommands instance.

    Arguments:
      usb: UsbHandle instance.
    """
    self._usb = usb
    self._protocol = self.protocol_handler(usb)

  @property
  def handle(self):
    """This instance's USB handle."""
    return self._usb

  def close(self):
    """Close the USB handle."""
    self._usb.close()

  def _simple_command(self, command, arg=None, **kwargs):
    """Send a simple command."""
    self._protocol.send_command(command, arg)
    return self._protocol.handle_simple_responses(**kwargs)

  # pylint: disable=too-many-arguments
  def flash_from_file(self, partition, source_file, source_len=0,
                      info_cb=DEFAULT_MESSAGE_CALLBACK, progress_callback=None,
                      timeout_ms=None):
    """Flashes a partition from the file on disk.

    Args:
      partition: Partition name to flash to.
      source_file: Filename to download to the device.
      source_len: Optional length of source_file, uses os.stat if not provided.
      info_cb: See Download.
      progress_callback: See Download.
      timeout_ms: The amount of time to wait on okay after flashing.

    Returns:
      Download and flash responses, normally nothing.
    """
    if source_len == 0:
      # Fall back to stat.
      source_len = os.stat(source_file).st_size
    download_response = self.download(
        source_file, source_len=source_len, info_cb=info_cb,
        progress_callback=progress_callback)
    flash_response = self.flash(partition, info_cb=info_cb,
                                timeout_ms=timeout_ms)
    return download_response + flash_response

  # pylint: enable=too-many-arguments

  def download(self, source_file, source_len=0,
               info_cb=DEFAULT_MESSAGE_CALLBACK, progress_callback=None):
    """Downloads a file to the device.

    Args:
      source_file: A filename or file-like object to download to the device.
      source_len: Optional length of source_file. If source_file is a file-like
          object and source_len is not provided, source_file is read into
          memory.
      info_cb: Optional callback accepting FastbootMessage for text sent from
          the bootloader.
      progress_callback: Optional callback called with the percent of the
          source_file downloaded. Note, this doesn't include progress of the
          actual flashing.

    Returns:
      Response to a download request, normally nothing.
    """
    if isinstance(source_file, six.string_types):
      source_len = os.stat(source_file).st_size
      source_file = open(source_file)

    if source_len == 0:
      # Fall back to storing it all in memory :(
      data = source_file.read()
      source_file = six.StringIO(data)
      source_len = len(data)

    self._protocol.send_command('download', '%08x' % source_len)
    return self._protocol.handle_data_sending(
        source_file, source_len, info_cb, progress_callback=progress_callback)

  def flash(self, partition, timeout_ms=None, info_cb=DEFAULT_MESSAGE_CALLBACK):
    """Flashes the last downloaded file to the given partition.

    Args:
      partition: Partition to flash.
      timeout_ms: Optional timeout in milliseconds to wait for it to finish.
      info_cb: See Download. Usually no messages.

    Returns:
      Response to a download request, normally nothing.
    """
    return self._simple_command('flash', arg=partition, info_cb=info_cb,
                                timeout_ms=timeout_ms)

  def erase(self, partition, timeout_ms=None):
    """Erases the given partition."""
    self._simple_command('erase', arg=partition, timeout_ms=timeout_ms)

  def get_var(self, var, info_cb=DEFAULT_MESSAGE_CALLBACK):
    """Returns the given variable's definition.

    Args:
      var: A variable the bootloader tracks, such as version.
      info_cb: See Download. Usually no messages.
    Returns:
      Value of var according to the current bootloader.
    """
    return self._simple_command('getvar', arg=var, info_cb=info_cb)

  def oem(self, command, timeout_ms=None, info_cb=DEFAULT_MESSAGE_CALLBACK):
    """Executes an OEM command on the device.

    Args:
      command: The command to execute, such as 'poweroff' or 'bootconfig read'.
      timeout_ms: Optional timeout in milliseconds to wait for a response.
      info_cb: See Download. Messages vary based on command.
    Returns:
      The final response from the device.
    """
    return self._simple_command(
        'oem %s' % command, timeout_ms=timeout_ms, info_cb=info_cb)

  def continue_(self):
    """Continues execution past fastboot into the system."""
    return self._simple_command('continue')

  def reboot(self, target_mode=None, timeout_ms=None):
    """Reboots the device.

    Args:
        target_mode: Normal reboot when unspecified (or None). Can specify
            other target modes, such as 'recovery' or 'bootloader'.
        timeout_ms: Optional timeout in milliseconds to wait for a response.
    Returns:
        Usually the empty string. Depends on the bootloader and the target_mode.
    """
    return self._simple_command('reboot', arg=target_mode,
                                timeout_ms=timeout_ms)

  def reboot_bootloader(self, timeout_ms=None):
    """Reboots into the bootloader, usually equiv to Reboot('bootloader')."""
    return self._simple_command('reboot-bootloader', timeout_ms=timeout_ms)

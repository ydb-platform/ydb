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


"""This module contains a class to encapsulate ADB messages.

See the following in the Android source for more details:
https://android.googlesource.com/platform/system/core/+/master/adb/protocol.txt

The ADB transport layer deals in "messages", which consist of a 24 byte header
followed (optionally) by a payload.  The header consists of 6 32 bit words which
are sent across the wire in little endian format:

struct message {
    unsigned command;       /* command identifier constant      */
    unsigned arg0;          /* first argument                   */
    unsigned arg1;          /* second argument                  */
    unsigned data_length;   /* length of payload (0 is allowed) */
    unsigned data_crc32;    /* crc32 of data payload            */
    unsigned magic;         /* command ^ 0xffffffff             */
};

Receipt of an invalid message header, corrupt message payload, or an
unrecognized command MUST result in the closing of the remote connection.  The
protocol depends on shared state and any break in the message stream will
result in state getting out of sync.

This class does not keep any of this state, but rather represents a single
message entity.  See adb_protocol.py for the stateful components.
"""

import collections
import logging
import string
import struct
import threading

from openhtf.plugs.usb import usb_exceptions
from openhtf.util import timeouts
import six

_LOG = logging.getLogger(__name__)

def make_wire_commands(*ids):
  """Assemble the commands."""
  cmd_to_wire = {
      cmd: sum(ord(c) << (i * 8) for i, c in enumerate(cmd)) for cmd in ids
  }
  wire_to_cmd = {wire: cmd for cmd, wire in six.iteritems(cmd_to_wire)}
  return cmd_to_wire, wire_to_cmd


class RawAdbMessage(collections.namedtuple('RawAdbMessage',
                                           ['cmd', 'arg0', 'arg1',
                                            'data_length', 'data_checksum',
                                            'magic'])):
  """Helper class for handling the struct -> AdbMessage mapping."""

  def to_adb_message(self, data):
    """Turn the data into an ADB message."""
    message = AdbMessage(AdbMessage.WIRE_TO_CMD.get(self.cmd),
                         self.arg0, self.arg1, data)
    if (len(data) != self.data_length or
        message.data_crc32 != self.data_checksum):
      raise usb_exceptions.AdbDataIntegrityError(
          '%s (%s) received invalid data: %s', message, self, repr(data))
    return message


class AdbTransportAdapter(object):
  """Transport adapter for reading/writing AdbMessages to another transport.

  This class handles the over-the-wire sending/receiving of AdbMessages, and
  has a utility method for ignoring messages we don't care about.  This is
  a 'transport' in that it has Read/Write methods, but those methods only
  work on AdbMessage instances, not arbitrary data, so it can't quite be
  dropped in anywhere a Transport is used.

  This class maintains its own Lock so that multiple threads can read
  AdbMessages from the same underlying transport without stealing each
  other's headers/data.
  """

  def __init__(self, transport):
    """Create an AdbTransportAdapter that writes to/reads from 'transport'."""
    self._transport = transport
    self._reader_lock = threading.Lock()
    self._writer_lock = threading.Lock()

  def __str__(self):
    trans = str(self._transport)
    return '<%s: (%s)' % (type(self).__name__, trans[1:])

  def close(self):
    """Close the connection."""
    self._transport.close()

  def write_message(self, message, timeout):
    """Send the given message over this transport.

    Args:
      message: The AdbMessage to send.
      timeout: Use this timeout for the entire write operation, it should be an
        instance of timeouts.PolledTimeout.
    """
    with self._writer_lock:
      self._transport.write(message.header, timeout.remaining_ms)

      # Use any remaining time to send the data.  Note that if we get this far,
      # we always at least try to send the data (with a minimum of 10ms timeout)
      # because we don't want the remote end to get out of sync because we sent
      # a header but no data.
      if timeout.has_expired():
        _LOG.warning('Timed out between AdbMessage header and data, sending '
                     'data anyway with 10ms timeout')
        timeout = timeouts.PolledTimeout.from_millis(10)
      self._transport.write(message.data, timeout.remaining_ms)

  def read_message(self, timeout):
    """Read an AdbMessage from this transport.

    Args:
      timeout: Timeout for the entire read operation, in the form of a
        timeouts.PolledTimeout instance.  Note that for packets with a data
        payload, two USB reads are performed.

    Returns:
      The ADB message read from the device.

    Raises:
      UsbReadFailedError: There's an error during read, including timeout.
      AdbProtocolError: A message is incorrectly formatted.
      AdbTimeoutError: timeout is already expired, or expires before we read the
        entire message, specifically between reading header and data packets.
    """
    with self._reader_lock:
      raw_header = self._transport.read(
          struct.calcsize(AdbMessage.HEADER_STRUCT_FORMAT),
          timeout.remaining_ms)
      if not raw_header:
        raise usb_exceptions.AdbProtocolError('Adb connection lost')

      try:
        raw_message = RawAdbMessage(*struct.unpack(
            AdbMessage.HEADER_STRUCT_FORMAT, raw_header))
      except struct.error as exception:
        raise usb_exceptions.AdbProtocolError(
            'Unable to unpack ADB command (%s): %s (%s)',
            AdbMessage.HEADER_STRUCT_FORMAT, raw_header, exception)

      if raw_message.data_length > 0:
        if timeout.has_expired():
          _LOG.warning('Timed out between AdbMessage header and data, reading '
                       'data anyway with 10ms timeout')
          timeout = timeouts.PolledTimeout.from_millis(10)
        data = self._transport.read(raw_message.data_length,
                                    timeout.remaining_ms)
      else:
        data = ''

      return raw_message.to_adb_message(data)

  def read_until(self, expected_commands, timeout):
    """Read AdbMessages from this transport until we get an expected command.

    The ADB protocol specifies that before a successful CNXN handshake, any
    other packets must be ignored, so this method provides the ability to
    ignore unwanted commands.  It's primarily used during the initial
    connection to the device.  See Read() for more details, including more
    exceptions that may be raised.

    Args:
      expected_commands: Iterable of expected command responses, like
          ('CNXN', 'AUTH').
      timeout: timeouts.PolledTimeout object to use for timeout.

    Returns:
      The ADB message received that matched one of expected_commands.

    Raises:
      AdbProtocolError: If timeout expires between reads, this can happen
        if we are getting spammed with unexpected commands.
    """
    msg = timeouts.loop_until_timeout_or_valid(
        timeout, lambda: self.read_message(timeout),
        lambda m: m.command in expected_commands, 0)
    if msg.command not in expected_commands:
      raise usb_exceptions.AdbTimeoutError(
          'Timed out establishing connection, waiting for: %s',
          expected_commands)
    return msg


class DebugAdbTransportAdapter(AdbTransportAdapter):
  """Debug transport adapter that logs messages read/written."""

  def __init__(self, transport):
    super(DebugAdbTransportAdapter, self).__init__(transport)
    self.messages = []
    _LOG.debug('%s logging messages', self)

  def close(self):
    _LOG.debug('%s logged messages:', self)
    for message in self.messages:
      _LOG.debug(message)
    super(DebugAdbTransportAdapter, self).close()

  def read_message(self, timeout):
    message = super(DebugAdbTransportAdapter, self).read_message(timeout)
    self.messages.append('READING: %s' % message)
    return message

  def write_message(self, message, timeout):
    self.messages.append('WRITING: %s' % message)
    super(DebugAdbTransportAdapter, self).write_message(message, timeout)


class AdbMessage(object):
  """ADB Protocol message class.

  This class encapsulates all host<->device communication for ADB.  The
  attributes of this class correspond roughly to the ADB message struct.  The
  'command' attribute of this class is a stringified version of the unsigned
  command struct value, and is one of the values in ids ('SYNC', 'CNXN', 'AUTH',
  etc).  The arg0 and arg1 attributes have different meanings depending on
  the command (see adb_protocol.py for more info).

  This class stores the 'data' associated with a message, but some messages
  have no data (data will default to '').  Additionally, reading/writing
  messages to the wire results in two reads/writes because the data is actually
  sent in a second USB transaction.  This may have implications to transport
  layers that aren't direct libusb reads/writes to a local device (ie, over
  a network).

  The 'header' attribute returns the over-the-wire format of the header for
  this message.  To send a message over the header, send its header, followed
  by its data if it has any.

  Attributes:
    header
    command
    arg0
    arg1
    data
    magic
  """

  PRINTABLE_DATA = set(string.printable) - set(string.whitespace)
  CMD_TO_WIRE, WIRE_TO_CMD = make_wire_commands('SYNC', 'CNXN', 'AUTH', 'OPEN',
                                                'OKAY', 'CLSE', 'WRTE')
  # An ADB message is 6 words in little-endian.
  HEADER_STRUCT_FORMAT = '<6I'

  def __init__(self, command, arg0=0, arg1=0, data=''):
    if command not in self.CMD_TO_WIRE:
      raise usb_exceptions.AdbProtocolError('Unrecognized ADB command: %s',
                                            command)
    self._command = self.CMD_TO_WIRE[command]
    self.arg0 = arg0
    self.arg1 = arg1
    self.data = data
    self.magic = self._command ^ 0xFFFFFFFF

  @property
  def header(self):
    """The message header."""
    return struct.pack(
        self.HEADER_STRUCT_FORMAT, self._command, self.arg0, self.arg1,
        len(self.data), self.data_crc32, self.magic)

  @property
  def command(self):
    """The ADB command."""
    return self.WIRE_TO_CMD[self._command]

  def __str__(self):
    return '<%s: %s(%s, %s): %s (%s bytes)>' % (
        type(self).__name__,
        self.command,
        self.arg0,
        self.arg1,
        ''.join(char if char in self.PRINTABLE_DATA
                else '.' for char in self.data[:64]),
        len(self.data))
  __repr__ = __str__

  @property
  def data_crc32(self):
    """Return the sum of all the data bytes.

    The "crc32" used by ADB is actually just a sum of all the bytes, but we
    name this data_crc32 to be consistent with ADB.
    """
    return sum([ord(x) for x in self.data]) & 0xFFFFFFFF

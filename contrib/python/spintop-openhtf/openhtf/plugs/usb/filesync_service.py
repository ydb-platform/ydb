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


"""Implementation of the ADB SYNC protocol, used for push/pull commands.

This protocol is build on top of ADB streams, see adb_protocol.py for a high
level overview of this ADB protocol implementation. Note that we only implement
the host side of the protocol here, not the device side.

adb_cnxn = adb_protocol.AdbConnection.connect(usb_transport)
filesync = filesync_service.FilesyncService.using_connection(adb_cnxn)
stat = filesync.stat('/bin/sh')
print 'stat: ', stat

Documentation of this protocol is tough to find, so here it is:

Message Format:
  Every message starts with a single 32-bit word. (Everything is little-endian).
  Depending on that first word, the rest of the data can have various meanings.
  Messages from the host/desktop to the device always start with a 'request'
    message of a u32 command, u32 size, and size more bytes that's a filename,
    directory or symlink.
  The command here is referred to as 'id' in the code and the 4-letter codes
    are prefixed by ID_, eg ID_STAT.

  STAT:
    request:
      u32 command = 'STAT' == 0x53544154
      u32 size = len(filename) < 1024
      u8 data[size] = filename (no null)

    response:
      struct stat st = lstat(filename)
      u32 command = 'STAT'
      u32 mode = st.st_mode
      u32 size = st.st_size
      u32 time = st.st_mtime

  LIST:
    request:
      u32 command = 'LIST' == 0x4C495354
      u32 size = len(path) < 1024
      u8 data[size] = path (no null)

    response:
    for each filename in listing of path:
      struct stat st = lstat(filename)
      u32 command = 'DENT' == 0x44454E54
      u32 mode = st.st_mode
      u32 size = st.st_size
      u32 time = st.st_mtime
      u32 namelen = len(filename)
      u8 data[namelen] = filename

    done (device -> host):
      u32 command = 'DONE' == 0x444F4E45
      u32[4] = 0

  SEND:
    struct stat st = lstat(filename)
    request:
      fileinfo = sprintf(',%d', st.st_mode)
      u32 command = 'SEND' == 0x53454E44
      u32 size = len(filename) + len(fileinfo) < 1024
      u8 data[size] = filename + fileinfo

    repeated data command (host -> device):
      u32 command = 'DATA' == 0x44415441
      u32 size < (64 * 1024)
      u8 data[size] = file contents

    finish command (host -> device):
      u32 command = 'DONE' == 0x444F4E45
      u32 timestamp = st.st_mtime

    response (device -> host):
      u32 command = 'OKAY' == 0x4F4B4159 or 'FAIL' == 0x4641494C
      u32 size = 0 if 'OKAY' else len(fail_message)
      u8 data[] = fail_message

  RECV:
    request:
      u32 command = 'RECV' == 0x52454356
      u32 size = len(filename)
      u8 data[size] = filename

    repeated data response (device -> host):
      u32 command = 'DATA' == 0x44415441
      u32 size < (64 * 1024)
      u8 data[size] = file contents

    finish response (device -> host):
      u32 command = 'DONE' == 0x444F4E45
      u32 size = 0
"""

import collections
import stat
import struct
import sys
import time

from future.utils import raise_with_traceback
from openhtf.plugs.usb import adb_message
from openhtf.plugs.usb import usb_exceptions

# Default mode for pushed files, ADB will copy user permissions to group and
# other permissions, so we just default to full perms.
DEFAULT_PUSH_MODE = stat.S_IFREG | stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
# Maximum size of a filesync DATA packet.
MAX_PUSH_DATA_BYTES = 64 * 1024


DeviceFileStat = collections.namedtuple('DeviceFileStat', [
    'filename', 'mode', 'size', 'mtime'])


def _make_message_type(name, attributes, has_data=True):
  """Make a message type for the AdbTransport subclasses."""

  def assert_command_is(self, command):  # pylint: disable=invalid-name
    """Assert that a message's command matches the given command."""
    if self.command != command:
      raise usb_exceptions.AdbProtocolError(
          'Expected %s command, received %s', command, self)

  return type(name, (collections.namedtuple(name, attributes),),
              {
                  'assert_command_is': assert_command_is,
                  'has_data': has_data,
                  # Struct format on the wire has an unsigned int for each attr.
                  'struct_format': '<%sI' % len(attributes.split()),
              })


class FilesyncService(object):
  """Implements the FileSync protocol as described above.

  This class provides access to the methods provided by the ADB :sync protocol.
  Looking at /system/core/adb/file_sync_service.c in the Android source, the
  switch statement at the bottom of the file shows what commands are supported.

  Because the :sync service on the device side sits in a read-loop until the
  stream is closed, we can reuse the same stream for subsequent requests and
  save a little overhead instead of establishing a new stream each time, so we
  take an AdbStream on creation rather than an AdbConnection.  A helper
  classmethod, using_connection, is provided to create a FilesyncService
  directly using an AdbConnection.
  """

  def __init__(self, stream):
    self.stream = stream

  def __del__(self):  # pylint: disable=invalid-name
    self.close()

  def close(self):
    """Close the stream."""
    self.stream.close()

  def stat(self, filename, timeout=None):
    """Return device file stat."""
    transport = StatFilesyncTransport(self.stream)
    transport.write_data('STAT', filename, timeout)
    stat_msg = transport.read_message(timeout)
    stat_msg.assert_command_is('STAT')
    return DeviceFileStat(filename, stat_msg.mode, stat_msg.size, stat_msg.time)

  def list(self, path, timeout=None):
    """List directory contents on the device.

    Args:
      path: List the contents of this directory.
      timeout: Timeout to use for this operation.

    Returns:
      Generator yielding DeviceFileStat tuples representing the contents of
    the requested path.
    """
    transport = DentFilesyncTransport(self.stream)
    transport.write_data('LIST', path, timeout)
    return (DeviceFileStat(dent_msg.name, dent_msg.mode,
                           dent_msg.size, dent_msg.time) for dent_msg in
            transport.read_until_done('DENT', timeout))

  def recv(self, filename, dest_file, timeout=None):
    """Retrieve a file from the device into the file-like dest_file."""
    transport = DataFilesyncTransport(self.stream)
    transport.write_data('RECV', filename, timeout)
    for data_msg in transport.read_until_done('DATA', timeout):
      dest_file.write(data_msg.data)

  def _check_for_fail_message(self, transport, exc_info, timeout):  # pylint: disable=no-self-use
    """Check for a 'FAIL' message from transport.

    This method always raises, if 'FAIL' was read, it will raise an
    AdbRemoteError with the message, otherwise it will raise based on
    exc_info, which should be a tuple as per sys.exc_info().

    Args:
      transport: Transport from which to read for a 'FAIL' message.
      exc_info: Exception info to raise if no 'FAIL' is read.
      timeout: Timeout to use for the read operation.

    Raises:
      AdbRemoteError: If a 'FAIL' is read, otherwise raises exc_info.
    """
    try:
      transport.read_message(timeout)
    except usb_exceptions.CommonUsbError:
      # If we got a remote error, raise that exception.
      if sys.exc_info()[0] is usb_exceptions.AdbRemoteError:
        raise
    # Otherwise reraise the original exception.
    raise_with_traceback(exc_info[0](exc_info[1]), traceback=exc_info[2])

  # pylint: disable=too-many-arguments
  def send(self, src_file, filename, st_mode=DEFAULT_PUSH_MODE, mtime=None,
           timeout=None):
    """Push a file-like object to the device.

    Args:
      src_file: File-like object for reading from
      filename: Filename to push to on the device
      st_mode: stat mode for filename on the device
      mtime: modification time to set for the file on the device
      timeout: Timeout to use for the send operation.

    Raises:
      AdbProtocolError: If we get an unexpected response.
      AdbRemoteError: If there's a remote error (but valid protocol).
    """
    transport = DataFilesyncTransport(self.stream)
    transport.write_data('SEND', '%s,%s' % (filename, st_mode), timeout)

    try:
      while True:
        data = src_file.read(MAX_PUSH_DATA_BYTES)
        if not data:
          break
        transport.write_data('DATA', data, timeout)

      mtime = mtime or int(time.time())
      transport.write_message(
          FilesyncMessageTypes.DoneMessage('DONE', mtime), timeout)
    except usb_exceptions.AdbStreamClosedError:
      # Try to do one last read to see if we can get any more information,
      # ignoring any errors for this Read attempt. Note that this always
      # raises, either a new AdbRemoteError, or the AdbStreamClosedError.
      self._check_for_fail_message(transport, sys.exc_info(), timeout)

    data_msg = transport.read_message(timeout)
    data_msg.assert_command_is('OKAY')

  # pylint: enable=too-many-arguments

  @classmethod
  def using_connection(cls, connection, timeout=None):
    """Create a new FilesyncService using the given AdbConnection."""
    return cls(connection.open_stream('sync:', timeout))


class AbstractFilesyncTransport(object):
  """Transport to handle statefulness of the ADB sync service.

  Because message commands don't uniquely identify the structure of the message,
  this transport and filesync messages work a little differently than their ADB
  counterparts.  After an AdbStream is established with a remote :sync service,
  a 'req' message is sent to start a filesync transaction.  This transaction
  continues until a DONE message is sent, after which a new 'req' message may
  be sent (STAT commands only receive a single STAT response, and SEND commands
  receive an OKAY after sending a DONE message, so each command is actually a
  little different).

  The format of the messages transferred following the 'req' message depends on
  the id field of the 'req' message.  See this module's docstring for more
  details.  The FilesyncTransport exists between the sending of the 'req'
  message and the receiving/sending of the respective final message for the
  request type.  Upon creation of a FilesyncTransport, the caller determines
  which message type to expect by instantiating the appropriate subclass, based
  on the following union from ADB source:

    typedef union {
        unsigned id;
        struct {
            unsigned id;
            unsigned namelen;
        } req;
        struct {
            unsigned id;
            unsigned mode;
            unsigned size;
            unsigned time;
        } stat;
        struct {
            unsigned id;
            unsigned mode;
            unsigned size;
            unsigned time;
            unsigned namelen;
        } dent;
        struct {
            unsigned id;
            unsigned size;
        } data;
        struct {
            unsigned id;
            unsigned msglen;
        } status;
    } syncmsg;

  Subclasses are expected to provide the following class attributes:
    RECV_MSG_TYPE: The message type we expect to receive, must be one of the
      types in FilesyncMessageTypes below.
    VALID_RESPONSES: A tuple of valid command responses we can expect to
      receive over this transport.
  """
  CMD_TO_WIRE, WIRE_TO_CMD = adb_message.make_wire_commands(
      'STAT', 'LIST', 'SEND', 'RECV', 'DENT', 'DONE', 'DATA', 'OKAY', 'FAIL',
  )

  def __init__(self, stream):
    """Wrap the given stream in a FilesyncTransport.

    This transport takes ownership of the given stream - no other transports
    should use that stream until this one has been closed.  Closing this
    transport does *not* close the underlying stream, so it may be reused after
    this transport has been closed.  Note that transports provide no guarantee
    of thread-safety, unlike streams.  This means reads/writes are generally
    safe becauese of the thread safety provided by streamed, but Close() is not.

    This base class should not be instantiated directly - instead instantiate
    one of the subclasses below, based on what response type is expected.

    Args:
      stream: AdbStream to wrap with a new FilesyncTransport, it should be
        connected to the sync: service remotely.
    """
    assert hasattr(self, 'RECV_MSG_TYPE'), 'No RECV_MSG_TYPE set!'
    assert hasattr(self, 'VALID_RESPONSES'), 'No VALID_RESPONSES set!'
    self.stream = stream
    # pylint: disable=no-member

  def __str__(self):
    return '<%s(%s) id(%x), Receives: %s>' % (type(self).__name__, self.stream,
                                              id(self),
                                              self.RECV_MSG_TYPE.__name__)
  __repr__ = __str__

  def write_data(self, command, data, timeout=None):
    """Shortcut for writing specifically a DataMessage."""
    self.write_message(FilesyncMessageTypes.DataMessage(command, data), timeout)

  # pylint: disable=protected-access
  def write_message(self, msg, timeout=None):
    """Write an arbitrary message (of one of the types above).

    For the host side implementation, this will only ever be a DataMessage, but
    it's implemented generically enough here that you could use
    FilesyncTransport to implement the device side if you wanted.

    Args:
      msg:  The message to send, must be one of the types above.
      timeout: timeouts.PolledTimeout to use for the operation.
    """
    replace_dict = {'command': self.CMD_TO_WIRE[msg.command]}
    if msg.has_data:
      # Swap out data for the data length for the wire.
      data = msg[-1]
      replace_dict[msg._fields[-1]] = len(data)

    self.stream.write(struct.pack(msg.struct_format,
                                  *msg._replace(**replace_dict)), timeout)
    if msg.has_data:
      self.stream.write(data, timeout)

  # pylint: enable=protected-access

  def read_until_done(self, command, timeout=None):
    """Yield messages read until we receive a 'DONE' command.

    Read messages of the given command until we receive a 'DONE' command.  If a
    command different than the requested one is received, an AdbProtocolError
    is raised.

    Args:
      command: The command to expect, like 'DENT' or 'DATA'.
      timeout: The timeouts.PolledTimeout to use for this operation.

    Yields:
      Messages read, of type self.RECV_MSG_TYPE, see read_message().

    Raises:
      AdbProtocolError: If an unexpected command is read.
      AdbRemoteError: If a 'FAIL' message is read.
    """
    message = self.read_message(timeout)
    while message.command != 'DONE':
      message.assert_command_is(command)
      yield message
      message = self.read_message(timeout)

  def read_message(self, timeout=None):
    """Read a message from this transport and return it.

    Reads a message of RECV_MSG_TYPE and returns it.  Note that this method
    abstracts the data length and data read so that the caller simply gets the
    data along with the header in the returned message.

    Args:
      timeout: timeouts.PolledTimeout to use for the operation.

    Returns:
      An instance of self.RECV_MSG_TYPE that was read from self.stream.

    Raises:
      AdbProtocolError: If an invalid response is received.
      AdbRemoteError: If a FAIL response is received.
    """
    raw_data = self.stream.read(
        struct.calcsize(self.RECV_MSG_TYPE.struct_format), timeout)
    try:
      raw_message = struct.unpack(self.RECV_MSG_TYPE.struct_format, raw_data)
    except struct.error:
      raise usb_exceptions.AdbProtocolError(
          '%s expected format "%s", got data %s', self,
          self.RECV_MSG_TYPE.struct_format, raw_data)

    if raw_message[0] not in self.WIRE_TO_CMD:
      raise usb_exceptions.AdbProtocolError(
          'Unrecognized command id: %s', raw_message)

    # Swap out the wire command with the string equivalent.
    raw_message = (self.WIRE_TO_CMD[raw_message[0]],) + raw_message[1:]

    if self.RECV_MSG_TYPE.has_data and raw_message[-1]:
      # For messages that have data, the length of the data is the last field
      # in the struct.  We do another read and swap out that length for the
      # actual data read before we create the namedtuple to return.
      data_len = raw_message[-1]
      raw_message = raw_message[:-1] + (self.stream.read(data_len, timeout),)

    if raw_message[0] not in self.VALID_RESPONSES:
      raise usb_exceptions.AdbProtocolError(
          '%s not a valid response for %s', raw_message[0], self)
    if raw_message[0] == 'FAIL':
      raise usb_exceptions.AdbRemoteError(
          'Remote ADB failure: %s', raw_message)
    return self.RECV_MSG_TYPE(*raw_message)


class FilesyncMessageTypes(object):
  """Container for the various message types used by the Filesync protocol.

  These message types correspond roughly to the struct types contained within
  the union above.  Note that which commands are valid for which message types
  varies, see the header at the top of this file for details.  Also, we use
  DataMessage for status and req structs because they have the same format. We
  create a new type 'DoneMessage' because ADB uses a data message *with no
  actual data* to terminate a SEND command - and uses the 'size' field to send
  the mtime of the file... derp, this is actually a different type entirely, so
  we do that.
  """
  # pylint: disable=invalid-name
  DoneMessage = _make_message_type('DoneMessage',
                                   'command mtime',
                                   has_data=False)
  StatMessage = _make_message_type('StatMessage', 'command mode size time',
                                   has_data=False)
  DentMessage = _make_message_type('DentMessage', 'command mode size time name')
  DataMessage = _make_message_type('DataMessage', 'command data')
  # pylint: enable=invalid-name


class StatFilesyncTransport(AbstractFilesyncTransport):
  """Subclass of AbstractFilesyncTransport for STAT requests."""
  RECV_MSG_TYPE = FilesyncMessageTypes.StatMessage
  VALID_RESPONSES = ('STAT',)


class DentFilesyncTransport(AbstractFilesyncTransport):
  """Subclass of AbstractFilesyncTransport for LIST requests."""
  RECV_MSG_TYPE = FilesyncMessageTypes.DentMessage
  VALID_RESPONSES = ('DENT', 'DONE')


class DataFilesyncTransport(AbstractFilesyncTransport):
  """Sublcass of AbstractFilesyncTransport for DATA-based requests."""
  RECV_MSG_TYPE = FilesyncMessageTypes.DataMessage
  VALID_RESPONSES = ('DATA', 'DONE', 'OKAY', 'FAIL')

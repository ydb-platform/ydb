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


"""ADB protocol implementation.

Implements the ADB protocol as seen in Android's adb/adbd binaries, but only the
host side.  ADB communication is organized into messages (see adb_message.py).
Streams are built on top of these messages, using local and remote id numbers
to multiplex over the single channel (called the 'transport' here).

When a host connects to a device, there is first a CNXN handshake, which may
require authorization via AUTH messages.  Then, to connect to a particular
service on the device, the host sends an OPEN message containing local id by
which to refer to the resulting stream.  The classes defined here represent a
connection to a device with an AdbConnection, and an individual stream with an
AdbStream.

Multiple AdbStreams can be opened at once and used by multiple threads, and a
single AdbStream can have multiple threads reading from or writing to it, but
the latter is not recommended.

This implementation of the ADB stack looks something like:

  AdbDevice (Provides an interface similar to the adb commandline tool)
   |                 |
   V                 |
  AdbShellService    |
   |                 V
   |            AdbFilesyncService (Provides file push/pull functionality)
   |                 |
   V                 V
  AdbStream (Many may be opened simultaneously)
     |
     V
  AdbStreamTransport (Handles stream multiplexing and data queueing)
     |
     V
  AdbConnection (Only one open on a particular transport at a time)
     |
     V
  AdbMessage (Many of these are sent, comprising handshaking and AdbStreams)
     |
     V
  AdbTransportAdapter (Handles serialization/deserialization of messages)
     |
     V
  UsbHandle (Really, any transport that provides Read/Write, but usually USB)

Only AdbStream, AdbStreamTransport, and AdbConnection are implemented here.
This stack includes various utility classes that are intended for internal use
only, the classes implemented here that are intended for external users are:

  AdbStream: Intended to be used by anyone implementing their own service
    within ADB (such as shell:, tcp:, etc).  If you want to add support for
    a new service, you'll take in an AdbStream and use it for reads/writes
    from/to the remote endpoint for your service.

Example usage of a connection and stream:
  # Establish a connection to a device connected via my_usb_handle.
  connection = adb_protocol.AdbConnection.connect(my_usb_handle)

  # Read data from a 'shell:ls' destination stream and print the output.
  for line in connection.streaming_command('shell', 'ls'):
    print line
"""

import collections
import itertools
import logging
import threading

from enum import Enum

from openhtf.plugs.usb import adb_message
from openhtf.plugs.usb import usb_exceptions
from openhtf.util import argv
from openhtf.util import exceptions
from openhtf.util import timeouts
from six.moves import queue


ADB_MESSAGE_LOG = False

ARG_PARSER = argv.ModuleParser()
ARG_PARSER.add_argument('--adb_messsage_log',
                        action=argv.StoreTrueInModule,
                        target='%s.ADB_MESSAGE_LOG' % __name__,
                        help='Set to True to save all incoming and outgoing '
                        'AdbMessages and print them on Close().')

_LOG = logging.getLogger(__name__)

# Maximum amount of data in an ADB packet, we would like to raise this, but the
# remote end ignores it (ugh), we only send it to comply with the protocol.
MAX_ADB_DATA = 4096
# ADB protocol version.
ADB_VERSION = 0x01000000
# The local banner that we send to the remote device.
ADB_BANNER = 'googlex_adb'
# One greater than the maximum value for an ADB stream id.  There is a hard
# limit at 2**32, because it's an unsigned int, but we start seeing memory
# problems and the like way sooner, so lower this to catch leaking stream ids.
STREAM_ID_LIMIT = (2**16)


class AuthSigner(object):
  """Signer for use with authenticated ADB, introduced in 4.4.x/KitKat."""

  def sign(self, data):
    """Signs given data using a private key."""
    raise NotImplementedError()

  def get_public_key(self):
    """Returns the public key in PEM format without headers or newlines."""
    raise NotImplementedError()


class AdbStream(object):
  """This class represents an open ADB stream to a particular service.

  It encapsulates a local/remote id pair and any state associated with this
  particular open service connection.  This allows multiple streams to be
  open simultaneously.

  AdbStreams are thread-safe, but a write() will block until any previous
  write() has completed. As such, doing a huge write() followed by a short
  write() with a short timeout will likely result in the second write() timing
  out.

  Note that users should not instantiate this class directly, but rather should
  call open_stream() on an AdbConnection object.
  """

  def __init__(self, destination, transport):
    """Create a new ADB stream.

    Args:
      destination: String identifier for the destination of this stream.
      transport: AdbStreamTransport to use for reads/writes.
    """
    self._destination = destination
    self._transport = transport

  def is_closed(self):
    """Return True if the stream is closed."""
    return self._transport.is_closed()

  def __str__(self):
    return '<%s: (%s, %s->%s)>' % (type(self).__name__,
                                   self._destination,
                                   self._transport.local_id,
                                   self._transport.remote_id)
  __repr__ = __str__

  def write(self, data, timeout_ms=None):
    """Write data to this stream.

    Args:
      data: Data to write.
      timeout_ms: Timeout to use for the write/Ack transaction, in
        milliseconds (or as a PolledTimeout object).

    Raises:
      AdbProtocolError: If an ACK is not received.
      AdbStreamClosedError: If the stream is already closed, or gets closed
        before the write completes.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)
    # Break the data up into our transport's maxdata sized WRTE messages.
    while data:
      self._transport.write(
          data[:self._transport.adb_connection.maxdata], timeout)
      data = data[self._transport.adb_connection.maxdata:]

  def read(self, length=0, timeout_ms=None):
    """Reads data from the remote end of this stream.

    Internally, this data will have been contained in AdbMessages, but
    users of streams shouldn't need to care about the transport mechanism.

    Args:
      length: If provided, the number of bytes to read, otherwise all available
        data will be returned (at least one byte).
      timeout_ms: Time to wait for a message to come in for this stream, in
        milliseconds (or as a PolledTimeout object).

    Returns:
      Data that was read, or None if the end of the stream was reached.

    Raises:
      AdbProtocolError: Received an unexpected wonky non-stream packet (like a
        CNXN ADB message).
      AdbStreamClosedError: The stream is already closed.
      AdbTimeoutError: Timed out waiting for a message.
    """
    return self._transport.read(
        length, timeouts.PolledTimeout.from_millis(timeout_ms))

  def read_until_close(self, timeout_ms=None):
    """Yield data until this stream is closed.

    Args:
      timeout_ms: Timeout in milliseconds to keep reading (or a PolledTimeout
        object).

    Yields:
      Data read from a single call to self.read(), until the stream is closed
    or timeout is reached.

    Raises:
      AdbTimeoutError: On timeout.
    """
    while True:
      try:
        yield self.read(timeout_ms=timeout_ms)
      except usb_exceptions.AdbStreamClosedError:
        break

  def close(self, timeout_ms=100):
    """Close the stream."""
    self._transport.close(timeout_ms)


class AdbStreamTransport(object): # pylint: disable=too-many-instance-attributes
  """This class encapsulates the transport aspect of an ADB stream.

  This class handles the interface between AdbStreams and an AdbConnection,
  including the queue of messages that the AdbConnection has multiplexed and the
  stream state logic. This enables AdbStreams to use a simple Read/Write
  interface, and offloads some of the complicated state so the AdbConnection
  doesn't have to maintain it for many AdbStreams.

  Attributes:
    local_id: The local stream id for the stream using this transport.
    remote_id: The remote stream id for the stream using this transport.
    message_queue: The Queue of AdbMessages intended for this stream.
    closed: True if this transport has been closed, from either end.
  """
  ClosedState = Enum('ClosedState', ['CLOSED', 'PENDING', 'OPEN'])

  def __init__(self, adb_connection, local_id, message_queue):
    self.adb_connection = adb_connection
    self.local_id = local_id
    self.message_queue = message_queue
    # When we read the first OKAY, remote_id and closed will get updated.
    self.remote_id = None
    self.closed_state = self.ClosedState.PENDING
    # Read buffer, deque of string containing data read for this stream.
    self._read_buffer = collections.deque()
    self._buffer_size = 0  # Total bytes in _read_buffer.
    self._read_buffer_lock = threading.Lock()
    # Lock to protect against multiple simultaneous in-flight writes.
    self._write_lock = threading.Lock()
    # Some locking/events to protect against race conditions reading messages.
    self._expecting_okay = True
    self._message_received = threading.Condition()
    self._reader_lock = threading.Lock()

  def __str__(self):
    return '<%s: (%s->%s)>' % (type(self).__name__,
                               self.local_id,
                               self.remote_id)
  __repr__ = __str__

  def _set_or_check_remote_id(self, remote_id):
    """Set or check the remote id."""
    if not self.remote_id:
      assert self.closed_state == self.ClosedState.PENDING, 'Bad ClosedState!'
      self.remote_id = remote_id
      self.closed_state = self.ClosedState.OPEN
    elif self.remote_id != remote_id:
      raise usb_exceptions.AdbProtocolError(
          '%s remote-id change to %s', self, remote_id)

  def _send_command(self, command, timeout, data=''):
    """Send the given command/data over this transport.

    We do a couple sanity checks in here to be sure we can format a valid
    AdbMessage for our underlying AdbConnection, and then send it.  This method
    can be used to send any message type, and doesn't do any state tracking or
    acknowledgement checking.

    Args:
      command: The command to send, should be one of 'OKAY', 'WRTE', or 'CLSE'
      timeout: timeouts.PolledTimeout to use for this operation
      data: If provided, data to send with the AdbMessage.
    """
    if len(data) > self.adb_connection.maxdata:
      raise usb_exceptions.AdbProtocolError('Message data too long (%s>%s): %s',
                                            len(data),
                                            self.adb_connection.maxdata, data)
    if not self.remote_id:
      # If we get here, we probably missed the OKAY response to our OPEN.  We
      # should have failed earlier, but in case someone does something tricky
      # with multiple threads, we sanity check this here.
      raise usb_exceptions.AdbProtocolError('%s send before OKAY: %s',
                                            self, data)
    self.adb_connection.transport.write_message(
        adb_message.AdbMessage(command, self.local_id, self.remote_id, data),
        timeout)

  def _handle_message(self, message, handle_wrte=True):
    """Handle a message that was read for this stream.

    For each message type, this means:
      OKAY: Check id's and make sure we are expecting an OKAY.  Clear the
        self._expecting_okay flag so any pending write()'s know.
      CLSE: Set our internal state to closed.
      WRTE: Add the data read to our internal read buffer.  Note we don't
        return the actual data because it may not be this thread that needs it.

    Args:
      message: Message that was read.
      handle_wrte: If True, we can handle WRTE messages, otherwise raise.

    Raises:
      AdbProtocolError: If we get a WRTE message but handle_wrte is False.
    """
    if message.command == 'OKAY':
      self._set_or_check_remote_id(message.arg0)
      if not self._expecting_okay:
        raise usb_exceptions.AdbProtocolError(
            '%s received unexpected OKAY: %s', self, message)
      self._expecting_okay = False
    elif message.command == 'CLSE':
      self.closed_state = self.ClosedState.CLOSED
    elif not handle_wrte:
      raise usb_exceptions.AdbProtocolError(
          '%s received WRTE before OKAY/CLSE: %s', self, message)
    else:
      with self._read_buffer_lock:
        self._read_buffer.append(message.data)
        self._buffer_size += len(message.data)

  def _read_messages_until_true(self, predicate, timeout):
    """Read a message from this stream and handle it.

    This method tries to read a message from this stream, blocking until a
    message is read.  Once read, it will handle it accordingly by calling
    self._handle_message().

    This is repeated as long as predicate() returns False.  There is some
    locking used internally here so that we don't end up with multiple threads
    blocked on a call to read_for_stream when another thread has read the
    message that caused predicate() to become True.

    Args:
      predicate: Callable, keep reading messages until it returns true.  Note
        that predicate() should not block, as doing so may cause this method to
        hang beyond its timeout.
      timeout: Timeout to use for this call.

    Raises:
      AdbStreamClosedError: If this stream is already closed.
    """
    while not predicate():
      # Hold the message_received Lock while we try to acquire the reader_lock
      # and waiting on the message_received condition, to prevent another reader
      # thread from notifying the condition between us failing to acquire the
      # reader_lock and waiting on the condition.
      self._message_received.acquire()
      if self._reader_lock.acquire(False):
        try:
          # Release the message_received Lock while we do the read so other
          # threads can wait() on the condition without having to block on
          # acquiring the message_received Lock (we may have a longer timeout
          # than them, so that would be bad).
          self._message_received.release()

          # We are now the thread responsible for reading a message.  Check
          # predicate() to make sure nobody else read a message between our last
          # check and acquiring the reader Lock.
          if predicate():
            return

          # Read and handle a message, using our timeout.
          self._handle_message(
              self.adb_connection.read_for_stream(self, timeout))

          # Notify anyone interested that we handled a message, causing them to
          # check their predicate again.
          with self._message_received:
            self._message_received.notify_all()
        finally:
          self._reader_lock.release()
      else:
        # There is some other thread reading a message.  Since we are already
        # holding the message_received Lock, we can immediately do the wait.
        try:
          self._message_received.wait(timeout.remaining)
          if timeout.has_expired():
            raise usb_exceptions.AdbTimeoutError(
                '%s timed out reading messages.', self)
        finally:
          # Make sure we release this even if an exception occurred.
          self._message_received.release()

  def ensure_opened(self, timeout):
    """Ensure this stream transport was successfully opened.

    Checks to make sure we receive our initial OKAY message.  This must be
    called after creating this AdbStreamTransport and before calling read() or
    write().

    Args:
      timeout: timeouts.PolledTimeout to use for this operation.

    Returns:
      True if this stream was successfully opened, False if the service was
    not recognized by the remote endpoint.  If False is returned, then this
    AdbStreamTransport will be already closed.

    Raises:
      AdbProtocolError: If we receive a WRTE message instead of OKAY/CLSE.
    """
    self._handle_message(self.adb_connection.read_for_stream(self, timeout),
                         handle_wrte=False)
    return self.is_open()

  def is_open(self):
    """Return True if the transport layer is open."""
    return self.closed_state == self.ClosedState.OPEN

  def is_closed(self):
    """Return true if the transport layer is closed."""
    return self.closed_state == self.ClosedState.CLOSED

  def enqueue_message(self, message, timeout):
    """Add the given message to this transport's queue.

    This method also handles ACKing any WRTE messages.

    Args:
      message: The AdbMessage to enqueue.
      timeout: The timeout to use for the operation.  Specifically, WRTE
        messages cause an OKAY to be sent; timeout is used for that send.
    """
    # Ack WRTE messages immediately, handle our OPEN ack if it gets enqueued.
    if message.command == 'WRTE':
      self._send_command('OKAY', timeout=timeout)
    elif message.command == 'OKAY':
      self._set_or_check_remote_id(message.arg0)
    self.message_queue.put(message)

  def write(self, data, timeout):
    """Write data to this stream, using the given timeouts.PolledTimeout."""
    if not self.remote_id:
      raise usb_exceptions.AdbStreamClosedError(
          'Cannot write() to half-opened %s', self)
    if self.closed_state != self.ClosedState.OPEN:
      raise usb_exceptions.AdbStreamClosedError(
          'Cannot write() to closed %s', self)
    elif self._expecting_okay:
      raise usb_exceptions.AdbProtocolError(
          'Previous WRTE failed, %s in unknown state', self)

    # Make sure we only have one WRTE in flight at a time, because ADB doesn't
    # identify which WRTE it is ACK'ing when it sends the OKAY message back.
    with self._write_lock:
      self._expecting_okay = True
      self._send_command('WRTE', timeout, data)
      self._read_messages_until_true(lambda: not self._expecting_okay, timeout)

  def read(self, length, timeout):
    """Read 'length' bytes from this stream transport.

    Args:
      length: If not 0, read this many bytes from the stream, otherwise read all
        available data (at least one byte).
      timeout: timeouts.PolledTimeout to use for this read operation.

    Returns:
      The bytes read from this stream.
    """
    self._read_messages_until_true(
        lambda: self._buffer_size and self._buffer_size >= length, timeout)

    with self._read_buffer_lock:
      data, push_back = ''.join(self._read_buffer), ''
      if length:
        data, push_back = data[:length], data[length:]
      self._read_buffer.clear()
      self._buffer_size = len(push_back)
      if push_back:
        self._read_buffer.appendleft(push_back)
    return data

  def close(self, timeout_ms):
    """Close this stream, future reads/writes will fail."""
    if self.closed_state == self.ClosedState.CLOSED:
      return

    self.closed_state = self.ClosedState.CLOSED
    try:
      if not self.adb_connection.close_stream_transport(
          self, timeouts.PolledTimeout.from_millis(timeout_ms)):
        _LOG.warning('Attempt to close mystery %s', self)
    except usb_exceptions.AdbTimeoutError:
      _LOG.warning('%s close() timed out, ignoring', self)


class AdbConnection(object):
  """This class represents a connection to an ADB device.

  In the context of the ADB documentation (/system/core/adb/protocol.txt in the
  Android source), this class corresponds to a session initiated with a CONNECT
  message, likely followed by an AUTH message.

  This class does NOT represent an individual stream, identified by remote and
  local id's in the ADB documentation.  Instead, this class has an OpenStream
  method that corresponds to the OPEN message in the ADB documentation.  That
  method returns an AdbStream that can be used to write to a particular service
  on the device.

  Clients should never instantiate this class directly, but instead should use
  the connect() classmethod to open a new connection to a device.

  Attributes:
    transport: Underlying transport for this AdbConnection, usually USB.
    maxdata: Max data payload size supported by this AdbConnection.
    systemtype: System type, according to ADB's protocol.txt, one of 'device',
      'recovery', etc.
    serial: The 'serial number', as reported by ADB in the remote banner.
    banner: The 'human readable' component of the remote banner.
  """

  # pylint: disable=too-many-instance-attributes

  # AUTH constants for arg0.
  AUTH_TOKEN = 1
  AUTH_SIGNATURE = 2
  AUTH_RSAPUBLICKEY = 3

  def __init__(self, transport, maxdata, remote_banner):
    """Create an ADB connection to a device.

    Args:
      transport: AdbTransportAdapter to use for reading/writing AdbMessages
      maxdata: Max data size the remote endpoint will accept.
      remote_banner: Banner received from the remote endpoint.
    """
    try:
      self.systemtype, self.serial, self.banner = remote_banner.split(':', 2)
    except ValueError:
      raise usb_exceptions.AdbProtocolError('Received malformed banner %s',
                                            remote_banner)
    self.transport = transport
    self.maxdata = maxdata
    self._last_id_used = 0
    self._reader_lock = threading.Lock()
    self._open_lock = threading.Lock()
    # Maps local_id: AdbStreamTransport object for the relevant stream.
    self._stream_transport_map = {}
    self._stream_transport_map_lock = threading.RLock()

  def _make_stream_transport(self):
    """Create an AdbStreamTransport with a newly allocated local_id."""
    msg_queue = queue.Queue()
    with self._stream_transport_map_lock:
      # Start one past the last id we used, and grab the first available one.
      # This mimics the ADB behavior of 'increment an unsigned and let it
      # overflow', but with a check to ensure we don't reuse an id in use,
      # even though that's unlikely with 2^32 - 1 of them available.  We try
      # at most 64 id's, if we've wrapped around and there isn't one available
      # in the first 64, there's a problem, better to fail fast than hang for
      # a potentially very long time (STREAM_ID_LIMIT can be very large).
      self._last_id_used = (self._last_id_used % STREAM_ID_LIMIT) + 1
      for local_id in itertools.islice(
          itertools.chain(
              range(self._last_id_used, STREAM_ID_LIMIT),
              range(1, self._last_id_used)), 64):
        if local_id not in list(self._stream_transport_map.keys()):
          self._last_id_used = local_id
          break
      else:
        raise usb_exceptions.AdbStreamUnavailableError('Ran out of local ids!')
      # Ignore this warning - the for loop will always have at least one
      # iteration, so local_id will always be set.
      # pylint: disable=undefined-loop-variable
      stream_transport = AdbStreamTransport(self, local_id, msg_queue)
      self._stream_transport_map[local_id] = stream_transport
    return stream_transport

  def _handle_message_for_stream(self, stream_transport, message, timeout):
    """Handle an incoming message, check if it's for the given stream.

    If the message is not for the stream, then add it to the appropriate
    message queue.

    Args:
      stream_transport: AdbStreamTransport currently waiting on a message.
      message: Message to check and handle.
      timeout: Timeout to use for the operation, should be an instance of
        timeouts.PolledTimeout.

    Returns:
      The message read if it was for this stream, None otherwise.

    Raises:
      AdbProtocolError: If we receive an unexpected message type.
    """
    if message.command not in ('OKAY', 'CLSE', 'WRTE'):
      raise usb_exceptions.AdbProtocolError(
          '%s received unexpected message: %s', self, message)

    if message.arg1 == stream_transport.local_id:
      # Ack writes immediately.
      if message.command == 'WRTE':
        # Make sure we don't get a WRTE before an OKAY/CLSE message.
        if not stream_transport.remote_id:
          raise usb_exceptions.AdbProtocolError(
              '%s received WRTE before OKAY/CLSE: %s',
              stream_transport, message)
        self.transport.write_message(adb_message.AdbMessage(
            'OKAY', stream_transport.local_id, stream_transport.remote_id),
                                     timeout)
      elif message.command == 'CLSE':
        self.close_stream_transport(stream_transport, timeout)
      return message
    else:
      # Message was not for this stream, add it to the right stream's queue.
      with self._stream_transport_map_lock:
        dest_transport = self._stream_transport_map.get(message.arg1)

      if dest_transport:
        if message.command == 'CLSE':
          self.close_stream_transport(dest_transport, timeout)
        dest_transport.enqueue_message(message, timeout)
      else:
        _LOG.warning('Received message for unknown local-id: %s', message)

  def close(self):
    """Close the connection."""
    self.transport.close()

  def open_stream(self, destination, timeout_ms=None):
    """Opens a new stream to a destination service on the device.

    Not the same as the posix 'open' or any other Open methods, this
    corresponds to the OPEN message described in the ADB protocol
    documentation mentioned above.  It creates a stream (uniquely identified
    by remote/local ids) that connects to a particular service endpoint.

    Args:
      destination: The service:command string, see ADB documentation.
      timeout_ms: Timeout in milliseconds for the Open to succeed (or as a
        PolledTimeout object).

    Raises:
      AdbProtocolError: Wrong local_id sent to us, or we didn't get a ready
        response.

    Returns:
      An AdbStream object that can be used to read/write data to the specified
      service endpoint, or None if the requested service couldn't be opened.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)

    stream_transport = self._make_stream_transport()
    self.transport.write_message(
        adb_message.AdbMessage(
            command='OPEN',
            arg0=stream_transport.local_id, arg1=0,
            data=destination + '\0'),
        timeout)
    if not stream_transport.ensure_opened(timeout):
      return None
    return AdbStream(destination, stream_transport)

  def close_stream_transport(self, stream_transport, timeout):
    """Remove the given stream transport's id from our map of id's.

    If the stream id is actually removed, we send a CLSE message to let the
    remote end know (this happens when we are ack'ing a CLSE message we
    received).  The ADB protocol doesn't say this is a requirement, but ADB
    does it, so we do too.

    Args:
      stream_transport: The stream transport to close.
      timeout: Timeout on the operation.

    Returns:
      True if the id was removed and message sent, False if it was already
    missing from the stream map (already closed).
    """
    with self._stream_transport_map_lock:
      if stream_transport.local_id in self._stream_transport_map:
        del self._stream_transport_map[stream_transport.local_id]
        # If we never got a remote_id, there's no CLSE message to send.
        if stream_transport.remote_id:
          self.transport.write_message(adb_message.AdbMessage(
              'CLSE', stream_transport.local_id, stream_transport.remote_id),
                                       timeout)
        return True
    return False

  def streaming_command(self, service, command='', timeout_ms=None):
    """One complete set of packets for a single command.

    Helper function to call open_stream and yield the output.  Sends
    service:command in a new connection, reading the data for the response. All
    the data is held in memory, large responses will be slow and can fill up
    memory.

    Args:
      service: The service on the device to talk to.
      command: The command to send to the service.
      timeout_ms: Timeout for the entire command, in milliseconds (or as a
        PolledTimeout object).

    Yields:
      The data contained in the responses from the service.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)
    stream = self.open_stream('%s:%s' % (service, command), timeout)
    if not stream:
      raise usb_exceptions.AdbStreamUnavailableError(
          '%s does not support service: %s', self, service)
    for data in stream.read_until_close(timeout):
      yield data

  def read_for_stream(self, stream_transport, timeout_ms=None):
    """Attempt to read a packet for the given stream transport.

    Will read packets from self.transport until one intended for the given
    AdbStream is found.  If another thread is already reading packets, this will
    block until that thread reads a packet for this stream, or timeout expires.
    Note that this method always returns None, but if a packet was read for the
    given stream, it will have been added to that stream's message queue.

    This is somewhat tricky to do - first we check if there's a message already
    in our queue.  If not, then we try to use our AdbConnection to read a
    message for this stream.

    If some other thread is already doing reads, then read_for_stream() will sit
    in a tight loop, with a short delay, checking our message queue for a
    message from the other thread.

    Note that we must pass the queue in from the AdbStream, rather than looking
    it up in the AdbConnection's map, because the AdbConnection may have
    removed the queue from its map (while it still had messages in it).  The
    AdbStream itself maintains a reference to the queue to avoid dropping those
    messages.

    The AdbMessage read is guaranteed to be one of 'OKAY', 'WRTE', or 'CLSE'.
    If it was a WRTE message, then it will have been automatically ACK'd with an
    OKAY message, if it was a CLSE message it will have been ACK'd with a
    corresponding CLSE message, and this AdbStream will be marked as closed.

    Args:
      stream_transport: The AdbStreamTransport for the stream that is reading
        an AdbMessage from this AdbConnection.
      timeout_ms: If provided, timeout, in milliseconds, to use.  Note this
        timeout applies to this entire call, not for each individual Read, since
        there may be multiple reads if messages for other streams are read.
        This argument may be a timeouts.PolledTimeout.

    Returns:
      AdbMessage that was read, guaranteed to be one of 'OKAY', 'CLSE', or
    'WRTE' command.

    Raises:
      AdbTimeoutError: If we don't get a packet for this stream before
        timeout expires.
      AdbStreamClosedError: If the given stream has been closed.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)
    # Bail when the timeout expires, or when we no longer have the given stream
    # in our map (it may have been closed, we don't want to leave a thread
    # hanging in this loop when that happens).
    while (not timeout.has_expired() and
           stream_transport.local_id in self._stream_transport_map):
      try:
        # Block for up to 10ms to rate-limit how fast we spin.
        return stream_transport.message_queue.get(True, .01)
      except queue.Empty:
        pass

      # If someone else has the Lock, just keep checking our queue.
      if not self._reader_lock.acquire(False):
        continue

      try:
        # Now that we've acquired the Lock, we have to check the queue again,
        # just in case someone had the Lock but hadn't yet added our message
        # to the queue when we checked the first time.  Now that we have the
        # Lock ourselves, we're sure there are no potentially in-flight reads.
        try:
          return stream_transport.message_queue.get_nowait()
        except queue.Empty:
          pass

        while not timeout.has_expired():
          msg = self._handle_message_for_stream(
              stream_transport, self.transport.read_message(timeout), timeout)
          if msg:
            return msg
      finally:
        self._reader_lock.release()

    if timeout.has_expired():
      raise usb_exceptions.AdbTimeoutError(
          'Read timed out for %s', stream_transport)

    # The stream is no longer in the map, so it's closed, but check for any
    # queued messages.
    try:
      return stream_transport.message_queue.get_nowait()
    except queue.Empty:
      raise usb_exceptions.AdbStreamClosedError(
          'Attempt to read from closed or unknown %s', stream_transport)

  @classmethod
  def connect(cls, transport, rsa_keys=None, timeout_ms=1000,
              auth_timeout_ms=100):
    """Establish a new connection to a device, connected via transport.

    Args:
      transport: A transport to use for reads/writes from/to the device,
        usually an instance of UsbHandle, but really it can be anything with
        read() and write() methods.
      rsa_keys: List of AuthSigner subclass instances to be used for
        authentication. The device can either accept one of these via the sign
        method, or we will send the result of get_public_key from the first one
        if the device doesn't accept any of them.
      timeout_ms: Timeout to wait for the device to respond to our CNXN
        request.  Actual timeout may take longer if the transport object passed
        has a longer default timeout than timeout_ms, or if auth_timeout_ms is
        longer than timeout_ms and public key auth is used.  This argument may
        be a PolledTimeout object.
      auth_timeout_ms: Timeout to wait for when sending a new public key. This
        is only relevant when we send a new public key. The device shows a
        dialog and this timeout is how long to wait for that dialog. If used
        in automation, this should be low to catch such a case as a failure
        quickly; while in interactive settings it should be high to allow
        users to accept the dialog. We default to automation here, so it's low
        by default.  This argument may be a PolledTimeout object.

    Returns:
      An instance of AdbConnection that is connected to the device.

    Raises:
      usb_exceptions.DeviceAuthError: When the device expects authentication,
        but we weren't given any valid keys.
      usb_exceptions.AdbProtocolError: When the device does authentication in an
        unexpected way, or fails to respond appropriately to our CNXN request.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)
    if ADB_MESSAGE_LOG:
      adb_transport = adb_message.DebugAdbTransportAdapter(transport)
    else:
      adb_transport = adb_message.AdbTransportAdapter(transport)
    adb_transport.write_message(
        adb_message.AdbMessage(
            command='CNXN', arg0=ADB_VERSION, arg1=MAX_ADB_DATA,
            data='host::%s\0' % ADB_BANNER),
        timeout)

    msg = adb_transport.read_until(('AUTH', 'CNXN'), timeout)
    if msg.command == 'CNXN':
      return cls(adb_transport, msg.arg1, msg.data)

    # We got an AUTH response, so we have to try to authenticate.
    if not rsa_keys:
      raise usb_exceptions.DeviceAuthError(
          'Device authentication required, no keys available.')

    # Loop through our keys, signing the last 'banner' or token.
    for rsa_key in rsa_keys:
      if msg.arg0 != cls.AUTH_TOKEN:
        raise usb_exceptions.AdbProtocolError('Bad AUTH response: %s', msg)

      signed_token = rsa_key.sign(msg.data)
      adb_transport.write_message(
          adb_message.AdbMessage(
              command='AUTH', arg0=cls.AUTH_SIGNATURE, arg1=0,
              data=signed_token),
          timeout)

      msg = adb_transport.read_until(('AUTH', 'CNXN'), timeout)
      if msg.command == 'CNXN':
        return cls(adb_transport, msg.arg1, msg.data)

    # None of the keys worked, so send a public key.
    adb_transport.write_message(
        adb_message.AdbMessage(
            command='AUTH', arg0=cls.AUTH_RSAPUBLICKEY, arg1=0,
            data=rsa_keys[0].get_public_key() + '\0'),
        timeout)
    try:
      msg = adb_transport.read_until(
          ('CNXN',), timeouts.PolledTimeout.from_millis(auth_timeout_ms))
    except usb_exceptions.UsbReadFailedError as exception:
      if exception.is_timeout():
        exceptions.reraise(usb_exceptions.DeviceAuthError,
                           'Accept auth key on device, then retry.')
      raise

    # The read didn't time-out, so we got a CNXN response.
    return cls(adb_transport, msg.arg1, msg.data)

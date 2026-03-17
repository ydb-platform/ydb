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


"""Some handy interfaces to the ADB :shell service.

The :shell service is pretty straightforward, you send 'shell:command' and
the device runs /bin/sh -c 'command'.  The ADB daemon on the device sets up a
PTY, similar to what ssh would do, to provide interactive terminal features.
This makes things difficult if you're trying to pipe binary data through a
remote command; however, we provide some facilities for 'raw' commands, where
we first execute an ioctl to turn off things like character translation and
local echo, providing a more sane programmatic interface.

The ShellService also provides some handy methods for running commands
asynchronously, either by returning a handle, or using a with: context.

Note that this service differs from the FilesyncService in that streams opened
to the :shell service are closed immediately after the command completes;
streams opened to the :sync service remain open for multiple sync requests,
until they are closed explicitly.  This means there's no point in keeping a
stream around ShellService, we need to keep an AdbConnection around instead.

Some examples of how to use this service:
  adb_cnxn = adb_protocol.AdbConnection.connect(my_transport)
  shell = shell_service.ShellService(adb_cnxn)

  # Run a simple command.
  output = shell.command('echo foo')
  # output == 'foo\r\n'

  # Run a command that outputs binary data, like recording a minute of audio.
  output = shell.RawCommand('arecord -Dhw:CARD=0,DEV=0 -c 2 -d 60')

  # Run a command in the background, do some other stuff, then read the
  # command's output, waiting on it to complete.
  cmd = shell.async_command('echo foo; sleep 10')
  bar = shell.command('echo bar')
  foo = cmd.wait()
  baz = shell.command('echo baz')

  # A version using a with context to do the same thing:
  with shell.async_command('echo foo; sleep 10') as c:
    bar = shell.command('echo bar')
    foo = c.wait()
  baz = shell.command('echo baz')

  # Run a command in the background while we do some other stuff, save the
  # output to a StringIO buffer so we can access it later.  Use a context to
  # automatically wait for the asynchronous command to finish.
  output = cStringIO.StringIO()
  with shell.AsyncRawCommand(
      'arecord -Dhw:CARD=0,DEV=0 -c 2 -d 60', stdout=output):
    # Do some stuff, play some sounds on some fixture speakers, for example.
    pass
  # Execution won't get here until the arecord command completes, and
  # output.getvalue() now contains the output of the arecord command.
"""

import threading
import time

from openhtf.plugs.usb import adb_protocol
from openhtf.plugs.usb import usb_exceptions

from openhtf.util import timeouts
import six


class AsyncCommandHandle(object):
  """This class is used for interacting with an asynchronous command.

  This handle is used to close a command or to wait on it to complete. Data is
  read from stdin and written to the command's stdin, and output from the
  command is written to stdout. If stdin is None, no input is written to the
  command.  If stdout is None, the output from the command is buffered
  internally, and will be returned from a call to wait() - see the wait() method
  for details.

  You can tell if a stream was closed locally by checking the
  'force_closed_or_timeout' attribute.  If a command completes instead of being
  closed by a call to Close (or a timeout), then 'force_closed_or_timeout' will
  be False, otherwise it will be True.
  """

  def __init__(self, stream, stdin, stdout, timeout, is_raw):  #pylint: disable=too-many-arguments
    """Create a handle to use for interfacing with an async_command.

    Args:
      stream: Stream to use for communicating with the running command.
      stdin: File-like object to use for reading stdin for the command, can be
        None, in which case no input is sent to the command.
      stdout: File-like object to use for writing output of the command to, can
        be None, in which case output can be obtained by calling wait().
      timeout: timeouts.PolledTimeout to use for the command.
      is_raw: If True, we'll do reads from stdin, otherwise we do readlines
        instead to play nicer with potential interactive uses (read doesn't
        return until EOF, but interactively you want to send each line and
        then see the response).  stdout is treated the same in either case,
        read is used - AdbStreams don't support readline.
    """
    self.stream = stream
    self.stdin = stdin
    self.stdout = stdout or six.StringIO()
    self.force_closed_or_timeout = False

    self.reader_thread = threading.Thread(target=self._reader_thread_proc,
                                          args=(timeout,))
    self.reader_thread.daemon = True
    self.reader_thread.start()

    if stdin:
      self.writer_thread = threading.Thread(target=self._writer_thread_proc,
                                            args=(is_raw,))
      self.writer_thread.daemon = True
      self.writer_thread.start()

    # Close ourselves after timeout expires, ignored if timeout won't expire.
    timeouts.execute_after_delay(timeout, self.close)

  def _writer_thread_proc(self, is_raw):
    """Write as long as the stream is not closed."""
    # If we're not in raw mode, do line-buffered reads to play nicer with
    # potential interactive uses, max of MAX_ADB_DATA, since anything we write
    # to the stream will get packetized to that size anyway.
    #
    # Loop until our stream gets closed, which will cause one of these
    # operations to raise.  Since we're in a separate thread, it'll just get
    # ignored, which is what we want.
    reader = self.stdin.read if is_raw else self.stdin.readline
    while not self.stream.is_closed():
      self.stream.write(reader(adb_protocol.MAX_ADB_DATA))

  def _reader_thread_proc(self, timeout):
    """Read until the stream is closed."""
    for data in self.stream.read_until_close(timeout_ms=timeout):
      if self.stdout is not None:
        self.stdout.write(data)

  def __enter__(self):  # pylint: disable=invalid-name
    return self

  def __exit__(self, exc_type, exc_value, exc_tb):  # pylint: disable=invalid-name
    if exc_type:
      return False
    self.wait()
    return True

  def close(self):
    """Close this handle immediately - you may lose output."""
    self.force_closed_or_timeout = True
    self.stream.close()

  def is_done(self):
    """Return True if this command has completed."""
    return self.stream.is_closed()

  def wait(self, timeout_ms=None):
    """Block until this command has completed.

    Args:
      timeout_ms: Timeout, in milliseconds, to wait.

    Returns:
      Output of the command if it complete and self.stdout is a StringIO
    object or was passed in as None.  Returns True if the command completed but
    stdout was provided (and was not a StringIO object).  Returns None if the
    timeout expired before the command completed.  Be careful to check the
    return value explicitly for None, as the output may be ''.
    """
    closed = timeouts.loop_until_timeout_or_true(
        timeouts.PolledTimeout.from_millis(timeout_ms),
        self.stream.is_closed, .1)
    if closed:
      if hasattr(self.stdout, 'getvalue'):
        return self.stdout.getvalue()
      return True
    return None


class ShellService(object):
  """Class providing a high-level interface to ADB's :shell service.

  This class provides synchronous and asynchronous commands, and a variety of
  ways for getting input into and out of them.
  """

  def __init__(self, adb_connection):
    self.adb_connection = adb_connection

  @staticmethod
  def _to_raw_command(command):
    """Convert the command to a raw signal."""
    # Android doesn't have stty, so we manually do the ioctl (yuck).  This ioctl
    # is a TCSETA (0x5403) with the following flags set:
    #   Control bits:
    #     B38400 (set baud rate)
    #     CS8 (8-bit bytes)
    #     CREAD (Enable input from terminal)
    #   Input, Output, Local bits all cleared
    #
    # We also update VMIN from 0x0 to 0xff so read() waits for at least one byte
    # to be ready before returning (we leave the default VTIME at 0x4).  Note
    # that we leave the other control characters at their defaults, but they
    # should be ignored since we disable them with flags and put the terminal
    # into non-canonical input mode (not newline delimited).
    return ('ioctl -l 23 -a 1 /proc/self/fd/0 0x5403 '  # TCSETA (0x5403)
            '0 0 0 0 0 0 0 0 0xbf 0 0 0 0 0 0 0 '  # Flags
            '0 0x3 0x1c 0x7f 0x15 0x4 0xff '  # Control characters
            '&>/dev/null;%s' % command)

  def command(self, command, raw=False, timeout_ms=None):
    """Run the given command and return the output."""
    return ''.join(self.streaming_command(command, raw, timeout_ms))

  def streaming_command(self, command, raw=False, timeout_ms=None):
    """Run the given command and yield the output as we receive it."""
    if raw:
      command = self._to_raw_command(command)
    return self.adb_connection.streaming_command('shell', command, timeout_ms)

  # pylint: disable=too-many-arguments
  def async_command(self, command, stdin=None, stdout=None, raw=False,
                    timeout_ms=None):
    """Run the given command on the device asynchronously.

    Input will be read from stdin, output written to stdout.  ADB doesn't
    distinguish between stdout and stdin on the device, so they get interleaved
    into stdout here.  stdin and stdout should be file-like objects, so you
    could use sys.stdin and sys.stdout to emulate the 'adb shell' commandline.

    Args:
      command: The command to run, will be run with /bin/sh -c 'command' on
        the device.
      stdin: File-like object to read from to pipe to the command's stdin.  Can
        be None, in which case nothing will be written to the command's stdin.
      stdout: File-like object to write the command's output to.  Can be None,
        in which case the command's output will be buffered internally, and can
        be access via the return value of wait().
      raw: If True, run the command as per RawCommand (see above).
      timeout_ms: Timeout for the command, in milliseconds.

    Returns:
      An AsyncCommandHandle instance that can be used to send/receive data to
    and from the command or wait on the command to finish.

    Raises:
      AdbStreamUnavailableError: If the remote devices doesn't support the
        shell: service.
    """
    timeout = timeouts.PolledTimeout.from_millis(timeout_ms)
    if raw:
      command = self._to_raw_command(command)
    stream = self.adb_connection.open_stream('shell:%s' % command, timeout)
    if not stream:
      raise usb_exceptions.AdbStreamUnavailableError(
          '%s does not support service: shell', self)
    if raw and stdin is not None:
      # Short delay to make sure the ioctl to set raw mode happens before we do
      # any writes to the stream, if we don't do this bad things happen...
      time.sleep(.1)
    return AsyncCommandHandle(stream, stdin, stdout, timeout, raw)
  # pylint: enable=too-many-arguments

  @classmethod
  def using_connection(cls, adb_connection):
    """Factory method to match the interface of FilesyncService."""
    return cls(adb_connection)

"""Capture C-level FD output on pipes

Use `wurlitzer.pipes` or `wurlitzer.sys_pipes` as context managers.
"""

from __future__ import print_function

__version__ = '3.1.1'

__all__ = [
    'pipes',
    'sys_pipes',
    'sys_pipes_forever',
    'stop_sys_pipes',
    'PIPE',
    'STDOUT',
    'Wurlitzer',
]

import ctypes
import errno
import io
import logging
import os
import platform
import selectors
import sys
import threading
import time
import warnings
from contextlib import contextmanager
from fcntl import F_GETFL, F_SETFL, fcntl
from functools import lru_cache
from queue import Queue

try:
    from fcntl import F_GETPIPE_SZ, F_SETPIPE_SZ
except ImportError:
    # ref: linux uapi/linux/fcntl.h
    F_SETPIPE_SZ = 1024 + 7
    F_GETPIPE_SZ = 1024 + 8

libc = ctypes.CDLL(None)


def _get_streams_cffi():
    """Use CFFI to lookup stdout/stderr pointers

    Should work ~everywhere, but requires compilation
    """
    try:
        import cffi
    except ImportError:
        raise ImportError(
            "Failed to lookup stdout symbols in libc. Fallback requires cffi."
        )

    try:
        _ffi = cffi.FFI()
        _ffi.cdef("const size_t c_stdout_p();")
        _ffi.cdef("const size_t c_stderr_p();")
        _lib = _ffi.verify(
            '\n'.join(
                [
                    "#include <stdio.h>",
                    "const size_t c_stdout_p() { return (size_t) (void*) stdout; }",
                    "const size_t c_stderr_p() { return (size_t) (void*) stderr; }",
                ]
            )
        )
        c_stdout_p = ctypes.c_void_p(_lib.c_stdout_p())
        c_stderr_p = ctypes.c_void_p(_lib.c_stderr_p())
    except Exception as e:
        warnings.warn(
            "Failed to lookup stdout with cffi: {}.\nStreams may not be flushed.".format(
                e
            )
        )
        return (None, None)
    else:
        return c_stdout_p, c_stderr_p


c_stdout_p = c_stderr_p = None
try:
    c_stdout_p = ctypes.c_void_p.in_dll(libc, 'stdout')
    c_stderr_p = ctypes.c_void_p.in_dll(libc, 'stderr')
except ValueError:
    # libc.stdout has a funny name on macOS
    try:
        c_stdout_p = ctypes.c_void_p.in_dll(libc, '__stdoutp')
        c_stderr_p = ctypes.c_void_p.in_dll(libc, '__stderrp')
    except ValueError:
        c_stdout_p, c_stderr_p = _get_streams_cffi()


STDOUT = 2
PIPE = 3

_default_encoding = getattr(sys.stdin, 'encoding', None) or 'utf8'
if _default_encoding.lower() == 'ascii':
    # don't respect ascii
    _default_encoding = 'utf8'  # pragma: no cover


def dup2(a, b, timeout=3):
    """Like os.dup2, but retry on EBUSY"""
    dup_err = None
    # give FDs 3 seconds to not be busy anymore
    for i in range(int(10 * timeout)):
        try:
            return os.dup2(a, b)
        except OSError as e:
            dup_err = e
            if e.errno == errno.EBUSY:
                time.sleep(0.1)
            else:
                raise
    if dup_err:
        raise dup_err


@lru_cache()
def _get_max_pipe_size():
    """Get max pipe size

    Reads /proc/sys/fs/pipe-max-size on Linux.
    Always returns None elsewhere.

    Returns integer (up to 1MB),
    or None if no value can be determined.

    Adapted from wal-e, (c) 2018, WAL-E Contributors
    used under BSD-3-clause
    """
    if platform.system() != 'Linux':
        return

    # If Linux procfs (or something that looks like it) exposes its
    # maximum F_SETPIPE_SZ, adjust the default buffer sizes.
    try:
        with open('/proc/sys/fs/pipe-max-size', 'r') as f:
            # Figure out OS max pipe size
            pipe_max_size = int(f.read())
    except Exception:
        pass
    else:
        if pipe_max_size > 1024 * 1024:
            # avoid unusually large values, limit to 1MB
            return 1024 * 1024
        elif pipe_max_size <= 65536:
            # smaller than default, don't do anything
            return None
        else:
            return pipe_max_size


class Wurlitzer:
    """Class for Capturing Process-level FD output via dup2

    Typically used via `wurlitzer.pipes`
    """

    flush_interval = 0.2

    def __init__(
        self,
        stdout=None,
        stderr=None,
        encoding=_default_encoding,
        bufsize=_get_max_pipe_size(),
    ):
        """
        Parameters
        ----------
        stdout: stream or None
            The stream for forwarding stdout.
        stderr = stream or None
            The stream for forwarding stderr.
        encoding: str or None
            The encoding to use, if streams should be interpreted as text.
        bufsize: int or None
            Set pipe buffer size using fcntl F_SETPIPE_SZ (linux only)
            default: use /proc/sys/fs/pipe-max-size up to a max of 1MB
            if 0, will do nothing.
        """
        # accept logger objects
        if stdout and isinstance(stdout, logging.Logger):
            stdout = _LogPipe(stdout, stream_name="stdout", level=logging.INFO)
        if stderr and isinstance(stderr, logging.Logger):
            stderr = _LogPipe(stderr, stream_name="stderr", level=logging.ERROR)

        self._stdout = stdout
        if stderr == STDOUT:
            self._stderr = self._stdout
        else:
            self._stderr = stderr
        self.encoding = encoding
        if bufsize is None:
            bufsize = _get_max_pipe_size()
        self._bufsize = bufsize
        self._save_fds = {}
        self._real_fds = {}
        self._handlers = {}
        self._handlers['stderr'] = self._handle_stderr
        self._handlers['stdout'] = self._handle_stdout

    def _setup_pipe(self, name):
        real_fd = getattr(sys, '__%s__' % name).fileno()
        save_fd = os.dup(real_fd)
        self._save_fds[name] = save_fd
        self._real_fds[name] = real_fd

        try:
            capture_fd = getattr(self, "_" + name).fileno()
        except Exception:
            pass
        else:
            # if it has a fileno(),
            # dup directly to capture file,
            # no pipes needed
            dup2(capture_fd, real_fd)
            return None

        pipe_out, pipe_in = os.pipe()
        # set max pipe buffer size (linux only)
        if self._bufsize:
            try:
                fcntl(pipe_in, F_SETPIPE_SZ, self._bufsize)
            except OSError as error:
                warnings.warn(
                    "Failed to set pipe buffer size: " + str(error), RuntimeWarning
                )

        dup2(pipe_in, real_fd)
        os.close(pipe_in)

        # make pipe_out non-blocking
        flags = fcntl(pipe_out, F_GETFL)
        fcntl(pipe_out, F_SETFL, flags | os.O_NONBLOCK)
        return pipe_out

    def _decode(self, data):
        """Decode data, if any

        Called before passing to stdout/stderr streams
        """
        if self.encoding:
            data = data.decode(self.encoding, 'replace')
        return data

    def _handle_stdout(self, data):
        if self._stdout:
            self._stdout.write(self._decode(data))

    def _handle_stderr(self, data):
        if self._stderr:
            self._stderr.write(self._decode(data))

    def _setup_handle(self):
        """Setup handle for output, if any"""
        self.handle = (self._stdout, self._stderr)

    def _finish_handle(self):
        """Finish handle, if anything should be done when it's all wrapped up."""
        pass

    def _flush(self):
        """flush sys.stdout/err and low-level FDs"""
        if self._stdout and sys.stdout:
            sys.stdout.flush()
        if self._stderr and sys.stderr:
            sys.stderr.flush()

        if c_stdout_p is not None:
            libc.fflush(c_stdout_p)

        if c_stderr_p is not None:
            libc.fflush(c_stderr_p)

    def __enter__(self):
        # flush anything out before starting
        self._flush()
        # setup handle
        self._setup_handle()

        # create pipe for stdout
        pipes = []
        names = {}
        if self._stdout:
            pipe = self._setup_pipe('stdout')
            if pipe:
                pipes.append(pipe)
                names[pipe] = 'stdout'
        if self._stderr:
            pipe = self._setup_pipe('stderr')
            if pipe:
                pipes.append(pipe)
                names[pipe] = 'stderr'

        if not pipes:
            # no pipes to handle (e.g. direct FD capture)
            # so no forwarder thread needed
            self.thread = None
            return self.handle

        # setup forwarder thread

        self._control_r, self._control_w = os.pipe()
        pipes.append(self._control_r)
        names[self._control_r] = "control"

        # flush pipes in a background thread to avoid blocking
        # the reader thread when the buffer is full
        flush_queue = Queue()

        def flush_main():
            while True:
                msg = flush_queue.get()
                if msg == 'stop':
                    return
                self._flush()

        flush_thread = threading.Thread(target=flush_main)
        flush_thread.daemon = True
        flush_thread.start()

        def forwarder():
            """Forward bytes on a pipe to stream messages"""
            draining = False
            flush_interval = 0
            poller = selectors.DefaultSelector()

            for pipe_ in pipes:
                poller.register(pipe_, selectors.EVENT_READ)

            while pipes:
                events = poller.select(flush_interval)
                if events:
                    # found something to read, don't block select until
                    # we run out of things to read
                    flush_interval = 0
                else:
                    # nothing to read
                    if draining:
                        # if we are draining and there's nothing to read, stop
                        break
                    else:
                        # nothing to read, get ready to wait.
                        # flush the streams in case there's something waiting
                        # to be written.
                        flush_queue.put('flush')
                        flush_interval = self.flush_interval
                        continue

                for selector_key, flags in events:
                    fd = selector_key.fd
                    if fd == self._control_r:
                        draining = True
                        pipes.remove(self._control_r)
                        poller.unregister(self._control_r)
                        os.close(self._control_r)
                        continue
                    name = names[fd]
                    data = os.read(fd, 1024)
                    if not data:
                        # pipe closed, stop polling it
                        pipes.remove(fd)
                        poller.unregister(fd)
                        os.close(fd)
                    else:
                        handler = getattr(self, '_handle_%s' % name)
                        handler(data)
                if not pipes:
                    # pipes closed, we are done
                    break
            # stop flush thread
            flush_queue.put('stop')
            flush_thread.join()
            # cleanup pipes
            [os.close(pipe) for pipe in pipes]
            poller.close()

        self.thread = threading.Thread(target=forwarder)
        self.thread.daemon = True
        self.thread.start()

        return self.handle

    def __exit__(self, exc_type, exc_value, traceback):
        # flush before exiting
        self._flush()
        if self.thread:
            # signal output is complete on control pipe
            os.write(self._control_w, b'\1')
            self.thread.join()
            os.close(self._control_w)

        # restore original state
        for name, real_fd in self._real_fds.items():
            save_fd = self._save_fds[name]
            dup2(save_fd, real_fd)
            os.close(save_fd)
        # finalize handle
        self._finish_handle()


@contextmanager
def pipes(stdout=PIPE, stderr=PIPE, encoding=_default_encoding, bufsize=None):
    """Capture C-level stdout/stderr in a context manager.

    The return value for the context manager is (stdout, stderr).

    Args:

    stdout (optional, default: PIPE): None or PIPE or Writable or Logger
    stderr (optional, default: PIPE): None or PIPE or STDOUT or Writable or Logger
    encoding (optional): probably 'utf-8'
    bufsize (optional): set explicit buffer size if the default doesn't work

    .. versionadded:: 3.1
        Accept Logger objects for stdout/stderr.
        If a Logger is specified, each line will produce a log message.
        stdout messages will be at INFO level, stderr messages at ERROR level.

    .. versionchanged:: 3.0

        when using `PIPE` (default), the type of captured output
        is `io.StringIO/BytesIO` instead of an OS pipe.
        This eliminates max buffer size issues (and hang when output exceeds 65536 bytes),
        but also means the buffer cannot be read with `.read()` methods
        until after the context exits.

    Examples
    --------

    >>> with pipes() as (stdout, stderr):
    ...     printf("C-level stdout")
    ... output = stdout.read()
    """
    stdout_pipe = stderr_pipe = False
    if encoding:
        PipeIO = io.StringIO
    else:
        PipeIO = io.BytesIO

    # accept logger objects
    if stdout and isinstance(stdout, logging.Logger):
        stdout = _LogPipe(stdout, stream_name="stdout", level=logging.INFO)
    if stderr and isinstance(stderr, logging.Logger):
        stderr = _LogPipe(stderr, stream_name="stderr", level=logging.ERROR)

    # setup stdout
    if stdout == PIPE:
        stdout_r = stdout_w = PipeIO()
        stdout_pipe = True
    else:
        stdout_r = stdout_w = stdout
    # setup stderr
    if stderr == STDOUT:
        stderr_r = None
        stderr_w = stdout_w
    elif stderr == PIPE:
        stderr_r = stderr_w = PipeIO()
        stderr_pipe = True
    else:
        stderr_r = stderr_w = stderr
    w = Wurlitzer(stdout=stdout_w, stderr=stderr_w, encoding=encoding, bufsize=bufsize)
    try:
        with w:
            yield stdout_r, stderr_r
    finally:
        if stdout and isinstance(stdout, _LogPipe):
            stdout.flush()
        if stderr and isinstance(stderr, _LogPipe):
            stderr.flush()
        # close pipes
        if stdout_pipe:
            # seek to 0 so that it can be read after exit
            stdout_r.seek(0)
        if stderr_pipe:
            # seek to 0 so that it can be read after exit
            stderr_r.seek(0)


class _LogPipe(io.BufferedWriter):
    """Writeable that writes lines to a Logger object as they arrive from captured pipes"""

    def __init__(self, logger, stream_name, level=logging.INFO):
        self.logger = logger
        self.stream_name = stream_name
        self._buf = ""
        self.level = level

    def _log(self, line):
        """Log one line"""
        self.logger.log(self.level, line.rstrip(), extra={"stream": self.stream_name})

    def write(self, chunk):
        """Given chunk, split into lines

        Log each line as a discrete message

        If it ends with a partial line, save it until the next one
        """
        lines = chunk.splitlines(True)
        if self._buf:
            lines[0] = self._buf + lines[0]
        if lines[-1].endswith("\n"):
            self._buf = ""
        else:
            # last line is incomplete
            self._buf = lines[-1]
            lines = lines[:-1]

        for line in lines:
            self._log(line)

    def flush(self):
        """Write buffer as a last message if there is one"""
        if self._buf:
            self._log(self._buf)
            self._buf = ""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.flush()


def sys_pipes(encoding=_default_encoding, bufsize=None):
    """Redirect C-level stdout/stderr to sys.stdout/stderr

    This is useful of sys.sdout/stderr are already being forwarded somewhere,
    e.g. in a Jupyter kernel.

    DO NOT USE THIS if sys.stdout and sys.stderr are not already being forwarded.
    """
    # check that we aren't forwarding stdout to itself
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name)
        capture_stream = getattr(sys, "__{}__".format(name))
        try:
            fd = stream.fileno()
            capture_fd = capture_stream.fileno()
        except Exception:
            # ignore errors - if sys.stdout doesn't need a fileno,
            # it's definitely not the original sys.__stdout__
            continue
        else:
            if fd == capture_fd:
                raise ValueError(
                    "Cannot forward sys.__{0}__ to sys.{0}: they are the same! Maybe you want wurlitzer.pipes()?".format(
                        name
                    )
                )
    return pipes(sys.stdout, sys.stderr, encoding=encoding, bufsize=bufsize)


_mighty_wurlitzer = None
_mighty_lock = threading.Lock()


def sys_pipes_forever(encoding=_default_encoding, bufsize=None):
    """Redirect all C output to sys.stdout/err

    This is not a context manager; it turns on C-forwarding permanently.
    """
    global _mighty_wurlitzer
    with _mighty_lock:
        if _mighty_wurlitzer is None:
            _mighty_wurlitzer = sys_pipes(encoding, bufsize)
            _mighty_wurlitzer.__enter__()


def stop_sys_pipes():
    """Stop permanent redirection started by sys_pipes_forever"""
    global _mighty_wurlitzer
    with _mighty_lock:
        if _mighty_wurlitzer is not None:
            _mighty_wurlitzer.__exit__(None, None, None)
            _mighty_wurlitzer = None


_extension_enabled = False


def load_ipython_extension(ip):
    """Register me as an IPython extension

    Captures all C output during execution and forwards to sys.

    Does nothing on terminal IPython.

    Use: %load_ext wurlitzer
    """
    global _extension_enabled

    if not getattr(ip, 'kernel', None):
        warnings.warn("wurlitzer extension doesn't do anything in terminal IPython")
        return
    for name in ("__stdout__", "__stderr__"):
        if getattr(sys, name) is None:
            warnings.warn("sys.{} is None. Wurlitzer can't capture output without it.")
            return

    ip.events.register('pre_execute', sys_pipes_forever)
    ip.events.register('post_execute', stop_sys_pipes)
    _extension_enabled = True


def unload_ipython_extension(ip):
    """Unload me as an IPython extension

    Use: %unload_ext wurlitzer
    """
    global _extension_enabled
    if not _extension_enabled:
        return

    ip.events.unregister('pre_execute', sys_pipes_forever)
    ip.events.unregister('post_execute', stop_sys_pipes)
    # sys_pipes_forever was called in pre_execute
    # after unregister we need to call it explicitly:
    stop_sys_pipes()
    _extension_enabled = False

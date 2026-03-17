__all__ = ['BlockingIOError', 'BrokenPipeError', 'ChildProcessError',
           'ConnectionRefusedError', 'ConnectionResetError',
           'InterruptedError', 'ConnectionAbortedError', 'PermissionError',
           'FileNotFoundError', 'ProcessLookupError',
           ]

import errno
import select
import socket
import sys
try:
    import ssl
except ImportError:
    ssl = None

from .compat import PY33

if PY33:
    import builtins
    BlockingIOError = builtins.BlockingIOError
    BrokenPipeError = builtins.BrokenPipeError
    ChildProcessError = builtins.ChildProcessError
    ConnectionRefusedError = builtins.ConnectionRefusedError
    ConnectionResetError = builtins.ConnectionResetError
    InterruptedError = builtins.InterruptedError
    ConnectionAbortedError = builtins.ConnectionAbortedError
    PermissionError = builtins.PermissionError
    FileNotFoundError = builtins.FileNotFoundError
    ProcessLookupError = builtins.ProcessLookupError

else:
    # Python < 3.3
    class BlockingIOError(OSError):
        pass

    class BrokenPipeError(OSError):
        pass

    class ChildProcessError(OSError):
        pass

    class ConnectionRefusedError(OSError):
        pass

    class InterruptedError(OSError):
        pass

    class ConnectionResetError(OSError):
        pass

    class ConnectionAbortedError(OSError):
        pass

    class PermissionError(OSError):
        pass

    class FileNotFoundError(OSError):
        pass

    class ProcessLookupError(OSError):
        pass


_MAP_ERRNO = {
    errno.EACCES: PermissionError,
    errno.EAGAIN: BlockingIOError,
    errno.EALREADY: BlockingIOError,
    errno.ECHILD: ChildProcessError,
    errno.ECONNABORTED: ConnectionAbortedError,
    errno.ECONNREFUSED: ConnectionRefusedError,
    errno.ECONNRESET: ConnectionResetError,
    errno.EINPROGRESS: BlockingIOError,
    errno.EINTR: InterruptedError,
    errno.ENOENT: FileNotFoundError,
    errno.EPERM: PermissionError,
    errno.EPIPE: BrokenPipeError,
    errno.ESHUTDOWN: BrokenPipeError,
    errno.EWOULDBLOCK: BlockingIOError,
    errno.ESRCH: ProcessLookupError,
}

if hasattr(errno, 'EBADF') and errno.EBADF not in _MAP_ERRNO:
    _MAP_ERRNO[errno.EBADF] = OSError

if sys.platform == 'win32':
    from trollius import _overlapped
    _MAP_ERRNO.update({
        _overlapped.ERROR_CONNECTION_REFUSED: ConnectionRefusedError,
        _overlapped.ERROR_CONNECTION_ABORTED: ConnectionAbortedError,
        _overlapped.ERROR_NETNAME_DELETED: ConnectionResetError,
    })


def get_error_class(key, default):
    return _MAP_ERRNO.get(key, default)


if sys.version_info >= (3,):
    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value
else:
    exec("""def reraise(tp, value, tb=None):
    raise tp, value, tb
""")


def _wrap_error(exc, mapping, key):
    if key not in mapping:
        return
    new_err_cls = mapping[key]
    new_err = new_err_cls(*exc.args)

    # raise a new exception with the original traceback
    if hasattr(exc, '__traceback__'):
        traceback = exc.__traceback__
    else:
        traceback = sys.exc_info()[2]
    reraise(new_err_cls, new_err, traceback)


if not PY33:
    def wrap_error(func, *args, **kw):
        """
        Wrap socket.error, IOError, OSError, select.error to raise new specialized
        exceptions of Python 3.3 like InterruptedError (PEP 3151).
        """
        try:
            return func(*args, **kw)
        except (socket.error, IOError, OSError) as exc:
            if ssl is not None and isinstance(exc, ssl.SSLError):
                raise
            if hasattr(exc, 'winerror'):
                _wrap_error(exc, _MAP_ERRNO, exc.winerror)
                # _MAP_ERRNO does not contain all Windows errors.
                # For some errors like "file not found", exc.errno should
                # be used (ex: ENOENT).
            _wrap_error(exc, _MAP_ERRNO, exc.errno)
            raise
        except select.error as exc:
            if exc.args:
                _wrap_error(exc, _MAP_ERRNO, exc.args[0])
            raise
else:
    def wrap_error(func, *args, **kw):
        return func(*args, **kw)

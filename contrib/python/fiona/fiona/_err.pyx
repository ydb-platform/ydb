"""fiona._err

Transformation of GDAL C API errors to Python exceptions using Python's
``with`` statement and an error-handling context manager class.

The ``cpl_errs`` error-handling context manager is intended for use in
Rasterio's Cython code. When entering the body of a ``with`` statement,
the context manager clears GDAL's error stack. On exit, the context
manager pops the last error off the stack and raises an appropriate
Python exception. It's otherwise pretty difficult to do this kind of
thing.  I couldn't make it work with a CPL error handler, Cython's
C code swallows exceptions raised from C callbacks.

When used to wrap a call to open a PNG in update mode

    with cpl_errs:
        cdef void *hds = GDALOpen('file.png', 1)
    if hds == NULL:
        raise ValueError("NULL dataset")

the ValueError of last resort never gets raised because the context
manager raises a more useful and informative error:

    Traceback (most recent call last):
      File "/Users/sean/code/rasterio/scripts/rio_insp", line 65, in <module>
        with rasterio.open(args.src, args.mode) as src:
      File "/Users/sean/code/rasterio/rasterio/__init__.py", line 111, in open
        s.start()
    ValueError: The PNG driver does not support update access to existing datasets.
"""

import contextlib
from contextvars import ContextVar
from enum import IntEnum
from itertools import zip_longest
import logging

log = logging.getLogger(__name__)

_ERROR_STACK = ContextVar("error_stack")
_ERROR_STACK.set([])


# Python exceptions expressing the CPL error numbers.

class CPLE_BaseError(Exception):
    """Base CPL error class
    Exceptions deriving from this class are intended for use only in
    Rasterio's Cython code. Let's not expose API users to them.
    """

    def __init__(self, error, errno, errmsg):
        self.error = error
        self.errno = errno
        self.errmsg = errmsg

    def __str__(self):
        return str(self.errmsg)

    @property
    def args(self):
        return self.error, self.errno, self.errmsg


class CPLE_AppDefinedError(CPLE_BaseError):
    pass


class CPLE_OutOfMemoryError(CPLE_BaseError):
    pass


class CPLE_FileIOError(CPLE_BaseError):
    pass


class CPLE_OpenFailedError(CPLE_BaseError):
    pass


class CPLE_IllegalArgError(CPLE_BaseError):
    pass


class CPLE_NotSupportedError(CPLE_BaseError):
    pass


class CPLE_AssertionFailedError(CPLE_BaseError):
    pass


class CPLE_NoWriteAccessError(CPLE_BaseError):
    pass


class CPLE_UserInterruptError(CPLE_BaseError):
    pass


class ObjectNullError(CPLE_BaseError):
    pass


class CPLE_HttpResponseError(CPLE_BaseError):
    pass


class CPLE_AWSBucketNotFoundError(CPLE_BaseError):
    pass


class CPLE_AWSObjectNotFoundError(CPLE_BaseError):
    pass


class CPLE_AWSAccessDeniedError(CPLE_BaseError):
    pass


class CPLE_AWSInvalidCredentialsError(CPLE_BaseError):
    pass


class CPLE_AWSSignatureDoesNotMatchError(CPLE_BaseError):
    pass


class CPLE_AWSError(CPLE_BaseError):
    pass


class FionaNullPointerError(CPLE_BaseError):
    """
    Returned from exc_wrap_pointer when a NULL pointer is passed, but no GDAL
    error was raised.
    """
    pass


class FionaCPLError(CPLE_BaseError):
    """
    Returned from exc_wrap_int when a error code is returned, but no GDAL
    error was set.
    """
    pass


cdef dict _LEVEL_MAP = {
    0: 0,
    1: logging.DEBUG,
    2: logging.WARNING,
    3: logging.ERROR,
    4: logging.CRITICAL
}

# Map of GDAL error numbers to the Python exceptions.
exception_map = {
    1: CPLE_AppDefinedError,
    2: CPLE_OutOfMemoryError,
    3: CPLE_FileIOError,
    4: CPLE_OpenFailedError,
    5: CPLE_IllegalArgError,
    6: CPLE_NotSupportedError,
    7: CPLE_AssertionFailedError,
    8: CPLE_NoWriteAccessError,
    9: CPLE_UserInterruptError,
    10: ObjectNullError,

    # error numbers 11-16 are introduced in GDAL 2.1. See
    # https://github.com/OSGeo/gdal/pull/98.
    11: CPLE_HttpResponseError,
    12: CPLE_AWSBucketNotFoundError,
    13: CPLE_AWSObjectNotFoundError,
    14: CPLE_AWSAccessDeniedError,
    15: CPLE_AWSInvalidCredentialsError,
    16: CPLE_AWSSignatureDoesNotMatchError,
    17: CPLE_AWSError
}

cdef dict _CODE_MAP = {
    0: 'CPLE_None',
    1: 'CPLE_AppDefined',
    2: 'CPLE_OutOfMemory',
    3: 'CPLE_FileIO',
    4: 'CPLE_OpenFailed',
    5: 'CPLE_IllegalArg',
    6: 'CPLE_NotSupported',
    7: 'CPLE_AssertionFailed',
    8: 'CPLE_NoWriteAccess',
    9: 'CPLE_UserInterrupt',
    10: 'ObjectNull',
    11: 'CPLE_HttpResponse',
    12: 'CPLE_AWSBucketNotFound',
    13: 'CPLE_AWSObjectNotFound',
    14: 'CPLE_AWSAccessDenied',
    15: 'CPLE_AWSInvalidCredentials',
    16: 'CPLE_AWSSignatureDoesNotMatch',
    17: 'CPLE_AWSError'
}

# CPL Error types as an enum.
class GDALError(IntEnum):
    none = CE_None
    debug = CE_Debug
    warning = CE_Warning
    failure = CE_Failure
    fatal = CE_Fatal


cdef class GDALErrCtxManager:
    """A manager for GDAL error handling contexts."""

    def __enter__(self):
        CPLErrorReset()
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        cdef int err_type = CPLGetLastErrorType()
        cdef int err_no = CPLGetLastErrorNo()
        cdef const char *msg = CPLGetLastErrorMsg()
        # TODO: warn for err_type 2?
        if err_type >= 2:
            raise exception_map[err_no](err_type, err_no, msg)


cdef inline object exc_check():
    """Checks GDAL error stack for fatal or non-fatal errors

    Returns
    -------
    An Exception, SystemExit, or None
    """
    cdef const char *msg_c = NULL

    err_type = CPLGetLastErrorType()
    err_no = CPLGetLastErrorNo()
    err_msg = CPLGetLastErrorMsg()

    if err_msg == NULL:
        msg = "No error message."
    else:
        # Reformat messages.
        msg_b = err_msg
        msg = msg_b.decode('utf-8')
        msg = msg.replace("`", "'")
        msg = msg.replace("\n", " ")

    if err_type == 3:
        CPLErrorReset()
        return exception_map.get(
            err_no, CPLE_BaseError)(err_type, err_no, msg)

    if err_type == 4:
        return SystemExit(f"Fatal error: {(err_type, err_no, msg)}")

    else:
        return


cdef get_last_error_msg():
    """Checks GDAL error stack for the latest error message

    Returns
    -------
    An error message or empty string

    """
    err_msg = CPLGetLastErrorMsg()

    if err_msg != NULL:
        # Reformat messages.
        msg_b = err_msg
        msg = msg_b.decode('utf-8')
        msg = msg.replace("`", "'")
        msg = msg.replace("\n", " ")
    else:
        msg = ""

    return msg


cdef int exc_wrap_int(int err) except -1:
    """Wrap a GDAL/OGR function that returns CPLErr or OGRErr (int)

    Raises a Rasterio exception if a non-fatal error has be set.
    """
    if err:
        exc = exc_check()
        if exc:
            raise exc
        else:
            raise FionaCPLError(-1, -1, "The wrapped function returned an error code, but no error message was set.")
    return err


cdef OGRErr exc_wrap_ogrerr(OGRErr err) except -1:
    """Wrap a function that returns OGRErr but does not use the
    CPL error stack.

    """
    if err == 0:
        return err
    exc = exc_check()
    if exc:
        raise exc
    raise CPLE_BaseError(3, err, f"OGR Error code {err}")


cdef void *exc_wrap_pointer(void *ptr) except NULL:
    """Wrap a GDAL/OGR function that returns GDALDatasetH etc (void *)
    Raises a Rasterio exception if a non-fatal error has be set.
    """
    if ptr == NULL:
        exc = exc_check()
        if exc:
            raise exc
        else:
            # null pointer was passed, but no error message from GDAL
            raise FionaNullPointerError(-1, -1, "NULL pointer error")
    return ptr


cdef VSILFILE *exc_wrap_vsilfile(VSILFILE *f) except NULL:
    """Wrap a GDAL/OGR function that returns GDALDatasetH etc (void *)

    Raises a Rasterio exception if a non-fatal error has be set.
    """
    if f == NULL:
        exc = exc_check()
        if exc:
            raise exc
    return f

cpl_errs = GDALErrCtxManager()


cdef class StackChecker:

    def __init__(self, error_stack=None):
        self.error_stack = error_stack or {}

    cdef int exc_wrap_int(self, int err) except -1:
        """Wrap a GDAL/OGR function that returns CPLErr (int).

        Raises a Rasterio exception if a non-fatal error has be set.
        """
        if err:
            stack = self.error_stack.get()
            for error, cause in zip_longest(stack[::-1], stack[::-1][1:]):
                if error is not None and cause is not None:
                    error.__cause__ = cause

            if stack:
                last = stack.pop()
                if last is not None:
                    raise last

        return err

    cdef void *exc_wrap_pointer(self, void *ptr) except NULL:
        """Wrap a GDAL/OGR function that returns a pointer.

        Raises a Rasterio exception if a non-fatal error has be set.
        """
        if ptr == NULL:
            stack = self.error_stack.get()
            for error, cause in zip_longest(stack[::-1], stack[::-1][1:]):
                if error is not None and cause is not None:
                    error.__cause__ = cause

            if stack:
                last = stack.pop()
                if last is not None:
                    raise last

        return ptr


cdef void log_error(
    CPLErr err_class,
    int err_no,
    const char* msg,
) noexcept with gil:
    """Send CPL errors to Python's logger.

    Because this function is called by GDAL with no Python context, we
    can't propagate exceptions that we might raise here. They'll be
    ignored.

    """
    if err_no in _CODE_MAP:
        # We've observed that some GDAL functions may emit multiple
        # ERROR level messages and yet succeed. We want to see those
        # messages in our log file, but not at the ERROR level. We
        # turn the level down to INFO.
        if err_class == 3:
            log.info(
                "GDAL signalled an error: err_no=%r, msg=%r",
                err_no,
                msg.decode("utf-8")
            )
        elif err_no == 0:
            log.log(_LEVEL_MAP[err_class], "%s", msg.decode("utf-8"))
        else:
            log.log(_LEVEL_MAP[err_class], "%s:%s", _CODE_MAP[err_no], msg.decode("utf-8"))
    else:
        log.info("Unknown error number %r", err_no)


IF UNAME_SYSNAME == "Windows":
    cdef void __stdcall chaining_error_handler(
        CPLErr err_class,
        int err_no,
        const char* msg
    ) noexcept with gil:
        global _ERROR_STACK
        log_error(err_class, err_no, msg)
        if err_class == 3:
            stack = _ERROR_STACK.get()
            stack.append(
                exception_map.get(err_no, CPLE_BaseError)(err_class, err_no, msg.decode("utf-8")),
            )
            _ERROR_STACK.set(stack)
ELSE:
    cdef void chaining_error_handler(
        CPLErr err_class,
        int err_no,
        const char* msg
    ) noexcept with gil:
        global _ERROR_STACK
        log_error(err_class, err_no, msg)
        if err_class == 3:
            stack = _ERROR_STACK.get()
            stack.append(
                exception_map.get(err_no, CPLE_BaseError)(err_class, err_no, msg.decode("utf-8")),
            )
            _ERROR_STACK.set(stack)


@contextlib.contextmanager
def stack_errors():
    # TODO: better name?
    # Note: this manager produces one chain of errors and thus assumes
    # that no more than one GDAL function is called.
    CPLErrorReset()
    global _ERROR_STACK
    _ERROR_STACK.set([])

    # chaining_error_handler (better name a TODO) records GDAL errors
    # in the order they occur and converts to exceptions.
    CPLPushErrorHandlerEx(<CPLErrorHandler>chaining_error_handler, NULL)

    # Run code in the `with` block.
    yield StackChecker(_ERROR_STACK)

    CPLPopErrorHandler()
    _ERROR_STACK.set([])
    CPLErrorReset()

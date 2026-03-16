# cython: c_string_type=unicode, c_string_encoding=utf8
"""rasterio._err

Exception-raising wrappers for GDAL API functions.

"""

import contextlib
from contextvars import ContextVar
from enum import IntEnum
from itertools import zip_longest
import logging

log = logging.getLogger(__name__)

_ERROR_STACK = ContextVar("error_stack")
_ERROR_STACK.set([])

_GDAL_DEBUG_DOCS = (
    "https://rasterio.readthedocs.io/en/latest/topics/errors.html"
    "#debugging-internal-gdal-functions"
)


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
        return self.__unicode__()

    def __unicode__(self):
        return "{}".format(self.errmsg)

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


cdef inline object exc_check():
    """Checks GDAL error stack for fatal or non-fatal errors

    Returns
    -------
    An Exception, SystemExit, or None

    """
    cdef const char *msg = NULL

    err_type = CPLGetLastErrorType()
    err_no = CPLGetLastErrorNo()
    msg = CPLGetLastErrorMsg()

    if msg == NULL:
        message = "No error message."
    else:
        message = msg
        message = message.replace("`", "'")
        message = message.replace("\n", " ")

    if err_type == 3:
        exception = exception_map.get(err_no, CPLE_BaseError)(err_type, err_no, message)
        CPLErrorReset()
        return exception
    elif err_type == 4:
        exception = SystemExit("Fatal error: {0}".format((err_type, err_no, message)))
        CPLErrorReset()
        return exception
    else:
        CPLErrorReset()
        return None


cdef int exc_wrap(int retval) except -1:
    """Wrap a GDAL function that returns int without checking the retval"""
    exc = exc_check()
    if exc:
        raise exc
    return retval


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
                msg
            )
        elif err_no == 0:
            log.log(_LEVEL_MAP[err_class], "%s", msg)
        else:
            log.log(_LEVEL_MAP[err_class], "%s:%s", _CODE_MAP[err_no], msg)
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
                exception_map.get(err_no, CPLE_BaseError)(err_class, err_no, msg),
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
                exception_map.get(err_no, CPLE_BaseError)(err_class, err_no, msg),
            )
            _ERROR_STACK.set(stack)


cdef int exc_wrap_int(int err) except -1:
    """Wrap a GDAL/OGR function that returns CPLErr or OGRErr (int)

    Raises a Rasterio exception if a non-fatal error has be set.
    """
    if err:
        exc = exc_check()
        if exc:
            raise exc
    CPLErrorReset()
    return err


cdef OGRErr exc_wrap_ogrerr(OGRErr err) except -1:
    """Wrap a function that returns OGRErr but does not use the
    CPL error stack.

    """
    if err == 0:
        CPLErrorReset()
        return err
    else:
        raise CPLE_BaseError(3, err, "OGR Error code {}".format(err))


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


cdef void *exc_wrap_pointer(void *ptr) except NULL:
    """Wrap a GDAL/OGR function that returns GDALDatasetH etc (void *)

    Raises a Rasterio exception if a non-fatal error has be set.
    """
    if ptr == NULL:
        exc = exc_check()
        if exc:
            raise exc
        raise SystemError(
            f"Unknown GDAL Error. To debug: {_GDAL_DEBUG_DOCS}"
        )
    CPLErrorReset()
    return ptr


cdef VSILFILE *exc_wrap_vsilfile(VSILFILE *vsifile) except NULL:
    """Wrap a GDAL/OGR function that returns GDALDatasetH etc (void *)

    Raises a Rasterio exception if a non-fatal error has be set.

    """
    if vsifile == NULL:
        exc = exc_check()
        if exc:
            raise exc
        raise SystemError(
            f"Unknown GDAL Error. To debug: {_GDAL_DEBUG_DOCS}"
        )
    CPLErrorReset()
    return vsifile

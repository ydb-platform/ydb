"""Error handling code for GDAL/OGR.

Ported from fiona::_err.pyx
"""

import contextlib
import warnings
from contextvars import ContextVar
from itertools import zip_longest

from pyogrio._ogr cimport (
    CE_Warning, CE_Failure, CE_Fatal, CPLErrorReset,
    CPLGetLastErrorType, CPLGetLastErrorNo, CPLGetLastErrorMsg,
    OGRERR_NONE, CPLErr, CPLErrorHandler, CPLDefaultErrorHandler,
    CPLPopErrorHandler, CPLPushErrorHandler)

_ERROR_STACK = ContextVar("error_stack")
_ERROR_STACK.set([])


class CPLE_BaseError(Exception):
    """Base CPL error class.

    For internal use within Cython only.
    """

    def __init__(self, error, errno, errmsg):
        self.error = error
        self.errno = errno
        self.errmsg = errmsg

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return u"{}".format(self.errmsg)

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


class NullPointerError(CPLE_BaseError):
    """
    Returned from check_pointer when a NULL pointer is passed, but no GDAL
    error was raised.
    """
    pass


class CPLError(CPLE_BaseError):
    """
    Returned from check_int when a error code is returned, but no GDAL
    error was set.
    """
    pass


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


cdef inline object check_last_error():
    """Checks if the last GDAL error was a fatal or non-fatal error.

    When a non-fatal error is found, an appropriate exception is raised.

    When a fatal error is found, SystemExit is called.

    Returns
    -------
    An Exception, SystemExit, or None
    """
    err_type = CPLGetLastErrorType()
    err_no = CPLGetLastErrorNo()
    err_msg = clean_error_message(CPLGetLastErrorMsg())
    if err_msg == "":
        err_msg = "No error message."

    if err_type == CE_Failure:
        CPLErrorReset()
        return exception_map.get(
            err_no, CPLE_BaseError)(err_type, err_no, err_msg)

    if err_type == CE_Fatal:
        return SystemExit("Fatal error: {0}".format((err_type, err_no, err_msg)))


cdef clean_error_message(const char* err_msg):
    """Cleans up error messages from GDAL.

    Parameters
    ----------
    err_msg : const char*
        The error message to clean up.

    Returns
    -------
    str
        The cleaned up error message or empty string
    """
    if err_msg != NULL:
        # Reformat message.
        msg_b = err_msg
        try:
            msg = msg_b.decode("utf-8")
            msg = msg.replace("`", "'")
            msg = msg.replace("\n", " ")
        except UnicodeDecodeError as exc:
            msg = f"Could not decode error message to UTF-8. Raw error: {msg_b}"

    else:
        msg = ""

    return msg


cdef void *check_pointer(void *ptr) except NULL:
    """Check the pointer returned by a GDAL/OGR function.

    If `ptr` is `NULL`, an exception inheriting from CPLE_BaseError is raised.
    When the last error registered by GDAL/OGR was a non-fatal error, the
    exception raised will be customized appropriately. Otherwise a
    NullPointerError is raised.
    """
    if ptr == NULL:
        exc = check_last_error()
        if exc:
            raise exc
        else:
            # null pointer was passed, but no error message from GDAL
            raise NullPointerError(-1, -1, "NULL pointer error")

    return ptr


cdef int check_int(int err) except -1:
    """Check the CPLErr (int) value returned by a GDAL/OGR function.

    If `err` is not OGRERR_NONE, an exception inheriting from CPLE_BaseError is raised.
    When the last error registered by GDAL/OGR was a non-fatal error, the
    exception raised will be customized appropriately. Otherwise a CPLError is
    raised.
    """
    if err != OGRERR_NONE:
        exc = check_last_error()
        if exc:
            raise exc
        else:
            # no error message from GDAL
            raise CPLError(-1, -1, "Unspecified OGR / GDAL error")

    return err


cdef void error_handler(
    CPLErr err_class, int err_no, const char* err_msg
) noexcept nogil:
    """Custom CPL error handler to match the Python behaviour.

    For non-fatal errors (CE_Failure), error printing to stderr (behaviour of
    the default GDAL error handler) is suppressed, because we already raise a
    Python exception that includes the error message.

    Warnings are converted to Python warnings.
    """
    if err_class == CE_Fatal:
        # If the error class is CE_Fatal, we want to have a message issued
        # because the CPL support code does an abort() before any exception
        # can be generated
        CPLDefaultErrorHandler(err_class, err_no, err_msg)
        return

    if err_class == CE_Failure:
        # For Failures, do nothing as those are explicitly caught
        # with error return codes and translated into Python exceptions
        return

    if err_class == CE_Warning:
        with gil:
            warnings.warn(clean_error_message(err_msg), RuntimeWarning)
        return

    # Fall back to the default handler for non-failure messages since
    # they won't be translated into exceptions.
    CPLDefaultErrorHandler(err_class, err_no, err_msg)


def _register_error_handler():
    CPLPushErrorHandler(<CPLErrorHandler>error_handler)


cdef class ErrorHandler:

    def __init__(self, error_stack=None):
        self.error_stack = error_stack or {}

    cdef int check_int(self, int err, bint squash_errors) except -1:
        """Check the CPLErr (int) value returned by a GDAL/OGR function.

        If `err` is not OGRERR_NONE, an exception inheriting from CPLE_BaseError is
        raised.
        When a non-fatal GDAL/OGR error was captured in the error stack, the
        exception raised will be customized appropriately. Otherwise, a
        CPLError is raised.

        Parameters
        ----------
        err : int
            The CPLErr returned by a GDAL/OGR function.
        squash_errors : bool
            True to squash all errors captured to one error with the exception type of
            the last error and all error messages concatenated.

        Returns
        -------
        int
            The `err` input parameter if it is OGRERR_NONE. Otherwise an exception is
            raised.

        """
        if err != OGRERR_NONE:
            if self.error_stack.get():
                self._handle_error_stack(squash_errors)
            else:
                raise CPLError(CE_Failure, err, "Unspecified OGR / GDAL error")

        return err

    cdef void *check_pointer(self, void *ptr, bint squash_errors) except NULL:
        """Check the pointer returned by a GDAL/OGR function.

        If `ptr` is `NULL`, an exception inheriting from CPLE_BaseError is
        raised.
        When a non-fatal GDAL/OGR error was captured in the error stack, the
        exception raised will be customized appropriately. Otherwise, a
        NullPointerError is raised.

        Parameters
        ----------
        ptr : pointer
            The pointer returned by a GDAL/OGR function.
        squash_errors : bool
            True to squash all errors captured to one error with the exception type of
            the last error and all error messages concatenated.

        Returns
        -------
        pointer
            The `ptr` input parameter if it is not `NULL`. Otherwise an exception is
            raised.

        """
        if ptr == NULL:
            if self.error_stack.get():
                self._handle_error_stack(squash_errors)
            else:
                raise NullPointerError(-1, -1, "NULL pointer error")

        return ptr

    cdef void _handle_error_stack(self, bint squash_errors):
        """Handle the errors in `error_stack`."""
        stack = self.error_stack.get()
        for error, cause in zip_longest(stack[::-1], stack[::-1][1:]):
            if error is not None and cause is not None:
                error.__cause__ = cause

        last = stack.pop()
        if last is not None:
            if squash_errors:
                # Concatenate all error messages, and raise a single exception
                errmsg = str(last)
                inner = last.__cause__
                while inner is not None:
                    errmsg = f"{errmsg}; {inner}"
                    inner = inner.__cause__

                if errmsg == "":
                    errmsg = "No error message."

                raise type(last)(-1, -1, errmsg)

            raise last


cdef void stacking_error_handler(
    CPLErr err_class,
    int err_no,
    const char* err_msg
) noexcept nogil:
    """Custom CPL error handler that adds non-fatal errors to a stack.

    All non-fatal errors (CE_Failure) are not printed to stderr (behaviour
    of the default GDAL error handler), but they are converted to python
    exceptions and added to a stack, so they can be dealt with afterwards.

    Warnings are converted to Python warnings.
    """
    if err_class == CE_Fatal:
        # If the error class is CE_Fatal, we want to have a message issued
        # because the CPL support code does an abort() before any exception
        # can be generated
        CPLDefaultErrorHandler(err_class, err_no, err_msg)
        return

    if err_class == CE_Failure:
        # For Failures, add them to the error exception stack
        with gil:
            stack = _ERROR_STACK.get()
            stack.append(
                exception_map.get(err_no, CPLE_BaseError)(
                    err_class, err_no, clean_error_message(err_msg)
                ),
            )
            _ERROR_STACK.set(stack)

        return

    if err_class == CE_Warning:
        with gil:
            warnings.warn(clean_error_message(err_msg), RuntimeWarning)
        return

    # Fall back to the default handler for non-failure messages since
    # they won't be translated into exceptions.
    CPLDefaultErrorHandler(err_class, err_no, err_msg)


@contextlib.contextmanager
def capture_errors():
    """A context manager that captures all GDAL non-fatal errors occurring.

    It adds all errors to a single stack, so it assumes that no more than one
    GDAL function is called.

    Yields an ErrorHandler object that can be used to handle the errors
    if any were captured.
    """
    CPLErrorReset()
    _ERROR_STACK.set([])

    # stacking_error_handler records GDAL errors in the order they occur and
    # converts them to exceptions.
    CPLPushErrorHandler(<CPLErrorHandler>stacking_error_handler)

    # Run code in the `with` block.
    yield ErrorHandler(_ERROR_STACK)

    CPLPopErrorHandler()
    _ERROR_STACK.set([])
    CPLErrorReset()

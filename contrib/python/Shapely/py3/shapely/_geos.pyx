#from shapely import GEOSException
from libc.stdio cimport snprintf
from libc.stdlib cimport free, malloc

import warnings

from shapely import GEOSException


cdef void geos_message_handler(const char* message, void* userdata) noexcept:
    snprintf(<char *>userdata, 1024, "%s", message)


cdef class get_geos_handle:
    '''This class provides a context manager that wraps the GEOS context handle.

    Example
    -------
    with get_geos_handle() as geos_handle:
        SomeGEOSFunc(geos_handle, ...<other params>)
    '''
    cdef GEOSContextHandle_t __enter__(self):
        self.handle = GEOS_init_r()
        self.last_error = <char *> malloc((1025) * sizeof(char))
        self.last_error[0] = 0
        self.last_warning = <char *> malloc((1025) * sizeof(char))
        self.last_warning[0] = 0
        GEOSContext_setErrorMessageHandler_r(
            self.handle, &geos_message_handler, self.last_error
        )
        GEOSContext_setNoticeMessageHandler_r(
            self.handle, &geos_message_handler, self.last_warning
        )
        return self.handle

    def __exit__(self, type, value, traceback):
        try:
            if self.last_error[0] != 0:
                raise GEOSException(self.last_error)
            if self.last_warning[0] != 0:
                warnings.warn(self.last_warning)
        finally:
            GEOS_finish_r(self.handle)
            free(self.last_error)
            free(self.last_warning)

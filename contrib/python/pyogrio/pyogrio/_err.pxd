cdef object check_last_error()
cdef int check_int(int retval) except -1
cdef void *check_pointer(void *ptr) except NULL

cdef class ErrorHandler:
    cdef object error_stack
    cdef int check_int(self, int retval, bint squash_errors) except -1
    cdef void *check_pointer(self, void *ptr, bint squash_errors) except NULL
    cdef void _handle_error_stack(self, bint squash_errors)

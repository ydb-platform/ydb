#------------------------------------------------------------------------------

cdef extern from * nogil: # "stddef.h"
    ctypedef unsigned int wchar_t

cdef extern from * nogil: # "string.h"
    int memcmp(const void *, const void *, size_t)
    void *memset(void *, int, size_t)
    void *memcpy(void *, const void *, size_t)
    void *memmove(void *, const void *, size_t)

cdef extern from * nogil: # "stdio.h"
    ctypedef struct FILE
    FILE *stdin, *stdout, *stderr
    int fprintf(FILE *, char *, ...)
    int fflush(FILE *)

#------------------------------------------------------------------------------

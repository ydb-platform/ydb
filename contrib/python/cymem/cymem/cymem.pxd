ctypedef void* (*malloc_t)(size_t n)
ctypedef void (*free_t)(void *p)

cdef class PyMalloc:
    cdef malloc_t malloc
    cdef void _set(self, malloc_t malloc)

cdef PyMalloc WrapMalloc(malloc_t malloc)

cdef class PyFree:
    cdef free_t free
    cdef void _set(self, free_t free)

cdef PyFree WrapFree(free_t free)

cdef class Pool:
    cdef readonly size_t size
    cdef readonly dict addresses
    cdef readonly list refs
    cdef readonly PyMalloc pymalloc
    cdef readonly PyFree pyfree

    cdef void* alloc(self, size_t number, size_t size) except NULL
    cdef void free(self, void* addr) except *
    cdef void* realloc(self, void* addr, size_t n) except NULL


cdef class Address:
    cdef void* ptr
    cdef readonly PyMalloc pymalloc
    cdef readonly PyFree pyfree

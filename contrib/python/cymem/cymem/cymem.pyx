# cython: embedsignature=True, freethreading_compatible=True

cimport cython
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset
from libc.string cimport memcpy
import warnings

WARN_ZERO_ALLOC = False

cdef class PyMalloc:
    cdef void _set(self, malloc_t malloc):
        self.malloc = malloc

cdef PyMalloc WrapMalloc(malloc_t malloc):
    cdef PyMalloc o = PyMalloc()
    o._set(malloc)
    return o

cdef class PyFree:
    cdef void _set(self, free_t free):
        self.free = free

cdef PyFree WrapFree(free_t free):
    cdef PyFree o = PyFree()
    o._set(free)
    return o

Default_Malloc = WrapMalloc(PyMem_Malloc)
Default_Free = WrapFree(PyMem_Free)

cdef class Pool:
    """Track allocated memory addresses, and free them all when the Pool is
    garbage collected.  This provides an easy way to avoid memory leaks, and
    removes the need for deallocation functions for complicated structs.

    >>> from cymem.cymem cimport Pool
    >>> cdef Pool mem = Pool()
    >>> data1 = <int*>mem.alloc(10, sizeof(int))
    >>> data2 = <float*>mem.alloc(12, sizeof(float))

    Attributes:
        size (size_t): The current size (in bytes) allocated by the pool.
        addresses (dict): The currently allocated addresses and their sizes. Read-only.
        pymalloc (PyMalloc): The allocator to use (default uses PyMem_Malloc).
        pyfree (PyFree): The free to use (default uses PyMem_Free).

    Thread-safety:
        All public methods in this class can be safely called from multiple threads.
        Testing the thread-safety of this class can be done by checking out the repository
        at https://github.com/lysnikolaou/test-cymem-threadsafety and following the
        instructions on the README.
    """

    def __cinit__(self, PyMalloc pymalloc=Default_Malloc,
                  PyFree pyfree=Default_Free):
        # size, addresses and refs are mutable. note that operations on
        # dicts and lists are atomic.
        self.size = 0
        self.addresses = {}
        self.refs = []

        # pymalloc and pyfree are immutable.
        self.pymalloc = pymalloc
        self.pyfree = pyfree

    def __dealloc__(self):
        # No need for locking here since the methods will be unreachable
        # by the time __dealloc__ is called
        cdef size_t addr
        if self.addresses is not None:
            for addr in self.addresses:
                if addr != 0:
                    self.pyfree.free(<void*>addr)

    cdef void* alloc(self, size_t number, size_t elem_size) except NULL:
        """Allocate a 0-initialized number*elem_size-byte block of memory, and
        remember its address. The block will be freed when the Pool is garbage
        collected. Throw warning when allocating zero-length size and 
        WARN_ZERO_ALLOC was set to True.
        """
        if WARN_ZERO_ALLOC and (number == 0 or elem_size == 0):
            warnings.warn("Allocating zero bytes")
        cdef void* p = self.pymalloc.malloc(number * elem_size)
        if p == NULL:
            raise MemoryError("Error assigning %d bytes" % (number * elem_size))
        memset(p, 0, number * elem_size)

        # We need a critical section here so that addresses and size get
        # updated atomically. If we were to acquire a critical section on self,
        # mutating the dictionary would try to acquire a critical section on
        # the dictionary and therefore release the critical section on self.
        # Acquiring a critical section on self.addresses works because that's
        # the only C API that gets called inside the block and acquiring the
        # critical section on the top-most held lock does not release it.
        with cython.critical_section(self.addresses):
            self.addresses[<size_t>p] = number * elem_size
            self.size += number * elem_size
        return p

    cdef void* realloc(self, void* p, size_t new_size) except NULL:
        """Resizes the memory block pointed to by p to new_size bytes, returning
        a non-NULL pointer to the new block. new_size must be larger than the
        original.

        If p is not in the Pool or new_size isn't larger than the previous size,
        a ValueError is raised.
        """
        cdef size_t old_size
        cdef void* new_ptr

        new_ptr = self.pymalloc.malloc(new_size)
        if new_ptr == NULL:
            raise MemoryError("Error reallocating to %d bytes" % new_size)

        # See comment in alloc on why we're acquiring a critical section on
        # self.addresses instead of self.
        with cython.critical_section(self.addresses):
            try:
                old_size = self.addresses.pop(<size_t>p)
            except KeyError:
                self.pyfree.free(new_ptr)
                raise ValueError("Pointer %d not found in Pool %s" % (<size_t>p, self.addresses))

            if old_size >= new_size:
                self.addresses[<size_t>p] = old_size
                self.pyfree.free(new_ptr)
                raise ValueError("Realloc requires new_size > previous size")

            memcpy(new_ptr, p, old_size)
            memset(<char*> new_ptr + old_size, 0, new_size - old_size)
            self.size += new_size - old_size
            self.addresses[<size_t>new_ptr] = new_size

        self.pyfree.free(p)
        return new_ptr

    cdef void free(self, void* p) except *:
        """Frees the memory block pointed to by p, which must have been returned
        by a previous call to Pool.alloc.  You don't necessarily need to free
        memory addresses manually --- you can instead let the Pool be garbage
        collected, at which point all the memory will be freed.

        If p is not in Pool.addresses, a KeyError is raised.
        """
        cdef size_t size

        # See comment in alloc on why we're acquiring a critical section on
        # self.addresses instead of self.
        with cython.critical_section(self.addresses):
            size = self.addresses.pop(<size_t>p)
            self.size -= size
        self.pyfree.free(p)

    def own_pyref(self, object py_ref):
        # Calling append here is atomic, no critical section needed.
        self.refs.append(py_ref)


cdef class Address:
    """A block of number * size-bytes of 0-initialized memory, tied to a Python
    ref-counted object. When the object is garbage collected, the memory is freed.

    >>> from cymem.cymem cimport Address
    >>> cdef Address address = Address(10, sizeof(double))
    >>> d10 = <double*>address.ptr

    Args:
        number (size_t): The number of elements in the memory block.
        elem_size (size_t): The size of each element.

    Attributes:
        ptr (void*): Pointer to the memory block.
        addr (size_t): Read-only size_t cast of the pointer.
        pymalloc (PyMalloc): The allocator to use (default uses PyMem_Malloc).
        pyfree (PyFree): The free to use (default uses PyMem_Free).
    """
    def __cinit__(self, size_t number, size_t elem_size,
                  PyMalloc pymalloc=Default_Malloc, PyFree pyfree=Default_Free):
        self.ptr = NULL
        self.pymalloc = pymalloc
        self.pyfree = pyfree

    def __init__(self, size_t number, size_t elem_size):
        self.ptr = self.pymalloc.malloc(number * elem_size)
        if self.ptr == NULL:
            raise MemoryError("Error assigning %d bytes" % number * elem_size)
        memset(self.ptr, 0, number * elem_size)

    @property
    def addr(self):
        return <size_t>self.ptr

    def __dealloc__(self):
        if self.ptr != NULL:
            self.pyfree.free(self.ptr)

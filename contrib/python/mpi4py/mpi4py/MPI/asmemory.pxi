#------------------------------------------------------------------------------

cdef extern from "Python.h":
    enum: PY_SSIZE_T_MAX
    void *PyMem_Malloc(size_t)
    void *PyMem_Calloc(size_t, size_t)
    void *PyMem_Realloc(void*, size_t)
    void  PyMem_Free(void*)

cdef extern from * nogil:
    """
    #if PY_VERSION_HEX < 0x03050000
    #   define PyMPI_RawMalloc(n)    malloc(((n)!=0)?(n):1)
    #   define PyMPI_RawCalloc(n,s)  ((n)&&(s))?calloc((n),(s)):calloc(1,1)
    #   define PyMPI_RawRealloc(p,n) realloc((p),(n)?(n):1))
    #   define PyMPI_RawFree         free
    #else
    #   define PyMPI_RawMalloc  PyMem_RawMalloc
    #   define PyMPI_RawCalloc  PyMem_RawCalloc
    #   define PyMPI_RawRealloc PyMem_RawRealloc
    #   define PyMPI_RawFree    PyMem_RawFree
    #endif
    """
    void *PyMPI_RawMalloc(size_t)
    void *PyMPI_RawCalloc(size_t, size_t)
    void *PyMPI_RawRealloc(void*, size_t)
    void  PyMPI_RawFree(void*)

#------------------------------------------------------------------------------

@cython.final
@cython.internal
cdef class _p_mem:
    cdef void *buf
    cdef size_t len
    cdef void (*free)(void*)
    def __cinit__(self):
        self.buf = NULL
        self.len = 0
        self.free = NULL
    def __dealloc__(self):
        if self.free:
            self.free(self.buf)


cdef inline _p_mem allocate(Py_ssize_t m, size_t b, void *buf):
  if m > PY_SSIZE_T_MAX/<Py_ssize_t>b:
      raise MemoryError("memory allocation size too large")
  if m < 0:
      raise RuntimeError("memory allocation with negative size")
  cdef _p_mem ob = _p_mem.__new__(_p_mem)
  ob.len  = <size_t>m * b
  ob.free = PyMem_Free
  ob.buf  = PyMem_Malloc(<size_t>m * b)
  if ob.buf == NULL: raise MemoryError
  if buf != NULL: (<void**>buf)[0] = ob.buf
  return ob


cdef inline _p_mem rawalloc(Py_ssize_t m, size_t b, bint clear, void *buf):
  if m > PY_SSIZE_T_MAX/<Py_ssize_t>b:
      raise MemoryError("memory allocation size too large")
  if m < 0:
      raise RuntimeError("memory allocation with negative size")
  cdef _p_mem ob = _p_mem.__new__(_p_mem)
  ob.len = <size_t>m * b
  ob.free = PyMPI_RawFree
  if clear:
      ob.buf = PyMPI_RawCalloc(<size_t>m, b)
  else:
      ob.buf = PyMPI_RawMalloc(<size_t>m * b)
  if ob.buf == NULL: raise MemoryError
  if buf != NULL: (<void**>buf)[0] = ob.buf
  return ob

#------------------------------------------------------------------------------

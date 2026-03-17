#------------------------------------------------------------------------------

cdef extern from "Python.h":
    int PyIndex_Check(object)
    int PySlice_Check(object)
    int PySlice_GetIndicesEx(object, Py_ssize_t,
                             Py_ssize_t *, Py_ssize_t *,
                             Py_ssize_t *, Py_ssize_t *) except -1
    Py_ssize_t PyNumber_AsSsize_t(object, object) except? -1

#------------------------------------------------------------------------------

# Python 3 buffer interface (PEP 3118)
cdef extern from "Python.h":
    ctypedef struct Py_buffer:
        PyObject *obj
        void *buf
        Py_ssize_t len
        Py_ssize_t itemsize
        bint readonly
        char *format
        #int ndim
        #Py_ssize_t *shape
        #Py_ssize_t *strides
        #Py_ssize_t *suboffsets
    cdef enum:
        PyBUF_SIMPLE
        PyBUF_WRITABLE
        PyBUF_FORMAT
        PyBUF_ND
        PyBUF_STRIDES
        PyBUF_ANY_CONTIGUOUS
    int  PyObject_CheckBuffer(object)
    int  PyObject_GetBuffer(object, Py_buffer *, int) except -1
    void PyBuffer_Release(Py_buffer *)
    int  PyBuffer_FillInfo(Py_buffer *, object,
                           void *, Py_ssize_t,
                           bint, int) except -1

# Python 2 buffer interface (legacy)
cdef extern from *:
    int _Py2_IsBuffer(object)
    int _Py2_AsBuffer(object, bint *, void **, Py_ssize_t *) except -1

cdef extern from "Python.h":
    object PyLong_FromVoidPtr(void*)
    void*  PyLong_AsVoidPtr(object) except? NULL

cdef extern from *:
    void *emptybuffer '((void*)"")'

cdef char BYTE_FMT[2]
BYTE_FMT[0] = c'B'
BYTE_FMT[1] = 0

#------------------------------------------------------------------------------

cdef extern from *:
    char*      PyByteArray_AsString(object) except NULL
    Py_ssize_t PyByteArray_Size(object) except -1

cdef type array_array
cdef type numpy_array
cdef int  pypy_have_numpy = 0
if PYPY:
    from array import array as array_array
    try:
        from _numpypy.multiarray import ndarray as numpy_array
        pypy_have_numpy = 1
    except ImportError:
        try:
            from numpypy import ndarray as numpy_array
            pypy_have_numpy = 1
        except ImportError:
            try:
                from numpy import ndarray as numpy_array
                pypy_have_numpy = 1
            except ImportError:
                pass

cdef int PyPy_GetBuffer(object obj, Py_buffer *view, int flags) except -1:
    cdef object addr
    cdef void *buf = NULL
    cdef Py_ssize_t size = 0
    cdef bint readonly = 0
    try:
        if not isinstance(obj, bytes):
            if PyObject_CheckBuffer(obj):
                return PyObject_GetBuffer(obj, view, flags)
    except SystemError:
        pass
    except TypeError:
        pass
    if isinstance(obj, bytes):
        buf  = PyBytes_AsString(obj)
        size = PyBytes_Size(obj)
        readonly = 1
    elif isinstance(obj, bytearray):
        buf  = PyByteArray_AsString(obj)
        size = PyByteArray_Size(obj)
        readonly = 0
    elif isinstance(obj, array_array):
        addr, size = obj.buffer_info()
        buf = PyLong_AsVoidPtr(addr)
        size *= obj.itemsize
        readonly = 0
    elif pypy_have_numpy and isinstance(obj, numpy_array):
        addr, readonly = obj.__array_interface__['data']
        buf = PyLong_AsVoidPtr(addr)
        size = obj.nbytes
    else:
        _Py2_AsBuffer(obj, &readonly, &buf, &size)
    if buf == NULL and size == 0: buf = emptybuffer
    PyBuffer_FillInfo(view, obj, buf, size, readonly, flags)
    if (flags & PyBUF_FORMAT) == PyBUF_FORMAT: view.format = BYTE_FMT
    return 0

#------------------------------------------------------------------------------

cdef int Py27_GetBuffer(object obj, Py_buffer *view, int flags) except -1:
    # Python 3 buffer interface (PEP 3118)
    if PyObject_CheckBuffer(obj):
        return PyObject_GetBuffer(obj, view, flags)
    # Python 2 buffer interface (legacy)
    _Py2_AsBuffer(obj, &view.readonly, &view.buf, &view.len)
    if view.buf == NULL and view.len == 0: view.buf = emptybuffer
    PyBuffer_FillInfo(view, obj, view.buf, view.len, view.readonly, flags)
    if (flags & PyBUF_FORMAT) == PyBUF_FORMAT: view.format = BYTE_FMT
    return 0

#------------------------------------------------------------------------------

include "asdlpack.pxi"
include "ascaibuf.pxi"

cdef int PyMPI_GetBuffer(object obj, Py_buffer *view, int flags) except -1:
    try:
        if PYPY: return PyPy_GetBuffer(obj, view, flags)
        if PY2:  return Py27_GetBuffer(obj, view, flags)
        return PyObject_GetBuffer(obj, view, flags)
    except BaseException:
        try: return Py_GetDLPackBuffer(obj, view, flags)
        except NotImplementedError: pass
        except BaseException: raise
        try: return Py_GetCAIBuffer(obj, view, flags)
        except NotImplementedError: pass
        except BaseException: raise
        raise

#------------------------------------------------------------------------------

@cython.final
cdef class memory:

    """
    Memory buffer
    """

    cdef Py_buffer view

    def __cinit__(self, *args):
        if args:
            PyMPI_GetBuffer(args[0], &self.view, PyBUF_SIMPLE)
        else:
            PyBuffer_FillInfo(&self.view, <object>NULL,
                              NULL, 0, 0, PyBUF_SIMPLE)

    def __dealloc__(self):
        PyBuffer_Release(&self.view)

    @staticmethod
    def allocate(
        Aint nbytes: int,
        bint clear: bool = False,
    ) -> memory:
        """Memory allocation"""
        cdef void *buf = NULL
        cdef Py_ssize_t size = nbytes
        if size < 0:
            raise ValueError("expecting non-negative size")
        cdef object ob = rawalloc(size, 1, clear, &buf)
        cdef memory mem = memory.__new__(memory)
        PyBuffer_FillInfo(&mem.view, ob, buf, size, 0, PyBUF_SIMPLE)
        return mem

    @staticmethod
    def frombuffer(
        obj: Buffer,
        bint readonly: bool = False,
    ) -> memory:
        """Memory from buffer-like object"""
        cdef int flags = PyBUF_SIMPLE
        if not readonly: flags |= PyBUF_WRITABLE
        cdef memory mem = memory.__new__(memory)
        PyMPI_GetBuffer(obj, &mem.view, flags)
        mem.view.readonly = readonly
        return mem

    @staticmethod
    def fromaddress(
        address: int,
        Aint nbytes: int,
        bint readonly: bool = False,
    ) -> memory:
        """Memory from address and size in bytes"""
        cdef void *buf = PyLong_AsVoidPtr(address)
        cdef Py_ssize_t size = nbytes
        if size < 0:
            raise ValueError("expecting non-negative buffer length")
        elif size > 0 and buf == NULL:
            raise ValueError("expecting non-NULL address")
        cdef memory mem = memory.__new__(memory)
        PyBuffer_FillInfo(&mem.view, <object>NULL,
                          buf, size, readonly, PyBUF_SIMPLE)
        return mem

    # properties

    property address:
        """Memory address"""
        def __get__(self) -> int:
            return PyLong_FromVoidPtr(self.view.buf)

    property obj:
        """The underlying object of the memory"""
        def __get__(self) -> Optional[Buffer]:
            if self.view.obj == NULL: return None
            return <object>self.view.obj

    property nbytes:
        """Memory size (in bytes)"""
        def __get__(self) -> int:
            return self.view.len

    property readonly:
        """Boolean indicating whether the memory is read-only"""
        def __get__(self) -> bool:
            return self.view.readonly

    property format:
        """A string with the format of each element"""
        def __get__(self) -> str:
            if self.view.format != NULL:
                return pystr(self.view.format)
            return pystr(BYTE_FMT)

    property itemsize:
        """The size in bytes of each element"""
        def __get__(self) -> int:
            return self.view.itemsize

    # convenience methods

    def tobytes(self, order: Optional[str] = None) -> bytes:
        """Return the data in the buffer as a byte string"""
        return PyBytes_FromStringAndSize(<char*>self.view.buf, self.view.len)

    def toreadonly(self) -> memory:
        """Return a readonly version of the memory object"""
        cdef void *buf = self.view.buf
        cdef Py_ssize_t size = self.view.len
        cdef object obj = self
        if self.view.obj != NULL:
            obj = <object>self.view.obj
        cdef memory mem = memory.__new__(memory)
        PyBuffer_FillInfo(&mem.view, obj,
                          buf, size, 1, PyBUF_SIMPLE)
        return mem

    def release(self) -> None:
        """Release the underlying buffer exposed by the memory object"""
        PyBuffer_Release(&self.view)
        PyBuffer_FillInfo(&self.view, <object>NULL,
                          NULL, 0, 0, PyBUF_SIMPLE)

    # buffer interface (PEP 3118)

    def __getbuffer__(self, Py_buffer *view, int flags):
        if view.obj == Py_None: Py_CLEAR(view.obj)
        PyBuffer_FillInfo(view, self,
                          self.view.buf, self.view.len,
                          self.view.readonly, flags)

    # buffer interface (legacy)

    def __getsegcount__(self, Py_ssize_t *lenp):
        if lenp != NULL:
            lenp[0] = self.view.len
        return 1

    def __getreadbuffer__(self, Py_ssize_t idx, void **p):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        p[0] = self.view.buf
        return self.view.len

    def __getwritebuffer__(self, Py_ssize_t idx, void **p):
        if self.view.readonly:
            raise TypeError("memory buffer is read-only")
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        p[0] = self.view.buf
        return self.view.len

    # sequence interface (basic)

    def __len__(self):
        return self.view.len

    def __getitem__(self, object item):
        cdef Py_ssize_t start=0, stop=0, step=1, slen=0
        cdef unsigned char *buf = <unsigned char*>self.view.buf
        cdef Py_ssize_t blen = self.view.len
        if PyIndex_Check(item):
            start = PyNumber_AsSsize_t(item, IndexError)
            if start < 0: start += blen
            if start < 0 or start >= blen:
                raise IndexError("index out of range")
            return <long>buf[start]
        elif PySlice_Check(item):
            PySlice_GetIndicesEx(item, blen, &start, &stop, &step, &slen)
            if step != 1: raise IndexError("slice with step not supported")
            return asbuffer(self, buf+start, slen, self.view.readonly)
        else:
            raise TypeError("index must be integer or slice")

    def __setitem__(self, object item, object value):
        if self.view.readonly:
            raise TypeError("memory buffer is read-only")
        cdef Py_ssize_t start=0, stop=0, step=1, slen=0
        cdef unsigned char *buf = <unsigned char*>self.view.buf
        cdef Py_ssize_t blen = self.view.len
        cdef memory inmem
        if PyIndex_Check(item):
            start = PyNumber_AsSsize_t(item, IndexError)
            if start < 0: start += blen
            if start < 0 or start >= blen:
                raise IndexError("index out of range")
            buf[start] = <unsigned char>value
        elif PySlice_Check(item):
            PySlice_GetIndicesEx(item, blen, &start, &stop, &step, &slen)
            if step != 1: raise IndexError("slice with step not supported")
            if PyIndex_Check(value):
                <void>memset(buf+start, <unsigned char>value, <size_t>slen)
            else:
                inmem = getbuffer(value, 1, 0)
                if inmem.view.len != slen:
                    raise ValueError("slice length does not match buffer")
                <void>memmove(buf+start, inmem.view.buf, <size_t>slen)
        else:
            raise TypeError("index must be integer or slice")

#------------------------------------------------------------------------------

cdef inline memory newbuffer():
    return memory.__new__(memory)

cdef inline memory getbuffer(object ob, bint readonly, bint format):
    cdef memory buf = newbuffer()
    cdef int flags = PyBUF_ANY_CONTIGUOUS
    if not readonly:
        flags |= PyBUF_WRITABLE
    if format:
        flags |= PyBUF_FORMAT
    PyMPI_GetBuffer(ob, &buf.view, flags)
    return buf

cdef inline object getformat(memory buf):
    cdef Py_buffer *view = &buf.view
    #
    if view.obj == NULL:
        if view.format != NULL:
            return pystr(view.format)
        else:
            return "B"
    elif view.format != NULL:
        # XXX this is a hack
        if view.format != BYTE_FMT:
            return pystr(view.format)
    #
    cdef object ob = <object>view.obj
    cdef object format = None
    try: # numpy.ndarray
        format = ob.dtype.char
    except (AttributeError, TypeError):
        try: # array.array
            format = ob.typecode
        except (AttributeError, TypeError):
            if view.format != NULL:
                format = pystr(view.format)
    return format

cdef inline memory getbuffer_r(object ob, void **base, MPI_Aint *size):
    cdef memory buf = getbuffer(ob, 1, 0)
    if base != NULL: base[0] = buf.view.buf
    if size != NULL: size[0] = buf.view.len
    return buf

cdef inline memory getbuffer_w(object ob, void **base, MPI_Aint *size):
    cdef memory buf = getbuffer(ob, 0, 0)
    if base != NULL: base[0] = buf.view.buf
    if size != NULL: size[0] = buf.view.len
    return buf

cdef inline memory asbuffer(object ob, void *base, MPI_Aint size, bint ro):
    cdef memory buf = newbuffer()
    PyBuffer_FillInfo(&buf.view, ob, base, size, ro, PyBUF_SIMPLE)
    return buf

#------------------------------------------------------------------------------

cdef inline memory asmemory(object ob, void **base, MPI_Aint *size):
    cdef memory mem
    if type(ob) is memory:
        mem = <memory> ob
    else:
        mem = getbuffer(ob, 1, 0)
    if base != NULL: base[0] = mem.view.buf
    if size != NULL: size[0] = mem.view.len
    return mem

cdef inline memory tomemory(void *base, MPI_Aint size):
    cdef memory mem = memory.__new__(memory)
    PyBuffer_FillInfo(&mem.view, <object>NULL, base, size, 0, PyBUF_SIMPLE)
    return mem

#------------------------------------------------------------------------------

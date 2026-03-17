from libc cimport stdio
from libc.string cimport const_char
cimport cython


cdef extern from "Python.h":
    """
    #if PY_VERSION_HEX >= 0x030C0000
      #undef PyUnicode_IS_READY
      #define PyUnicode_IS_READY(s)  (1)
      #undef PyUnicode_READY
      #define PyUnicode_READY(s)  (0)
      #undef PyUnicode_AS_DATA
      #define PyUnicode_AS_DATA(s)  (0)
      #undef PyUnicode_GET_DATA_SIZE
      #define PyUnicode_GET_DATA_SIZE(s)  (0)
      #undef PyUnicode_GET_SIZE
      #define PyUnicode_GET_SIZE(s)  (0)
    #elif PY_VERSION_HEX <= 0x03030000
      #define PyUnicode_IS_READY(op)    (0)
      #define PyUnicode_GET_LENGTH(u)   PyUnicode_GET_SIZE(u)
      #define PyUnicode_KIND(u)         (sizeof(Py_UNICODE))
      #define PyUnicode_DATA(u)         ((void*)PyUnicode_AS_UNICODE(u))
    #endif
    """

    ctypedef struct PyObject
    cdef int PY_SSIZE_T_MAX
    cdef int PY_VERSION_HEX

    cdef void Py_INCREF(object o)
    cdef void Py_DECREF(object o)
    cdef void Py_XDECREF(PyObject* o)

    cdef stdio.FILE* PyFile_AsFile(object p)

    # PEP 393
    cdef bint PyUnicode_IS_READY(object u)
    cdef Py_ssize_t PyUnicode_GET_LENGTH(object u)
    cdef int PyUnicode_KIND(object u)
    cdef void* PyUnicode_DATA(object u)

    cdef bytes PyUnicode_AsEncodedString(object u, char* encoding,
                                         char* errors)
    cdef cython.unicode PyUnicode_FromFormat(char* format, ...) # Python 3
    cdef cython.unicode PyUnicode_Decode(char* s, Py_ssize_t size,
                                         char* encoding, char* errors)
    cdef cython.unicode PyUnicode_DecodeUTF8(char* s, Py_ssize_t size, char* errors)
    cdef cython.unicode PyUnicode_DecodeLatin1(char* s, Py_ssize_t size, char* errors)
    cdef object PyUnicode_RichCompare(object o1, object o2, int op)
    cdef bytes PyUnicode_AsUTF8String(object ustring)
    cdef bytes PyUnicode_AsASCIIString(object ustring)
    cdef char* PyUnicode_AS_DATA(object ustring)
    cdef Py_ssize_t PyUnicode_GET_DATA_SIZE(object ustring)
    cdef Py_ssize_t PyUnicode_GET_SIZE(object ustring)
    cdef bytes PyBytes_FromStringAndSize(char* s, Py_ssize_t size)
    cdef bytes PyBytes_FromFormat(char* format, ...)
    cdef Py_ssize_t PyBytes_GET_SIZE(object s)

    cdef object PyNumber_Int(object value)

    cdef Py_ssize_t PyTuple_GET_SIZE(object t)
    cdef object PyTuple_GET_ITEM(object o, Py_ssize_t pos)

    cdef object PyList_New(Py_ssize_t index)
    cdef Py_ssize_t PyList_GET_SIZE(object l)
    cdef object PyList_GET_ITEM(object l, Py_ssize_t index)
    cdef void PyList_SET_ITEM(object l, Py_ssize_t index, object value)
    cdef int PyList_Insert(object l, Py_ssize_t index, object o) except -1
    cdef object PyList_AsTuple(object l)

    cdef PyObject* PyDict_GetItemString(object d, char* key)
    cdef PyObject* PyDict_GetItem(object d, object key)
    cdef object PyDictProxy_New(object d)
    cdef object PySequence_List(object o)
    cdef object PySequence_Tuple(object o)

    cdef bint PyNumber_Check(object instance)
    cdef bint PySequence_Check(object instance)
    cdef bint PyType_Check(object instance)
    cdef bint PyTuple_CheckExact(object instance)

    cdef int _PyEval_SliceIndex(object value, Py_ssize_t* index) except 0
    cdef int PySlice_GetIndicesEx "_lx_PySlice_GetIndicesEx" (
            object slice, Py_ssize_t length,
            Py_ssize_t *start, Py_ssize_t *stop, Py_ssize_t *step,
            Py_ssize_t *slicelength) except -1

    cdef object PyObject_RichCompare(object o1, object o2, int op)

    PyObject* PyWeakref_NewRef(object ob, PyObject* callback) except NULL  # used for PyPy only
    object PyWeakref_LockObject(PyObject* ob) # PyPy only

    cdef void* PyMem_Malloc(size_t size)
    cdef void* PyMem_Realloc(void* p, size_t size)
    cdef void PyMem_Free(void* p)

    # always returns NULL to pass on the exception
    cdef object PyErr_SetFromErrno(object type)

    cdef PyObject* PyThreadState_GetDict()

    # some handy functions
    cdef char* _cstr "PyBytes_AS_STRING" (object s)
    cdef char* __cstr "PyBytes_AS_STRING" (PyObject* s)

    # Py_buffer related flags
    cdef int PyBUF_SIMPLE
    cdef int PyBUF_WRITABLE
    cdef int PyBUF_LOCK
    cdef int PyBUF_FORMAT
    cdef int PyBUF_ND
    cdef int PyBUF_STRIDES
    cdef int PyBUF_C_CONTIGUOUS
    cdef int PyBUF_F_CONTIGUOUS
    cdef int PyBUF_ANY_CONTIGUOUS
    cdef int PyBUF_INDIRECT

cdef extern from "pythread.h":
    ctypedef void* PyThread_type_lock
    cdef PyThread_type_lock PyThread_allocate_lock()
    cdef void PyThread_free_lock(PyThread_type_lock lock)
    cdef int  PyThread_acquire_lock(PyThread_type_lock lock, int mode) nogil
    cdef void PyThread_release_lock(PyThread_type_lock lock)
    cdef long PyThread_get_thread_ident()

    ctypedef enum __WaitLock:
        WAIT_LOCK
        NOWAIT_LOCK

cdef extern from "includes/etree_defs.h": # redefines some functions as macros
    cdef void* lxml_malloc(size_t count, size_t item_size)
    cdef void* lxml_realloc(void* mem, size_t count, size_t item_size)
    cdef void lxml_free(void* mem)
    cdef void* lxml_unpack_xmldoc_capsule(object capsule, bint* is_owned) except? NULL
    cdef bint _isString(object obj)
    cdef const_char* _fqtypename(object t)
    cdef object PY_NEW(object t)
    cdef bint LXML_UNICODE_STRINGS
    cdef bint IS_PYTHON2
    cdef bint IS_PYTHON3  # legacy, avoid
    cdef bint IS_PYPY
    cdef object PY_FSPath "lxml_PyOS_FSPath" (object obj)

cdef extern from "lxml_endian.h":
    cdef bint PY_BIG_ENDIAN  # defined in later Py3.x versions

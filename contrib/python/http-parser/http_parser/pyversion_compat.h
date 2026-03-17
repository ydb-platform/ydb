#include "Python.h"

#if PY_VERSION_HEX < 0x02070000
    #if PY_VERSION_HEX < 0x02060000
        #define PyObject_CheckBuffer(object) (0)

        #define PyObject_GetBuffer(obj, view, flags) (PyErr_SetString(PyExc_NotImplementedError, \
                        "new buffer interface is not available"), -1)
        #define PyBuffer_FillInfo(view, obj, buf, len, readonly, flags) (PyErr_SetString(PyExc_NotImplementedError, \
                    "new buffer interface is not available"), -1)
        #define PyBuffer_Release(obj) (PyErr_SetString(PyExc_NotImplementedError, \
                        "new buffer interface is not available"), -1)
        // Bytes->String
        #define PyBytes_FromStringAndSize PyString_FromStringAndSize
        #define PyBytes_FromString PyString_FromString
        #define PyBytes_AsString PyString_AsString
        #define PyBytes_Size PyString_Size
    #endif

    #define PyMemoryView_FromBuffer(info) (PyErr_SetString(PyExc_NotImplementedError, \
                    "new buffer interface is not available"), (PyObject *)NULL)
    #define PyMemoryView_FromObject(object)     (PyErr_SetString(PyExc_NotImplementedError, \
                                        "new buffer interface is not available"), (PyObject *)NULL)
#endif

#if PY_VERSION_HEX >= 0x03000000
    // for buffers
    #define Py_END_OF_BUFFER ((Py_ssize_t) 0)

    #define PyObject_CheckReadBuffer(object) (0)

    #define PyBuffer_FromMemory(ptr, s) (PyErr_SetString(PyExc_NotImplementedError, \
                            "old buffer interface is not available"), (PyObject *)NULL)
    #define PyBuffer_FromReadWriteMemory(ptr, s) (PyErr_SetString(PyExc_NotImplementedError, \
                            "old buffer interface is not available"), (PyObject *)NULL)
    #define PyBuffer_FromObject(object, offset, size)  (PyErr_SetString(PyExc_NotImplementedError, \
                            "old buffer interface is not available"), (PyObject *)NULL)
    #define PyBuffer_FromReadWriteObject(object, offset, size)  (PyErr_SetString(PyExc_NotImplementedError, \
                            "old buffer interface is not available"), (PyObject *)NULL)

#endif


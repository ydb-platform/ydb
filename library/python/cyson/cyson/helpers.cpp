#include "helpers.h"

#include <util/system/compiler.h>

namespace NCYson {
    void SetPrettyTypeError(PyObject* obj, const char* expected) {
#if PY_MAJOR_VERSION >= 3
        PyObject* bytes_repr = nullptr;
        PyObject* tmp = PyObject_Repr(obj);
        if (Y_LIKELY(tmp)) {
            bytes_repr = PyUnicode_AsUTF8String(tmp);
            Py_DECREF(tmp);
        }
#else
        PyObject* bytes_repr = PyObject_Repr(obj);
#endif
        assert(PyBytes_Check(bytes_repr));

        PyErr_Format(
            PyExc_TypeError,
            "expected %s, got %s (%s)",
            expected,
            Py_TYPE(obj)->tp_name,
            bytes_repr ? PyBytes_AS_STRING(bytes_repr) : "<repr failed>");

        Py_XDECREF(bytes_repr);
    }

    PyObject* ConvertPyStringToPyBytes(PyObject* obj) {
        if (PyBytes_Check(obj)) {
            Py_INCREF(obj);
            return obj;
        }

        if (PyUnicode_Check(obj)) {
            return PyUnicode_AsUTF8String(obj);
        }

        SetPrettyTypeError(obj, "bytes or unicode");

        return nullptr;
    }

#define FILL_DATA_FROM_BUFFER  \
    *data = (char*)view.buf;   \
    *size = (size_t)view.len;  \
    PyBuffer_Release(&view)

    PyObject* GetCharBufferAndOwner(PyObject* obj, const char** data, size_t* size) {
#if PY_MAJOR_VERSION >= 3
        Py_buffer view;
#endif

        if (PyUnicode_Check(obj)) {
            PyObject* encoded = PyUnicode_AsUTF8String(obj);
            if (!encoded) {
                return nullptr;
            }

#if PY_MAJOR_VERSION >= 3
            if (PyObject_GetBuffer(encoded, &view, PyBUF_SIMPLE) < 0) {
#else
            if (PyObject_AsCharBuffer(encoded, data, (Py_ssize_t*)size) < 0) {
#endif
                Py_DECREF(encoded);
                return nullptr;
            }

#if PY_MAJOR_VERSION >= 3
            FILL_DATA_FROM_BUFFER;
#endif

            return encoded;
        }

#if PY_MAJOR_VERSION >= 3
        if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) < 0) {
#else
        if (PyObject_AsCharBuffer(obj, data, (Py_ssize_t*)size) < 0) {
#endif
            return nullptr;
        }

#if PY_MAJOR_VERSION >= 3
        FILL_DATA_FROM_BUFFER;
#endif

        Py_INCREF(obj);

        return obj;
    }

#undef FILL_DATA_FROM_BUFFER

    PyObject* ConvertPyStringToPyNativeString(PyObject* obj) {
        if (PyBytes_Check(obj)) {
#if PY_MAJOR_VERSION >=3
            return PyUnicode_DecodeUTF8(PyBytes_AS_STRING(obj), PyBytes_GET_SIZE(obj), nullptr);
#else
            Py_INCREF(obj);
            return obj;
#endif
        }

        if (PyUnicode_Check(obj)) {
#if PY_MAJOR_VERSION >=3
            Py_INCREF(obj);
            return obj;
#else
            return PyUnicode_AsUTF8String(obj);
#endif
        }

        SetPrettyTypeError(obj, "bytes or unicode");

        return nullptr;
    }

    PyObject* ConvertPyLongToPyBytes(PyObject* obj) {
        PyObject* result;

        if (!PyLong_Check(obj)) {
            SetPrettyTypeError(obj, "long");
            return nullptr;
        }

#if PY_MAJOR_VERSION >= 3
        PyObject* tmp = _PyLong_Format(obj, 10);
        if (!tmp) {
            return nullptr;
        }

        result = PyUnicode_AsUTF8String(tmp);

        Py_DECREF(tmp);
#else
        result = _PyLong_Format(obj, 10, 0, 0);
#endif
        return result;
    }

    namespace NPrivate {
        TPyObjectPtr::TPyObjectPtr() {
            Ptr_ = nullptr;
        }

        void TPyObjectPtr::Reset(PyObject* ptr) {
            PyObject* tmp = Ptr_;
            Py_XINCREF(ptr);
            Ptr_ = ptr;
            Py_XDECREF(tmp);
        }

        PyObject* TPyObjectPtr::GetNew() {
            Py_XINCREF(Ptr_);
            return Ptr_;
        }

        PyObject* TPyObjectPtr::GetBorrowed() {
            return Ptr_;
        }

        TPyObjectPtr::~TPyObjectPtr() {
#if PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 7
            if (_Py_IsFinalizing()) {
                return;
            }
#elif PY_MAJOR_VERSION >= 3
            // https://github.com/python/cpython/blob/3.6/Python/sysmodule.c#L1345
            if (_Py_Finalizing != NULL) {
                return;
            }
#endif
            PyObject* tmp = Ptr_;
            Ptr_ = nullptr;
            Py_XDECREF(tmp);
        }
    }
}

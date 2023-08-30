#pragma once

#include <Python.h>

namespace NCYson {
    constexpr bool PY3 = PY_MAJOR_VERSION == 3;
    constexpr bool PY2 = PY_MAJOR_VERSION == 2;

    void SetPrettyTypeError(PyObject*, const char*);
    PyObject* ConvertPyStringToPyBytes(PyObject*);
    PyObject* GetCharBufferAndOwner(PyObject*, const char**, size_t*);
    PyObject* ConvertPyStringToPyNativeString(PyObject*);
    PyObject* ConvertPyLongToPyBytes(PyObject*);

    inline PyObject* GetSelf(PyObject* self) {
        Py_INCREF(self);
        return self;
    }

    namespace NPrivate {
        class TPyObjectPtr {
        public:
            void Reset(PyObject*);
            PyObject* GetNew();
            PyObject* GetBorrowed();

            TPyObjectPtr();
            ~TPyObjectPtr();

        private:
            PyObject* Ptr_;
        };
    }
}

#if PY_MAJOR_VERSION >= 3
#define GenericCheckBuffer PyObject_CheckBuffer
#define PyFile_CheckExact(x) 0
#define PyFile_AsFile(x) (FILE*)(PyErr_Format(PyExc_NotImplementedError, "PyFile_AsFile not implemented for Python3"))
#else
#define GenericCheckBuffer PyObject_CheckReadBuffer
#endif

#if PY_VERSION_HEX < 0x030900A4 && !defined(Py_SET_SIZE)
static inline void _Py_SET_SIZE(PyVarObject *ob, Py_ssize_t size)
{ ob->ob_size = size; }
#define Py_SET_SIZE(ob, size) _Py_SET_SIZE((PyVarObject*)(ob), size)
#endif

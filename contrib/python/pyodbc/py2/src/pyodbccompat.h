#ifndef PYODBCCOMPAT_H
#define PYODBCCOMPAT_H

// Macros and functions to ease compatibility with Python 2 and Python 3.

#if PY_VERSION_HEX >= 0x03000000 && PY_VERSION_HEX < 0x03010000
#error Python 3.0 is not supported.  Please use 3.1 and higher.
#endif

// Macros introduced in 2.6, backported for 2.4 and 2.5.
#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(type, size) PyObject_HEAD_INIT(type) size,
#endif
#ifndef Py_TYPE
#define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

// Macros were introduced in 2.6 to map "bytes" to "str" in Python 2.  Back port to 2.5.
#if PY_VERSION_HEX >= 0x02060000
    #include <bytesobject.h>
#else
    #define PyBytes_AS_STRING PyString_AS_STRING
    #define PyBytes_Check PyString_Check
    #define PyBytes_CheckExact PyString_CheckExact
    #define PyBytes_FromStringAndSize PyString_FromStringAndSize
    #define PyBytes_GET_SIZE PyString_GET_SIZE
    #define PyBytes_Size PyString_Size
    #define _PyBytes_Resize _PyString_Resize
#endif

// Used for items that are ANSI in Python 2 and Unicode in Python 3 or in int 2 and long in 3.

#if PY_MAJOR_VERSION >= 3
  #define PyString_FromString PyUnicode_FromString
  #define PyString_FromStringAndSize PyUnicode_FromStringAndSize
  #define PyString_Check PyUnicode_Check
  #define PyString_Type PyUnicode_Type
  #define PyString_Size PyUnicode_Size
  #define PyInt_FromLong PyLong_FromLong
  #define PyInt_AsLong PyLong_AsLong
  #define PyInt_AS_LONG PyLong_AS_LONG
  #define PyInt_Type PyLong_Type
  #define PyString_FromFormatV PyUnicode_FromFormatV
  #define PyString_FromFormat PyUnicode_FromFormat
  #define Py_TPFLAGS_HAVE_ITER 0
  #define PyString_AsString PyUnicode_AsString

  #define TEXT_T Py_UNICODE

  #define PyString_Join PyUnicode_Join

inline void PyString_ConcatAndDel(PyObject** lhs, PyObject* rhs)
{
    PyUnicode_Concat(*lhs, rhs);
    Py_DECREF(rhs);
}

#else
  #include <stringobject.h>
  #include <intobject.h>
  #include <bufferobject.h>

  #define TEXT_T char

  #define PyString_Join _PyString_Join

#endif

inline PyObject* Text_New(Py_ssize_t length)
{
    // Returns a new, uninitialized String (Python 2) or Unicode object (Python 3) object.
#if PY_MAJOR_VERSION < 3
    return PyString_FromStringAndSize(0, length);
#else
    return PyUnicode_FromUnicode(0, length);
#endif
}

inline TEXT_T* Text_Buffer(PyObject* o)
{
#if PY_MAJOR_VERSION < 3
    I(PyString_Check(o));
    return PyString_AS_STRING(o);
#else
    I(PyUnicode_Check(o));
    return PyUnicode_AS_UNICODE(o);
#endif
}


inline bool IntOrLong_Check(PyObject* o)
{
    // A compatibility function to check for an int or long.  Python 3 doesn't differentate
    // anymore.
    // A compatibility function that determines if the object is a string, based on the version of Python.
    // For Python 2, an ASCII or Unicode string is allowed.  For Python 3, it must be a Unicode object.
#if PY_MAJOR_VERSION < 3
    if (o && PyInt_Check(o))
        return true;
#endif
    return o && PyLong_Check(o);
}

inline bool Text_Check(PyObject* o)
{
    // A compatibility function that determines if the object is a string, based on the version of Python.
    // For Python 2, an ASCII or Unicode string is allowed.  For Python 3, it must be a Unicode object.
#if PY_MAJOR_VERSION < 3
    if (o && PyString_Check(o))
        return true;
#endif
    return o && PyUnicode_Check(o);
}

bool Text_EqualsI(PyObject* lhs, const char* rhs);
// Case-insensitive comparison for a Python string object (Unicode in Python 3, ASCII or Unicode in Python 2) against
// an ASCII string.  If lhs is 0 or None, false is returned.


inline Py_ssize_t Text_Size(PyObject* o)
{
#if PY_MAJOR_VERSION < 3
    if (o && PyString_Check(o))
        return PyString_GET_SIZE(o);
#endif
    return (o && PyUnicode_Check(o)) ? PyUnicode_GET_SIZE(o) : 0;
}

inline Py_ssize_t TextCopyToUnicode(Py_UNICODE* buffer, PyObject* o)
{
    // Copies a String or Unicode object to a Unicode buffer and returns the number of characters copied.
    // No NULL terminator is appended!

#if PY_MAJOR_VERSION < 3
    if (PyBytes_Check(o))
    {
        const Py_ssize_t cch = PyBytes_GET_SIZE(o);
        const char * pch = PyBytes_AS_STRING(o);
        for (Py_ssize_t i = 0; i < cch; i++)
            *buffer++ = (Py_UNICODE)*pch++;
        return cch;
    }
    else
    {
#endif
        Py_ssize_t cch = PyUnicode_GET_SIZE(o);
        memcpy(buffer, PyUnicode_AS_UNICODE(o), cch * sizeof(Py_UNICODE));
        return cch;
#if PY_MAJOR_VERSION < 3
    }
#endif
}

#if PY_MAJOR_VERSION < 3
int PyCodec_KnownEncoding(const char *encoding);
#endif

#endif // PYODBCCOMPAT_H

#include "pyodbc.h"

bool Text_EqualsI(PyObject* lhs, const char* rhs)
{
#if PY_MAJOR_VERSION < 3
    // In Python 2, allow ANSI strings.
    if (lhs && PyString_Check(lhs))
        return _strcmpi(PyString_AS_STRING(lhs), rhs) == 0;
#endif

    if (lhs == 0 || !PyUnicode_Check(lhs))
        return false;

    Py_ssize_t cchLHS = PyUnicode_GET_SIZE(lhs);
    Py_ssize_t cchRHS = (Py_ssize_t)strlen(rhs);
    if (cchLHS != cchRHS)
        return false;

    Py_UNICODE* p = PyUnicode_AS_UNICODE(lhs);
    for (Py_ssize_t i = 0; i < cchLHS; i++)
    {
        int chL = (int)Py_UNICODE_TOUPPER(p[i]);
        int chR = (int)toupper(rhs[i]);
        if (chL != chR)
            return false;
    }

    return true;
}

#if PY_MAJOR_VERSION < 3
int PyCodec_KnownEncoding(const char *encoding)
{
    PyObject* codec = _PyCodec_Lookup(encoding);

    if (codec)
    {
        Py_DECREF(codec);
        return 1;
    }

    PyErr_Clear();
    return 0;
}
#endif

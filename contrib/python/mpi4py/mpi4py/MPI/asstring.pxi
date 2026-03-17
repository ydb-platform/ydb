#------------------------------------------------------------------------------

cdef extern from "Python.h":
    int    PyUnicode_Check(object)
    object PyUnicode_AsUTF8String(object)
    object PyUnicode_AsASCIIString(object)
    object PyUnicode_FromString(const char[])
    object PyUnicode_FromStringAndSize(const char[],Py_ssize_t)
    object PyBytes_FromString(const char[])
    object PyBytes_FromStringAndSize(const char[],Py_ssize_t)
    int    PyBytes_AsStringAndSize(object,char*[],Py_ssize_t*) except -1

#------------------------------------------------------------------------------

cdef inline object asmpistr(object ob, char *s[]):
    if PyUnicode_Check(ob):
        if PY3: ob = PyUnicode_AsUTF8String(ob);
        else:   ob = PyUnicode_AsASCIIString(ob);
    PyBytes_AsStringAndSize(ob, s, NULL)
    return ob

cdef inline object tompistr(const char s[], int n):
    if PY3: return PyUnicode_FromStringAndSize(s, n)
    else:   return PyBytes_FromStringAndSize(s, n)

cdef inline object mpistr(const char s[]):
    if PY3: return PyUnicode_FromString(s)
    else:   return PyBytes_FromString(s)

cdef inline object pystr(const char s[]):
    if PY3: return PyUnicode_FromString(s)
    else:   return PyBytes_FromString(s)

#------------------------------------------------------------------------------

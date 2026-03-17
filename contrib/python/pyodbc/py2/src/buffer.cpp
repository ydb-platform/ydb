
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "pyodbc.h"

#if PY_MAJOR_VERSION < 3


#include "buffer.h"
#include "pyodbcmodule.h"

Py_ssize_t
PyBuffer_GetMemory(PyObject* buffer, const char** pp)
{
    PyBufferProcs* procs = Py_TYPE(buffer)->tp_as_buffer;

    if (!procs || !PyType_HasFeature(Py_TYPE(buffer), Py_TPFLAGS_HAVE_GETCHARBUFFER))
    {
        // Can't access the memory directly because the buffer object doesn't support it.
        return -1;
    }

    if (procs->bf_getsegcount(buffer, 0) != 1)
    {
        // Can't access the memory directly because there is more than one segment.
        return -1;
    }

#if PY_VERSION_HEX >= 0x02050000
    char* pT = 0;
#else
    const char* pT = 0;
#endif
    Py_ssize_t cb = procs->bf_getcharbuffer(buffer, 0, &pT);

    if (pp)
        *pp = pT;

    return cb;
}

Py_ssize_t
PyBuffer_Size(PyObject* self)
{
    if (!PyBuffer_Check(self))
    {
        PyErr_SetString(PyExc_TypeError, "Not a buffer!");
        return 0;
    }

    Py_ssize_t total_len = 0;
    Py_TYPE(self)->tp_as_buffer->bf_getsegcount(self, &total_len);
    return total_len;
}
#endif

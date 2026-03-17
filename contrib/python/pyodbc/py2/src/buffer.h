
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef _BUFFER_H
#define _BUFFER_H

#if PY_MAJOR_VERSION < 3

// If the buffer object has a single, accessible segment, returns the length of the buffer.  If 'pp' is not NULL, the
// address of the segment is also returned.  If there is more than one segment or if it cannot be accessed, -1 is
// returned and 'pp' is not modified.
Py_ssize_t
PyBuffer_GetMemory(PyObject* buffer, const char** pp);

// Returns the size of a Python buffer.
//
// If an error occurs, zero is returned, but zero is a valid buffer size (I guess), so use PyErr_Occurred to determine
// if it represents a failure.
Py_ssize_t
PyBuffer_Size(PyObject* self);


class BufferSegmentIterator
{
    PyObject* pBuffer;
    Py_ssize_t iSegment;
    Py_ssize_t cSegments;
    
public:
    BufferSegmentIterator(PyObject* _pBuffer)
    {
        pBuffer   = _pBuffer;
        PyBufferProcs* procs = Py_TYPE(pBuffer)->tp_as_buffer;
        iSegment  = 0;
        cSegments = procs->bf_getsegcount(pBuffer, 0);
    }
    
    bool Next(byte*& pb, SQLLEN &cb)
    {
        if (iSegment >= cSegments)
            return false;

        PyBufferProcs* procs = Py_TYPE(pBuffer)->tp_as_buffer;
        cb = procs->bf_getreadbuffer(pBuffer, iSegment++, (void**)&pb);
        return true;
    }
};

#endif // PY_MAJOR_VERSION


#endif

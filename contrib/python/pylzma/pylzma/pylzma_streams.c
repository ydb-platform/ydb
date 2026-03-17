/*
 * Python Bindings for LZMA
 *
 * Copyright (c) 2004-2015 by Joachim Bauch, mail@joachim-bauch.de
 * 7-Zip Copyright (C) 1999-2010 Igor Pavlov
 * LZMA SDK Copyright (C) 1999-2010 Igor Pavlov
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 * 
 * $Id$
 *
 */

#include "pylzma.h"
#include "pylzma_streams.h"

#ifdef _WIN32
// Initial size of output streams
#define INITIAL_BLOCKSIZE   1048576
// Maximum number of bytes to increase output streams
#define MAX_BLOCKSIZE       16777216
#else
// Initial size of output streams
#define INITIAL_BLOCKSIZE   262144
// Maximum number of bytes to increase output streams
#define MAX_BLOCKSIZE       1048576
#endif

static SRes
MemoryInStream_Read(const ISeqInStream *p, void *buf, size_t *size)
{
    CMemoryInStream *self = (CMemoryInStream *) p;
    size_t toread = *size;
    if (toread > self->avail) {
        toread = self->avail;
    }
    memcpy(buf, self->data, toread);
    self->data += toread;
    self->avail -= toread;
    *size = toread;
    return SZ_OK;
}

void
CreateMemoryInStream(CMemoryInStream *stream, Byte *data, size_t size)
{
    stream->s.Read = MemoryInStream_Read;
    stream->data = data;
    stream->avail = size;
}

static SRes
MemoryInOutStream_Read(const ISeqInStream *p, void *buf, size_t *size)
{
    CMemoryInOutStream *self = (CMemoryInOutStream *) p;
    size_t toread = *size;
    if (toread > self->size) {
        toread = self->size;
    }
    memcpy(buf, self->data, toread);
    if (self->size > toread) {
        memmove(self->data, self->data + toread, self->size - toread);
        self->size -= toread;
    } else {
        self->size = 0;
    }
    *size = toread;
    return SZ_OK;
}

void
CreateMemoryInOutStream(CMemoryInOutStream *stream)
{
    stream->s.Read = MemoryInOutStream_Read;
    stream->data = (Byte *) malloc(INITIAL_BLOCKSIZE);
    stream->size = 0;
    stream->avail = INITIAL_BLOCKSIZE;
}

BoolInt
MemoryInOutStreamAppend(CMemoryInOutStream *stream, Byte *data, size_t size)
{
    if (!size) {
        return 1;
    }
    while (stream->avail - stream->size < size) {
        stream->data = (Byte *) realloc(stream->data, stream->avail + min(stream->avail, MAX_BLOCKSIZE));
        if (stream->data == NULL) {
            stream->size = stream->avail = 0;
            return 0;
        }
        stream->avail += min(stream->avail, MAX_BLOCKSIZE);
    }
    memcpy(stream->data + stream->size, data, size);
    stream->size += size;
    return 1;
}

static SRes
PythonInStream_Read(const ISeqInStream *p, void *buf, size_t *size)
{
    CPythonInStream *self = (CPythonInStream *) p;
    size_t toread = *size;
    PyObject *data;
    SRes res;
    
    START_BLOCK_THREADS
    data = PyObject_CallMethod(self->file, "read", "i", toread);
    if (data == NULL) {
        PyErr_Print();
        res = SZ_ERROR_READ;
    } else if (!PyBytes_Check(data)) {
        res = SZ_ERROR_READ;
    } else {
        *size = PyBytes_GET_SIZE(data);
        memcpy(buf, PyBytes_AS_STRING(data), min(*size, toread));
        res = SZ_OK;
    }
    Py_XDECREF(data);    
    END_BLOCK_THREADS
    return res;
}

void
CreatePythonInStream(CPythonInStream *stream, PyObject *file)
{
    stream->s.Read = PythonInStream_Read;
    stream->file = file;
}

static size_t
MemoryOutStream_Write(const ISeqOutStream *p, const void *buf, size_t size)
{
    CMemoryOutStream *self = (CMemoryOutStream *) p;
    while (self->avail - self->size < size) {
        self->data = (Byte *) realloc(self->data, self->avail + min(self->avail, MAX_BLOCKSIZE));
        if (self->data == NULL) {
            self->size = self->avail = 0;
            return 0;
        }
        self->avail += min(self->avail, MAX_BLOCKSIZE);
    }
    memcpy(self->data + self->size, buf, size);
    self->size += size;
    return size;
}

void
CreateMemoryOutStream(CMemoryOutStream *stream)
{
    stream->s.Write = MemoryOutStream_Write;
    stream->data = (Byte *) malloc(INITIAL_BLOCKSIZE);
    stream->size = 0;
    stream->avail = INITIAL_BLOCKSIZE;
}

void
MemoryOutStreamDiscard(CMemoryOutStream *stream, size_t size)
{
    if (size >= stream->size) {
        // Clear stream
        stream->size = 0;
    } else {
        memmove(stream->data, stream->data + size, stream->size - size);
        stream->size -= size;
    }
}

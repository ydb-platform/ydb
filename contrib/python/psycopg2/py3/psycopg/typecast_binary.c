/* typecast_binary.c - binary typecasting functions to python types
 *
 * Copyright (C) 2001-2019 Federico Di Gregorio <fog@debian.org>
 * Copyright (C) 2020-2021 The Psycopg Team
 *
 * This file is part of psycopg.
 *
 * psycopg2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link this program with the OpenSSL library (or with
 * modified versions of OpenSSL that use the same license as OpenSSL),
 * and distribute linked combinations including the two.
 *
 * You must obey the GNU Lesser General Public License in all respects for
 * all of the code used other than OpenSSL.
 *
 * psycopg2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 */

#include "typecast_binary.h"

#include <stdlib.h>


/* Python object holding a memory chunk. The memory is deallocated when
   the object is destroyed. This type is used to let users directly access
   memory chunks holding unescaped binary data through the buffer interface.
 */

static void
chunk_dealloc(chunkObject *self)
{
    Dprintf("chunk_dealloc: deallocating memory at %p, size "
        FORMAT_CODE_PY_SSIZE_T,
        self->base, self->len
      );
    PyMem_Free(self->base);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
chunk_repr(chunkObject *self)
{
    return PyString_FromFormat(
        "<memory chunk at %p size " FORMAT_CODE_PY_SSIZE_T ">",
        self->base, self->len
      );
}

/* 3.0 buffer interface */
int chunk_getbuffer(PyObject *_self, Py_buffer *view, int flags)
{
    int rv;
    chunkObject *self = (chunkObject*)_self;
    rv = PyBuffer_FillInfo(view, _self, self->base, self->len, 1, flags);
    if (rv == 0) {
        view->format = "c";
    }
    return rv;
}

static PyBufferProcs chunk_as_buffer =
{
    chunk_getbuffer,
    NULL,
};

#define chunk_doc "memory chunk"

PyTypeObject chunkType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psycopg2._psycopg.chunk",
    sizeof(chunkObject), 0,
    (destructor) chunk_dealloc, /* tp_dealloc*/
    0,                          /* tp_print */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_compare */
    (reprfunc) chunk_repr,      /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash */
    0,                          /* tp_call */
    0,                          /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    &chunk_as_buffer,           /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /* tp_flags */
    chunk_doc                   /* tp_doc */
};


static char *parse_hex(
        const char *bufin, Py_ssize_t sizein, Py_ssize_t *sizeout);
static char *parse_escape(
        const char *bufin, Py_ssize_t sizein, Py_ssize_t *sizeout);

/* The function is not static and not hidden as we use ctypes to test it. */
PyObject *
typecast_BINARY_cast(const char *s, Py_ssize_t l, PyObject *curs)
{
    chunkObject *chunk = NULL;
    PyObject *res = NULL;
    char *buffer = NULL;
    Py_ssize_t len;

    if (s == NULL) { Py_RETURN_NONE; }

    if (s[0] == '\\' && s[1] == 'x') {
        /* This is a buffer escaped in hex format: libpq before 9.0 can't
         * parse it and we can't detect reliably the libpq version at runtime.
         * So the only robust option is to parse it ourselves - luckily it's
         * an easy format.
         */
        if (NULL == (buffer = parse_hex(s, l, &len))) {
            goto exit;
        }
    }
    else {
        /* This is a buffer in the classic bytea format. So we can handle it
         * to the PQunescapeBytea to have it parsed, right? ...Wrong. We
         * could, but then we'd have to record whether buffer was allocated by
         * Python or by the libpq to dispose it properly. Furthermore the
         * PQunescapeBytea interface is not the most brilliant as it wants a
         * null-terminated string even if we have known its length thus
         * requiring a useless memcpy and strlen.
         * So we'll just have our better integrated parser, let's finish this
         * story.
         */
        if (NULL == (buffer = parse_escape(s, l, &len))) {
            goto exit;
        }
    }

    chunk = (chunkObject *) PyObject_New(chunkObject, &chunkType);
    if (chunk == NULL) goto exit;

    /* **Transfer** ownership of buffer's memory to the chunkObject: */
    chunk->base = buffer;
    buffer = NULL;
    chunk->len = (Py_ssize_t)len;

    if ((res = PyMemoryView_FromObject((PyObject*)chunk)) == NULL)
        goto exit;

exit:
    Py_XDECREF((PyObject *)chunk);
    PyMem_Free(buffer);

    return res;
}


static const char hex_lut[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

/* Parse a bytea output buffer encoded in 'hex' format.
 *
 * the format is described in
 * https://www.postgresql.org/docs/current/static/datatype-binary.html
 *
 * Parse the buffer in 'bufin', whose length is 'sizein'.
 * Return a new buffer allocated by PyMem_Malloc and set 'sizeout' to its size.
 * In case of error set an exception and return NULL.
 */
static char *
parse_hex(const char *bufin, Py_ssize_t sizein, Py_ssize_t *sizeout)
{
    char *ret = NULL;
    const char *bufend = bufin + sizein;
    const char *pi = bufin + 2;     /* past the \x */
    char *bufout;
    char *po;

    po = bufout = PyMem_Malloc((sizein - 2) >> 1);   /* output size upper bound */
    if (NULL == bufout) {
        PyErr_NoMemory();
        goto exit;
    }

    /* Implementation note: we call this function upon database response, not
     * user input (because we are parsing the output format of a buffer) so we
     * don't expect errors. On bad input we reserve the right to return a bad
     * output, not an error.
     */
    while (pi < bufend) {
        char c;
        while (-1 == (c = hex_lut[*pi++ & '\x7f'])) {
            if (pi >= bufend) { goto endloop; }
        }
        *po = c << 4;

        while (-1 == (c = hex_lut[*pi++ & '\x7f'])) {
            if (pi >= bufend) { goto endloop; }
        }
        *po++ |= c;
    }
endloop:

    ret = bufout;
    *sizeout = po - bufout;

exit:
    return ret;
}

/* Parse a bytea output buffer encoded in 'escape' format.
 *
 * the format is described in
 * https://www.postgresql.org/docs/current/static/datatype-binary.html
 *
 * Parse the buffer in 'bufin', whose length is 'sizein'.
 * Return a new buffer allocated by PyMem_Malloc and set 'sizeout' to its size.
 * In case of error set an exception and return NULL.
 */
static char *
parse_escape(const char *bufin, Py_ssize_t sizein, Py_ssize_t *sizeout)
{
    char *ret = NULL;
    const char *bufend = bufin + sizein;
    const char *pi = bufin;
    char *bufout;
    char *po;

    po = bufout = PyMem_Malloc(sizein);   /* output size upper bound */
    if (NULL == bufout) {
        PyErr_NoMemory();
        goto exit;
    }

    while (pi < bufend) {
        if (*pi != '\\') {
            /* Unescaped char */
            *po++ = *pi++;
            continue;
        }
        if ((pi[1] >= '0' && pi[1] <= '3') &&
            (pi[2] >= '0' && pi[2] <= '7') &&
            (pi[3] >= '0' && pi[3] <= '7'))
        {
            /* Escaped octal value */
            *po++ = ((pi[1] - '0') << 6) |
                    ((pi[2] - '0') << 3) |
                    ((pi[3] - '0'));
            pi += 4;
        }
        else {
            /* Escaped char */
            *po++ = pi[1];
            pi += 2;
        }
    }

    ret = bufout;
    *sizeout = po - bufout;

exit:
    return ret;
}

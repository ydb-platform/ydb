/*
Copyright (c) 2011, Andres Moreira <andres@andresmoreira.com>
              2011, Felipe Cruz <felipecruz@loogica.net>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the authors nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ANDRES MOREIRA BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "pythoncapi_compat.h"
#include <string.h>
#include <stdio.h>
#include <snappy-c.h>
#include "crc32c.h"

#define MODULE_VERSION "0.4.1"
#define RESIZE_TOLERATION 0.75

struct module_state {
    PyObject *error;
};

#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif


/* if support for Python 2.5 is dropped the bytesobject.h will do this for us */
#if PY_MAJOR_VERSION < 3
#define PyBytes_FromStringAndSize PyString_FromStringAndSize
#define _PyBytes_Resize _PyString_Resize
#define PyBytes_AS_STRING PyString_AS_STRING
#endif

static PyObject *SnappyCompressError,
    *SnappyUncompressError,
    *SnappyInvalidCompressedInputError,
    *SnappyCompressedLengthError;

static inline PyObject *
maybe_resize(PyObject *str, size_t expected_size, size_t actual_size)
{
    // Tolerate up to 25% slop, to reduce the likelihood of
    // reallocation and copying.
    if (actual_size != expected_size) {
	    if (actual_size < expected_size * RESIZE_TOLERATION) {
	        _PyBytes_Resize(&str, actual_size);
	        return str;
	    }
	    Py_SET_SIZE(str, actual_size);
    }
    return str;
}

static const char *
snappy_strerror(snappy_status status)
{
    switch (status) {
        case SNAPPY_OK:
	        return "no error";
        case SNAPPY_INVALID_INPUT:
	        return "invalid input";
        case SNAPPY_BUFFER_TOO_SMALL:
	        return "buffer too small";
        default:
	        return "unknown error";
    }
}

static PyObject *
snappy__compress(PyObject *self, PyObject *args)
{
    Py_buffer input;
    size_t compressed_size, actual_size;
    PyObject * result;
    snappy_status status;

#if PY_MAJOR_VERSION >= 3
    if (!PyArg_ParseTuple(args, "y*", &input))
#else
    if (!PyArg_ParseTuple(args, "s*", &input))
#endif
        return NULL;

    // Ask for the max size of the compressed object.
    compressed_size = snappy_max_compressed_length(input.len);

    // Make snappy compression
    result = PyBytes_FromStringAndSize(NULL, compressed_size);
    if (result) {
        actual_size = compressed_size;
        Py_BEGIN_ALLOW_THREADS
        status = snappy_compress((const char *) input.buf, input.len,
                                 PyBytes_AS_STRING(result), &actual_size);
        Py_END_ALLOW_THREADS
        PyBuffer_Release(&input);
        if (status == SNAPPY_OK) {
            return maybe_resize(result, compressed_size, actual_size);
        }
        else {
            Py_DECREF(result);
        }
        PyErr_Format(SnappyCompressError,
    		 "Error while compressing: %s", snappy_strerror(status));
    }
    else {
        PyBuffer_Release(&input);
        PyErr_Format(SnappyCompressError,
                     "Error while compressing: unable to acquire output string");
    }
    return NULL;
}

static PyObject *
snappy__uncompress(PyObject *self, PyObject *args)
{
    Py_buffer compressed;
    size_t uncomp_size, actual_size;
    PyObject * result;
    snappy_status status;

#if PY_MAJOR_VERSION >=3
    if (!PyArg_ParseTuple(args, "y*", &compressed))
#else
    if (!PyArg_ParseTuple(args, "s*", &compressed))
#endif
        return NULL;

    status = snappy_uncompressed_length((const char *) compressed.buf, compressed.len,
                                        &uncomp_size);
    if (status != SNAPPY_OK) {
        PyBuffer_Release(&compressed);
        PyErr_SetString(SnappyCompressedLengthError,
            "Can not calculate uncompressed length");
        return NULL;
    }

    result = PyBytes_FromStringAndSize(NULL, uncomp_size);
    if (result) {
        actual_size = uncomp_size;
        Py_BEGIN_ALLOW_THREADS
        status = snappy_uncompress((const char *) compressed.buf, compressed.len,
                                   PyBytes_AS_STRING(result), &actual_size);
        Py_END_ALLOW_THREADS
        PyBuffer_Release(&compressed);
        if (SNAPPY_OK == status) {
            return maybe_resize(result, uncomp_size, actual_size);
        }
        else {
            Py_DECREF(result);
            PyErr_Format(SnappyUncompressError,
                         "Error while decompressing: %s", snappy_strerror(status));
        }
    }
    else {
        PyBuffer_Release(&compressed);
    }
    return NULL;
}


static PyObject *
snappy__is_valid_compressed_buffer(PyObject *self, PyObject *args)
{
    const char * compressed;
    Py_ssize_t comp_size;
    snappy_status status;

#if PY_MAJOR_VERSION >=3
    if (!PyArg_ParseTuple(args, "y#", &compressed, &comp_size))
#else
    if (!PyArg_ParseTuple(args, "s#", &compressed, &comp_size))
#endif
        return NULL;

    status = snappy_validate_compressed_buffer(compressed, comp_size);
    if (status == SNAPPY_OK)
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

static PyObject *
snappy__crc32c(PyObject *self, PyObject *args)
{
    Py_buffer input;
    PyObject * result;

#if PY_MAJOR_VERSION >= 3
    if (!PyArg_ParseTuple(args, "y*", &input))
#else
    if (!PyArg_ParseTuple(args, "s*", &input))
#endif
        return NULL;

    result = PyLong_FromUnsignedLong(
            crc_finalize(crc_update(crc_init(), (const unsigned char *) input.buf, input.len)));

    PyBuffer_Release(&input);

    return result;
}

static PyMethodDef snappy_methods[] = {
    {"compress",  snappy__compress, METH_VARARGS,
        "Compress a string using the snappy library."},
    {"uncompress",  snappy__uncompress, METH_VARARGS,
        "Uncompress a string compressed with the snappy library."},
    {"decompress",  snappy__uncompress, METH_VARARGS,
        "Alias to Uncompress method, to be compatible with zlib."},
    {"isValidCompressed",  snappy__is_valid_compressed_buffer, METH_VARARGS,
        "Returns True if the compressed buffer is valid, False otherwise"},
    {"_crc32c",  snappy__crc32c, METH_VARARGS,
        "Generate an RFC3720, section 12.1 CRC-32C"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3

static int snappy_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}

static int snappy_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}


static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_snappy",
    NULL,
    sizeof(struct module_state),
    snappy_methods,
    NULL,
    snappy_traverse,
    snappy_clear,
    NULL
};


#define INITERROR return NULL

PyMODINIT_FUNC
PyInit__snappy(void)

#else
#define INITERROR return

PyMODINIT_FUNC
init_snappy(void)
#endif
{
    PyObject *m;

    #if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
    #else
    m = Py_InitModule("_snappy", snappy_methods);
    #endif

    if (m == NULL)
        INITERROR;

    SnappyCompressError = PyErr_NewException((char*)"snappy.CompressError",
        NULL, NULL);
    SnappyUncompressError = PyErr_NewException((char*)"snappy.UncompressError",
        NULL, NULL);
    SnappyInvalidCompressedInputError = PyErr_NewException(
        (char*)"snappy.InvalidCompressedInputError", NULL, NULL);
    SnappyCompressedLengthError = PyErr_NewException(
        (char*)"snappy.CompressedLengthError", NULL, NULL);

    Py_INCREF(SnappyCompressError);
    Py_INCREF(SnappyUncompressError);
    Py_INCREF(SnappyInvalidCompressedInputError);
    Py_INCREF(SnappyCompressedLengthError);

    PyModule_AddObject(m, "CompressError", SnappyCompressError);
    PyModule_AddObject(m, "UncompressError", SnappyUncompressError);
    PyModule_AddObject(m, "InvalidCompressedInputError",
        SnappyInvalidCompressedInputError);
    PyModule_AddObject(m, "CompressedLengthError", SnappyCompressedLengthError);

    /* Version = MODULE_VERSION */
    if (PyModule_AddStringConstant(m, "__version__", MODULE_VERSION))
        INITERROR;

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}

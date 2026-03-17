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

#include <Python.h>

#include "../sdk/LzmaEnc.h"

#include "pylzma.h"
#include "pylzma_streams.h"

typedef struct {
    PyObject_HEAD
    CLzmaEncHandle encoder;
    CMemoryInOutStream inStream;
    CMemoryOutStream outStream;
} CCompressionObject;

static char
doc_comp_compress[] = \
    "docstring is todo\n";

static PyObject *
pylzma_comp_compress(CCompressionObject *self, PyObject *args)
{
    PyObject *result = NULL;
    char *data;
    PARSE_LENGTH_TYPE length;
    PY_LONG_LONG bufsize=BLOCK_SIZE;
    int res;
    size_t before;
    
    if (!PyArg_ParseTuple(args, "s#|L", &data, &length, &bufsize))
        return NULL;
    
    if (!MemoryInOutStreamAppend(&self->inStream, (Byte *) data, length)) {
        return PyErr_NoMemory();
    }
    
    Py_BEGIN_ALLOW_THREADS
    while (1) {
        before = self->inStream.avail;
        res = LzmaEnc_CodeOneBlock(self->encoder, False, 0, 0);
        if (res != SZ_OK || self->outStream.size >= bufsize || self->inStream.avail == before) {
            break;
        }
    }
    Py_END_ALLOW_THREADS
    if (res != SZ_OK) {
        PyErr_Format(PyExc_TypeError, "Error during compressing: %d", res);
        return NULL;
    }
    
    length = min(self->outStream.size, bufsize);
    result = PyString_FromStringAndSize((const char *)self->outStream.data, length);
    if (result != NULL) {
        MemoryOutStreamDiscard(&self->outStream, length);
    }
    
    return result;
}

static char 
doc_comp_flush[] = \
    "flush() -- Finishes the compression and returns any remaining compressed data.";

static PyObject *
pylzma_comp_flush(CCompressionObject *self, PyObject *args)
{
    PyObject *result;
    int res;
    
    Py_BEGIN_ALLOW_THREADS
    while (1) {
        res = LzmaEnc_CodeOneBlock(self->encoder, False, 0, 0);
        if (res != SZ_OK || LzmaEnc_IsFinished(self->encoder)) {
            break;
        }
    }
    Py_END_ALLOW_THREADS
    if (res != SZ_OK) {
        PyErr_Format(PyExc_TypeError, "Error during compressing: %d", res);
        return NULL;
    }
    
    LzmaEnc_Finish(self->encoder);
    result = PyString_FromStringAndSize((const char *) self->outStream.data, self->outStream.size);
    if (result != NULL) {
        MemoryOutStreamDiscard(&self->outStream, self->outStream.size);
    }
    
    return result;
}

PyMethodDef
pylzma_comp_methods[] = {
    {"compress",   (PyCFunction)pylzma_comp_compress, METH_VARARGS, doc_comp_compress},
    {"flush",      (PyCFunction)pylzma_comp_flush,    METH_NOARGS,  doc_comp_flush},
    {NULL, NULL},
};

static void
pylzma_comp_dealloc(CCompressionObject *self)
{
    LzmaEnc_Destroy(self->encoder, &allocator, &allocator);
    if (self->outStream.data != NULL) {
        free(self->outStream.data);
    }
    if (self->inStream.data != NULL) {
        free(self->inStream.data);
    }
    self->ob_type->tp_free((PyObject*)self);
}

int
pylzma_comp_init(CCompressionObject *self, PyObject *args, PyObject *kwargs)
{
    CLzmaEncProps props;
    Byte header[LZMA_PROPS_SIZE];
    size_t headerSize = LZMA_PROPS_SIZE;
    int result=-1;
    
    // possible keywords for this function
    static char *kwlist[] = {"dictionary", "fastBytes", "literalContextBits",
                             "literalPosBits", "posBits", "algorithm", "eos", "multithreading", "matchfinder", NULL};
    int dictionary = 23;         // [0,28], default 23 (8MB)
    int fastBytes = 128;         // [5,255], default 128
    int literalContextBits = 3;  // [0,8], default 3
    int literalPosBits = 0;      // [0,4], default 0
    int posBits = 2;             // [0,4], default 2
    int eos = 1;                 // write "end of stream" marker?
    int multithreading = 1;      // use multithreading if available?
    char *matchfinder = NULL;    // matchfinder algorithm
    int algorithm = 2;
    int res;
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iiiiiiiis", kwlist, &dictionary, &fastBytes,
                                                                 &literalContextBits, &literalPosBits, &posBits, &algorithm, &eos, &multithreading, &matchfinder))
        return -1;
    
    CHECK_RANGE(dictionary,         0,  28, "dictionary must be between 0 and 28");
    CHECK_RANGE(fastBytes,          5, 255, "fastBytes must be between 5 and 255");
    CHECK_RANGE(literalContextBits, 0,   8, "literalContextBits must be between 0 and 8");
    CHECK_RANGE(literalPosBits,     0,   4, "literalPosBits must be between 0 and 4");
    CHECK_RANGE(posBits,            0,   4, "posBits must be between 0 and 4");

    if (matchfinder != NULL) {
#if (PY_VERSION_HEX >= 0x02050000)
        PyErr_WarnEx(PyExc_DeprecationWarning, "matchfinder selection is deprecated and will be ignored", 1);
#else
        PyErr_Warn(PyExc_DeprecationWarning, "matchfinder selection is deprecated and will be ignored");
#endif
    }
    
    self->encoder = LzmaEnc_Create(&allocator);
    if (self->encoder == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    
    LzmaEncProps_Init(&props);
    
    props.dictSize = 1 << dictionary;
    props.lc = literalContextBits;
    props.lp = literalPosBits;
    props.pb = posBits;
    props.algo = algorithm;
    props.fb = fastBytes;
    // props.btMode = 1;
    // props.numHashBytes = 4;
    // props.mc = 32;
    props.writeEndMark = eos ? 1 : 0;
    props.numThreads = multithreading ? 2 : 1;
    LzmaEncProps_Normalize(&props);
    res = LzmaEnc_SetProps(self->encoder, &props);
    if (res != SZ_OK) {
        PyErr_Format(PyExc_TypeError, "could not set encoder properties: %d", res);
        return -1;
    }

    CreateMemoryInOutStream(&self->inStream);
    CreateMemoryOutStream(&self->outStream);

    LzmaEnc_WriteProperties(self->encoder, header, &headerSize);
    if (self->outStream.s.Write(&self->outStream, header, headerSize) != headerSize) {
        PyErr_SetString(PyExc_TypeError, "could not generate stream header");
        goto exit;
    }
    
    LzmaEnc_Prepare(self->encoder, &self->inStream.s, &self->outStream.s, &allocator, &allocator);
    result = 0;
    
exit:
    return result;
}

PyTypeObject
CCompressionObject_Type = {
    //PyObject_HEAD_INIT(&PyType_Type)
    PyObject_HEAD_INIT(NULL)
    0,
    "pylzma.compressobj",                /* char *tp_name; */
    sizeof(CCompressionObject),          /* int tp_basicsize; */
    0,                                   /* int tp_itemsize;       // not used much */
    (destructor)pylzma_comp_dealloc,     /* destructor tp_dealloc; */
    NULL,                                /* printfunc  tp_print;   */
    NULL,                                /* getattrfunc  tp_getattr; // __getattr__ */
    NULL,                                /* setattrfunc  tp_setattr;  // __setattr__ */
    NULL,                                /* cmpfunc  tp_compare;  // __cmp__ */
    NULL,                                /* reprfunc  tp_repr;    // __repr__ */
    NULL,                                /* PyNumberMethods *tp_as_number; */
    NULL,                                /* PySequenceMethods *tp_as_sequence; */
    NULL,                                /* PyMappingMethods *tp_as_mapping; */
    NULL,                                /* hashfunc tp_hash;     // __hash__ */
    NULL,                                /* ternaryfunc tp_call;  // __call__ */
    NULL,                                /* reprfunc tp_str;      // __str__ */
    0,                                   /* tp_getattro*/
    0,                                   /* tp_setattro*/
    0,                                   /* tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,  /*tp_flags*/
    "Compression class",                 /* tp_doc */
    0,                                   /* tp_traverse */
    0,                                   /* tp_clear */
    0,                                   /* tp_richcompare */
    0,                                   /* tp_weaklistoffset */
    0,                                   /* tp_iter */
    0,                                   /* tp_iternext */
    pylzma_comp_methods,                 /* tp_methods */
    0,                                   /* tp_members */
    0,                                   /* tp_getset */
    0,                                   /* tp_base */
    0,                                   /* tp_dict */
    0,                                   /* tp_descr_get */
    0,                                   /* tp_descr_set */
    0,                                   /* tp_dictoffset */
    (initproc)pylzma_comp_init,          /* tp_init */
    0,                                   /* tp_alloc */
    0,                                   /* tp_new */
};

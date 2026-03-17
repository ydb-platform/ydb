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
#if PY_MAJOR_VERSION < 3
#include <cStringIO.h>
#endif

#include "LzmaEnc.h"
#include "7zTypes.h"

#include "pylzma.h"
#include "pylzma_streams.h"
#include "pylzma_compress.h"
#include "pylzma_compressfile.h"

void
pylzma_init_compfile(void)
{
#if PY_MAJOR_VERSION < 3
    PycString_IMPORT;
#endif
}

typedef struct {
    PyObject_HEAD
    CLzmaEncHandle encoder;
    CPythonInStream inStream;
    CMemoryOutStream outStream;
    PyObject *inFile;
} CCompressionFileObject;

static char
doc_compfile_read[] = \
    "docstring is todo\n";

static PyObject *
pylzma_compfile_read(CCompressionFileObject *self, PyObject *args)
{
    PyObject *result = NULL;
    int length, bufsize=0, res=SZ_OK;
    
    if (!PyArg_ParseTuple(args, "|i", &bufsize))
        return NULL;

    while (!bufsize || self->outStream.size < (size_t) bufsize)
    {
        Py_BEGIN_ALLOW_THREADS
        res = LzmaEnc_CodeOneBlock(self->encoder, 0, 0);
        Py_END_ALLOW_THREADS
        if (res != SZ_OK || LzmaEnc_IsFinished(self->encoder)) {
            break;
        }
    }
    
    if (LzmaEnc_IsFinished(self->encoder)) {
        LzmaEnc_Finish(self->encoder);
    }
    
    if (bufsize)
        length = min((size_t) bufsize, self->outStream.size);
    else
        length = self->outStream.size;
    
    result = PyBytes_FromStringAndSize((const char *)self->outStream.data, length);
    if (result == NULL) {
        PyErr_NoMemory();
        goto exit;
    }
    
    MemoryOutStreamDiscard(&self->outStream, length);

exit:

    return result;
}

static PyMethodDef
pylzma_compfile_methods[] = {
    {"read",   (PyCFunction)pylzma_compfile_read, METH_VARARGS, doc_compfile_read},
    {NULL, NULL},
};

static void
pylzma_compfile_dealloc(CCompressionFileObject *self)
{
    DEC_AND_NULL(self->inFile);
    if (self->encoder != NULL) {
        LzmaEnc_Destroy(self->encoder, &allocator, &allocator);
    }
    if (self->outStream.data != NULL) {
        free(self->outStream.data);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
pylzma_compfile_init(CCompressionFileObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *inFile;
    CLzmaEncProps props;
    Byte header[LZMA_PROPS_SIZE];
    size_t headerSize = LZMA_PROPS_SIZE;
    int result = -1;
    
    // possible keywords for this function
    static char *kwlist[] = {"infile", "dictionary", "fastBytes", "literalContextBits",
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
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|iiiiiiiis", kwlist, &inFile, &dictionary, &fastBytes,
                                                                 &literalContextBits, &literalPosBits, &posBits, &algorithm, &eos, &multithreading, &matchfinder))
        return -1;
    
    CHECK_RANGE(dictionary,         0,  28, "dictionary must be between 0 and 28");
    CHECK_RANGE(fastBytes,          5, 255, "fastBytes must be between 5 and 255");
    CHECK_RANGE(literalContextBits, 0,   8, "literalContextBits must be between 0 and 8");
    CHECK_RANGE(literalPosBits,     0,   4, "literalPosBits must be between 0 and 4");
    CHECK_RANGE(posBits,            0,   4, "posBits must be between 0 and 4");
    CHECK_RANGE(algorithm,          0,   2, "algorithm must be between 0 and 2");
    
    if (matchfinder != NULL) {
#if (PY_VERSION_HEX >= 0x02050000)
        PyErr_WarnEx(PyExc_DeprecationWarning, "matchfinder selection is deprecated and will be ignored", 1);
#else
        PyErr_Warn(PyExc_DeprecationWarning, "matchfinder selection is deprecated and will be ignored");
#endif
    }
    
    if (PyBytes_Check(inFile)) {
#if PY_MAJOR_VERSION >= 3
        PyErr_SetString(PyExc_TypeError, "first parameter must be a file-like object");
        return -1;
#else
        // create new cStringIO object from string
        inFile = PycStringIO->NewInput(inFile);
        if (inFile == NULL)
        {
            PyErr_NoMemory();
            return -1;
        }
#endif
    } else if (!PyObject_HasAttrString(inFile, "read")) {
        PyErr_SetString(PyExc_TypeError, "first parameter must be a file-like object");
        return -1;
    } else {
        // protect object from being refcounted out...
        Py_INCREF(inFile);
    }
    
    self->encoder = LzmaEnc_Create(&allocator);
    if (self->encoder == NULL) {
        Py_DECREF(inFile);
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
        Py_DECREF(inFile);
        PyErr_Format(PyExc_TypeError, "could not set encoder properties: %d", res);
        return -1;
    }

    self->inFile = inFile;
    CreatePythonInStream(&self->inStream, inFile);
    CreateMemoryOutStream(&self->outStream);

    LzmaEnc_WriteProperties(self->encoder, header, &headerSize);
    if (self->outStream.s.Write((const ISeqOutStream*) &self->outStream, header, headerSize) != headerSize) {
        PyErr_SetString(PyExc_TypeError, "could not generate stream header");
        goto exit;
    }
    
    LzmaEnc_Prepare(self->encoder, &self->outStream.s, &self->inStream.s, &allocator, &allocator);
    result = 0;
    
exit:
    return result;
}

PyTypeObject
CCompressionFileObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pylzma.compressfile",                  /* char *tp_name; */
    sizeof(CCompressionFileObject),      /* int tp_basicsize; */
    0,                                   /* int tp_itemsize;       // not used much */
    (destructor)pylzma_compfile_dealloc, /* destructor tp_dealloc; */
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
    "File compression class",           /* tp_doc */
    0,                                   /* tp_traverse */
    0,                                   /* tp_clear */
    0,                                   /* tp_richcompare */
    0,                                   /* tp_weaklistoffset */
    0,                                   /* tp_iter */
    0,                                   /* tp_iternext */
    pylzma_compfile_methods,             /* tp_methods */
    0,                                   /* tp_members */
    0,                                   /* tp_getset */
    0,                                   /* tp_base */
    0,                                   /* tp_dict */
    0,                                   /* tp_descr_get */
    0,                                   /* tp_descr_set */
    0,                                   /* tp_dictoffset */
    (initproc)pylzma_compfile_init,      /* tp_init */
    0,                                   /* tp_alloc */
    0,                                   /* tp_new */
};

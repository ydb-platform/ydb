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

#include "Aes.h"
#include "7zTypes.h"

#include "pylzma.h"

#define ALIGNMENT       16
#define ALIGNMENT_MASK  (ALIGNMENT-1)

typedef struct {
    PyObject_HEAD
    Byte aesBuf[(AES_NUM_IVMRK_WORDS*sizeof(UInt32)) + ALIGNMENT];
    UInt32 *aes;
} CAESDecryptObject;

int
aesdecrypt_init(CAESDecryptObject *self, PyObject *args, PyObject *kwargs)
{
    char *key=NULL;
    PARSE_LENGTH_TYPE keylength=0;
    char *iv=NULL;
    PARSE_LENGTH_TYPE ivlength=0;
    int offset;
    
    // possible keywords for this function
    static char *kwlist[] = {"key", "iv", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|s#s#", kwlist, &key, &keylength, &iv, &ivlength))
        return -1;
    
    memset(&self->aesBuf, 0, sizeof(self->aesBuf));
    self->aes = (UInt32 *) self->aesBuf;
    // AES code expects aligned memory
    offset = ((uintptr_t) self->aes) & (ALIGNMENT_MASK);
    if (offset != 0) {
        self->aes = (UInt32 *) &self->aesBuf[ALIGNMENT - offset];
        assert(((uintptr_t) self->aes & ALIGNMENT_MASK) == 0);
    }

    if (keylength > 0) {
        if (keylength != 16 && keylength != 24 && keylength != 32) {
            PyErr_Format(PyExc_TypeError, "key must be 16, 24 or 32 bytes, got " PARSE_LENGTH_FORMAT, keylength);
            return -1;
        }

        Aes_SetKey_Dec(self->aes + 4, (Byte *) key, keylength);
    }
    if (ivlength > 0) {
        if (ivlength != AES_BLOCK_SIZE) {
            PyErr_Format(PyExc_TypeError, "iv must be %d bytes, got " PARSE_LENGTH_FORMAT, AES_BLOCK_SIZE, ivlength);
            return -1;
        }

        AesCbc_Init(self->aes, (Byte *) iv);
    }
    return 0;
}

static char 
doc_aesdecrypt_decrypt[] = \
    "decrypt(data) -- Decrypt given data.";

static PyObject *
aesdecrypt_decrypt(CAESDecryptObject *self, PyObject *args)
{
    char *data;
    PARSE_LENGTH_TYPE length;
    PyObject *result;
    char *out;
    PARSE_LENGTH_TYPE outlength;
    char *tmpdata = NULL;
    
    if (!PyArg_ParseTuple(args, "s#", &data, &length))
        return NULL;
    
    if (length % AES_BLOCK_SIZE) {
        PyErr_Format(PyExc_TypeError, "data must be a multiple of %d bytes, got " PARSE_LENGTH_FORMAT, AES_BLOCK_SIZE, length);
        return NULL;
    }
    
    result = PyBytes_FromStringAndSize(NULL, length);
    if (result == NULL) {
        return NULL;
    }

    out = PyBytes_AS_STRING(result);
    outlength = PyBytes_Size(result);
    Py_BEGIN_ALLOW_THREADS
    // AES code expects aligned memory
    if ((uintptr_t) out & ALIGNMENT_MASK) {
        int offset;
        tmpdata = out = (char *) malloc(length + ALIGNMENT);
        if (tmpdata == NULL) {
            goto exit;
        }

        offset = (uintptr_t) tmpdata & ALIGNMENT_MASK;
        if (offset != 0) {
            out = tmpdata + (ALIGNMENT - offset);
        }
        assert(((uintptr_t) out & ALIGNMENT_MASK) == 0);
    }
    memcpy(out, data, length);
    g_AesCbc_Decode(self->aes, (Byte *) out, outlength / AES_BLOCK_SIZE);
    if (tmpdata != NULL) {
        memcpy(PyBytes_AS_STRING(result), out, length);
    }

exit:
    Py_END_ALLOW_THREADS
    if (out == NULL) {
        Py_DECREF(result);
        result = NULL;
        PyErr_NoMemory();
    }
    free(tmpdata);
    return result;
}

PyMethodDef
aesdecrypt_methods[] = {
    {"decrypt",     (PyCFunction)aesdecrypt_decrypt,    METH_VARARGS,  doc_aesdecrypt_decrypt},
    {NULL, NULL},
};

PyTypeObject
CAESDecrypt_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "pylzma.AESDecrypt",                 /* char *tp_name; */
    sizeof(CAESDecryptObject),           /* int tp_basicsize; */
    0,                                   /* int tp_itemsize;       // not used much */
    NULL,                                /* destructor tp_dealloc; */
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
    "AES decryption class",                 /* tp_doc */
    0,                                   /* tp_traverse */
    0,                                   /* tp_clear */
    0,                                   /* tp_richcompare */
    0,                                   /* tp_weaklistoffset */
    0,                                   /* tp_iter */
    0,                                   /* tp_iternext */
    aesdecrypt_methods,                  /* tp_methods */
    0,                                   /* tp_members */
    0,                                   /* tp_getset */
    0,                                   /* tp_base */
    0,                                   /* tp_dict */
    0,                                   /* tp_descr_get */
    0,                                   /* tp_descr_set */
    0,                                   /* tp_dictoffset */
    (initproc)aesdecrypt_init,           /* tp_init */
    0,                                   /* tp_alloc */
    0,                                   /* tp_new */
};

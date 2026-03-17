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

#include "7zVersion.h"
#include "Sha256.h"
#include "Aes.h"
#include "Bra.h"

#include "pylzma.h"
#include "pylzma_compress.h"
#include "pylzma_decompress.h"
#include "pylzma_decompressobj.h"
#if 0
#include "pylzma_compressobj.h"
#endif
#include "pylzma_compressfile.h"
#include "pylzma_aes.h"
#ifdef WITH_COMPAT
#include "pylzma_decompress_compat.h"
#include "pylzma_decompressobj_compat.h"
#endif

#if defined(WITH_THREAD) && !defined(PYLZMA_USE_GILSTATE)
PyInterpreterState* _pylzma_interpreterState = NULL;
#endif

#define STRINGIZE(x) #x
#define STRINGIZE_VALUE_OF(x) STRINGIZE(x)

const char
doc_calculate_key[] = \
    "calculate_key(password, cycles, salt=None, digest='sha256') -- Calculate decryption key.";

static PyObject *
pylzma_calculate_key(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *password;
    PARSE_LENGTH_TYPE pwlen;
    int cycles;
    PyObject *pysalt=NULL;
    char *salt;
    Py_ssize_t saltlen;
    char *digest="sha256";
    char key[32];
    // possible keywords for this function
    static char *kwlist[] = {"password", "cycles", "salt", "digest", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#i|Os", kwlist, &password, &pwlen, &cycles, &pysalt, &digest))
        return NULL;
    
    if (pysalt == Py_None) {
        pysalt = NULL;
    } else if (!PyBytes_Check(pysalt)) {
        PyErr_Format(PyExc_TypeError, "salt must be a string, got a %s", pysalt->ob_type->tp_name);
        return NULL;
    }
    
    if (strcmp(digest, "sha256") != 0) {
        PyErr_Format(PyExc_TypeError, "digest %s is unsupported", digest);
        return NULL;
    }
    
    if (pysalt != NULL) {
        salt = PyBytes_AS_STRING(pysalt);
        saltlen = PyBytes_Size(pysalt);
    } else {
        salt = NULL;
        saltlen = 0;
    }
    
    if (cycles == 0x3f) {
        int pos;
        int i;
        for (pos = 0; pos < saltlen; pos++)
            key[pos] = salt[pos];
        for (i = 0; i<pwlen && pos < 32; i++)
            key[pos++] = password[i];
        for (; pos < 32; pos++)
            key[pos] = 0;
    } else {
        CSha256 sha;
        long round;
        int i;
        long rounds = (long) 1 << cycles;
        unsigned char temp[8] = { 0,0,0,0,0,0,0,0 };
        Py_BEGIN_ALLOW_THREADS
        Sha256_Init(&sha);
        for (round = 0; round < rounds; round++) {
            Sha256_Update(&sha, (Byte *) salt, saltlen);
            Sha256_Update(&sha, (Byte *) password, pwlen);
            Sha256_Update(&sha, (Byte *) temp, 8);
            for (i = 0; i < 8; i++)
                if (++(temp[i]) != 0)
                    break;
        }
        Sha256_Final(&sha, (Byte *) &key);
        Py_END_ALLOW_THREADS
    }
    
    return PyBytes_FromStringAndSize(key, 32);
}

const char
doc_bcj_x86_convert[] = \
    "bcj_x86_convert(data) -- Perform BCJ x86 conversion.";

static PyObject *
pylzma_bcj_x86_convert(PyObject *self, PyObject *args)
{
    char *data;
    PARSE_LENGTH_TYPE length;
    int encoding=0;
    PyObject *result;
    
    if (!PyArg_ParseTuple(args, "s#|i", &data, &length, &encoding)) {
        return NULL;
    }
    
    if (!length) {
        return PyBytes_FromString("");
    }
    
    result = PyBytes_FromStringAndSize(data, length);
    if (result != NULL) {
        UInt32 state;
        Py_BEGIN_ALLOW_THREADS
        x86_Convert_Init(state);
        x86_Convert((Byte *) PyBytes_AS_STRING(result), length, 0, &state, encoding);
        Py_END_ALLOW_THREADS
    }
    
    return result;
}

#define DEFINE_BCJ_CONVERTER(id, name) \
const char \
doc_bcj_##id##_convert[] = \
    "bcj_" #id "_convert(data) -- Perform BCJ " #name " conversion."; \
\
static PyObject * \
pylzma_bcj_##id##_convert(PyObject *self, PyObject *args) \
{ \
    char *data; \
    PARSE_LENGTH_TYPE length; \
    int encoding=0; \
    PyObject *result; \
     \
    if (!PyArg_ParseTuple(args, "s#|i", &data, &length, &encoding)) { \
        return NULL; \
    } \
     \
    if (!length) { \
        return PyBytes_FromString(""); \
    } \
     \
    result = PyBytes_FromStringAndSize(data, length); \
    if (result != NULL) { \
        Py_BEGIN_ALLOW_THREADS \
        name##_Convert((Byte *) PyBytes_AS_STRING(result), length, 0, encoding); \
        Py_END_ALLOW_THREADS \
    } \
     \
    return result; \
}

DEFINE_BCJ_CONVERTER(arm, ARM);
DEFINE_BCJ_CONVERTER(armt, ARMT);
DEFINE_BCJ_CONVERTER(ppc, PPC);
DEFINE_BCJ_CONVERTER(sparc, SPARC);
DEFINE_BCJ_CONVERTER(ia64, IA64);

PyMethodDef
methods[] = {
    // exported functions
    {"compress",      (PyCFunction)pylzma_compress,      METH_VARARGS | METH_KEYWORDS, (char *)&doc_compress},
    {"decompress",    (PyCFunction)pylzma_decompress,    METH_VARARGS | METH_KEYWORDS, (char *)&doc_decompress},
#ifdef WITH_COMPAT
    // compatibility functions
    {"decompress_compat",    (PyCFunction)pylzma_decompress_compat,    METH_VARARGS | METH_KEYWORDS, (char *)&doc_decompress_compat},
    {"decompressobj_compat", (PyCFunction)pylzma_decompressobj_compat, METH_VARARGS,                 (char *)&doc_decompressobj_compat},
#endif
    {"calculate_key",   (PyCFunction)pylzma_calculate_key,  METH_VARARGS | METH_KEYWORDS,   (char *)&doc_calculate_key},
    // BCJ
    {"bcj_x86_convert",     (PyCFunction)pylzma_bcj_x86_convert,    METH_VARARGS,   (char *)&doc_bcj_x86_convert},
    {"bcj_arm_convert",     (PyCFunction)pylzma_bcj_arm_convert,    METH_VARARGS,   (char *)&doc_bcj_arm_convert},
    {"bcj_armt_convert",    (PyCFunction)pylzma_bcj_armt_convert,   METH_VARARGS,   (char *)&doc_bcj_armt_convert},
    {"bcj_ppc_convert",     (PyCFunction)pylzma_bcj_ppc_convert,    METH_VARARGS,   (char *)&doc_bcj_ppc_convert},
    {"bcj_sparc_convert",   (PyCFunction)pylzma_bcj_sparc_convert,  METH_VARARGS,   (char *)&doc_bcj_sparc_convert},
    {"bcj_ia64_convert",    (PyCFunction)pylzma_bcj_ia64_convert,   METH_VARARGS,   (char *)&doc_bcj_ia64_convert},
    {NULL, NULL},
};

static void *Alloc(ISzAllocPtr p, size_t size) { (void)p; return malloc(size); }
static void Free(ISzAllocPtr p, void *address) { (void)p; free(address); }
ISzAlloc allocator = { Alloc, Free };

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef
pylzma_module = {
   PyModuleDef_HEAD_INIT,
   "pylzma",
   NULL,
   -1,
   methods
};
#define RETURN_MODULE_ERROR     return NULL
#else
#define RETURN_MODULE_ERROR     return
#endif

PyMODINIT_FUNC
#if PY_MAJOR_VERSION >= 3
PyInit_pylzma(void)
#else
initpylzma(void)
#endif
{
    PyObject *m;

    CDecompressionObject_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&CDecompressionObject_Type) < 0)
        RETURN_MODULE_ERROR;
#if 0
    CCompressionObject_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&CCompressionObject_Type) < 0)
        RETURN_MODULE_ERROR;
#endif
    CCompressionFileObject_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&CCompressionFileObject_Type) < 0)
        RETURN_MODULE_ERROR;

    CAESDecrypt_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&CAESDecrypt_Type) < 0)
        RETURN_MODULE_ERROR;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&pylzma_module);
#else
    m = Py_InitModule("pylzma", methods);
#endif

    Py_INCREF(&CDecompressionObject_Type);
    PyModule_AddObject(m, "decompressobj", (PyObject *)&CDecompressionObject_Type);
#if 0
    Py_INCREF(&CCompressionObject_Type);
    PyModule_AddObject(m, "compressobj", (PyObject *)&CCompressionObject_Type);
#endif   
    Py_INCREF(&CCompressionFileObject_Type);
    PyModule_AddObject(m, "compressfile", (PyObject *)&CCompressionFileObject_Type);

    Py_INCREF(&CAESDecrypt_Type);
    PyModule_AddObject(m, "AESDecrypt", (PyObject *)&CAESDecrypt_Type);

    PyModule_AddIntConstant(m, "SDK_VER_MAJOR", MY_VER_MAJOR);
    PyModule_AddIntConstant(m, "SDK_VER_MINOR", MY_VER_MINOR);
    PyModule_AddIntConstant(m, "SDK_VER_BUILD ", MY_VER_BUILD);
    PyModule_AddStringConstant(m, "SDK_VERSION", MY_VERSION);
    PyModule_AddStringConstant(m, "SDK_DATE", MY_DATE);
    PyModule_AddStringConstant(m, "SDK_COPYRIGHT", MY_COPYRIGHT);
    PyModule_AddStringConstant(m, "SDK_VERSION_COPYRIGHT_DATE", MY_VERSION_COPYRIGHT_DATE);

#ifdef PYLZMA_VERSION
    PyModule_AddStringConstant(m, "__version__", STRINGIZE_VALUE_OF(PYLZMA_VERSION));
#else
    PyModule_AddStringConstant(m, "__version__", "unreleased");
#endif

    AesGenTables();
    pylzma_init_compfile();

#if defined(WITH_THREAD)
    PyEval_InitThreads();

#if !defined(PYLZMA_USE_GILSTATE)
    /* Save the current interpreter, so compressing file objects works. */
    _pylzma_interpreterState = PyThreadState_Get()->interp;
#endif

#endif
#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}

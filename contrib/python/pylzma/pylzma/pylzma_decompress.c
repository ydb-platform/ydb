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

#include "LzmaDec.h"
#include "Lzma2Dec.h"

#include "pylzma.h"
#include "pylzma_streams.h"

const char
doc_decompress[] = \
    "decompress(data[, maxlength]) -- Decompress the data, returning a string containing the decompressed data. "\
    "If the string has been compressed without an EOS marker, you must provide the maximum length as keyword parameter.\n" \
    "decompress(data, bufsize[, maxlength]) -- Decompress the data using an initial output buffer of size bufsize. "\
    "If the string has been compressed without an EOS marker, you must provide the maximum length as keyword parameter.\n";

PyObject *
pylzma_decompress(PyObject *self, PyObject *args, PyObject *kwargs)
{
    unsigned char *data;
    Byte *tmp;
    PARSE_LENGTH_TYPE length;
    int bufsize=BLOCK_SIZE;
    PY_LONG_LONG totallength=-1;
    int lzma2 = 0;
    PARSE_LENGTH_TYPE avail;
    PyObject *result=NULL;
    union {
        CLzmaDec lzma;
        CLzma2Dec lzma2;
    } state;
    ELzmaStatus status;
    size_t srcLen, destLen;
    int res;
    CMemoryOutStream outStream;
    int propertiesLength;
    // possible keywords for this function
    static char *kwlist[] = {"data", "bufsize", "maxlength", "lzma2", NULL};
    
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#|iLi", kwlist, &data, &length, &bufsize, &totallength, &lzma2))
        return NULL;

    propertiesLength = lzma2 ? 1 : LZMA_PROPS_SIZE;

    if (totallength != -1) {
        // We know the decompressed size, run simple case
        result = PyBytes_FromStringAndSize(NULL, totallength);
        if (result == NULL) {
            return NULL;
        }
        
        tmp = (Byte *) PyBytes_AS_STRING(result);
        srcLen = length - propertiesLength;
        destLen = totallength;
        Py_BEGIN_ALLOW_THREADS
        if (lzma2) {
            res = Lzma2Decode(tmp, &destLen, (Byte *) (data + propertiesLength), &srcLen, data[0], LZMA_FINISH_ANY, &status, &allocator);
        } else {
            res = LzmaDecode(tmp, &destLen, (Byte *) (data + propertiesLength), &srcLen, data, propertiesLength, LZMA_FINISH_ANY, &status, &allocator);
        }
        Py_END_ALLOW_THREADS
        if (res != SZ_OK) {
            Py_DECREF(result);
            result = NULL;
            PyErr_Format(PyExc_TypeError, "Error while decompressing: %d", res);
        } else if (destLen < (size_t) totallength) {
            _PyBytes_Resize(&result, destLen);
        }
        return result;
    }
    
    CreateMemoryOutStream(&outStream);
    tmp = (Byte *) malloc(bufsize);
    if (tmp == NULL) {
        return PyErr_NoMemory();
    }

    if (lzma2) {
        Lzma2Dec_Construct(&state.lzma2);
        res = Lzma2Dec_Allocate(&state.lzma2, data[0], &allocator);
        if (res != SZ_OK) {
            PyErr_SetString(PyExc_TypeError, "Incorrect stream properties");
            goto exit;
        }
    } else {
        LzmaDec_Construct(&state.lzma);
        res = LzmaDec_Allocate(&state.lzma, data, propertiesLength, &allocator);
        if (res != SZ_OK) {
            PyErr_SetString(PyExc_TypeError, "Incorrect stream properties");
            goto exit;
        }
    }

    data += propertiesLength;
    avail = length - propertiesLength;
    Py_BEGIN_ALLOW_THREADS
    if (lzma2) {
        Lzma2Dec_Init(&state.lzma2);
    } else {
        LzmaDec_Init(&state.lzma);
    }
    for (;;) {
        srcLen = avail;
        destLen = bufsize;
        
        if (lzma2) {
            res = Lzma2Dec_DecodeToBuf(&state.lzma2, tmp, &destLen, data, &srcLen, LZMA_FINISH_ANY, &status);
        } else {
            res = LzmaDec_DecodeToBuf(&state.lzma, tmp, &destLen, data, &srcLen, LZMA_FINISH_ANY, &status);
        }
        data += srcLen;
        avail -= srcLen;
        if (res == SZ_OK && destLen > 0 && outStream.s.Write((const ISeqOutStream*) &outStream, tmp, destLen) != destLen) {
            res = SZ_ERROR_WRITE;
        }
        if (res != SZ_OK || status == LZMA_STATUS_FINISHED_WITH_MARK || status == LZMA_STATUS_NEEDS_MORE_INPUT) {
            break;
        }
        
    }
    Py_END_ALLOW_THREADS
    
    if (status == LZMA_STATUS_NEEDS_MORE_INPUT) {
        PyErr_SetString(PyExc_ValueError, "data error during decompression");
    } else if (res != SZ_OK) {
        PyErr_Format(PyExc_TypeError, "Error while decompressing: %d", res);
    } else {
        result = PyBytes_FromStringAndSize((const char *) outStream.data, outStream.size);
    }
    
exit:
    if (outStream.data != NULL) {
        free(outStream.data);
    }
    if (lzma2) {
        Lzma2Dec_Free(&state.lzma2, &allocator);
    } else {
        LzmaDec_Free(&state.lzma, &allocator);
    }
    free(tmp);
    
    return result;
}

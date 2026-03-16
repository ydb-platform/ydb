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

#include "LzmaEnc.h"

#include "pylzma.h"
#include "pylzma_streams.h"

const char
doc_compress[] = \
    "compress(string, dictionary=23, fastBytes=128, literalContextBits=3, literalPosBits=0, posBits=2, algorithm=2, eos=1, multithreading=1, matchfinder='bt4') -- Compress the data in string using the given parameters, returning a string containing the compressed data.";

PyObject *
pylzma_compress(PyObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *result = NULL;
    CLzmaEncProps props;
    CLzmaEncHandle encoder=NULL;
    CMemoryOutStream outStream;
    CMemoryInStream inStream;
    Byte header[LZMA_PROPS_SIZE];
    size_t headerSize = LZMA_PROPS_SIZE;
    int res;    
    // possible keywords for this function
    static char *kwlist[] = {"data", "dictionary", "fastBytes", "literalContextBits",
                             "literalPosBits", "posBits", "algorithm", "eos", "multithreading", "matchfinder", NULL};
    int dictionary = 23;         // [0,27], default 23 (8MB)
    int fastBytes = 128;         // [5,273], default 128
    int literalContextBits = 3;  // [0,8], default 3
    int literalPosBits = 0;      // [0,4], default 0
    int posBits = 2;             // [0,4], default 2
    int eos = 1;                 // write "end of stream" marker?
    int multithreading = 1;      // use multithreading if available?
    char *matchfinder = NULL;    // matchfinder algorithm
    int algorithm = 2;
    char *data;
    PARSE_LENGTH_TYPE length;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#|iiiiiiiis", kwlist, &data, &length, &dictionary, &fastBytes,
                                                                  &literalContextBits, &literalPosBits, &posBits, &algorithm, &eos, &multithreading, &matchfinder))
        return NULL;
    
    outStream.data = NULL;
    CHECK_RANGE(dictionary,         0,  27, "dictionary must be between 0 and 27");
    CHECK_RANGE(fastBytes,          5, 273, "fastBytes must be between 5 and 273");
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
    
    encoder = LzmaEnc_Create(&allocator);
    if (encoder == NULL)
        return PyErr_NoMemory();
    
    CreateMemoryInStream(&inStream, (Byte *) data, length);
    CreateMemoryOutStream(&outStream);
    
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
    res = LzmaEnc_SetProps(encoder, &props);
    if (res != SZ_OK) {
        PyErr_Format(PyExc_TypeError, "could not set encoder properties: %d", res);
        goto exit;
    }

    Py_BEGIN_ALLOW_THREADS
    LzmaEnc_WriteProperties(encoder, header, &headerSize);
    if (outStream.s.Write((const ISeqOutStream*) &outStream, header, headerSize) != headerSize) {
        res = SZ_ERROR_WRITE;
    } else {
        res = LzmaEnc_Encode(encoder, &outStream.s, &inStream.s, NULL, &allocator, &allocator);
    }
    Py_END_ALLOW_THREADS
    if (res != SZ_OK) {
        PyErr_Format(PyExc_TypeError, "Error during compressing: %d", res);
        goto exit;
    }
    
    result = PyBytes_FromStringAndSize((const char *) outStream.data, outStream.size);
    
exit:
    if (encoder != NULL) {
        LzmaEnc_Destroy(encoder, &allocator, &allocator);
    }
    if (outStream.data != NULL) {
        free(outStream.data);
    }
    
    return result;
}

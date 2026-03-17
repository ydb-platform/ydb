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

#include "pylzma.h"
#include "pylzma_decompress_compat.h"

void free_lzma_stream(lzma_stream *stream)
{
    if (stream->dynamicData)
        lzmafree(stream->dynamicData);
    stream->dynamicData = NULL;
    
    if (stream->dictionary)
        lzmafree(stream->dictionary);
    stream->dictionary = NULL;
}

const char doc_decompress_compat[] = \
    "decompress_compat(string) -- Decompress the data in string, returning a string containing the decompressed data.\n" \
    "decompress_compat(string, bufsize) -- Decompress the data in string using an initial output buffer of size bufsize.\n";

PyObject *pylzma_decompress_compat(PyObject *self, PyObject *args)
{
    char *data;
    PARSE_LENGTH_TYPE length;
    PY_LONG_LONG blocksize=BLOCK_SIZE;
    PyObject *result = NULL;
    lzma_stream stream;
    int res;
    char *output;
    
    if (!PyArg_ParseTuple(args, "s#|L", &data, &length, &blocksize))
        return NULL;
    
    memset(&stream, 0, sizeof(stream));
    if (!(output = (char *)malloc(blocksize)))
    {
        PyErr_NoMemory();
        goto exit;
    }
    
    lzmaCompatInit(&stream);
    stream.next_in = (Byte *)data;
    stream.avail_in = length;
    stream.next_out = (Byte *)output;
    stream.avail_out = blocksize;
    
    // decompress data
    while (1)
    {
        Py_BEGIN_ALLOW_THREADS
        res = lzmaCompatDecode(&stream);
        Py_END_ALLOW_THREADS
        
        if (res == LZMA_STREAM_END) {
            break;
        } else if (res == LZMA_NOT_ENOUGH_MEM) {
            // out of memory during decompression
            PyErr_NoMemory();
            goto exit;
        } else if (res == LZMA_DATA_ERROR) {
            PyErr_SetString(PyExc_ValueError, "data error during decompression");
            goto exit;
        } else if (res == LZMA_OK) {
            // check if we need to adjust the output buffer
            if (stream.avail_out == 0)
            {
                output = (char *)realloc(output, blocksize+BLOCK_SIZE);
                stream.avail_out = BLOCK_SIZE;
                stream.next_out = (Byte *)&output[blocksize];
                blocksize += BLOCK_SIZE;
            };
        } else {
            PyErr_Format(PyExc_ValueError, "unknown return code from lzmaDecode: %d", res);
            goto exit;
        }
        
        // if we exit here, decompression finished without returning LZMA_STREAM_END
        // XXX: why is this sometimes?
        if (stream.avail_in == 0)
            break;
    }

    result = PyBytes_FromStringAndSize(output, stream.totalOut);
    
exit:
    free_lzma_stream(&stream);
    if (output != NULL)
        free(output);
    
    return result;
}

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

#ifndef ___PYLZMA_STREAMS__H___
#define ___PYLZMA_STREAMS__H___

#include <Python.h>

#include "7zTypes.h"

typedef struct
{
    ISeqInStream s;
    Byte *data;
    size_t size;
    size_t avail;
} CMemoryInStream;

void CreateMemoryInStream(CMemoryInStream *stream, Byte *data, size_t size);

typedef CMemoryInStream CMemoryInOutStream;

void CreateMemoryInOutStream(CMemoryInOutStream *stream);
BoolInt MemoryInOutStreamAppend(CMemoryInOutStream *stream, Byte *data, size_t size);

typedef struct
{
    ISeqInStream s;
    PyObject *file;
} CPythonInStream;

void CreatePythonInStream(CPythonInStream *stream, PyObject *file);

typedef struct
{
    ISeqOutStream s;
    Byte *data;
    size_t size;
    size_t avail;
} CMemoryOutStream;

void CreateMemoryOutStream(CMemoryOutStream *stream);
void MemoryOutStreamDiscard(CMemoryOutStream *stream, size_t size);

#endif

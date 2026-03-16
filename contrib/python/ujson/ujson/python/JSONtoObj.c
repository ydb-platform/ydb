/*
Developed by ESN, an Electronic Arts Inc. studio.
Copyright (c) 2014, Electronic Arts Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
* Neither the name of ESN, Electronic Arts Inc. nor the
names of its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS INC. BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


Portions of code from MODP_ASCII - Ascii transformations (upper/lower, etc)
http://code.google.com/p/stringencoders/
Copyright (c) 2007  Nick Galbreath -- nickg [at] modp [dot] com. All rights reserved.

Numeric decoder derived from from TCL library
http://www.opensource.apple.com/source/tcl/tcl-14/tcl/license.terms
 * Copyright (c) 1988-1993 The Regents of the University of California.
 * Copyright (c) 1994 Sun Microsystems, Inc.
*/

#include <stdbool.h>

#include <Python.h>
#include <ultrajson.h>
#include "ujson.h"

// Import JSONDecodeError from ujson.c
extern PyObject* JSONDecodeError;


//#define PRINTMARK() fprintf(stderr, "%s: MARK(%d)\n", __FILE__, __LINE__)
#define PRINTMARK()

static void Object_objectAddKey(void *prv, JSOBJ obj, JSOBJ name, JSOBJ value)
{
  int result = PyDict_SetItem(obj, name, value);
  if (result == -1) {
    // Clear the Python error state to prevent SystemError
    PyErr_Clear();
    // Set our own error
    PyErr_SetString(JSONDecodeError, "Invalid JSON: object keys must be strings");
  }
  Py_DECREF( (PyObject *) name);
  Py_DECREF( (PyObject *) value);
  return;
}

static void Object_arrayAddItem(void *prv, JSOBJ obj, JSOBJ value)
{
  PyList_Append(obj, value);
  Py_DECREF( (PyObject *) value);
  return;
}

/*
Check that Py_UCS4 is the same as JSUINT32, else Object_newString will fail.
Based on Linux's check in vbox_vmmdev_types.h.
This should be replaced with
  _Static_assert(sizeof(Py_UCS4) == sizeof(JSUINT32));
when C11 is made mandatory (CPython 3.11+, PyPy ?).
*/
typedef char assert_py_ucs4_is_jsuint32[1 - 2*!(sizeof(Py_UCS4) == sizeof(JSUINT32))];

static JSOBJ Object_newString(void *prv, JSUINT32 *start, JSUINT32 *end)
{
  return PyUnicode_FromKindAndData (PyUnicode_4BYTE_KIND, (Py_UCS4 *) start, (end - start));
}

static JSOBJ Object_newTrue(void *prv)
{
  Py_RETURN_TRUE;
}

static JSOBJ Object_newFalse(void *prv)
{
  Py_RETURN_FALSE;
}

static JSOBJ Object_newNull(void *prv)
{
  Py_RETURN_NONE;
}

static JSOBJ Object_newNaN(void *prv)
{
    return PyFloat_FromDouble(Py_NAN);
}

static JSOBJ Object_newPosInf(void *prv)
{
    return PyFloat_FromDouble(Py_HUGE_VAL);
}

static JSOBJ Object_newNegInf(void *prv)
{
    return PyFloat_FromDouble(-Py_HUGE_VAL);
}

static JSOBJ Object_newObject(void *prv)
{
  return PyDict_New();
}

static JSOBJ Object_newArray(void *prv)
{
  return PyList_New(0);
}

static JSOBJ Object_newInteger(void *prv, JSINT32 value)
{
  return PyLong_FromLong( (long) value);
}

static JSOBJ Object_newLong(void *prv, JSINT64 value)
{
  return PyLong_FromLongLong (value);
}

static JSOBJ Object_newUnsignedLong(void *prv, JSUINT64 value)
{
  return PyLong_FromUnsignedLongLong (value);
}

static JSOBJ Object_newIntegerFromString(void *prv, char *value, size_t length)
{
  // PyLong_FromString requires a NUL-terminated string in CPython, contrary to the documentation: https://github.com/python/cpython/issues/59200
  char *buf = PyObject_Malloc(length + 1);
  memcpy(buf, value, length);
  buf[length] = '\0';
  return PyLong_FromString(buf, NULL, 10);
}

static JSOBJ Object_newDouble(void *prv, double value)
{
  return PyFloat_FromDouble(value);
}

static void Object_releaseObject(void *prv, JSOBJ obj)
{
  Py_DECREF( ((PyObject *)obj));
}

static char *g_kwlist[] = {"obj", NULL};

PyObject* JSONToObj(PyObject* self, PyObject *args, PyObject *kwargs)
{
  PyObject *ret;
  PyObject *sarg;
  PyObject *arg;
  JSONObjectDecoder decoder =
  {
    Object_newString,
    Object_objectAddKey,
    Object_arrayAddItem,
    Object_newTrue,
    Object_newFalse,
    Object_newNull,
    Object_newNaN,
    Object_newPosInf,
    Object_newNegInf,
    Object_newObject,
    Object_newArray,
    Object_newInteger,
    Object_newLong,
    Object_newUnsignedLong,
    Object_newIntegerFromString,
    Object_newDouble,
    Object_releaseObject,
    PyObject_Malloc,
    PyObject_Free,
    PyObject_Realloc
  };

  decoder.prv = NULL;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", g_kwlist, &arg))
  {
      return NULL;
  }

  Py_buffer buffer;
  size_t sarg_length;
  char * raw;
  bool is_bytes_like = !PyObject_GetBuffer(arg, &buffer, PyBUF_C_CONTIGUOUS);
  if (is_bytes_like)
  {
    #ifdef PYPY_VERSION
      // PyPy's buffer protocol implementation is buggy: https://foss.heptapod.net/pypy/pypy/-/issues/3872
      if (!PyBytes_Check(arg) && !PyByteArray_Check(arg)) {
        PyBuffer_Release(&buffer);
        PyErr_Format(PyExc_TypeError, "Arbitrary bytes-like objects are not supported on PyPy, Use either string, bytes, or bytearray");
        return NULL;
      }
    #endif
    raw = buffer.buf;
    sarg_length = buffer.len;
  }
  else
  {
    PyErr_Clear();
    if (PyUnicode_Check(arg))
    {
      sarg = PyUnicode_AsEncodedString(arg, NULL, "surrogatepass");
      if (sarg == NULL)
      {
        //Exception raised above us by codec according to docs
        return NULL;
      }
      sarg_length = PyBytes_Size(sarg);
      raw = PyBytes_AsString(sarg);
    }
    else
    {
      #ifdef PYPY_VERSION
        PyErr_Format(PyExc_TypeError, "Expected string, bytes, or bytearray");
      #else
        PyErr_Format(PyExc_TypeError, "Expected string or C-contiguous bytes-like object");
      #endif
      return NULL;
    }
  }

  decoder.errorStr = NULL;
  decoder.errorOffset = NULL;

  decoder.s2d = NULL;
  dconv_s2d_init(&decoder.s2d, DCONV_S2D_ALLOW_TRAILING_JUNK, 0.0, 0.0, "Infinity", "NaN");

  ret = JSON_DecodeObject(&decoder, raw, sarg_length);

  dconv_s2d_free(&decoder.s2d);

  if (!is_bytes_like)
  {
    Py_DECREF(sarg);
  }
  else
  {
    PyBuffer_Release(&buffer);
  }

  // Check if a Python error occurred during decoding
  if (PyErr_Occurred())
  {
    if (ret)
    {
        Py_DECREF( (PyObject *) ret);
    }
    return NULL;
  }

  if (decoder.errorStr)
  {
    /*
    FIXME: It's possible to give a much nicer error message here with actual failing element in input etc*/

    PyErr_Format (JSONDecodeError, "%s", decoder.errorStr);

    if (ret)
    {
        Py_DECREF( (PyObject *) ret);
    }

    return NULL;
  }

  return ret;
}

PyObject* JSONFileToObj(PyObject* self, PyObject *args, PyObject *kwargs)
{
  PyObject *read;
  PyObject *string;
  PyObject *result;
  PyObject *file = NULL;
  PyObject *argtuple;

  if (!PyArg_ParseTuple (args, "O", &file))
  {
    return NULL;
  }

  if (!PyObject_HasAttrString (file, "read"))
  {
    PyErr_Format (PyExc_TypeError, "expected file");
    return NULL;
  }

  read = PyObject_GetAttrString (file, "read");

  if (!PyCallable_Check (read)) {
    Py_XDECREF(read);
    PyErr_Format (PyExc_TypeError, "expected file");
    return NULL;
  }

  string = PyObject_CallObject (read, NULL);
  Py_XDECREF(read);

  if (string == NULL)
  {
    return NULL;
  }

  argtuple = PyTuple_Pack(1, string);

  result = JSONToObj (self, argtuple, kwargs);

  Py_XDECREF(argtuple);
  Py_XDECREF(string);

  if (result == NULL) {
    return NULL;
  }

  return result;
}

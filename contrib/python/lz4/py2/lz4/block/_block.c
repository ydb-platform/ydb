/*
 * Copyright (c) 2012-2018, Steeve Morin, Jonathan Underwood
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of Steeve Morin nor the names of its contributors may be
 *    used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#if defined(_WIN32) && defined(_MSC_VER)
#define inline __inline
#elif defined(__SUNPRO_C) || defined(__hpux) || defined(_AIX)
#define inline
#endif

#include <py3c.h>
#include <py3c/capsulethunk.h>

#include <stdlib.h>
#include <math.h>
#include <lz4.h>
#include <lz4hc.h>

#ifndef Py_UNUSED /* This is already defined for Python 3.4 onwards */
#ifdef __GNUC__
#define Py_UNUSED(name) _unused_ ## name __attribute__((unused))
#else
#define Py_UNUSED(name) _unused_ ## name
#endif
#endif

#if defined(_WIN32) && defined(_MSC_VER)
#if _MSC_VER >= 1600
#include <stdint.h>
#else /* _MSC_VER >= 1600 */
typedef signed char int8_t;
typedef signed short int16_t;
typedef signed int int32_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned int uint32_t;
#endif /* _MSC_VER >= 1600 */
#endif

static inline void
store_le32 (char *c, uint32_t x)
{
  c[0] = x & 0xff;
  c[1] = (x >> 8) & 0xff;
  c[2] = (x >> 16) & 0xff;
  c[3] = (x >> 24) & 0xff;
}

static inline uint32_t
load_le32 (const char *c)
{
  const uint8_t *d = (const uint8_t *) c;
  return d[0] | (d[1] << 8) | (d[2] << 16) | (d[3] << 24);
}

static const size_t hdr_size = sizeof (uint32_t);

typedef enum
{
  DEFAULT,
  FAST,
  HIGH_COMPRESSION
} compression_type;

static PyObject * LZ4BlockError;

static inline int
lz4_compress_generic (int comp, char* source, char* dest, int source_size, int dest_size,
                      char* dict, int dict_size, int acceleration, int compression)
{
  if (comp != HIGH_COMPRESSION)
    {
      LZ4_stream_t lz4_state;
      LZ4_resetStream (&lz4_state);
      if (dict)
        {
          LZ4_loadDict (&lz4_state, dict, dict_size);
        }
      if (comp != FAST)
        {
          acceleration = 1;
        }
      return LZ4_compress_fast_continue (&lz4_state, source, dest, source_size, dest_size, acceleration);
    }
  else
    {
      LZ4_streamHC_t lz4_state;
      LZ4_resetStreamHC (&lz4_state, compression);
      if (dict)
        {
          LZ4_loadDictHC (&lz4_state, dict, dict_size);
        }
      return LZ4_compress_HC_continue (&lz4_state, source, dest, source_size, dest_size);
    }
}

#ifdef inline
#undef inline
#endif

static PyObject *
compress (PyObject * Py_UNUSED (self), PyObject * args, PyObject * kwargs)
{
  const char *mode = "default";
  size_t dest_size, total_size;
  int acceleration = 1;
  int compression = 9;
  int store_size = 1;
  PyObject *py_dest;
  char *dest, *dest_start;
  compression_type comp;
  int output_size;
  Py_buffer source;
  int source_size;
  int return_bytearray = 0;
  Py_buffer dict = {0};
  static char *argnames[] = {
    "source",
    "mode",
    "store_size",
    "acceleration",
    "compression",
    "return_bytearray",
    "dict",
    NULL
  };


#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, kwargs, "y*|spiipz*", argnames,
                                    &source,
                                    &mode, &store_size, &acceleration, &compression,
                                    &return_bytearray, &dict))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, kwargs, "s*|siiiiz*", argnames,
                                    &source,
                                    &mode, &store_size, &acceleration, &compression,
                                    &return_bytearray, &dict))
    {
      return NULL;
    }
#endif

  if (source.len > INT_MAX)
    {
      PyBuffer_Release(&source);
      PyBuffer_Release(&dict);
      PyErr_Format(PyExc_OverflowError,
                   "Input too large for LZ4 API");
      return NULL;
    }

  if (dict.len > INT_MAX)
    {
      PyBuffer_Release(&source);
      PyBuffer_Release(&dict);
      PyErr_Format(PyExc_OverflowError,
                   "Dictionary too large for LZ4 API");
      return NULL;
    }

  source_size = (int) source.len;

  if (!strncmp (mode, "default", sizeof ("default")))
    {
      comp = DEFAULT;
    }
  else if (!strncmp (mode, "fast", sizeof ("fast")))
    {
      comp = FAST;
    }
  else if (!strncmp (mode, "high_compression", sizeof ("high_compression")))
    {
      comp = HIGH_COMPRESSION;
    }
  else
    {
      PyBuffer_Release(&source);
      PyBuffer_Release(&dict);
      PyErr_Format (PyExc_ValueError,
                    "Invalid mode argument: %s. Must be one of: standard, fast, high_compression",
                    mode);
      return NULL;
    }

  dest_size = LZ4_compressBound (source_size);

  if (store_size)
    {
      total_size = dest_size + hdr_size;
    }
  else
    {
      total_size = dest_size;
    }

  dest = PyMem_Malloc (total_size * sizeof * dest);
  if (dest == NULL)
    {
      return PyErr_NoMemory();
    }

  Py_BEGIN_ALLOW_THREADS

  if (store_size)
    {
      store_le32 (dest, source_size);
      dest_start = dest + hdr_size;
    }
  else
    {
      dest_start = dest;
    }

  output_size = lz4_compress_generic (comp, source.buf, dest_start, source_size,
                                      (int) dest_size, dict.buf, (int) dict.len,
                                      acceleration, compression);

  Py_END_ALLOW_THREADS

  PyBuffer_Release(&source);
  PyBuffer_Release(&dict);

  if (output_size <= 0)
    {
      PyErr_SetString (LZ4BlockError, "Compression failed");
      PyMem_Free (dest);
      return NULL;
    }

  if (store_size)
    {
      output_size += (int) hdr_size;
    }

  if (return_bytearray)
    {
      py_dest = PyByteArray_FromStringAndSize (dest, (Py_ssize_t) output_size);
    }
  else
    {
      py_dest = PyBytes_FromStringAndSize (dest, (Py_ssize_t) output_size);
    }

  PyMem_Free (dest);

  if (py_dest == NULL)
    {
      return PyErr_NoMemory ();
    }

  return py_dest;
}

static PyObject *
decompress (PyObject * Py_UNUSED (self), PyObject * args, PyObject * kwargs)
{
  Py_buffer source;
  const char * source_start;
  size_t source_size;
  PyObject *py_dest;
  char *dest;
  int output_size;
  size_t dest_size;
  int uncompressed_size = -1;
  int return_bytearray = 0;
  Py_buffer dict = {0};
  static char *argnames[] = {
    "source",
    "uncompressed_size",
    "return_bytearray",
    "dict",
    NULL
  };

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, kwargs, "y*|ipz*", argnames,
                                    &source, &uncompressed_size,
                                    &return_bytearray, &dict))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, kwargs, "s*|iiz*", argnames,
                                    &source, &uncompressed_size,
                                    &return_bytearray, &dict))
    {
      return NULL;
    }
#endif

  if (source.len > INT_MAX)
    {
      PyBuffer_Release(&source);
      PyBuffer_Release(&dict);
      PyErr_Format(PyExc_OverflowError,
                   "Input too large for LZ4 API");
      return NULL;
    }

  if (dict.len > INT_MAX)
    {
      PyBuffer_Release(&source);
      PyBuffer_Release(&dict);
      PyErr_Format(PyExc_OverflowError,
                   "Dictionary too large for LZ4 API");
      return NULL;
    }

  source_start = (const char *) source.buf;
  source_size = (int) source.len;

  if (uncompressed_size >= 0)
    {
      dest_size = uncompressed_size;
    }
  else
    {
      if (source_size < hdr_size)
        {
          PyBuffer_Release(&source);
          PyBuffer_Release(&dict);
          PyErr_SetString (PyExc_ValueError, "Input source data size too small");
          return NULL;
        }
      dest_size = load_le32 (source_start);
      source_start += hdr_size;
      source_size -= hdr_size;
    }

  if (dest_size > INT_MAX)
    {
      PyBuffer_Release(&source);
      PyBuffer_Release(&dict);
      PyErr_Format (PyExc_ValueError, "Invalid size: 0x%zu",
                    dest_size);
      return NULL;
    }

  dest = PyMem_Malloc (dest_size * sizeof * dest);
  if (dest == NULL)
    {
      return PyErr_NoMemory();
    }

  Py_BEGIN_ALLOW_THREADS

  output_size =
    LZ4_decompress_safe_usingDict (source_start, dest, source_size, (int) dest_size,
                                   dict.buf, (int) dict.len);

  Py_END_ALLOW_THREADS

  PyBuffer_Release(&source);
  PyBuffer_Release(&dict);

  if (output_size < 0)
    {
      PyErr_Format (LZ4BlockError,
                    "Decompression failed: corrupt input or insufficient space in destination buffer. Error code: %u",
                    -output_size);
      PyMem_Free (dest);
      return NULL;
    }
  else if (((size_t)output_size != dest_size) && (uncompressed_size < 0))
    {
      PyErr_Format (LZ4BlockError,
                    "Decompressor wrote %u bytes, but %zu bytes expected from header",
                    output_size, dest_size);
      PyMem_Free (dest);
      return NULL;
    }

  if (return_bytearray)
    {
      py_dest = PyByteArray_FromStringAndSize (dest, (Py_ssize_t) output_size);
    }
  else
    {
      py_dest = PyBytes_FromStringAndSize (dest, (Py_ssize_t) output_size);
    }

  PyMem_Free (dest);

  if (py_dest == NULL)
    {
      return PyErr_NoMemory ();
    }

  return py_dest;
}

PyDoc_STRVAR(compress__doc,
             "compress(source, mode='default', acceleration=1, compression=0, return_bytearray=False)\n\n" \
             "Compress source, returning the compressed data as a string.\n" \
             "Raises an exception if any error occurs.\n"               \
             "\n"                                                       \
             "Args:\n"                                                  \
             "    source (str, bytes or buffer-compatible object): Data to compress\n" \
             "\n"                                                       \
             "Keyword Args:\n"                                          \
             "    mode (str): If ``'default'`` or unspecified use the default LZ4\n" \
             "        compression mode. Set to ``'fast'`` to use the fast compression\n" \
             "        LZ4 mode at the expense of compression. Set to\n" \
             "        ``'high_compression'`` to use the LZ4 high-compression mode at\n" \
             "        the exepense of speed.\n"                         \
             "    acceleration (int): When mode is set to ``'fast'`` this argument\n" \
             "        specifies the acceleration. The larger the acceleration, the\n" \
             "        faster the but the lower the compression. The default\n" \
             "        compression corresponds to a value of ``1``.\n"       \
             "    compression (int): When mode is set to ``high_compression`` this\n" \
             "        argument specifies the compression. Valid values are between\n" \
             "        ``1`` and ``12``. Values between ``4-9`` are recommended, and\n" \
             "        ``9`` is the default.\n"
             "    store_size (bool): If ``True`` (the default) then the size of the\n" \
             "        uncompressed data is stored at the start of the compressed\n" \
             "        block.\n"                                         \
             "    return_bytearray (bool): If ``False`` (the default) then the function\n" \
             "        will return a bytes object. If ``True``, then the function will\n" \
             "        return a bytearray object.\n\n"                   \
             "    dict (str, bytes or buffer-compatible object): If specified, perform\n" \
             "        compression using this initial dictionary.\n"     \
             "Returns:\n"                                               \
             "    bytes or bytearray: Compressed data.\n");

PyDoc_STRVAR(decompress__doc,
             "decompress(source, uncompressed_size=-1, return_bytearray=False)\n\n" \
             "Decompress source, returning the uncompressed data as a string.\n" \
             "Raises an exception if any error occurs.\n"               \
             "\n"                                                       \
             "Args:\n"                                                  \
             "    source (str, bytes or buffer-compatible object): Data to decompress.\n" \
             "\n"                                                       \
             "Keyword Args:\n"                                          \
             "    uncompressed_size (int): If not specified or negative, the uncompressed\n" \
             "        data size is read from the start of the source block. If specified,\n" \
             "        it is assumed that the full source data is compressed data. If this\n" \
             "        argument is specified, it is considered to be a maximum possible size\n" \
             "        for the buffer used to hold the uncompressed data, and so less data\n" \
             "        may be returned. If `uncompressed_size` is too small, `LZ4BlockError`\n" \
             "        will be raised. By catching `LZ4BlockError` it is possible to increase\n" \
             "        `uncompressed_size` and try again.\n"             \
             "    return_bytearray (bool): If ``False`` (the default) then the function\n" \
             "        will return a bytes object. If ``True``, then the function will\n" \
             "        return a bytearray object.\n\n" \
             "    dict (str, bytes or buffer-compatible object): If specified, perform\n" \
             "        decompression using this initial dictionary.\n"   \
             "\n"                                                       \
             "Returns:\n"                                               \
             "    bytes or bytearray: Decompressed data.\n"             \
             "\n"                                                       \
             "Raises:\n"                                                \
             "    LZ4BlockError: raised if the call to the LZ4 library fails. This can be\n" \
             "        caused by `uncompressed_size` being too small, or invalid data.\n");

PyDoc_STRVAR(lz4block__doc,
             "A Python wrapper for the LZ4 block protocol"
             );

static PyMethodDef module_methods[] = {
  {
    "compress",
    (PyCFunction) compress,
    METH_VARARGS | METH_KEYWORDS,
    compress__doc
  },
  {
    "decompress",
    (PyCFunction) decompress,
    METH_VARARGS | METH_KEYWORDS,
    decompress__doc
  },
  {
    /* Sentinel */
    NULL,
    NULL,
    0,
    NULL
  }
};

static struct PyModuleDef moduledef =
{
  PyModuleDef_HEAD_INIT,
  "_block",
  lz4block__doc,
  -1,
  module_methods
};

MODULE_INIT_FUNC (_block)
{
  PyObject *module = PyModule_Create (&moduledef);

  if (module == NULL)
    return NULL;

  PyModule_AddIntConstant (module, "HC_LEVEL_MIN", LZ4HC_CLEVEL_MIN);
  PyModule_AddIntConstant (module, "HC_LEVEL_DEFAULT", LZ4HC_CLEVEL_DEFAULT);
  PyModule_AddIntConstant (module, "HC_LEVEL_OPT_MIN", LZ4HC_CLEVEL_OPT_MIN);
  PyModule_AddIntConstant (module, "HC_LEVEL_MAX", LZ4HC_CLEVEL_MAX);

  LZ4BlockError = PyErr_NewExceptionWithDoc("_block.LZ4BlockError", "Call to LZ4 library failed.", NULL, NULL);
  if (LZ4BlockError == NULL)
    {
      return NULL;
    }
  Py_INCREF(LZ4BlockError);
  PyModule_AddObject(module, "LZ4BlockError", LZ4BlockError);
  
  return module;
}

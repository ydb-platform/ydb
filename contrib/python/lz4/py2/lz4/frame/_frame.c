/*
 * Copyright (c) 2015, 2016 Jerry Ryle and Jonathan G. Underwood
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
 * 3. Neither the name of the copyright holders nor the names of its
 *    contributors may be used to endorse or promote products derived from this
 *    software without specific prior written permission.
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
 *
 */
#if defined(_WIN32) && defined(_MSC_VER)
#define inline __inline
#elif defined(__SUNPRO_C) || defined(__hpux) || defined(_AIX)
#define inline
#endif

#include <py3c.h>
#include <py3c/capsulethunk.h>

#include <stdlib.h>
#include <lz4.h> /* Needed for LZ4_VERSION_NUMBER only. */
#include <lz4frame.h>

#ifndef Py_UNUSED		/* This is already defined for Python 3.4 onwards */
#ifdef __GNUC__
#define Py_UNUSED(name) _unused_ ## name __attribute__((unused))
#else
#define Py_UNUSED(name) _unused_ ## name
#endif
#endif

static const char * compression_context_capsule_name = "_frame.LZ4F_cctx";
static const char * decompression_context_capsule_name = "_frame.LZ4F_dctx";

struct compression_context
{
  LZ4F_cctx * context;
  LZ4F_preferences_t preferences;
};

/*****************************
* create_compression_context *
******************************/
static void
destroy_compression_context (PyObject * py_context)
{
#ifndef PyCapsule_Type
  struct compression_context *context =
    PyCapsule_GetPointer (py_context, compression_context_capsule_name);
#else
  /* Compatibility with 2.6 via capsulethunk. */
  struct compression_context *context =  py_context;
#endif
  Py_BEGIN_ALLOW_THREADS
  LZ4F_freeCompressionContext (context->context);
  Py_END_ALLOW_THREADS

  PyMem_Free (context);
}

static PyObject *
create_compression_context (PyObject * Py_UNUSED (self))
{
  struct compression_context * context;
  LZ4F_errorCode_t result;

  context =
    (struct compression_context *)
    PyMem_Malloc (sizeof (struct compression_context));

  if (!context)
    {
      return PyErr_NoMemory ();
    }

  Py_BEGIN_ALLOW_THREADS

  result =
    LZ4F_createCompressionContext (&context->context,
                                   LZ4F_VERSION);
  Py_END_ALLOW_THREADS

  if (LZ4F_isError (result))
    {
      LZ4F_freeCompressionContext (context->context);
      PyMem_Free (context);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_createCompressionContext failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

  return PyCapsule_New (context, compression_context_capsule_name,
                        destroy_compression_context);
}

/************
 * compress *
 ************/
static PyObject *
compress (PyObject * Py_UNUSED (self), PyObject * args,
          PyObject * keywds)
{
  Py_buffer source;
  Py_ssize_t source_size;
  int store_size = 1;
  int return_bytearray = 0;
  int content_checksum = 0;
  int block_checksum = 0;
  int block_linked = 1;
  LZ4F_preferences_t preferences;
  size_t destination_size;
  size_t compressed_size;
  PyObject *py_destination;
  char *destination;

  static char *kwlist[] = { "data",
                            "compression_level",
                            "block_size",
                            "content_checksum",
                            "block_checksum",
                            "block_linked",
                            "store_size",
                            "return_bytearray",
                            NULL
                          };


  memset (&preferences, 0, sizeof preferences);

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "y*|iippppp", kwlist,
                                    &source,
                                    &preferences.compressionLevel,
                                    &preferences.frameInfo.blockSizeID,
                                    &content_checksum,
                                    &block_checksum,
                                    &block_linked,
                                    &store_size,
                                    &return_bytearray))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "s*|iiiiiii", kwlist,
                                    &source,
                                    &preferences.compressionLevel,
                                    &preferences.frameInfo.blockSizeID,
                                    &content_checksum,
                                    &block_checksum,
                                    &block_linked,
                                    &store_size,
                                    &return_bytearray))
    {
      return NULL;
    }
#endif

  if (content_checksum)
    {
      preferences.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    }
  else
    {
      preferences.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    }

  if (block_linked)
    {
      preferences.frameInfo.blockMode = LZ4F_blockLinked;
    }
  else
    {
      preferences.frameInfo.blockMode = LZ4F_blockIndependent;
    }

  if (LZ4_versionNumber() >= 10800)
    {
      if (block_checksum)
        {
          preferences.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
        }
      else
        {
          preferences.frameInfo.blockChecksumFlag = LZ4F_noBlockChecksum;
        }
    }
  else if (block_checksum)
    {
      PyErr_SetString (PyExc_RuntimeError,
                       "block_checksum specified but not supported by LZ4 library version");
      return NULL;
    }

  source_size = source.len;

  preferences.autoFlush = 0;
  if (store_size)
    {
      preferences.frameInfo.contentSize = source_size;
    }
  else
    {
      preferences.frameInfo.contentSize = 0;
    }

  Py_BEGIN_ALLOW_THREADS
  destination_size =
    LZ4F_compressFrameBound (source_size, &preferences);
  Py_END_ALLOW_THREADS

  if (destination_size > PY_SSIZE_T_MAX)
    {
      PyBuffer_Release(&source);
      PyErr_Format (PyExc_ValueError,
                    "Input data could require %zu bytes, which is larger than the maximum supported size of %zd bytes",
                    destination_size, PY_SSIZE_T_MAX);
      return NULL;
    }

  destination = PyMem_Malloc (destination_size * sizeof * destination);
  if (destination == NULL)
    {
      PyBuffer_Release(&source);
      return PyErr_NoMemory();
    }

  Py_BEGIN_ALLOW_THREADS
  compressed_size =
    LZ4F_compressFrame (destination, destination_size, source.buf, source_size,
                        &preferences);
  Py_END_ALLOW_THREADS

  PyBuffer_Release(&source);

  if (LZ4F_isError (compressed_size))
    {
      PyMem_Free (destination);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_compressFrame failed with code: %s",
                    LZ4F_getErrorName (compressed_size));
      return NULL;
    }

  if (return_bytearray)
    {
      py_destination = PyByteArray_FromStringAndSize (destination, (Py_ssize_t) compressed_size);
    }
  else
    {
      py_destination = PyBytes_FromStringAndSize (destination, (Py_ssize_t) compressed_size);
    }

  PyMem_Free (destination);

  if (py_destination == NULL)
    {
      return PyErr_NoMemory ();
    }

  return py_destination;
}

/******************
 * compress_begin *
 ******************/
static PyObject *
compress_begin (PyObject * Py_UNUSED (self), PyObject * args,
                PyObject * keywds)
{
  PyObject *py_context = NULL;
  Py_ssize_t source_size = (Py_ssize_t) 0;
  int return_bytearray = 0;
  int content_checksum = 0;
  int block_checksum = 0;
  int block_linked = 1;
  LZ4F_preferences_t preferences;
  PyObject *py_destination;
  char * destination;
  /* The destination buffer needs to be large enough for a header, which is 15
   * bytes. Unfortunately, the lz4 library doesn't provide a #define for this.
   * We over-allocate to allow for larger headers in the future. */
  const size_t header_size = 32;
  struct compression_context *context;
  size_t result;
  static char *kwlist[] = { "context",
                            "source_size",
                            "compression_level",
                            "block_size",
                            "content_checksum",
                            "block_checksum",
                            "block_linked",
                            "auto_flush",
                            "return_bytearray",
                            NULL
                          };

  memset (&preferences, 0, sizeof preferences);

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "O|kiippppp", kwlist,
                                    &py_context,
                                    &source_size,
                                    &preferences.compressionLevel,
                                    &preferences.frameInfo.blockSizeID,
                                    &content_checksum,
                                    &block_checksum,
                                    &block_linked,
                                    &preferences.autoFlush,
                                    &return_bytearray
                                    ))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "O|kiiiiiii", kwlist,
                                    &py_context,
                                    &source_size,
                                    &preferences.compressionLevel,
                                    &preferences.frameInfo.blockSizeID,
                                    &content_checksum,
                                    &block_checksum,
                                    &block_linked,
                                    &preferences.autoFlush,
                                    &return_bytearray
                                    ))
    {
      return NULL;
    }
#endif
  if (content_checksum)
    {
      preferences.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    }
  else
    {
      preferences.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    }

  if (block_linked)
    {
      preferences.frameInfo.blockMode = LZ4F_blockLinked;
    }
  else
    {
      preferences.frameInfo.blockMode = LZ4F_blockIndependent;
    }

  if (LZ4_versionNumber() >= 10800)
    {
      if (block_checksum)
        {
          preferences.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
        }
      else
        {
          preferences.frameInfo.blockChecksumFlag = LZ4F_noBlockChecksum;
        }
    }
  else if (block_checksum)
    {
      PyErr_SetString (PyExc_RuntimeError,
                       "block_checksum specified but not supported by LZ4 library version");
      return NULL;
    }

  if (block_linked)
    {
      preferences.frameInfo.blockMode = LZ4F_blockLinked;
    }
  else
    {
      preferences.frameInfo.blockMode = LZ4F_blockIndependent;
    }


  preferences.frameInfo.contentSize = source_size;

  context =
    (struct compression_context *) PyCapsule_GetPointer (py_context, compression_context_capsule_name);

  if (!context || !context->context)
    {
      PyErr_SetString (PyExc_ValueError, "No valid compression context supplied");
      return NULL;
    }

  context->preferences = preferences;

  destination = PyMem_Malloc (header_size * sizeof * destination);
  if (destination == NULL)
    {
      return PyErr_NoMemory();
    }

  Py_BEGIN_ALLOW_THREADS
  result = LZ4F_compressBegin (context->context,
                               destination,
                               header_size,
                               &context->preferences);
  Py_END_ALLOW_THREADS

  if (LZ4F_isError (result))
    {
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_compressBegin failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

  if (return_bytearray)
    {
      py_destination = PyByteArray_FromStringAndSize (destination, (Py_ssize_t) result);
    }
  else
    {
      py_destination = PyBytes_FromStringAndSize (destination, (Py_ssize_t) result);
    }

  PyMem_Free (destination);

  if (py_destination == NULL)
    {
      return PyErr_NoMemory ();
    }

  return py_destination;
}

/******************
 * compress_chunk *
 ******************/
static PyObject *
compress_chunk (PyObject * Py_UNUSED (self), PyObject * args,
                 PyObject * keywds)
{
  PyObject *py_context = NULL;
  Py_buffer source;
  Py_ssize_t source_size;
  struct compression_context *context;
  size_t compressed_bound;
  PyObject *py_destination;
  char *destination;
  LZ4F_compressOptions_t compress_options;
  size_t result;
  int return_bytearray = 0;
  static char *kwlist[] = { "context",
                            "data",
                            "return_bytearray",
                            NULL
  };

  memset (&compress_options, 0, sizeof compress_options);

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "Oy*|p", kwlist,
                                    &py_context,
                                    &source,
                                    &return_bytearray))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "Os*|i", kwlist,
                                    &py_context,
                                    &source,
                                    &return_bytearray))
    {
      return NULL;
    }
#endif

  source_size = source.len;

  context =
    (struct compression_context *) PyCapsule_GetPointer (py_context, compression_context_capsule_name);
  if (!context || !context->context)
    {
      PyBuffer_Release(&source);
      PyErr_Format (PyExc_ValueError, "No compression context supplied");
      return NULL;
    }

  /* If autoFlush is enabled, then the destination buffer only needs to be as
     big as LZ4F_compressFrameBound specifies for this source size. However, if
     autoFlush is disabled, previous calls may have resulted in buffered data,
     and so we need instead to use LZ4F_compressBound to find the size required
     for the destination buffer. This means that with autoFlush disabled we may
     frequently allocate more memory than needed. */
  Py_BEGIN_ALLOW_THREADS
  if (context->preferences.autoFlush == 1)
    {
      compressed_bound =
        LZ4F_compressFrameBound (source_size, &context->preferences);
    }
  else
    {
      compressed_bound =
        LZ4F_compressBound (source_size, &context->preferences);
    }
  Py_END_ALLOW_THREADS

  if (compressed_bound > PY_SSIZE_T_MAX)
    {
      PyBuffer_Release(&source);
      PyErr_Format (PyExc_ValueError,
                    "input data could require %zu bytes, which is larger than the maximum supported size of %zd bytes",
                    compressed_bound, PY_SSIZE_T_MAX);
      return NULL;
    }

  destination = PyMem_Malloc (compressed_bound * sizeof * destination);
  if (destination == NULL)
    {
      PyBuffer_Release(&source);
      return PyErr_NoMemory();
    }

  compress_options.stableSrc = 0;

  Py_BEGIN_ALLOW_THREADS
  result =
    LZ4F_compressUpdate (context->context, destination,
                         compressed_bound, source.buf, source_size,
                         &compress_options);
  Py_END_ALLOW_THREADS

  PyBuffer_Release(&source);

  if (LZ4F_isError (result))
    {
      PyMem_Free (destination);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_compressUpdate failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

  if (return_bytearray)
    {
      py_destination = PyByteArray_FromStringAndSize (destination, (Py_ssize_t) result);
    }
  else
    {
      py_destination = PyBytes_FromStringAndSize (destination, (Py_ssize_t) result);
    }

  PyMem_Free (destination);

  if (py_destination == NULL)
    {
      return PyErr_NoMemory ();
    }

  return py_destination;
}

/******************
 * compress_flush *
 ******************/
static PyObject *
compress_flush (PyObject * Py_UNUSED (self), PyObject * args, PyObject * keywds)
{
  PyObject *py_context = NULL;
  LZ4F_compressOptions_t compress_options;
  struct compression_context *context;
  size_t destination_size;
  int return_bytearray = 0;
  int end_frame = 1;
  PyObject *py_destination;
  char * destination;
  size_t result;
  static char *kwlist[] = { "context",
                            "end_frame",
                            "return_bytearray",
                            NULL
  };

  memset (&compress_options, 0, sizeof compress_options);

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "O|pp", kwlist,
                                    &py_context,
                                    &end_frame,
                                    &return_bytearray))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "O|ii", kwlist,
                                    &py_context,
                                    &end_frame,
                                    &return_bytearray))
    {
      return NULL;
    }
#endif
  if (!end_frame && LZ4_versionNumber() < 10800)
    {
      PyErr_SetString (PyExc_RuntimeError,
                       "Flush without ending a frame is not supported with this version of the LZ4 library");
      return NULL;
    }

  context =
    (struct compression_context *) PyCapsule_GetPointer (py_context, compression_context_capsule_name);
  if (!context || !context->context)
    {
      PyErr_SetString (PyExc_ValueError, "No compression context supplied");
      return NULL;
    }

  compress_options.stableSrc = 0;

  /* Calling LZ4F_compressBound with srcSize equal to 0 returns a size
     sufficient to fit (i) any remaining buffered data (when autoFlush is
     disabled) and the footer size, which is either 4 or 8 bytes depending on
     whether checksums are enabled. See: https://github.com/lz4/lz4/issues/280
     and https://github.com/lz4/lz4/issues/290. Prior to 1.7.5, it was necessary
     to call LZ4F_compressBound with srcSize equal to 1. Since we now require a
     minimum version to 1.7.5 we'll call this with srcSize equal to 0. */
  Py_BEGIN_ALLOW_THREADS
  destination_size = LZ4F_compressBound (0, &(context->preferences));
  Py_END_ALLOW_THREADS

  destination = PyMem_Malloc (destination_size * sizeof * destination);
  if (destination == NULL)
    {
      return PyErr_NoMemory();
    }

  Py_BEGIN_ALLOW_THREADS
  if (end_frame)
    {
      result =
        LZ4F_compressEnd (context->context, destination,
                          destination_size, &compress_options);
    }
  else
    {
      result =
        LZ4F_flush (context->context, destination,
                    destination_size, &compress_options);
    }
  Py_END_ALLOW_THREADS

  if (LZ4F_isError (result))
    {
      PyMem_Free (destination);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_compressEnd failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

  if (return_bytearray)
    {
      py_destination = PyByteArray_FromStringAndSize (destination, (Py_ssize_t) result);
    }
  else
    {
      py_destination = PyBytes_FromStringAndSize (destination, (Py_ssize_t) result);
    }

  PyMem_Free (destination);

  if (py_destination == NULL)
    {
      return PyErr_NoMemory ();
    }

  return py_destination;
}

/******************
 * get_frame_info *
 ******************/
static PyObject *
get_frame_info (PyObject * Py_UNUSED (self), PyObject * args,
                PyObject * keywds)
{
  Py_buffer py_source;
  char *source;
  size_t source_size;
  LZ4F_decompressionContext_t context;
  LZ4F_frameInfo_t frame_info;
  size_t result;
  unsigned int block_size;
  unsigned int block_size_id;
  int block_linked;
  int content_checksum;
  int block_checksum;
  int skippable;

  static char *kwlist[] = { "data",
                            NULL
  };

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "y*", kwlist,
                                    &py_source))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "s*", kwlist,
                                    &py_source))
    {
      return NULL;
    }
#endif

  Py_BEGIN_ALLOW_THREADS

  result = LZ4F_createDecompressionContext (&context, LZ4F_VERSION);

  if (LZ4F_isError (result))
    {
      Py_BLOCK_THREADS
      PyBuffer_Release (&py_source);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_createDecompressionContext failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

  source = (char *) py_source.buf;
  source_size = (size_t) py_source.len;

  result =
    LZ4F_getFrameInfo (context, &frame_info, source, &source_size);

  if (LZ4F_isError (result))
    {
      LZ4F_freeDecompressionContext (context);
      Py_BLOCK_THREADS
      PyBuffer_Release (&py_source);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_getFrameInfo failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

  result = LZ4F_freeDecompressionContext (context);

  Py_END_ALLOW_THREADS

  PyBuffer_Release (&py_source);

  if (LZ4F_isError (result))
    {
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_freeDecompressionContext failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }

#define KB *(1<<10)
#define MB *(1<<20)
  switch (frame_info.blockSizeID)
    {
    case LZ4F_default:
    case LZ4F_max64KB:
      block_size = 64 KB;
      block_size_id = LZ4F_max64KB;
      break;
    case LZ4F_max256KB:
      block_size = 256 KB;
      block_size_id = LZ4F_max256KB;
      break;
    case LZ4F_max1MB:
      block_size = 1 MB;
      block_size_id = LZ4F_max1MB;
      break;
    case LZ4F_max4MB:
      block_size = 4 MB;
      block_size_id = LZ4F_max4MB;
      break;
    default:
      PyErr_Format (PyExc_RuntimeError,
                    "Unrecognized blockSizeID in get_frame_info: %d",
                    frame_info.blockSizeID);
      return NULL;
    }
#undef KB
#undef MB

  if (frame_info.blockMode == LZ4F_blockLinked)
    {
      block_linked = 1;
    }
  else if (frame_info.blockMode == LZ4F_blockIndependent)
    {
      block_linked = 0;
    }
  else
    {
      PyErr_Format (PyExc_RuntimeError,
                    "Unrecognized blockMode in get_frame_info: %d",
                    frame_info.blockMode);
      return NULL;
    }

  if (frame_info.contentChecksumFlag == LZ4F_noContentChecksum)
    {
      content_checksum = 0;
    }
  else if (frame_info.contentChecksumFlag == LZ4F_contentChecksumEnabled)
    {
      content_checksum = 1;
    }
  else
    {
      PyErr_Format (PyExc_RuntimeError,
                    "Unrecognized contentChecksumFlag in get_frame_info: %d",
                    frame_info.contentChecksumFlag);
      return NULL;
    }

  if (LZ4_versionNumber() >= 10800)
    {
      if (frame_info.blockChecksumFlag == LZ4F_noBlockChecksum)
        {
          block_checksum = 0;
        }
      else if (frame_info.blockChecksumFlag == LZ4F_blockChecksumEnabled)
        {
          block_checksum = 1;
        }
      else
        {
          PyErr_Format (PyExc_RuntimeError,
                        "Unrecognized blockChecksumFlag in get_frame_info: %d",
                        frame_info.blockChecksumFlag);
          return NULL;
        }
    }
  else
    {
      /* Prior to LZ4 1.8.0 the blockChecksum functionality wasn't exposed in the
         frame API, and blocks weren't checksummed, so we'll always return 0
         here. */
      block_checksum = 0;
    }

  if (frame_info.frameType == LZ4F_frame)
    {
      skippable = 0;
    }
  else if (frame_info.frameType == LZ4F_skippableFrame)
    {
      skippable = 1;
    }
  else
    {
      PyErr_Format (PyExc_RuntimeError,
                    "Unrecognized frameType in get_frame_info: %d",
                    frame_info.frameType);
      return NULL;
    }

  return Py_BuildValue ("{s:I,s:I,s:O,s:O,s:O,s:O,s:K}",
                        "block_size", block_size,
                        "block_size_id", block_size_id,
                        "block_linked", block_linked ? Py_True : Py_False,
                        "content_checksum", content_checksum ? Py_True : Py_False,
                        "block_checksum", block_checksum ? Py_True : Py_False,
                        "skippable", skippable ? Py_True : Py_False,
                        "content_size", frame_info.contentSize);
}

/********************************
 * create_decompression_context *
 ********************************/
static void
destroy_decompression_context (PyObject * py_context)
{
#ifndef PyCapsule_Type
  LZ4F_dctx * context =
    PyCapsule_GetPointer (py_context, decompression_context_capsule_name);
#else
  /* Compatibility with 2.6 via capsulethunk. */
  LZ4F_dctx * context =  py_context;
#endif
  Py_BEGIN_ALLOW_THREADS
  LZ4F_freeDecompressionContext (context);
  Py_END_ALLOW_THREADS
}

static PyObject *
create_decompression_context (PyObject * Py_UNUSED (self))
{
  LZ4F_dctx * context;
  LZ4F_errorCode_t result;

  Py_BEGIN_ALLOW_THREADS
  result = LZ4F_createDecompressionContext (&context, LZ4F_VERSION);
  if (LZ4F_isError (result))
    {
      Py_BLOCK_THREADS
      LZ4F_freeDecompressionContext (context);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_createDecompressionContext failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }
  Py_END_ALLOW_THREADS

  return PyCapsule_New (context, decompression_context_capsule_name,
                        destroy_decompression_context);
}

/*******************************
 * reset_decompression_context *
 *******************************/
static PyObject *
reset_decompression_context (PyObject * Py_UNUSED (self), PyObject * args,
                             PyObject * keywds)
{
  LZ4F_dctx * context;
  PyObject * py_context = NULL;
  static char *kwlist[] = { "context",
                            NULL
  };

  if (!PyArg_ParseTupleAndKeywords (args, keywds, "O", kwlist,
                                    &py_context
                                    ))
    {
      return NULL;
    }

  context = (LZ4F_dctx *)
    PyCapsule_GetPointer (py_context, decompression_context_capsule_name);

  if (!context)
    {
      PyErr_SetString (PyExc_ValueError,
                       "No valid decompression context supplied");
      return NULL;
    }

  if (LZ4_versionNumber() >= 10800) /* LZ4 >= v1.8.0 has LZ4F_resetDecompressionContext */
    {
      /* No error checking possible here - this is always successful. */
      Py_BEGIN_ALLOW_THREADS
      LZ4F_resetDecompressionContext (context);
      Py_END_ALLOW_THREADS
    }
  else
    {
      /* No resetDecompressionContext available, so we'll destroy the context
         and create a new one. */
      int result;

      Py_BEGIN_ALLOW_THREADS
      LZ4F_freeDecompressionContext (context);

      result = LZ4F_createDecompressionContext (&context, LZ4F_VERSION);
      if (LZ4F_isError (result))
        {
          LZ4F_freeDecompressionContext (context);
          Py_BLOCK_THREADS
          PyErr_Format (PyExc_RuntimeError,
                        "LZ4F_createDecompressionContext failed with code: %s",
                        LZ4F_getErrorName (result));
          return NULL;
        }
      Py_END_ALLOW_THREADS

      result = PyCapsule_SetPointer(py_context, context);
      if (result)
        {
          LZ4F_freeDecompressionContext (context);
          PyErr_SetString (PyExc_RuntimeError,
                           "PyCapsule_SetPointer failed with code: %s");
          return NULL;
        }
    }

  Py_RETURN_NONE;
}

static inline PyObject *
__decompress(LZ4F_dctx * context, char * source, size_t source_size,
             Py_ssize_t max_length, int full_frame,
             int return_bytearray, int return_bytes_read)
{
  size_t source_remain;
  size_t source_read;
  char * source_cursor;
  char * source_end;
  char * destination;
  size_t destination_write;
  char * destination_cursor;
  size_t destination_written;
  size_t destination_size;
  PyObject * py_destination;
  size_t result = 0;
  LZ4F_frameInfo_t frame_info;
  LZ4F_decompressOptions_t options;
  int end_of_frame = 0;

  memset(&options, 0, sizeof options);

  Py_BEGIN_ALLOW_THREADS

  source_cursor = source;
  source_end = source + source_size;
  source_remain = source_size;

  if (full_frame)
    {
      source_read = source_size;

      result =
        LZ4F_getFrameInfo (context, &frame_info,
                           source_cursor, &source_read);

      if (LZ4F_isError (result))
        {
          Py_BLOCK_THREADS
          PyErr_Format (PyExc_RuntimeError,
                        "LZ4F_getFrameInfo failed with code: %s",
                        LZ4F_getErrorName (result));
          return NULL;
        }

      /* Advance the source_cursor pointer past the header - the call to
         getFrameInfo above replaces the passed source_read value with the
         number of bytes read. Also reduce source_remain accordingly. */
      source_cursor += source_read;
      source_remain -= source_read;

      /* If the uncompressed content size is available, we'll use that to size
         the destination buffer. Otherwise, guess at twice the remaining source
         source as a starting point, and adjust if needed. */
      if (frame_info.contentSize > 0)
        {
          destination_size = frame_info.contentSize;
        }
      else
        {
          destination_size = 2 * source_remain;
        }
    }
  else
    {
      if (max_length >= (Py_ssize_t) 0)
        {
          destination_size = (size_t) max_length;
        }
      else
        {
          /* Choose an initial destination size as twice the source size, and we'll
             grow the allocation as needed. */
          destination_size = 2 * source_remain;
        }
    }

  Py_BLOCK_THREADS

  destination = PyMem_Malloc (destination_size * sizeof * destination);
  if (destination == NULL)
    {
      return PyErr_NoMemory();
    }

  Py_UNBLOCK_THREADS

  /* Only set stableDst = 1 if we are sure no PyMem_Realloc will be called since
     when stableDst = 1 the LZ4 library stores a pointer to the last compressed
     data, which may be invalid after a PyMem_Realloc. */
  if (full_frame && max_length >= (Py_ssize_t) 0)
    {
      options.stableDst = 1;
    }
  else
    {
      options.stableDst = 0;
    }

  source_read = source_remain;

  destination_write = destination_size;
  destination_cursor = destination;
  destination_written = 0;

  while (1)
    {
      /* Decompress from the source string and write to the destination
         until there's no more source string to read, or until we've reached the
         frame end.

         On calling LZ4F_decompress, source_read is set to the remaining length
         of source available to read. On return, source_read is set to the
         actual number of bytes read from source, which may be less than
         available. NB: LZ4F_decompress does not explicitly fail on empty input.

         On calling LZ4F_decompress, destination_write is the number of bytes in
         destination available for writing. On exit, destination_write is set to
         the actual number of bytes written to destination. */
      result = LZ4F_decompress (context,
                                destination_cursor,
                                &destination_write,
                                source_cursor,
                                &source_read,
                                &options);

      if (LZ4F_isError (result))
        {
          Py_BLOCK_THREADS
          PyErr_Format (PyExc_RuntimeError,
                        "LZ4F_decompress failed with code: %s",
                        LZ4F_getErrorName (result));
          return NULL;
        }

      destination_written += destination_write;
      source_cursor += source_read;
      source_read = source_end - source_cursor;

      if (result == 0)
        {
          /* We've reached the end of the frame. */
          end_of_frame = 1;
          break;
        }
      else if (source_cursor == source_end)
        {
          /* We've reached end of input. */
          break;
        }
      else if (destination_written == destination_size)
        {
          /* Destination buffer is full. So, stop decompressing if
             max_length is set. Otherwise expand the destination
             buffer. */
          if (max_length >= (Py_ssize_t) 0)
            {
              break;
            }
          else
            {
              /* Expand destination buffer. result is an indication of number of
                 source bytes remaining, so we'll use this to estimate the new
                 size of the destination buffer. */
              char * buff;
              destination_size += 3 * result;

              Py_BLOCK_THREADS
              buff = PyMem_Realloc (destination, destination_size);
              if (buff == NULL)
                {
                  PyErr_SetString (PyExc_RuntimeError,
                                   "Failed to resize buffer");
                  return NULL;
                }
              else
                {
                  destination = buff;
                }
              Py_UNBLOCK_THREADS
            }
        }
      /* Data still remaining to be decompressed, so increment the destination
         cursor location, and reset destination_write ready for the next
         iteration. Important to re-initialize destination_cursor here (as
         opposed to simply incrementing it) so we're pointing to the realloc'd
         memory location. */
      destination_cursor = destination + destination_written;
      destination_write = destination_size - destination_written;
    }

  Py_END_ALLOW_THREADS

  if (result > 0 && full_frame)
    {
      PyErr_Format (PyExc_RuntimeError,
                    "Frame incomplete. LZ4F_decompress returned: %zu", result);
      PyMem_Free (destination);
      return NULL;
    }

  if (LZ4F_isError (result))
    {
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_freeDecompressionContext failed with code: %s",
                    LZ4F_getErrorName (result));
      PyMem_Free (destination);
      return NULL;
    }

  if (return_bytearray)
    {
      py_destination = PyByteArray_FromStringAndSize (destination, (Py_ssize_t) destination_written);
    }
  else
    {
      py_destination = PyBytes_FromStringAndSize (destination, (Py_ssize_t) destination_written);
    }

  PyMem_Free (destination);

  if (py_destination == NULL)
    {
      return PyErr_NoMemory ();
    }

  if (full_frame)
    {
      if (return_bytes_read)
        {
          return Py_BuildValue ("Ni",
                                py_destination,
                                source_cursor - source);
        }
      else
        {
          return py_destination;
        }
    }
  else
    {
      return Py_BuildValue ("NiO",
                            py_destination,
                            source_cursor - source,
                            end_of_frame ? Py_True : Py_False);
    }
}

/**************
 * decompress *
 **************/
static PyObject *
decompress (PyObject * Py_UNUSED (self), PyObject * args,
            PyObject * keywds)
{
  LZ4F_dctx * context;
  LZ4F_errorCode_t result;
  Py_buffer py_source;
  char * source;
  size_t source_size;
  PyObject * ret;
  int return_bytearray = 0;
  int return_bytes_read = 0;
  static char *kwlist[] = { "data",
                            "return_bytearray",
                            "return_bytes_read",
                            NULL
                          };

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "y*|pp", kwlist,
                                    &py_source,
                                    &return_bytearray,
                                    &return_bytes_read
                                    ))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "s*|ii", kwlist,
                                    &py_source,
                                    &return_bytearray,
                                    &return_bytes_read
                                    ))
    {
      return NULL;
    }
#endif

  Py_BEGIN_ALLOW_THREADS
  result = LZ4F_createDecompressionContext (&context, LZ4F_VERSION);
  if (LZ4F_isError (result))
    {
      LZ4F_freeDecompressionContext (context);
      Py_BLOCK_THREADS
      PyBuffer_Release(&py_source);
      PyErr_Format (PyExc_RuntimeError,
                    "LZ4F_createDecompressionContext failed with code: %s",
                    LZ4F_getErrorName (result));
      return NULL;
    }
  Py_END_ALLOW_THREADS

  /* MSVC can't do pointer arithmetic on void * pointers, so cast to char * */
  source = (char *) py_source.buf;
  source_size = py_source.len;

  ret = __decompress (context,
                      source,
                      source_size,
                      -1,
                      1,
                      return_bytearray,
                      return_bytes_read);

  PyBuffer_Release(&py_source);

  Py_BEGIN_ALLOW_THREADS
  LZ4F_freeDecompressionContext (context);
  Py_END_ALLOW_THREADS

  return ret;
}

/********************
 * decompress_chunk *
 ********************/
static PyObject *
decompress_chunk (PyObject * Py_UNUSED (self), PyObject * args,
                  PyObject * keywds)
{
  PyObject * py_context = NULL;
  PyObject * ret;
  LZ4F_dctx * context;
  Py_buffer py_source;
  char * source;
  size_t source_size;
  Py_ssize_t max_length = (Py_ssize_t) -1;
  int return_bytearray = 0;
  static char *kwlist[] = { "context",
                            "data",
                            "max_length",
                            "return_bytearray",
                            NULL
                          };

#if IS_PY3
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "Oy*|np", kwlist,
                                    &py_context,
                                    &py_source,
                                    &max_length,
                                    &return_bytearray
                                    ))
    {
      return NULL;
    }
#else
  if (!PyArg_ParseTupleAndKeywords (args, keywds, "Os*|ni", kwlist,
                                    &py_context,
                                    &py_source,
                                    &max_length,
                                    &return_bytearray
                                    ))
    {
      return NULL;
    }
#endif

  context = (LZ4F_dctx *)
    PyCapsule_GetPointer (py_context, decompression_context_capsule_name);

  if (!context)
    {
      PyBuffer_Release(&py_source);
      PyErr_SetString (PyExc_ValueError,
                       "No valid decompression context supplied");
      return NULL;
    }

  /* MSVC can't do pointer arithmetic on void * pointers, so cast to char * */
  source = (char *) py_source.buf;
  source_size = py_source.len;

  ret = __decompress (context,
                      source,
                      source_size,
                      max_length,
                      0,
                      return_bytearray,
                      0);

  PyBuffer_Release(&py_source);

  return ret;
}

PyDoc_STRVAR(
 create_compression_context__doc,
 "create_compression_context()\n"                                       \
 "\n"                                                                   \
 "Creates a compression context object.\n"                              \
 "\n"                                                                   \
 "The compression object is required for compression operations.\n"     \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    cCtx: A compression context\n"
 );

#define COMPRESS_KWARGS_DOCSTRING                                       \
  "    block_size (int): Sepcifies the maximum blocksize to use.\n"     \
  "        Options:\n\n"                                                \
  "        - `lz4.frame.BLOCKSIZE_DEFAULT`: the lz4 library default\n" \
  "        - `lz4.frame.BLOCKSIZE_MAX64KB`: 64 kB\n"             \
  "        - `lz4.frame.BLOCKSIZE_MAX256KB`: 256 kB\n"           \
  "        - `lz4.frame.BLOCKSIZE_MAX1MB`: 1 MB\n"               \
  "        - `lz4.frame.BLOCKSIZE_MAX4MB`: 4 MB\n\n"             \
  "        If unspecified, will default to `lz4.frame.BLOCKSIZE_DEFAULT`\n" \
  "        which is currently equal to `lz4.frame.BLOCKSIZE_MAX64KB`.\n" \
  "    block_linked (bool): Specifies whether to use block-linked\n"    \
  "        compression. If ``True``, the compression ratio is improved,\n" \
  "        particularly for small block sizes. Default is ``True``.\n"  \
  "    compression_level (int): Specifies the level of compression used.\n" \
  "        Values between 0-16 are valid, with 0 (default) being the\n"     \
  "        lowest compression (0-2 are the same value), and 16 the highest.\n" \
  "        Values below 0 will enable \"fast acceleration\", proportional\n" \
  "        to the value. Values above 16 will be treated as 16.\n"      \
  "        The following module constants are provided as a convenience:\n\n" \
  "        - `lz4.frame.COMPRESSIONLEVEL_MIN`: Minimum compression (0, the\n" \
  "          default)\n"                                                \
  "        - `lz4.frame.COMPRESSIONLEVEL_MINHC`: Minimum high-compression\n" \
  "          mode (3)\n"                                                \
  "        - `lz4.frame.COMPRESSIONLEVEL_MAX`: Maximum compression (16)\n\n" \
  "    content_checksum (bool): Specifies whether to enable checksumming\n" \
  "        of the uncompressed content. If True, a checksum is stored at the\n" \
  "        end of the frame, and checked during decompression. Default is\n" \
  "        ``False``.\n"                                                    \
  "    block_checksum (bool): Specifies whether to enable checksumming of\n" \
  "        the uncompressed content of each block. If `True` a checksum of\n" \
  "        the uncompressed data in each block in the frame is stored at\n\n" \
  "        the end of each block. If present, these checksums will be used\n\n" \
  "        to validate the data during decompression. The default is\n" \
  "        ``False`` meaning block checksums are not calculated and stored.\n" \
  "        This functionality is only supported if the underlying LZ4\n" \
  "        library has version >= 1.8.0. Attempting to set this value\n" \
  "        to ``True`` with a version of LZ4 < 1.8.0 will cause a\n"    \
  "        ``RuntimeError`` to be raised.\n"                            \
  "    return_bytearray (bool): If ``True`` a ``bytearray`` object will be\n" \
  "        returned. If ``False``, a string of bytes is returned. The default\n" \
  "        is ``False``.\n" \

PyDoc_STRVAR(
 compress__doc,
 "compress(data, compression_level=0, block_size=0, content_checksum=0,\n" \
 "block_linked=True, store_size=True, return_bytearray=False)\n"        \
 "\n"                                                                   \
 "Compresses ``data`` returning the compressed data as a complete frame.\n" \
 "\n"                                                                   \
 "The returned data includes a header and endmark and so is suitable\n" \
 "for writing to a file.\n"                                           \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    data (str, bytes or buffer-compatible object): data to compress\n" \
 "\n"                                                                   \
 "Keyword Args:\n"                                                      \
 COMPRESS_KWARGS_DOCSTRING                                              \
 "    store_size (bool): If ``True`` then the frame will include an 8-byte\n" \
 "        header field that is the uncompressed size of data included\n" \
 "        within the frame. Default is ``True``.\n"                     \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    bytes or bytearray: Compressed data\n"
 );
PyDoc_STRVAR
(
 compress_begin__doc,
 "compress_begin(context, source_size=0, compression_level=0, block_size=0,\n" \
 "content_checksum=0, content_size=1, block_mode=0, frame_type=0,\n"    \
 "auto_flush=1)\n"                                                      \
 "\n"                                                                   \
 "Creates a frame header from a compression context.\n\n"               \
 "Args:\n"                                                              \
 "    context (cCtx): A compression context.\n\n"                       \
 "Keyword Args:\n"                                                      \
 COMPRESS_KWARGS_DOCSTRING                                              \
 "    auto_flush (bool): Enable or disable autoFlush. When autoFlush is disabled\n"                \
 "         the LZ4 library may buffer data internally until a block is full.\n" \
 "         Default is ``False`` (autoFlush disabled).\n\n" \
 "    source_size (int): This optionally specifies the uncompressed size\n" \
 "        of the data to be compressed. If specified, the size will be stored\n" \
 "        in the frame header for use during decompression. Default is ``True``\n"   \
 "    return_bytearray (bool): If ``True`` a bytearray object will be returned.\n" \
 "        If ``False``, a string of bytes is returned. Default is ``False``.\n\n" \
 "Returns:\n"                                                           \
 "    bytes or bytearray: Frame header.\n"
 );

#undef COMPRESS_KWARGS_DOCSTRING

PyDoc_STRVAR
(
 compress_chunk__doc,
 "compress_chunk(context, data)\n"                                      \
 "\n"                                                                   \
 "Compresses blocks of data and returns the compressed data.\n"         \
 "\n"                                                                   \
 "The returned data should be concatenated with the data returned from\n" \
 "`lz4.frame.compress_begin` and any subsequent calls to\n"             \
 "`lz4.frame.compress_chunk`.\n"                                        \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    context (cCtx): compression context\n"                            \
 "    data (str, bytes or buffer-compatible object): data to compress\n" \
 "\n"                                                                   \
 "Keyword Args:\n"                                                      \
 "    return_bytearray (bool): If ``True`` a bytearray object will be\n" \
 "        returned. If ``False``, a string of bytes is returned. The\n" \
 "        default is False.\n"                                          \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    bytes or bytearray: Compressed data.\n\n"                          \
 "Notes:\n"                                                             \
 "    If auto flush is disabled (``auto_flush=False`` when calling\n" \
 "    `lz4.frame.compress_begin`) this function may buffer and retain\n" \
 "    some or all  of the compressed data for future calls to\n"        \
 "    `lz4.frame.compress`.\n"
 );

PyDoc_STRVAR
(
 compress_flush__doc,
 "compress_flush(context, end_frame=True, return_bytearray=False)\n"    \
 "\n"                                                                   \
 "Flushes any buffered data held in the compression context.\n" \
 "\n"                                                                   \
 "This flushes any data buffed in the compression context, returning it as\n" \
 "compressed data. The returned data should be appended to the output of\n" \
 "previous calls to ``lz4.frame.compress_chunk``.\n" \
 "\n"                                                                   \
 "The ``end_frame`` argument specifies whether or not the frame should be\n" \
 "ended. If this is ``True`` and end of frame marker will be appended to\n" \
 "the returned data. In this case, if ``content_checksum`` was ``True``\n" \
 "when calling `lz4.frame.compress_begin`, then a checksum of the uncompressed\n" \
 "data will also be included in the returned data.\n"                   \
 "\n"                                                                   \
 "If the ``end_frame`` argument is ``True``, the compression context will be\n" \
 "reset and can be re-used.\n"                                          \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    context (cCtx): Compression context\n"                            \
 "\n"                                                                   \
 "Keyword Args:\n"                                                      \
 "    end_frame (bool): If ``True`` the frame will be ended. Default is\n" \
 "        ``True``.\n"                                                  \
 "    return_bytearray (bool): If ``True`` a ``bytearray`` object will\n" \
 "        be returned. If ``False``, a ``bytes`` object is returned.\n" \
 "        The default is ``False``.\n"                                  \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    bytes or bytearray: compressed data.\n"                           \
 "\n"                                                                   \
 "Notes:\n"                                                             \
 "    If ``end_frame`` is ``False`` but the underlying LZ4 library does not" \
 "    support flushing without ending the frame, a ``RuntimeError`` will be\n" \
 "    raised.\n"
 );

PyDoc_STRVAR
(
 get_frame_info__doc,
 "get_frame_info(frame)\n\n"                                            \
 "Given a frame of compressed data, returns information about the frame.\n" \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    frame (str, bytes or buffer-compatible object): LZ4 compressed frame\n" \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    dict: Dictionary with keys:\n"                                    \
 "\n"                                                                   \
 "    - ``block_size`` (int): the maximum size (in bytes) of each block\n" \
 "    - ``block_size_id`` (int): identifier for maximum block size\n"   \
 "    - ``content_checksum`` (bool): specifies whether the frame\n"     \
 "        contains a checksum of the uncompressed content\n"            \
 "    - ``content_size`` (int): uncompressed size in bytes of\n"        \
 "      frame content\n"                                                \
 "    - ``block_linked`` (bool): specifies whether the frame contains\n" \
 "      blocks which are independently compressed (``False``) or linked\n" \
 "      linked (``True``)\n"                                            \
 "    - ``block_checksum`` (bool): specifies whether each block contains a\n" \
 "      checksum of its contents\n"                                     \
 "    - ``skippable`` (bool): whether the block is skippable (``True``) or\n" \
 "      not (``False``)\n"
 );

PyDoc_STRVAR
(
 create_decompression_context__doc,
 "create_decompression_context()\n"                                     \
 "\n"                                                                   \
 "Creates a decompression context object.\n"                            \
 "\n"                                                                   \
 "A decompression context is needed for decompression operations.\n"    \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    dCtx: A decompression context\n"
 );

PyDoc_STRVAR
(
 reset_decompression_context__doc,
 "reset_decompression_context(context)\n"                               \
 "\n"                                                                   \
 "Resets a decompression context object.\n" \
 "\n" \
 "This is useful for recovering from an error or for stopping an unfinished\n" \
 "decompression and starting a new one with the same context\n"         \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    context (dCtx): A decompression context\n"
 );

PyDoc_STRVAR
(
 decompress__doc,
 "decompress(data, return_bytearray=False, return_bytes_read=False)\n"  \
 "\n"                                                                   \
 "Decompresses a frame of data and returns it as a string of bytes.\n"  \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    data (str, bytes or buffer-compatible object): data to decompress.\n" \
 "       This should contain a complete LZ4 frame of compressed data.\n" \
 "\n"                                                                   \
 "Keyword Args:\n"                                                      \
 "    return_bytearray (bool): If ``True`` a bytearray object will be\n" \
 "        returned. If ``False``, a string of bytes is returned. The\n" \
 "        default is ``False``.\n"                                      \
 "    return_bytes_read (bool): If ``True`` then the number of bytes read\n" \
 "        from ``data`` will also be returned. Default is ``False``\n"  \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    bytes/bytearray or tuple: Uncompressed data and optionally the number" \
 "        of bytes read\n"                                              \
 "\n"                                                                   \
 "    If the ``return_bytes_read`` argument is ``True`` this function\n" \
 "    returns a tuple consisting of:\n"                                 \
 "\n"                                                                   \
 "    - bytes or bytearray: Uncompressed data\n"                        \
 "    - int: Number of bytes consumed from ``data``\n"
 );

PyDoc_STRVAR
(
 decompress_chunk__doc,
 "decompress_chunk(context, data, max_length=-1)\n"                     \
 "\n"                                                                   \
 "Decompresses part of a frame of compressed data.\n"                   \
 "\n"                                                                   \
 "The returned uncompressed data should be concatenated with the data\n" \
 "returned from previous calls to `lz4.frame.decompress_chunk`\n"       \
 "\n"                                                                   \
 "Args:\n"                                                              \
 "    context (dCtx): decompression context\n"                          \
 "    data (str, bytes or buffer-compatible object): part of a LZ4\n"   \
 "        frame of compressed data\n"                                   \
 "\n"                                                                   \
 "Keyword Args:\n"                                                      \
 "    max_length (int): if non-negative this specifies the maximum number\n" \
 "         of bytes of uncompressed data to return. Default is ``-1``.\n" \
 "    return_bytearray (bool): If ``True`` a bytearray object will be\n" \
 "        returned.If ``False``, a string of bytes is returned. The\n"  \
 "        default is ``False``.\n"                                      \
 "\n"                                                                   \
 "Returns:\n"                                                           \
 "    tuple: uncompressed data, bytes read, end of frame indicator\n"   \
 "\n"                                                                   \
 "    This function returns a tuple consisting of:\n"                   \
 "\n"                                                                   \
 "    - The uncompressed data as a ``bytes`` or ``bytearray`` object\n" \
 "    - The number of bytes consumed from input ``data`` as an ``int``\n" \
 "    - The end of frame indicator as a ``bool``.\n"                    \
 "\n"
 "The end of frame indicator is ``True`` if the end of the compressed\n" \
 "frame has been reached, or ``False`` otherwise\n"
  );

static PyMethodDef module_methods[] =
{
  {
    "create_compression_context", (PyCFunction) create_compression_context,
    METH_NOARGS, create_compression_context__doc
  },
  {
    "compress", (PyCFunction) compress,
    METH_VARARGS | METH_KEYWORDS, compress__doc
  },
  {
    "compress_begin", (PyCFunction) compress_begin,
    METH_VARARGS | METH_KEYWORDS, compress_begin__doc
  },
  {
    "compress_chunk", (PyCFunction) compress_chunk,
    METH_VARARGS | METH_KEYWORDS, compress_chunk__doc
  },
  {
    "compress_flush", (PyCFunction) compress_flush,
    METH_VARARGS | METH_KEYWORDS, compress_flush__doc
  },
  {
    "get_frame_info", (PyCFunction) get_frame_info,
    METH_VARARGS | METH_KEYWORDS, get_frame_info__doc
  },
  {
    "create_decompression_context", (PyCFunction) create_decompression_context,
    METH_NOARGS, create_decompression_context__doc
  },
  {
    "reset_decompression_context", (PyCFunction) reset_decompression_context,
    METH_VARARGS | METH_KEYWORDS, reset_decompression_context__doc
  },
  {
    "decompress", (PyCFunction) decompress,
    METH_VARARGS | METH_KEYWORDS, decompress__doc
  },
  {
    "decompress_chunk", (PyCFunction) decompress_chunk,
    METH_VARARGS | METH_KEYWORDS, decompress_chunk__doc
  },
  {NULL, NULL, 0, NULL}		/* Sentinel */
};

PyDoc_STRVAR(lz4frame__doc,
             "A Python wrapper for the LZ4 frame protocol"
             );

static struct PyModuleDef moduledef =
{
  PyModuleDef_HEAD_INIT,
  "_frame",
  lz4frame__doc,
  -1,
  module_methods
};

MODULE_INIT_FUNC (_frame)
{
  PyObject *module = PyModule_Create (&moduledef);

  if (module == NULL)
    return NULL;

  PyModule_AddIntConstant (module, "BLOCKSIZE_DEFAULT", LZ4F_default);
  PyModule_AddIntConstant (module, "BLOCKSIZE_MAX64KB", LZ4F_max64KB);
  PyModule_AddIntConstant (module, "BLOCKSIZE_MAX256KB", LZ4F_max256KB);
  PyModule_AddIntConstant (module, "BLOCKSIZE_MAX1MB", LZ4F_max1MB);
  PyModule_AddIntConstant (module, "BLOCKSIZE_MAX4MB", LZ4F_max4MB);

  return module;
}

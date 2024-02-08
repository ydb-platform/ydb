/*
 * Copyright (c) 2019, Samuel Martin
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
 */

#if defined(_WIN32) && defined(_MSC_VER)
#define inline __inline
#elif defined(__SUNPRO_C) || defined(__hpux) || defined(_AIX)
#define inline
#endif

#include <Python.h>

#include <stdlib.h>
#include <math.h>
#include <lz4.h>
#include <lz4hc.h>
#include <stddef.h>
#include <stdio.h>

#if defined(_WIN32) && defined(_MSC_VER) && _MSC_VER < 1600
/* MSVC 2008 and earlier lacks stdint.h */
typedef signed __int8 int8_t;
typedef signed __int16 int16_t;
typedef signed __int32 int32_t;
typedef signed __int64 int64_t;
typedef unsigned __int8 uint8_t;
typedef unsigned __int16 uint16_t;
typedef unsigned __int32 uint32_t;
typedef unsigned __int64 uint64_t;

#if !defined(UINT8_MAX)
#define UINT8_MAX 0xff
#endif
#if !defined(UINT16_MAX)
#define UINT16_MAX 0xffff
#endif
#if !defined(UINT32_MAX)
#define UINT32_MAX 0xffffffff
#endif
#if !defined(INT32_MAX)
#define INT32_MAX 0x7fffffff
#endif
#if !defined(CHAR_BIT)
#define CHAR_BIT 8
#endif

#else
/* Not MSVC, or MSVC 2010 or higher */
#include <stdint.h>
#endif /* _WIN32 && _MSC_VER && _MSC_VER < 1600 */

#define LZ4_VERSION_NUMBER_1_9_0 10900

static const char * stream_context_capsule_name = "_stream.LZ4S_ctx";

typedef enum {
  DOUBLE_BUFFER,
  RING_BUFFER,

  BUFFER_STRATEGY_COUNT /* must be the last entry */
} buffer_strategy_e;

typedef enum {
  COMPRESS,
  DECOMPRESS,
} direction_e;

typedef enum {
  DEFAULT,
  FAST,
  HIGH_COMPRESSION,
} compression_type_e;


/* Forward declarations */
static PyObject * LZ4StreamError;

#define DOUBLE_BUFFER_PAGE_COUNT (2)

#define DOUBLE_BUFFER_INDEX_MIN (0)
#define DOUBLE_BUFFER_INDEX_INVALID (-1)

#define _GET_MAX_UINT(byte_depth, type) (type)( ( 1ULL << ( CHAR_BIT * (byte_depth) ) ) - 1 )
#define _GET_MAX_UINT32(byte_depth) _GET_MAX_UINT((byte_depth), uint32_t)

typedef struct {
  char * buf;
  unsigned int len;
} buffer_t;

/* forward declaration */
typedef struct stream_context_t stream_context_t;

typedef struct {
  /**
   * Release buffer strategy's resources.
   *
   * \param[inout] context  Stream context.
   */
  void (*release_resources) (stream_context_t *context);

  /**
   * Reserve buffer strategy's resources.
   *
   * \param[inout] context      Stream context.
   * \param[in]    buffer_size  Base buffer size to allocate and initialize.
   *
   * \return 0 on success, non-0 otherwise
   */
  int (*reserve_resources) (stream_context_t * context, unsigned int buffer_size);

  /**
   * Return a pointer on the work buffer.
   *
   * \param[inout] context  Stream context.
   *
   * \return A pointer on the work buffer.
   */
  char * (*get_work_buffer) (const stream_context_t * context);

  /**
   * Return the length of (available space in) the work buffer.
   *
   * \param[inout] context  Stream context.
   *
   * \return The length of the work buffer.
   */
  unsigned int (*get_work_buffer_size) (const stream_context_t * context);

  /**
   * Return the length of the output buffer.
   *
   * \param[inout] context  Stream context.
   *
   * \return The length the output buffer.
   */
  unsigned int (*get_dest_buffer_size) (const stream_context_t * context);

  /**
   * Update the stream context at the end of the LZ4 operation (a block compression or
   * decompression).
   *
   * \param[inout] context  Stream context.
   *
   * \return 0 on success, non-0 otherwise
   */
  int  (*update_context_after_process) (stream_context_t * context);
} strategy_ops_t;

struct stream_context_t {
  /* Buffer strategy resources */
  struct {
    strategy_ops_t * ops;
    union {
      /* Double-buffer */
      struct {
        char * buf;
        unsigned int page_size;
        char * pages[DOUBLE_BUFFER_PAGE_COUNT];
        int index;
      } double_buffer;

      /* Ring-buffer (not implemented) */
      struct {
        char * buf;
        unsigned int size;
      } ring_buffer;
    } data;
  } strategy;

  buffer_t output;

  /* LZ4 state */
  union {
    union {
      LZ4_stream_t * fast;
      LZ4_streamHC_t * hc;
    } compress;
    LZ4_streamDecode_t * decompress;
    void * context;
  } lz4_state;

  /* LZ4 configuration */
  struct {
    int acceleration;
    int compression_level;
    int store_comp_size;
    int return_bytearray;
    direction_e direction;
    compression_type_e comp;
  } config;
};


#ifndef PyCapsule_Type
#define _PyCapsule_get_context(py_ctx) \
  ((stream_context_t *) PyCapsule_GetPointer((py_ctx), stream_context_capsule_name))
#else
/* Compatibility with 2.6 via capsulethunk. */
#define _PyCapsule_get_context(py_ctx) \
  ((stream_context_t *) (py_ctx))
#endif


static inline void
store_le8 (char * c, uint8_t x)
{
  c[0] = x & 0xff;
}


/*************************
 * block content helpers *
 *************************/
static inline uint8_t
load_le8 (const char * c)
{
  const uint8_t * d = (const uint8_t *) c;
  return d[0];
}

static inline void
store_le16 (char * c, uint16_t x)
{
  c[0] = x & 0xff;
  c[1] = (x >> 8) & 0xff;
}

static inline uint16_t
load_le16 (const char * c)
{
  const uint8_t * d = (const uint8_t *) c;
  return (d[0] | (d[1] << 8));
}

static inline void
store_le32 (char * c, uint32_t x)
{
  c[0] = x & 0xff;
  c[1] = (x >> 8) & 0xff;
  c[2] = (x >> 16) & 0xff;
  c[3] = (x >> 24) & 0xff;
}

static inline uint32_t
load_le32 (const char * c)
{
  const uint8_t * d = (const uint8_t *) c;
  return (d[0] | (d[1] << 8) | (d[2] << 16) | (d[3] << 24));
}

static inline int
load_block_length (int block_length_size, const char *buf)
{
  int block_length = -1;
  switch (block_length_size)
    {
      case 1:
        block_length = load_le8 (buf);
        break;

      case 2:
        block_length = load_le16 (buf);
        break;

      case 4:
        block_length = load_le32 (buf);
        break;

      case 0:
        /* fallthrough */
      default:
        break;
    }

  return block_length;
}

static inline int
store_block_length (int block_length, int block_length_size, char * buf)
{
  int status = 1;

  switch (block_length_size)
    {
      case 0: /* do nothing */
        break;

      case 1:
        {
          if (block_length > UINT8_MAX)
            {
              status = 0;
              break;
            }
          store_le8 (buf, (uint8_t) (block_length & UINT8_MAX));
        }
        break;

      case 2:
        {
          if (block_length > UINT16_MAX)
            {
              status = 0;
              break;
            }
          store_le16 (buf, (uint16_t) (block_length & UINT16_MAX));
        }
        break;

      case 4:
        {
          if (block_length > INT32_MAX)
            {
              status = 0;
              break;
            }
          store_le32 (buf, (uint32_t) (block_length & UINT32_MAX));
        }
        break;

      default: /* unsupported cases */
        status = 0;
        break;
    }

  if (status != 1)
    {
      PyErr_SetString (LZ4StreamError, "Compressed stream size too large");
    }

  return status;
}


/***************
 * LZ4 helpers *
 ***************/
static inline uint32_t
get_compress_bound(uint32_t input_size)
{
  /* result of LZ4_compressBound is null or positive */
  return (uint32_t)LZ4_compressBound(input_size);
}


static inline uint32_t
get_input_bound(uint32_t compress_max_size)
{
  uint64_t isize = 0;
  uint64_t csize = (uint64_t) compress_max_size;

  /*  Reversing the LZ4_COMPRESSBOUND macro gives:
   *    isize = ((csize - 16) * 255) / 256
   *          = (((csize - 16) * 256) - (csize - 16)) / 256
   *          = ((csize * 256) - (16 * 256) - (csize - 16)) / 256
   *          = ((csize << 8) - (16 << 8) - csize + 16) >> 8
   *          = ((csize << 8) - csize + 16 - (16 << 8)) >> 8
   *          = ((csize << 8) - csize - 4080) >> 8
   *
   *  Notes:
   *  - Using 64-bit long integer for intermediate computation to avoid any
   *    truncation when shifting left large csize values.
   *  - Due to the round integer approximation, running the following
   *    calculation can give a non-null result:
   *      result = n - _LZ4_inputBound( _LZ4_compressBound( n ) )
   *    but in all cases, this difference is between 0 and 1.
   *    Thus, the valid maximal input size returned by this funtcion is
   *    incremented by 1 to avoid any buffer overflow in case of decompression
   *    in a dynamically allocated buffer.
   *  - For small compressed length (shorter than 16 bytes), make sure a
   *    non-null size is returned.
   */
  if (csize < 16)
    {
      csize = 17;
    }

  if (csize <= get_compress_bound (LZ4_MAX_INPUT_SIZE))
    {
      isize = ((csize << 8) - csize - 4080) >> 8;

      if (isize > (uint32_t)LZ4_MAX_INPUT_SIZE)
        {
          isize = 0;
        }
      else
        {
          isize += 1;
        }
    }

  return (uint32_t)(isize & UINT32_MAX);
}


/**************************************
 * LZ4 version-compatibility wrappers *
 **************************************/
#if defined (__GNUC__)
/* Runtime detection of the support of new functions in the LZ4 API
 * (old functions remain available but are deprecated and will trigger
 * compilation warnings.
 *
 * Declare weak symbols on the required functions provided by recent versions
 * of the library.
 * This way, these symbols will always be available - NULL if the (old version
 * of the) library does not define them.
 */

/* Function introduced in LZ4 >= 1.9.0 */
__attribute__ ((weak)) void
LZ4_resetStreamHC_fast (LZ4_streamHC_t* streamHCPtr, int compressionLevel);

/* Function introduced in LZ4 >= 1.8.2 */
__attribute__ ((weak)) void
LZ4_resetStream_fast (LZ4_stream_t* streamPtr);

#else
/* Assuming the bundled LZ4 library sources are always used, so meet the
 * LZ4 minimal version requirements.
 */
#endif

static inline void reset_stream (LZ4_stream_t* streamPtr)
{
  if (LZ4_versionNumber () >= LZ4_VERSION_NUMBER_1_9_0)
    {
      if (LZ4_resetStream_fast)
        {
          LZ4_resetStream_fast (streamPtr);
        }
      else
        {
          PyErr_SetString (PyExc_RuntimeError,
                           "Inconsistent LZ4 library version/available APIs");
        }
    }
  else
    {
      LZ4_resetStream (streamPtr);
    }
}


static inline void reset_stream_hc (LZ4_streamHC_t* streamHCPtr, int compressionLevel)
{
  if (LZ4_versionNumber () >= LZ4_VERSION_NUMBER_1_9_0)
    {
      if (LZ4_resetStreamHC_fast)
        {
          LZ4_resetStreamHC_fast (streamHCPtr, compressionLevel);
        }
      else
        {
          PyErr_SetString (PyExc_RuntimeError,
                           "Inconsistent LZ4 library version/available APIs");
        }
    }
  else
    {
      LZ4_resetStreamHC (streamHCPtr, compressionLevel);
    }
}


/*************************
 * Double-buffer helpers *
 *************************/
static int
double_buffer_update_index (stream_context_t * context)
{
#if DOUBLE_BUFFER_PAGE_COUNT != 2
#error "DOUBLE_BUFFER_PAGE_COUNT must be 2."
#endif /* DOUBLE_BUFFER_PAGE_COUNT != 2 */
  context->strategy.data.double_buffer.index = (context->strategy.data.double_buffer.index + 1) & 0x1; /* modulo 2 */

  return 0;
}

static char *
double_buffer_get_compression_page (const stream_context_t * context)
{
  return context->strategy.data.double_buffer.pages[context->strategy.data.double_buffer.index];
}

static void
double_buffer_release_resources (stream_context_t * context)
{
  unsigned int i;
  for (i = DOUBLE_BUFFER_INDEX_MIN;
       i < (DOUBLE_BUFFER_INDEX_MIN + DOUBLE_BUFFER_PAGE_COUNT);
       ++i)
    {
      context->strategy.data.double_buffer.pages[i] = NULL;
    }

  if (context->strategy.data.double_buffer.buf != NULL)
    {
      PyMem_Free (context->strategy.data.double_buffer.buf);
    }
  context->strategy.data.double_buffer.buf = NULL;
  context->strategy.data.double_buffer.index = DOUBLE_BUFFER_INDEX_INVALID;
  context->strategy.data.double_buffer.page_size = 0;
}

static int
double_buffer_reserve_resources (stream_context_t * context, unsigned int buffer_size)
{
  int status = 0;
  unsigned int i;

  context->strategy.data.double_buffer.page_size = buffer_size;
  context->strategy.data.double_buffer.buf = PyMem_Malloc (buffer_size * DOUBLE_BUFFER_PAGE_COUNT);

  if (context->strategy.data.double_buffer.buf == NULL)
    {
      PyErr_Format (PyExc_MemoryError,
                    "Could not allocate double-buffer");
      status = -1;
      goto exit_now;
    }

  for (i = DOUBLE_BUFFER_INDEX_MIN;
       i < (DOUBLE_BUFFER_INDEX_MIN + DOUBLE_BUFFER_PAGE_COUNT);
       ++i)
    {
      context->strategy.data.double_buffer.pages[i] = context->strategy.data.double_buffer.buf +
                                                 (i * buffer_size);
    }

  context->strategy.data.double_buffer.index = DOUBLE_BUFFER_INDEX_MIN;

exit_now:
  return status;
}

static unsigned int
double_buffer_get_work_buffer_size (const stream_context_t * context)
{
  return context->strategy.data.double_buffer.page_size;
}

static unsigned int
double_buffer_get_dest_buffer_size (const stream_context_t * context)
{
  unsigned int len;

  if (context->config.direction == COMPRESS)
    {
      len = context->output.len;
    }
  else
    {
      len = context->strategy.data.double_buffer.page_size;
    }

  return len;
}


/****************************************
 * Ring-buffer helpers: Not implemented *
 ****************************************/
static void
ring_buffer_release_resources (stream_context_t * context)
{
  (void) context; /* unused */

  /* Not implemented (yet) */
  PyErr_Format (PyExc_NotImplementedError,
                "Buffer strategy not implemented: ring_buffer");

  return;
}

static int
ring_buffer_reserve_resources (stream_context_t * context, unsigned int buffer_size)
{
  (void) context; /* unused */
  (void) buffer_size; /* unused */

  /* Not implemented (yet) */
  PyErr_Format (PyExc_NotImplementedError,
                "Buffer strategy not implemented: ring_buffer");
  return -1;
}

static unsigned int
ring_buffer_get_dest_buffer_size (const stream_context_t * context)
{
  (void) context; /* unused */

  /* Not implemented (yet) */
  PyErr_Format (PyExc_NotImplementedError,
                "Buffer strategy not implemented: ring_buffer");

  return 0;
}

static unsigned int
ring_buffer_get_work_buffer_size (const stream_context_t * context)
{
  (void) context; /* unused */

  /* Not implemented (yet) */
  PyErr_Format (PyExc_NotImplementedError,
                "Buffer strategy not implemented: ring_buffer");

  return 0;
}

static char *
ring_buffer_get_buffer_position (const stream_context_t * context)
{
  (void) context; /* unused */

  /* Not implemented (yet) */
  PyErr_Format (PyExc_NotImplementedError,
                "Buffer strategy not implemented: ring_buffer");

  return NULL;
}

static int
ring_buffer_update_context (stream_context_t * context)
{
  (void) context; /* unused */

  /* Not implemented (yet) */
  PyErr_Format (PyExc_NotImplementedError,
                "Buffer strategy not implemented: ring_buffer");

  return -1;
}


/**********************
 * strategy operators *
 **********************/
static strategy_ops_t strategy_ops[BUFFER_STRATEGY_COUNT] = {
  /* [DOUBLE_BUFFER] = */
  {
    /* .release_resources             */ double_buffer_release_resources,
    /* .reserve_resources             */ double_buffer_reserve_resources,
    /* .get_work_buffer               */ double_buffer_get_compression_page,
    /* .get_work_buffer_size          */ double_buffer_get_work_buffer_size,
    /* .get_dest_buffer_size          */ double_buffer_get_dest_buffer_size,
    /* .update_context_after_process  */ double_buffer_update_index,
  },
  /* [RING_BUFFER] = */
  {
    /* .release_resources             */ ring_buffer_release_resources,
    /* .reserve_resources             */ ring_buffer_reserve_resources,
    /* .get_work_buffer               */ ring_buffer_get_buffer_position,
    /* .get_work_buffer_size          */ ring_buffer_get_work_buffer_size,
    /* .get_dest_buffer_size          */ ring_buffer_get_dest_buffer_size,
    /* .update_context_after_process  */ ring_buffer_update_context,
  },
};


/*******************
 * generic helpers *
 *******************/
static void
destroy_context (stream_context_t * context)
{
  if (context == NULL)
    {
      return;
    }

  /* Release lz4 state */
  Py_BEGIN_ALLOW_THREADS
  if (context->lz4_state.context != NULL)
    {
      if (context->config.direction == COMPRESS)
        {
          if (context->config.comp == HIGH_COMPRESSION)
            {
              LZ4_freeStreamHC (context->lz4_state.compress.hc);
            }
          else
            {
              LZ4_freeStream (context->lz4_state.compress.fast);
            }
        }
      else /* context->config.direction == DECOMPRESS */
        {
          LZ4_freeStreamDecode (context->lz4_state.decompress);
        }
    }
  Py_END_ALLOW_THREADS
  context->lz4_state.context = NULL;

  /* Release strategy resources */
  if (context->strategy.ops != NULL)
    {
      context->strategy.ops->release_resources (context);
    }
  context->strategy.ops = NULL;

  /* Release output buffer */
  if (context->output.buf != NULL)
    {
      PyMem_Free (context->output.buf);
    }
  context->output.buf = NULL;
  context->output.len = 0;

  /* Release python memory */
  PyMem_Free (context);
}

static void
destroy_py_context (PyObject * py_context)
{
  if (py_context == NULL)
    {
      return;
    }

  destroy_context (_PyCapsule_get_context (py_context));
}


/**************
 * Python API *
 **************/
static PyObject *
_create_context (PyObject * Py_UNUSED (self), PyObject * args, PyObject * kwds)
{
  stream_context_t * context = NULL;

  const char * direction = "";
  const char * strategy_name = "";
  unsigned int buffer_size;

  buffer_strategy_e strategy = BUFFER_STRATEGY_COUNT;
  const char * mode = "default";
  int acceleration = 1;
  int compression_level = 9;
  int store_comp_size = 4;
  int return_bytearray = 0;
  Py_buffer dict = { NULL, NULL, };

  int status = 0;
  unsigned int total_size = 0;
  uint32_t store_max_size;

  static char * argnames[] = {
    "strategy",
    "direction",
    "buffer_size",
    "mode",
    "acceleration",
    "compression_level",
    "return_bytearray",
    "store_comp_size",
    "dictionary",
    NULL
  };

  if (!PyArg_ParseTupleAndKeywords (args, kwds, "ssI|sIIpIz*", argnames,
                                    &strategy_name, &direction, &buffer_size,
                                    &mode, &acceleration, &compression_level, &return_bytearray,
                                    &store_comp_size, &dict))
    {
      goto abort_now;
    }

  /* Sanity checks on arguments */
  if (dict.len > INT_MAX)
    {
      PyErr_Format (PyExc_OverflowError,
                    "Dictionary too large for LZ4 API");
      goto abort_now;
    }

  /* Input max length limited to 0x7E000000 (2 113 929 216 bytes < 2GiB).
   *   https://github.com/lz4/lz4/blob/dev/lib/lz4.h#L161
   *
   * So, restrict the block length bitwise to 32 (signed 32 bit integer). */
  if ((store_comp_size != 0) && (store_comp_size != 1) &&
      (store_comp_size != 2) && (store_comp_size != 4))
    {
      PyErr_Format (PyExc_ValueError,
                    "Invalid store_comp_size, valid values: 0, 1, 2 or 4");
      goto abort_now;
    }

  context = (stream_context_t *) PyMem_Malloc (sizeof (stream_context_t));
  if (context == NULL)
    {
      PyErr_NoMemory ();
      goto abort_now;
    }

  memset (context, 0x00, sizeof (stream_context_t));

  /* Set buffer strategy */
  if (!strncmp (strategy_name, "double_buffer", sizeof ("double_buffer")))
    {
      strategy = DOUBLE_BUFFER;
    }
  else if (!strncmp (strategy_name, "ring_buffer", sizeof ("ring_buffer")))
    {
      strategy = RING_BUFFER;
    }
  else
    {
      PyErr_Format (PyExc_ValueError,
                    "Invalid strategy argument: %s. Must be one of: double_buffer, ring_buffer",
                    strategy_name);
      goto abort_now;
    }

  /* Set direction */
  if (!strncmp (direction, "compress", sizeof ("compress")))
    {
      context->config.direction = COMPRESS;
    }
  else if (!strncmp (direction, "decompress", sizeof ("decompress")))
    {
      context->config.direction = DECOMPRESS;
    }
  else
    {
      PyErr_Format (PyExc_ValueError,
                    "Invalid direction argument: %s. Must be one of: compress, decompress",
                    direction);
      goto abort_now;
    }

  /* Set compression mode */
  if (!strncmp (mode, "default", sizeof ("default")))
    {
      context->config.comp = DEFAULT;
    }
  else if (!strncmp (mode, "fast", sizeof ("fast")))
    {
      context->config.comp = FAST;
    }
  else if (!strncmp (mode, "high_compression", sizeof ("high_compression")))
    {
      context->config.comp = HIGH_COMPRESSION;
    }
  else
    {
      PyErr_Format (PyExc_ValueError,
                    "Invalid mode argument: %s. Must be one of: default, fast, high_compression",
                    mode);
      goto abort_now;
    }

  /* Initialize the output buffer
   *
   * In out-of-band block size case, use a best-effort strategy for scaling
   * buffers.
   */
  if (store_comp_size == 0)
    {
      store_max_size = _GET_MAX_UINT32(4);
    }
  else
    {
      store_max_size = _GET_MAX_UINT32(store_comp_size);
    }

  if (context->config.direction == COMPRESS)
    {
      context->output.len = get_compress_bound (buffer_size);
      total_size = context->output.len + store_comp_size;

      if (context->output.len == 0)
        {
          PyErr_Format (PyExc_ValueError,
                        "Invalid buffer_size argument: %u. Cannot define output buffer size. "
                        "Must be lesser or equal to %u",
                        buffer_size, LZ4_MAX_INPUT_SIZE);
          goto abort_now;
        }

      /* Assert the output buffer size and the store_comp_size values are consistent */
      if (context->output.len > store_max_size)
        {
          /* The maximal/"worst case" compressed data length cannot fit in the
           * store_comp_size bytes. */
          PyErr_Format (LZ4StreamError,
                        "Inconsistent buffer_size/store_comp_size values. "
                        "Maximal compressed length (%u) cannot fit in a %u byte-long integer",
                        buffer_size, store_comp_size);
          goto abort_now;
        }
    }
  else /* context->config.direction == DECOMPRESS */
    {
      if (store_max_size > LZ4_MAX_INPUT_SIZE)
        {
          store_max_size = LZ4_MAX_INPUT_SIZE;
        }

      context->output.len = buffer_size;
      total_size = context->output.len;

      /* Here we cannot assert the maximal theorical decompressed chunk length
       * will fit in one page of the double_buffer, i.e.:
       *    assert( !(double_buffer.page_size < _LZ4_inputBound(store_max_size)) )
       *
       * Doing such a check would require aligning the page_size on the maximal
       * value of the store_comp_size prefix, i.e.:
       *    page_size = 256B if store_comp_size == 1
       *    page_size = 64KB if store_comp_size == 2
       *    page_size = 4GB if store_comp_size == 4
       *
       * This constraint is too strict, so is not implemented.
       *
       * On the other hand, the compression logic tells the page size cannot be
       * larger than the maximal value fitting in store_comp_size bytes.
       * So here, the check could be checking the page_size is smaller or equal
       * to the maximal decompressed chunk length, i.e.:
       *    assert( !(double_buffer.page_size > _LZ4_inputBound(store_max_size)) )
       *
       * But this check is not really relevant and could bring other limitations.
       *
       * So, on the decompression case, no check regarding the page_size and the
       * store_comp_size values can reliably be done during the LZ4 context
       * initialization, they will be deferred in the decompression process.
       */
    }

  /* Set all remaining settings in the context */
  context->config.store_comp_size = store_comp_size;
  context->config.acceleration = acceleration;
  context->config.compression_level = compression_level;
  context->config.return_bytearray = !!return_bytearray;

  /* Set internal resources related to the buffer strategy */
  context->strategy.ops = &strategy_ops[strategy];

  status = context->strategy.ops->reserve_resources (context, buffer_size);
  if (status != 0)
    {
      /* Python exception already set in the strategy's resource creation helper */
      goto abort_now;
    }

  /* Set output buffer */
  context->output.buf = PyMem_Malloc (total_size * sizeof (* (context->output.buf)));
  if (context->output.buf == NULL)
    {
      PyErr_Format (PyExc_MemoryError,
                    "Could not allocate output buffer");
      goto abort_now;
    }

  /* Initialize lz4 state */
  if (context->config.direction == COMPRESS)
    {
      if (context->config.comp == HIGH_COMPRESSION)
        {
          context->lz4_state.compress.hc = LZ4_createStreamHC ();
          if (context->lz4_state.compress.hc == NULL)
            {
              PyErr_Format (PyExc_MemoryError,
                            "Could not create LZ4 state");
              goto abort_now;
            }

            reset_stream_hc (context->lz4_state.compress.hc, context->config.compression_level);

          if (dict.len > 0)
            {
              LZ4_loadDictHC (context->lz4_state.compress.hc, dict.buf, dict.len);
            }
        }
      else
        {
          context->lz4_state.compress.fast = LZ4_createStream ();
          if (context->lz4_state.compress.fast == NULL)
            {
              PyErr_Format (PyExc_MemoryError,
                            "Could not create LZ4 state");
              goto abort_now;
            }

          reset_stream (context->lz4_state.compress.fast);

          if (dict.len > 0)
            {
              LZ4_loadDict (context->lz4_state.compress.fast, dict.buf, dict.len);
            }
        }
    }
  else /* context->config.direction == DECOMPRESS */
    {
      context->lz4_state.decompress = LZ4_createStreamDecode ();
      if (context->lz4_state.decompress == NULL)
        {
          PyErr_Format (PyExc_MemoryError,
                        "Could not create LZ4 state");
          goto abort_now;
        }

      if (!LZ4_setStreamDecode (context->lz4_state.decompress, dict.buf, dict.len))
        {
          PyErr_Format (PyExc_RuntimeError,
                        "Could not initialize LZ4 state");
          LZ4_freeStreamDecode (context->lz4_state.decompress);
          goto abort_now;
        }
    }

  PyBuffer_Release (&dict);

  return PyCapsule_New (context, stream_context_capsule_name, destroy_py_context);

abort_now:
  if (dict.buf != NULL)
    {
      PyBuffer_Release (&dict);
    }
  destroy_context (context);

  return NULL;
}

static PyObject *
_compress_bound (PyObject * Py_UNUSED (self), PyObject * args)
{
  PyObject * py_dest = NULL;
  uint32_t input_size;


  /* Positional arguments: input_size
   * Keyword arguments   : none
   */
  if (!PyArg_ParseTuple (args, "OI", &input_size))
    {
      goto exit_now;
    }

  py_dest = PyLong_FromUnsignedLong (get_compress_bound (input_size));

  if (py_dest == NULL)
    {
      PyErr_NoMemory ();
    }

exit_now:
  return py_dest;
}

static PyObject *
_input_bound (PyObject * Py_UNUSED (self), PyObject * args)
{
  PyObject * py_dest = NULL;
  uint32_t compress_max_size;

  /* Positional arguments: compress_max_size
   * Keyword arguments   : none
   */
  if (!PyArg_ParseTuple (args, "I", &compress_max_size))
    {
      goto exit_now;
    }

  py_dest = PyLong_FromUnsignedLong (get_input_bound (compress_max_size));

  if (py_dest == NULL)
    {
      PyErr_NoMemory ();
    }

exit_now:
  return py_dest;
}

static inline int
_compress_generic (stream_context_t * lz4_ctxt, char * source, int source_size,
                  char * dest, int dest_size)
{
  int comp_len;

  if (lz4_ctxt->config.comp == HIGH_COMPRESSION)
    {
      LZ4_streamHC_t * lz4_state = lz4_ctxt->lz4_state.compress.hc;

      comp_len = LZ4_compress_HC_continue (lz4_state, source, dest, source_size, dest_size);
    }
  else
    {
      LZ4_stream_t * lz4_state = lz4_ctxt->lz4_state.compress.fast;
      int acceleration = (lz4_ctxt->config.comp != FAST)
                          ? 1 /* defaults */
                          : lz4_ctxt->config.acceleration;

      comp_len = LZ4_compress_fast_continue (lz4_state, source, dest, source_size, dest_size,
                                             acceleration);
    }

  return comp_len;
}

#ifdef inline
#undef inline
#endif

static PyObject *
_compress (PyObject * Py_UNUSED (self), PyObject * args)
{
  stream_context_t * context = NULL;
  PyObject * py_context = NULL;
  PyObject * py_dest = NULL;
  int output_size;
  Py_buffer source = { NULL, NULL, };

  /* Positional arguments: capsule_context, source
   * Keyword arguments   : none
   */
  if (!PyArg_ParseTuple (args, "Oy*", &py_context, &source))
    {
      goto exit_now;
    }

  context = _PyCapsule_get_context (py_context);
  if ((context == NULL) || (context->lz4_state.context == NULL))
    {
      PyErr_SetString (PyExc_ValueError, "No valid LZ4 stream context supplied");
      goto exit_now;
    }

  if (source.len > context->strategy.ops->get_work_buffer_size (context))
    {
      PyErr_SetString (PyExc_OverflowError,
                       "Input too large for LZ4 API");
      goto exit_now;
    }

  memcpy (context->strategy.ops->get_work_buffer (context), source.buf, source.len);

  Py_BEGIN_ALLOW_THREADS

  output_size = _compress_generic (context,
                                   context->strategy.ops->get_work_buffer (context),
                                   source.len,
                                   context->output.buf + context->config.store_comp_size,
                                   context->output.len);

  Py_END_ALLOW_THREADS

  if (output_size <= 0)
    {
      /* No error code set in output_size! */
      PyErr_SetString (LZ4StreamError,
                       "Compression failed");
      goto exit_now;
    }

  if (!store_block_length (output_size, context->config.store_comp_size, context->output.buf))
    {
      PyErr_SetString (LZ4StreamError,
                       "Compressed stream size too large");
      goto exit_now;
    }

  output_size += context->config.store_comp_size;

  if (context->config.return_bytearray)
    {
      py_dest = PyByteArray_FromStringAndSize (context->output.buf, (Py_ssize_t) output_size);
    }
  else
    {
      py_dest = PyBytes_FromStringAndSize (context->output.buf, (Py_ssize_t) output_size);
    }

  if (py_dest == NULL)
    {
      PyErr_NoMemory ();
      goto exit_now;
    }

  if (context->strategy.ops->update_context_after_process (context) != 0)
    {
      PyErr_Format (PyExc_RuntimeError, "Internal error");
      goto exit_now;
    }

exit_now:
  if (source.buf != NULL)
    {
      PyBuffer_Release (&source);
    }

  return py_dest;
}

static PyObject *
_get_block (PyObject * Py_UNUSED (self), PyObject * args)
{
  stream_context_t * context = NULL;
  PyObject * py_context = NULL;
  PyObject * py_dest = NULL;
  Py_buffer source = { NULL, NULL, };
  buffer_t block = { NULL, 0, };

  /* Positional arguments: capsule_context, source
   * Keyword arguments   : none
   */

  if (!PyArg_ParseTuple (args, "Oy*", &py_context, &source))
    {
      goto exit_now;
    }

  context = _PyCapsule_get_context (py_context);
  if ((context == NULL) || (context->lz4_state.context == NULL))
    {
      PyErr_SetString (PyExc_ValueError, "No valid LZ4 stream context supplied");
      goto exit_now;
    }

  if (source.len > INT_MAX)
    {
      PyErr_Format (PyExc_OverflowError,
                    "Input too large for LZ4 API");
      goto exit_now;
    }

  if (context->config.store_comp_size == 0)
    {
      PyErr_Format (LZ4StreamError,
                    "LZ4 context is configured for storing block size out-of-band");
      goto exit_now;
    }

  if (source.len < context->config.store_comp_size)
    {
      PyErr_Format (LZ4StreamError,
                    "Invalid source, too small for holding any block");
      goto exit_now;
    }

  block.buf = (char *) source.buf + context->config.store_comp_size;
  block.len = load_block_length (context->config.store_comp_size, source.buf);

  if ((source.len - context->config.store_comp_size) < block.len)
    {
      PyErr_Format (LZ4StreamError,
                    "Requested input size (%d) larger than source size (%ld)",
                    block.len, (source.len - context->config.store_comp_size));
      goto exit_now;
    }

  if (context->config.return_bytearray)
    {
      py_dest = PyByteArray_FromStringAndSize (block.buf, (Py_ssize_t) block.len);
    }
  else
    {
      py_dest = PyBytes_FromStringAndSize (block.buf, (Py_ssize_t) block.len);
    }

  if (py_dest == NULL)
    {
      PyErr_NoMemory ();
    }

exit_now:
  if (source.buf != NULL)
    {
      PyBuffer_Release (&source);
    }

  return py_dest;
}

static PyObject *
_decompress (PyObject * Py_UNUSED (self), PyObject * args)
{
  stream_context_t * context = NULL;
  PyObject * py_context = NULL;
  PyObject * py_dest = NULL;
  int output_size = 0;
  uint32_t source_size_max = 0;
  Py_buffer source = { NULL, NULL, };

  /* Positional arguments: capsule_context, source
   * Keyword arguments   : none
   */
  if (!PyArg_ParseTuple (args, "Oy*", &py_context, &source))
    {
      goto exit_now;
    }

  context = _PyCapsule_get_context (py_context);
  if ((context == NULL) || (context->lz4_state.context == NULL))
    {
      PyErr_SetString (PyExc_ValueError, "No valid LZ4 stream context supplied");
      goto exit_now;
    }

  /* In out-of-band block size case, use a best-effort strategy for scaling
   * buffers.
   */
  if (context->config.store_comp_size == 0)
    {
      source_size_max = _GET_MAX_UINT32(4);
    }
  else
    {
      source_size_max = _GET_MAX_UINT32(context->config.store_comp_size);
    }

  if (source.len > source_size_max)
    {
      PyErr_Format (PyExc_OverflowError,
                    "Source length (%ld) too large for LZ4 store_comp_size (%d) value",
                    source.len, context->config.store_comp_size);
      goto exit_now;
    }

  if ((get_input_bound (source.len) == 0) ||
      (get_input_bound (source.len) > context->strategy.ops->get_dest_buffer_size (context)))
    {
      PyErr_Format (LZ4StreamError,
                    "Maximal decompressed data (%d) cannot fit in LZ4 internal buffer (%u)",
                    get_input_bound (source.len),
                    context->strategy.ops->get_dest_buffer_size (context));
      goto exit_now;
    }

  Py_BEGIN_ALLOW_THREADS

  output_size = LZ4_decompress_safe_continue (context->lz4_state.decompress,
                                              (const char *) source.buf,
                                              context->strategy.ops->get_work_buffer (context),
                                              source.len,
                                              context->strategy.ops->get_dest_buffer_size (context));

  Py_END_ALLOW_THREADS

  if (output_size < 0)
    {
      /* In case of LZ4 decompression error, output_size holds the error code */
      PyErr_Format (LZ4StreamError,
                    "Decompression failed. error: %d",
                    -output_size);
      goto exit_now;
    }

  if ((unsigned int) output_size > context->output.len)
    {
      output_size = -1;
      PyErr_Format (PyExc_OverflowError,
                    "Decompressed stream too large for LZ4 API");
      goto exit_now;
    }

  memcpy (context->output.buf,
          context->strategy.ops->get_work_buffer (context),
          output_size);

  if ( context->strategy.ops->update_context_after_process (context) != 0)
    {
      PyErr_Format (PyExc_RuntimeError, "Internal error");
      goto exit_now;
    }

  if (context->config.return_bytearray)
    {
      py_dest = PyByteArray_FromStringAndSize (context->output.buf, (Py_ssize_t) output_size);
    }
  else
    {
      py_dest = PyBytes_FromStringAndSize (context->output.buf, (Py_ssize_t) output_size);
    }

  if (py_dest == NULL)
    {
      PyErr_NoMemory ();
    }

exit_now:
  if (source.buf != NULL)
    {
      PyBuffer_Release (&source);
    }

  return py_dest;
}


PyDoc_STRVAR (_compress_bound__doc,
              "_compress_bound(input_size)\n"                                                     \
              "\n"                                                                                \
              "Provides the maximum size that LZ4 compression may output in a \"worst case\"\n"   \
              "scenario (input data not compressible).\n"                                         \
              "This function is primarily useful for memory allocation purposes (destination\n"   \
              "buffer size).\n"                                                                   \
              "\n"                                                                                \
              "Args:\n"                                                                           \
              "    input_size (int): Input data size.\n"                                          \
              "\n"                                                                                \
              "Returns:\n"                                                                        \
              "    int: Maximal (worst case) size of the compressed data;\n"                      \
              "         or 0 if the input size is greater than 2 113 929 216.\n");

PyDoc_STRVAR (_input_bound__doc,
              "_input_bound(compress_max_size)\n"                                                 \
              "\n"                                                                                \
              "Provides the maximum size that LZ4 decompression may output in a \"worst case\"\n" \
              "scenario (compressed data with null compression ratio).\n"                         \
              "This function is primarily useful for memory allocation purposes (destination\n"   \
              "buffer size).\n"                                                                   \
              "\n"                                                                                \
              "Args:\n"                                                                           \
              "    compress_max_size (int): Compressed data size.\n"                              \
              "\n"                                                                                \
              "Returns:\n"                                                                        \
              "    int: Maximal (worst case) size of the input data;\n"                           \
              "         or 0 if the compressed maximal size is lower than the LZ4 compression\n"  \
              "         minimal overhead, or if the computed input size is greater than\n"        \
              "         2 113 929 216.\n");

PyDoc_STRVAR (_compress__doc,
              "_compress(context, source)\n"                                                      \
              "\n"                                                                                \
              "Compress source, using the given LZ4 stream context, returning the compressed\n"   \
              "data as a bytearray or as a bytes object.\n"                                       \
              "Raises an exception if any error occurs.\n"                                        \
              "\n"                                                                                \
              "Args:\n"                                                                           \
              "    context (ctx): LZ4 stream context.\n"                                          \
              "    source (str, bytes or buffer-compatible object): Data to compress.\n"          \
              "\n"                                                                                \
              "Returns:\n"                                                                        \
              "    bytes or bytearray: Compressed data.\n"                                        \
              "\n"                                                                                \
              "Raises:\n"                                                                         \
              "    OverflowError: raised if the source is too large for being compressed in\n"    \
              "        the given context.\n"                                                      \
              "    RuntimeError: raised if some internal resources cannot be updated.\n"          \
              "    LZ4StreamError: raised if the call to the LZ4 library fails.\n");

PyDoc_STRVAR (_get_block__doc,
              "_get_block(context, source)\n"                                                     \
              "\n"                                                                                \
              "Return the first LZ4 compressed block from ``'source'``. \n"                       \
              "\n"                                                                                \
              "Args:\n"                                                                           \
              "    context (ctx): LZ4 stream context.\n"                                          \
              "    source (str, bytes or buffer-compatible object): LZ4 compressed stream.\n"     \
              "\n"                                                                                \
              "Returns:\n"                                                                        \
              "    bytes or bytearray: LZ4 compressed data block.\n"                              \
              "\n"                                                                                \
              "Raises:\n"                                                                         \
              "    MemoryError: raised if the output buffer cannot be allocated.\n"               \
              "    OverflowError: raised if the source is too large for being handled by  \n"     \
              "        the given context.\n");

PyDoc_STRVAR (_decompress__doc,
              "_decompress(context, source)\n"                                                    \
              "\n"                                                                                \
              "Decompress source, using the given LZ4 stream context, returning the\n"            \
              "uncompressed data as a bytearray or as a bytes object.\n"                          \
              "Raises an exception if any error occurs.\n"                                        \
              "\n"                                                                                \
              "Args:\n"                                                                           \
              "    context (obj): LZ4 stream context.\n"                                          \
              "    source (str, bytes or buffer-compatible object): Data to uncompress.\n"        \
              "\n"                                                                                \
              "Returns:\n"                                                                        \
              "    bytes or bytearray: Uncompressed data.\n"                                      \
              "\n"                                                                                \
              "Raises:\n"                                                                         \
              "    ValueError: raised if the source is inconsistent with a finite LZ4\n"          \
              "        stream block chain.\n"                                                     \
              "    MemoryError: raised if the work output buffer cannot be allocated.\n"          \
              "    OverflowError: raised if the source is too large for being decompressed\n"     \
              "        in the given context.\n"                                                   \
              "    RuntimeError: raised if some internal resources cannot be updated.\n"          \
              "    LZ4StreamError: raised if the call to the LZ4 library fails.\n");

PyDoc_STRVAR (_create_context__doc,
              "_create_context(strategy, direction, buffer_size,\n"                               \
              "                mode='default', acceleration=1, compression_level=9,\n"            \
              "                return_bytearray=0, store_comp_size=4, dict=None)\n"               \
              "\n"                                                                                \
              "Instantiates and initializes a LZ4 stream context.\n"                              \
              "Raises an exception if any error occurs.\n"                                        \
              "\n"                                                                                \
              "Args:\n"                                                                           \
              "    strategy (str): Can be ``'double_buffer'``.\n"                                 \
              "        Only ``'double_buffer'`` is currently implemented.\n"                      \
              "    direction (str): Can be ``'compress'`` or ``'decompress'``.\n"                 \
              "    buffer_size (int): Base size of the buffer(s) used internally for stream\n"    \
              "        compression/decompression.\n"                                              \
              "        For the ``'double_buffer'`` strategy, this is the size of each buffer\n"   \
              "        of the double-buffer.\n"                                                   \
              "\n"                                                                                \
              "Keyword Args:\n"                                                                   \
              "    mode (str): If ``'default'`` or unspecified use the default LZ4\n"             \
              "        compression mode. Set to ``'fast'`` to use the fast compression\n"         \
              "        LZ4 mode at the expense of compression. Set to\n"                          \
              "        ``'high_compression'`` to use the LZ4 high-compression mode at\n"          \
              "        the expense of speed.\n"                                                   \
              "    acceleration (int): When mode is set to ``'fast'`` this argument\n"            \
              "        specifies the acceleration. The larger the acceleration, the\n"            \
              "        faster the but the lower the compression. The default\n"                   \
              "        compression corresponds to a value of ``1``.\n"                            \
              "        Only relevant if ``'direction'`` is ``'compress'``.\n"                     \
              "    compression_level (int): When mode is set to ``high_compression`` this\n"      \
              "        argument specifies the compression. Valid values are between\n"            \
              "        ``1`` and ``12``. Values between ``4-9`` are recommended, and\n"           \
              "        ``9`` is the default.\n"                                                   \
              "        Only relevant if ``'direction'`` is ``'compress'`` and ``'mode'`` .\n"     \
              "        is ``'high_compression'``.\n"                                              \
              "    return_bytearray (bool): If ``False`` (the default) then the function\n"       \
              "        will return a bytes object. If ``True``, then the function will\n"         \
              "        return a bytearray object.\n"                                              \
              "    store_comp_size (int): Specify the size in bytes of  the following\n"          \
              "        compressed block. Can be: ``1``, ``2`` or ``4`` (default: ``4``).\n"       \
              "    dict (str, bytes or buffer-compatible object): If specified, perform\n"        \
              "        compression using this initial dictionary.\n"                              \
              "\n"                                                                                \
              "Returns:\n"                                                                        \
              "    lz4_ctx: A LZ4 stream context.\n"                                              \
              "\n"                                                                                \
              "Raises:\n"                                                                         \
              "    OverflowError: raised if the ``dict`` parameter is too large for the\n"        \
              "        LZ4 context.\n"                                                            \
              "    ValueError: raised if some parameters are invalid.\n"                          \
              "    MemoryError: raised if some internal resources cannot be allocated.\n"         \
              "    RuntimeError: raised if some internal resources cannot be initialized.\n");

PyDoc_STRVAR (lz4stream__doc,
              "A Python wrapper for the LZ4 stream protocol"
              );

static PyMethodDef module_methods[] = {
  {
    "_create_context", (PyCFunction) _create_context,
    METH_VARARGS | METH_KEYWORDS,
    _create_context__doc
  },

  {
    "_compress",
    (PyCFunction) _compress,
    METH_VARARGS,
    _compress__doc
  },
  {
    "_decompress",
    (PyCFunction) _decompress,
    METH_VARARGS,
    _decompress__doc
  },
  {
    "_get_block",
    (PyCFunction) _get_block,
    METH_VARARGS,
    _get_block__doc
  },
  {
    "_compress_bound",
    (PyCFunction) _compress_bound,
    METH_VARARGS,
    _compress_bound__doc
  },
  {
    "_input_bound",
    (PyCFunction) _input_bound,
    METH_VARARGS,
    _input_bound__doc
  },
  {
    /* Sentinel */
    NULL,
    NULL,
    0,
    NULL
  }
};

static PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    /* m_name     */ "_stream",
    /* m_doc      */ lz4stream__doc,
    /* m_size     */ -1,
    /* m_methods  */ module_methods,
};


PyMODINIT_FUNC
PyInit__stream(void)
{
  PyObject * module = PyModule_Create (&moduledef);

  if (module == NULL)
    {
      return NULL;
    }

  PyModule_AddIntConstant (module, "HC_LEVEL_MIN", LZ4HC_CLEVEL_MIN);
  PyModule_AddIntConstant (module, "HC_LEVEL_DEFAULT", LZ4HC_CLEVEL_DEFAULT);
  PyModule_AddIntConstant (module, "HC_LEVEL_OPT_MIN", LZ4HC_CLEVEL_OPT_MIN);
  PyModule_AddIntConstant (module, "HC_LEVEL_MAX", LZ4HC_CLEVEL_MAX);
  PyModule_AddIntConstant (module, "LZ4_MAX_INPUT_SIZE", LZ4_MAX_INPUT_SIZE);

  LZ4StreamError = PyErr_NewExceptionWithDoc ("_stream.LZ4StreamError",
                                              "Call to LZ4 library failed.",
                                              NULL, NULL);
  if (LZ4StreamError == NULL)
    {
      return NULL;
    }
  Py_INCREF (LZ4StreamError);
  PyModule_AddObject (module, "LZ4StreamError", LZ4StreamError);

  return module;
}

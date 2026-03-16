/**
 * @file libaec.h
 *
 * @section LICENSE
 * Copyright 2026 Mathis Rosenhauer, Moritz Hanke, Joerg Behrens, Luis Kornblueh
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @section DESCRIPTION
 *
 * Adaptive Entropy Coding library
 *
 */

#ifndef LIBAEC_H
#define LIBAEC_H 1

#define AEC_VERSION_MAJOR 1
#define AEC_VERSION_MINOR 1
#define AEC_VERSION_PATCH 6
#define AEC_VERSION_STR "1.1.6"

#include <stddef.h>

#ifdef __cplusplus
extern "C"{
#endif

#if defined _WIN32 || defined __CYGWIN__
#  define LIBAEC_HELPER_DLL_IMPORT __declspec(dllimport)
#  define LIBAEC_HELPER_DLL_EXPORT __declspec(dllexport)
#else
#  if __GNUC__ >= 4
#    define LIBAEC_HELPER_DLL_IMPORT
#    define LIBAEC_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
#  else
#    define LIBAEC_HELPER_DLL_IMPORT
#    define LIBAEC_HELPER_DLL_EXPORT
#  endif
#endif

#ifdef LIBAEC_SHARED
#  ifdef LIBAEC_BUILD
#    define LIBAEC_DLL_EXPORTED LIBAEC_HELPER_DLL_EXPORT
#  else
#    define LIBAEC_DLL_EXPORTED LIBAEC_HELPER_DLL_IMPORT
#  endif
#else
#  define LIBAEC_DLL_EXPORTED
#endif

struct internal_state;

struct aec_stream {
    const unsigned char *next_in;

    /* number of bytes available at next_in */
    size_t avail_in;

    /* total number of input bytes read so far */
    size_t total_in;

    unsigned char *next_out;

    /* remaining free space at next_out */
    size_t avail_out;

    /* total number of bytes output so far */
    size_t total_out;

    /* resolution in bits per sample (n = 1, ..., 32) */
    unsigned int bits_per_sample;

    /* block size in samples */
    unsigned int block_size;

    /* Reference sample interval, the number of blocks
     * between consecutive reference samples (up to 4096). */
    unsigned int rsi;

    unsigned int flags;

    struct internal_state *state;
};

/*********************************/
/* Sample data description flags */
/*********************************/

/* Samples are signed. Telling libaec this results in a slightly
 * better compression ratio. Default is unsigned. */
#define AEC_DATA_SIGNED 1

/* 24 bit samples are coded in 3 bytes */
#define AEC_DATA_3BYTE 2

/* Samples are stored with their most significant bit first. This has
 * nothing to do with the endianness of the host. Default is LSB. */
#define AEC_DATA_MSB 4

/* Set if preprocessor should be used */
#define AEC_DATA_PREPROCESS 8

/* Use restricted set of code options */
#define AEC_RESTRICTED 16

/* Pad RSI to byte boundary. Only used for decoding some CCSDS sample
 * data. Do not use this to produce new data as it violates the
 * standard. */
#define AEC_PAD_RSI 32

/* Do not enforce standard regarding legal block sizes. */
#define AEC_NOT_ENFORCE 64

/*************************************/
/* Return codes of library functions */
/*************************************/
#define AEC_OK 0
#define AEC_CONF_ERROR (-1)
#define AEC_STREAM_ERROR (-2)
#define AEC_DATA_ERROR (-3)
#define AEC_MEM_ERROR (-4)
#define AEC_RSI_OFFSETS_ERROR (-5)

/************************/
/* Options for flushing */
/************************/

/* Do not enforce output flushing. More input may be provided with
 * later calls. So far only relevant for encoding. */
#define AEC_NO_FLUSH 0

/* Flush output and end encoding. The last call to aec_encode() must
 * set AEC_FLUSH to drain all output.
 *
 * It is not possible to continue encoding of the same stream after it
 * has been flushed. For one, the last block may be padded zeros after
 * preprocessing. Secondly, the last encoded byte may be padded with
 * fill bits. */
#define AEC_FLUSH 1

/*********************************************/
/* Streaming encoding and decoding functions */
/*********************************************/
LIBAEC_DLL_EXPORTED int aec_encode_init(struct aec_stream *strm);
LIBAEC_DLL_EXPORTED int aec_encode_enable_offsets(struct aec_stream *strm);
LIBAEC_DLL_EXPORTED int aec_encode_count_offsets(struct aec_stream *strm, size_t *rsi_offsets_count);
LIBAEC_DLL_EXPORTED int aec_encode_get_offsets(struct aec_stream *strm, size_t *rsi_offsets, size_t rsi_offsets_count);
LIBAEC_DLL_EXPORTED int aec_buffer_seek(struct aec_stream *strm, size_t offset);
LIBAEC_DLL_EXPORTED int aec_encode(struct aec_stream *strm, int flush);
LIBAEC_DLL_EXPORTED int aec_encode_end(struct aec_stream *strm);

LIBAEC_DLL_EXPORTED int aec_decode_init(struct aec_stream *strm);
LIBAEC_DLL_EXPORTED int aec_decode_enable_offsets(struct aec_stream *strm);
LIBAEC_DLL_EXPORTED int aec_decode_count_offsets(struct aec_stream *strm, size_t *rsi_offsets_count);
LIBAEC_DLL_EXPORTED int aec_decode_get_offsets(struct aec_stream *strm, size_t *rsi_offsets, size_t rsi_offsets_count);
LIBAEC_DLL_EXPORTED int aec_decode(struct aec_stream *strm, int flush);
LIBAEC_DLL_EXPORTED int aec_decode_range(struct aec_stream *strm, const size_t *rsi_offsets, size_t rsi_offsets_count, size_t pos, size_t size);
LIBAEC_DLL_EXPORTED int aec_decode_end(struct aec_stream *strm);

/***************************************************************/
/* Utility functions for encoding or decoding a memory buffer. */
/***************************************************************/
LIBAEC_DLL_EXPORTED int aec_buffer_encode(struct aec_stream *strm);
LIBAEC_DLL_EXPORTED int aec_buffer_decode(struct aec_stream *strm);

#ifdef __cplusplus
}
#endif

#endif /* LIBAEC_H */

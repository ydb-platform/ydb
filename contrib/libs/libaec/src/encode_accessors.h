/**
 * @file encode_accessors.h
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
 * Read various data types from input stream
 *
 */

#ifndef ENCODE_ACCESSORS_H
#define ENCODE_ACCESSORS_H 1

#include "config.h"
#include "libaec.h"
#include <stdint.h>

uint32_t aec_get_8(struct aec_stream *strm);
uint32_t aec_get_lsb_16(struct aec_stream *strm);
uint32_t aec_get_msb_16(struct aec_stream *strm);
uint32_t aec_get_lsb_32(struct aec_stream *strm);
uint32_t aec_get_msb_24(struct aec_stream *strm);
uint32_t aec_get_lsb_24(struct aec_stream *strm);
uint32_t aec_get_msb_32(struct aec_stream *strm);

void aec_get_rsi_8(struct aec_stream *strm);
void aec_get_rsi_lsb_16(struct aec_stream *strm);
void aec_get_rsi_msb_16(struct aec_stream *strm);
void aec_get_rsi_lsb_24(struct aec_stream *strm);
void aec_get_rsi_msb_24(struct aec_stream *strm);
void aec_get_rsi_lsb_32(struct aec_stream *strm);
void aec_get_rsi_msb_32(struct aec_stream *strm);

#endif /* ENCODE_ACCESSORS_H */

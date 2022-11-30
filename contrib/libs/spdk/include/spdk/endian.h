/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * \file
 * Endian conversion functions
 */

#ifndef SPDK_ENDIAN_H
#define SPDK_ENDIAN_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline uint16_t
from_be16(const void *ptr)
{
	const uint8_t *tmp = (const uint8_t *)ptr;
	return (((uint16_t)tmp[0] << 8) | tmp[1]);
}

static inline void
to_be16(void *out, uint16_t in)
{
	uint8_t *tmp = (uint8_t *)out;
	tmp[0] = (in >> 8) & 0xFF;
	tmp[1] = in & 0xFF;
}

static inline uint32_t
from_be32(const void *ptr)
{
	const uint8_t *tmp = (const uint8_t *)ptr;
	return (((uint32_t)tmp[0] << 24) |
		((uint32_t)tmp[1] << 16) |
		((uint32_t)tmp[2] << 8) |
		((uint32_t)tmp[3]));
}

static inline void
to_be32(void *out, uint32_t in)
{
	uint8_t *tmp = (uint8_t *)out;
	tmp[0] = (in >> 24) & 0xFF;
	tmp[1] = (in >> 16) & 0xFF;
	tmp[2] = (in >> 8) & 0xFF;
	tmp[3] = in & 0xFF;
}

static inline uint64_t
from_be64(const void *ptr)
{
	const uint8_t *tmp = (const uint8_t *)ptr;
	return (((uint64_t)tmp[0] << 56) |
		((uint64_t)tmp[1] << 48) |
		((uint64_t)tmp[2] << 40) |
		((uint64_t)tmp[3] << 32) |
		((uint64_t)tmp[4] << 24) |
		((uint64_t)tmp[5] << 16) |
		((uint64_t)tmp[6] << 8) |
		((uint64_t)tmp[7]));
}

static inline void
to_be64(void *out, uint64_t in)
{
	uint8_t *tmp = (uint8_t *)out;
	tmp[0] = (in >> 56) & 0xFF;
	tmp[1] = (in >> 48) & 0xFF;
	tmp[2] = (in >> 40) & 0xFF;
	tmp[3] = (in >> 32) & 0xFF;
	tmp[4] = (in >> 24) & 0xFF;
	tmp[5] = (in >> 16) & 0xFF;
	tmp[6] = (in >> 8) & 0xFF;
	tmp[7] = in & 0xFF;
}

static inline uint16_t
from_le16(const void *ptr)
{
	const uint8_t *tmp = (const uint8_t *)ptr;
	return (((uint16_t)tmp[1] << 8) | tmp[0]);
}

static inline void
to_le16(void *out, uint16_t in)
{
	uint8_t *tmp = (uint8_t *)out;
	tmp[1] = (in >> 8) & 0xFF;
	tmp[0] = in & 0xFF;
}

static inline uint32_t
from_le32(const void *ptr)
{
	const uint8_t *tmp = (const uint8_t *)ptr;
	return (((uint32_t)tmp[3] << 24) |
		((uint32_t)tmp[2] << 16) |
		((uint32_t)tmp[1] << 8) |
		((uint32_t)tmp[0]));
}

static inline void
to_le32(void *out, uint32_t in)
{
	uint8_t *tmp = (uint8_t *)out;
	tmp[3] = (in >> 24) & 0xFF;
	tmp[2] = (in >> 16) & 0xFF;
	tmp[1] = (in >> 8) & 0xFF;
	tmp[0] = in & 0xFF;
}

static inline uint64_t
from_le64(const void *ptr)
{
	const uint8_t *tmp = (const uint8_t *)ptr;
	return (((uint64_t)tmp[7] << 56) |
		((uint64_t)tmp[6] << 48) |
		((uint64_t)tmp[5] << 40) |
		((uint64_t)tmp[4] << 32) |
		((uint64_t)tmp[3] << 24) |
		((uint64_t)tmp[2] << 16) |
		((uint64_t)tmp[1] << 8) |
		((uint64_t)tmp[0]));
}

static inline void
to_le64(void *out, uint64_t in)
{
	uint8_t *tmp = (uint8_t *)out;
	tmp[7] = (in >> 56) & 0xFF;
	tmp[6] = (in >> 48) & 0xFF;
	tmp[5] = (in >> 40) & 0xFF;
	tmp[4] = (in >> 32) & 0xFF;
	tmp[3] = (in >> 24) & 0xFF;
	tmp[2] = (in >> 16) & 0xFF;
	tmp[1] = (in >> 8) & 0xFF;
	tmp[0] = in & 0xFF;
}

#ifdef __cplusplus
}
#endif

#endif

/*-
 *   BSD LICENSE
 *
 *   Copyright (C) 2008-2012 Daisuke Aoyama <aoyama@peach.ne.jp>.
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

#ifndef SPDK_UTIL_INTERNAL_H
#define SPDK_UTIL_INTERNAL_H

#include "spdk/stdinc.h"

/**
 * IEEE CRC-32 polynomial (bit reflected)
 */
#define SPDK_CRC32_POLYNOMIAL_REFLECT 0xedb88320UL

/**
 * CRC-32C (Castagnoli) polynomial (bit reflected)
 */
#define SPDK_CRC32C_POLYNOMIAL_REFLECT 0x82f63b78UL

struct spdk_crc32_table {
	uint32_t table[256];
};

/**
 * Initialize a CRC32 lookup table for a given polynomial.
 *
 * \param table Table to fill with precalculated CRC-32 data.
 * \param polynomial_reflect Bit-reflected CRC-32 polynomial.
 */
void crc32_table_init(struct spdk_crc32_table *table,
		      uint32_t polynomial_reflect);


/**
 * Calculate a partial CRC-32 checksum.
 *
 * \param table CRC-32 table initialized with crc32_table_init().
 * \param buf Data buffer to checksum.
 * \param len Length of buf in bytes.
 * \param crc Previous CRC-32 value.
 * \return Updated CRC-32 value.
 */
uint32_t crc32_update(const struct spdk_crc32_table *table,
		      const void *buf, size_t len,
		      uint32_t crc);

#endif /* SPDK_UTIL_INTERNAL_H */

#include <contrib/libs/spdk/ndebug.h>
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

#include "util_internal.h"
#include "spdk/crc32.h"

void
crc32_table_init(struct spdk_crc32_table *table, uint32_t polynomial_reflect)
{
	int i, j;
	uint32_t val;

	for (i = 0; i < 256; i++) {
		val = i;
		for (j = 0; j < 8; j++) {
			if (val & 1) {
				val = (val >> 1) ^ polynomial_reflect;
			} else {
				val = (val >> 1);
			}
		}
		table->table[i] = val;
	}
}

#ifdef SPDK_HAVE_ARM_CRC

uint32_t
crc32_update(const struct spdk_crc32_table *table, const void *buf, size_t len, uint32_t crc)
{
	size_t count;
	const uint64_t *dword_buf;

	count = len & 7;
	while (count--) {
		crc = __crc32b(crc, *(const uint8_t *)buf);
		buf++;
	}
	dword_buf = (const uint64_t *)buf;

	count = len / 8;
	while (count--) {
		crc = __crc32d(crc, *dword_buf);
		dword_buf++;
	}

	return crc;
}

#else

uint32_t
crc32_update(const struct spdk_crc32_table *table, const void *buf, size_t len, uint32_t crc)
{
	const uint8_t *buf_u8 = buf;
	size_t i;

	for (i = 0; i < len; i++) {
		crc = (crc >> 8) ^ table->table[(crc ^ buf_u8[i]) & 0xff];
	}

	return crc;
}

#endif

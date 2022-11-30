#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright(c) Intel Corporation. All rights reserved.
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

#include "spdk/stdinc.h"
#include "spdk/util.h"
#include "spdk/assert.h"

/* The following will automatically generate several version of
 * this function, targeted at different architectures. This
 * is only supported by GCC 6 or newer. */
#if defined(__GNUC__) && __GNUC__ >= 6 && !defined(__clang__) \
	&& (defined(__i386__) || defined(__x86_64__)) \
	&& defined(__ELF__)
__attribute__((target_clones("bmi", "arch=core2", "arch=atom", "default")))
#endif
uint32_t
spdk_u32log2(uint32_t x)
{
	if (x == 0) {
		/* log(0) is undefined */
		return 0;
	}
	SPDK_STATIC_ASSERT(sizeof(x) == sizeof(unsigned int), "Incorrect size");
	return 31u - __builtin_clz(x);
}

/* The following will automatically generate several version of
 * this function, targeted at different architectures. This
 * is only supported by GCC 6 or newer. */
#if defined(__GNUC__) && __GNUC__ >= 6 && !defined(__clang__) \
	&& (defined(__i386__) || defined(__x86_64__)) \
	&& defined(__ELF__)
__attribute__((target_clones("bmi", "arch=core2", "arch=atom", "default")))
#endif
uint64_t
spdk_u64log2(uint64_t x)
{
	if (x == 0) {
		/* log(0) is undefined */
		return 0;
	}
	SPDK_STATIC_ASSERT(sizeof(x) == sizeof(unsigned long long), "Incorrect size");
	return 63u - __builtin_clzll(x);
}

/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2019 Mellanox Technologies LTD. All rights reserved.
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

/** \file
 * General utility functions
 */

#ifndef SPDK_UTIL_H
#define SPDK_UTIL_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_CACHE_LINE_SIZE 64

#define spdk_min(a,b) (((a)<(b))?(a):(b))
#define spdk_max(a,b) (((a)>(b))?(a):(b))

#define SPDK_COUNTOF(arr) (sizeof(arr) / sizeof((arr)[0]))

#define SPDK_CONTAINEROF(ptr, type, member) ((type *)((uintptr_t)ptr - offsetof(type, member)))

#define SPDK_SEC_TO_USEC 1000000ULL
#define SPDK_SEC_TO_NSEC 1000000000ULL

/* Ceiling division of unsigned integers */
#define SPDK_CEIL_DIV(x,y) (((x)+(y)-1)/(y))

/**
 * Macro to align a value to a given power-of-two. The resultant value
 * will be of the same type as the first parameter, and will be no
 * bigger than the first parameter. Second parameter must be a
 * power-of-two value.
 */
#define SPDK_ALIGN_FLOOR(val, align) \
	(typeof(val))((val) & (~((typeof(val))((align) - 1))))
/**
 * Macro to align a value to a given power-of-two. The resultant value
 * will be of the same type as the first parameter, and will be no lower
 * than the first parameter. Second parameter must be a power-of-two
 * value.
 */
#define SPDK_ALIGN_CEIL(val, align) \
	SPDK_ALIGN_FLOOR(((val) + ((typeof(val)) (align) - 1)), align)

uint32_t spdk_u32log2(uint32_t x);

static inline uint32_t
spdk_align32pow2(uint32_t x)
{
	return 1u << (1 + spdk_u32log2(x - 1));
}

uint64_t spdk_u64log2(uint64_t x);

static inline uint64_t
spdk_align64pow2(uint64_t x)
{
	return 1ULL << (1 + spdk_u64log2(x - 1));
}

/**
 * Check if a uint32_t is a power of 2.
 */
static inline bool
spdk_u32_is_pow2(uint32_t x)
{
	if (x == 0) {
		return false;
	}

	return (x & (x - 1)) == 0;
}

static inline uint64_t
spdk_divide_round_up(uint64_t num, uint64_t divisor)
{
	return (num + divisor - 1) / divisor;
}

/**
 * Copy the data described by the source iovec to the destination iovec.
 *
 * \return The number of bytes copied.
 */
size_t spdk_iovcpy(struct iovec *siov, size_t siovcnt, struct iovec *diov, size_t diovcnt);


/**
 * Scan build is really pessimistic and assumes that mempool functions can
 * dequeue NULL buffers even if they return success. This is obviously a false
 * possitive, but the mempool dequeue can be done in a DPDK inline function that
 * we can't decorate with usual assert(buf != NULL). Instead, we'll
 * preinitialize the dequeued buffer array with some dummy objects.
 */
#define SPDK_CLANG_ANALYZER_PREINIT_PTR_ARRAY(arr, arr_size, buf_size) \
	do { \
		static char dummy_buf[buf_size]; \
		int i; \
		for (i = 0; i < arr_size; i++) { \
			arr[i] = (void *)dummy_buf; \
		} \
	} while (0)

/**
 * Add two sequece numbers s1 and s2
 *
 * \param s1 First sequence number
 * \param s2 Second sequence number
 *
 * \return Sum of s1 and s2 based on serial number arithmetic.
 */
static inline uint32_t
spdk_sn32_add(uint32_t s1, uint32_t s2)
{
	return (uint32_t)(s1 + s2);
}

#define SPDK_SN32_CMPMAX	(1U << (32 - 1))

/**
 * Compare if sequence number s1 is less than s2.
 *
 * \param s1 First sequence number
 * \param s2 Second sequence number
 *
 * \return true if s1 is less than s2, or false otherwise.
 */
static inline bool
spdk_sn32_lt(uint32_t s1, uint32_t s2)
{
	return (s1 != s2) &&
	       ((s1 < s2 && s2 - s1 < SPDK_SN32_CMPMAX) ||
		(s1 > s2 && s1 - s2 > SPDK_SN32_CMPMAX));
}

/**
 * Compare if sequence number s1 is greater than s2.
 *
 * \param s1 First sequence number
 * \param s2 Second sequence number
 *
 * \return true if s1 is greater than s2, or false otherwise.
 */
static inline bool
spdk_sn32_gt(uint32_t s1, uint32_t s2)
{
	return (s1 != s2) &&
	       ((s1 < s2 && s2 - s1 > SPDK_SN32_CMPMAX) ||
		(s1 > s2 && s1 - s2 < SPDK_SN32_CMPMAX));
}

#ifdef __cplusplus
}
#endif

#endif

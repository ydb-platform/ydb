/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Cavium, Inc
 */

#ifndef _RTE_MEMCPY_ARM64_H_
#define _RTE_MEMCPY_ARM64_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "generic/rte_memcpy.h"

#ifdef RTE_ARCH_ARM64_MEMCPY
#include <rte_common.h>
#include <rte_branch_prediction.h>

/*
 * The memory copy performance differs on different AArch64 micro-architectures.
 * And the most recent glibc (e.g. 2.23 or later) can provide a better memcpy()
 * performance compared to old glibc versions. It's always suggested to use a
 * more recent glibc if possible, from which the entire system can get benefit.
 *
 * This implementation improves memory copy on some aarch64 micro-architectures,
 * when an old glibc (e.g. 2.19, 2.17...) is being used. It is disabled by
 * default and needs "RTE_ARCH_ARM64_MEMCPY" defined to activate. It's not
 * always providing better performance than memcpy() so users need to run unit
 * test "memcpy_perf_autotest" and customize parameters in customization section
 * below for best performance.
 *
 * Compiler version will also impact the rte_memcpy() performance. It's observed
 * on some platforms and with the same code, GCC 7.2.0 compiled binaries can
 * provide better performance than GCC 4.8.5 compiled binaries.
 */

/**************************************
 * Beginning of customization section
 **************************************/
#ifndef RTE_ARM64_MEMCPY_ALIGN_MASK
#define RTE_ARM64_MEMCPY_ALIGN_MASK ((RTE_CACHE_LINE_SIZE >> 3) - 1)
#endif

#ifndef RTE_ARM64_MEMCPY_STRICT_ALIGN
/* Only src unalignment will be treated as unaligned copy */
#define RTE_ARM64_MEMCPY_IS_UNALIGNED_COPY(dst, src) \
	((uintptr_t)(src) & RTE_ARM64_MEMCPY_ALIGN_MASK)
#else
/* Both dst and src unalignment will be treated as unaligned copy */
#define RTE_ARM64_MEMCPY_IS_UNALIGNED_COPY(dst, src) \
	(((uintptr_t)(dst) | (uintptr_t)(src)) & RTE_ARM64_MEMCPY_ALIGN_MASK)
#endif


/*
 * If copy size is larger than threshold, memcpy() will be used.
 * Run "memcpy_perf_autotest" to determine the proper threshold.
 */
#ifdef RTE_ARM64_MEMCPY_ALIGNED_THRESHOLD
#define USE_ALIGNED_RTE_MEMCPY(dst, src, n) \
(!RTE_ARM64_MEMCPY_IS_UNALIGNED_COPY(dst, src) && \
n <= (size_t)RTE_ARM64_MEMCPY_ALIGNED_THRESHOLD)
#else
#define USE_ALIGNED_RTE_MEMCPY(dst, src, n) \
(!RTE_ARM64_MEMCPY_IS_UNALIGNED_COPY(dst, src))
#endif
#ifdef RTE_ARM64_MEMCPY_UNALIGNED_THRESHOLD
#define USE_UNALIGNED_RTE_MEMCPY(dst, src, n) \
(RTE_ARM64_MEMCPY_IS_UNALIGNED_COPY(dst, src) && \
n <= (size_t)RTE_ARM64_MEMCPY_UNALIGNED_THRESHOLD)
#else
#define USE_UNALIGNED_RTE_MEMCPY(dst, src, n) \
(RTE_ARM64_MEMCPY_IS_UNALIGNED_COPY(dst, src))
#endif
/*
 * The logic of USE_RTE_MEMCPY() can also be modified to best fit platform.
 */
#if defined(RTE_ARM64_MEMCPY_ALIGNED_THRESHOLD) \
|| defined(RTE_ARM64_MEMCPY_UNALIGNED_THRESHOLD)
#define USE_RTE_MEMCPY(dst, src, n) \
(USE_ALIGNED_RTE_MEMCPY(dst, src, n) || USE_UNALIGNED_RTE_MEMCPY(dst, src, n))
#else
#define USE_RTE_MEMCPY(dst, src, n) (1)
#endif
/**************************************
 * End of customization section
 **************************************/


#if RTE_CC_IS_GNU && !defined RTE_ARM64_MEMCPY_SKIP_GCC_VER_CHECK
#if (GCC_VERSION < 50400)
#warning "The GCC version is quite old, which may result in sub-optimal \
performance of the compiled code. It is suggested that at least GCC 5.4.0 \
be used."
#endif
#endif

static __rte_always_inline
void rte_mov16(uint8_t *dst, const uint8_t *src)
{
	__uint128_t *dst128 = (__uint128_t *)dst;
	const __uint128_t *src128 = (const __uint128_t *)src;
	*dst128 = *src128;
}

static __rte_always_inline
void rte_mov32(uint8_t *dst, const uint8_t *src)
{
	__uint128_t *dst128 = (__uint128_t *)dst;
	const __uint128_t *src128 = (const __uint128_t *)src;
	const __uint128_t x0 = src128[0], x1 = src128[1];
	dst128[0] = x0;
	dst128[1] = x1;
}

static __rte_always_inline
void rte_mov48(uint8_t *dst, const uint8_t *src)
{
	__uint128_t *dst128 = (__uint128_t *)dst;
	const __uint128_t *src128 = (const __uint128_t *)src;
	const __uint128_t x0 = src128[0], x1 = src128[1], x2 = src128[2];
	dst128[0] = x0;
	dst128[1] = x1;
	dst128[2] = x2;
}

static __rte_always_inline
void rte_mov64(uint8_t *dst, const uint8_t *src)
{
	__uint128_t *dst128 = (__uint128_t *)dst;
	const __uint128_t *src128 = (const __uint128_t *)src;
	const __uint128_t
		x0 = src128[0], x1 = src128[1], x2 = src128[2], x3 = src128[3];
	dst128[0] = x0;
	dst128[1] = x1;
	dst128[2] = x2;
	dst128[3] = x3;
}

static __rte_always_inline
void rte_mov128(uint8_t *dst, const uint8_t *src)
{
	__uint128_t *dst128 = (__uint128_t *)dst;
	const __uint128_t *src128 = (const __uint128_t *)src;
	/* Keep below declaration & copy sequence for optimized instructions */
	const __uint128_t
		x0 = src128[0], x1 = src128[1], x2 = src128[2], x3 = src128[3];
	dst128[0] = x0;
	__uint128_t x4 = src128[4];
	dst128[1] = x1;
	__uint128_t x5 = src128[5];
	dst128[2] = x2;
	__uint128_t x6 = src128[6];
	dst128[3] = x3;
	__uint128_t x7 = src128[7];
	dst128[4] = x4;
	dst128[5] = x5;
	dst128[6] = x6;
	dst128[7] = x7;
}

static __rte_always_inline
void rte_mov256(uint8_t *dst, const uint8_t *src)
{
	rte_mov128(dst, src);
	rte_mov128(dst + 128, src + 128);
}

static __rte_always_inline void
rte_memcpy_lt16(uint8_t *dst, const uint8_t *src, size_t n)
{
	if (n & 0x08) {
		/* copy 8 ~ 15 bytes */
		*(uint64_t *)dst = *(const uint64_t *)src;
		*(uint64_t *)(dst - 8 + n) = *(const uint64_t *)(src - 8 + n);
	} else if (n & 0x04) {
		/* copy 4 ~ 7 bytes */
		*(uint32_t *)dst = *(const uint32_t *)src;
		*(uint32_t *)(dst - 4 + n) = *(const uint32_t *)(src - 4 + n);
	} else if (n & 0x02) {
		/* copy 2 ~ 3 bytes */
		*(uint16_t *)dst = *(const uint16_t *)src;
		*(uint16_t *)(dst - 2 + n) = *(const uint16_t *)(src - 2 + n);
	} else if (n & 0x01) {
		/* copy 1 byte */
		*dst = *src;
	}
}

static __rte_always_inline
void rte_memcpy_ge16_lt128(uint8_t *dst, const uint8_t *src, size_t n)
{
	if (n < 64) {
		if (n == 16) {
			rte_mov16(dst, src);
		} else if (n <= 32) {
			rte_mov16(dst, src);
			rte_mov16(dst - 16 + n, src - 16 + n);
		} else if (n <= 48) {
			rte_mov32(dst, src);
			rte_mov16(dst - 16 + n, src - 16 + n);
		} else {
			rte_mov48(dst, src);
			rte_mov16(dst - 16 + n, src - 16 + n);
		}
	} else {
		rte_mov64((uint8_t *)dst, (const uint8_t *)src);
		if (n > 48 + 64)
			rte_mov64(dst - 64 + n, src - 64 + n);
		else if (n > 32 + 64)
			rte_mov48(dst - 48 + n, src - 48 + n);
		else if (n > 16 + 64)
			rte_mov32(dst - 32 + n, src - 32 + n);
		else if (n > 64)
			rte_mov16(dst - 16 + n, src - 16 + n);
	}
}

static __rte_always_inline
void rte_memcpy_ge128(uint8_t *dst, const uint8_t *src, size_t n)
{
	do {
		rte_mov128(dst, src);
		src += 128;
		dst += 128;
		n -= 128;
	} while (likely(n >= 128));

	if (likely(n)) {
		if (n <= 16)
			rte_mov16(dst - 16 + n, src - 16 + n);
		else if (n <= 32)
			rte_mov32(dst - 32 + n, src - 32 + n);
		else if (n <= 48)
			rte_mov48(dst - 48 + n, src - 48 + n);
		else if (n <= 64)
			rte_mov64(dst - 64 + n, src - 64 + n);
		else
			rte_memcpy_ge16_lt128(dst, src, n);
	}
}

static __rte_always_inline
void rte_memcpy_ge16_lt64(uint8_t *dst, const uint8_t *src, size_t n)
{
	if (n == 16) {
		rte_mov16(dst, src);
	} else if (n <= 32) {
		rte_mov16(dst, src);
		rte_mov16(dst - 16 + n, src - 16 + n);
	} else if (n <= 48) {
		rte_mov32(dst, src);
		rte_mov16(dst - 16 + n, src - 16 + n);
	} else {
		rte_mov48(dst, src);
		rte_mov16(dst - 16 + n, src - 16 + n);
	}
}

static __rte_always_inline
void rte_memcpy_ge64(uint8_t *dst, const uint8_t *src, size_t n)
{
	do {
		rte_mov64(dst, src);
		src += 64;
		dst += 64;
		n -= 64;
	} while (likely(n >= 64));

	if (likely(n)) {
		if (n <= 16)
			rte_mov16(dst - 16 + n, src - 16 + n);
		else if (n <= 32)
			rte_mov32(dst - 32 + n, src - 32 + n);
		else if (n <= 48)
			rte_mov48(dst - 48 + n, src - 48 + n);
		else
			rte_mov64(dst - 64 + n, src - 64 + n);
	}
}

#if RTE_CACHE_LINE_SIZE >= 128
static __rte_always_inline
void *rte_memcpy(void *dst, const void *src, size_t n)
{
	if (n < 16) {
		rte_memcpy_lt16((uint8_t *)dst, (const uint8_t *)src, n);
		return dst;
	}
	if (n < 128) {
		rte_memcpy_ge16_lt128((uint8_t *)dst, (const uint8_t *)src, n);
		return dst;
	}
	__builtin_prefetch(src, 0, 0);
	__builtin_prefetch(dst, 1, 0);
	if (likely(USE_RTE_MEMCPY(dst, src, n))) {
		rte_memcpy_ge128((uint8_t *)dst, (const uint8_t *)src, n);
		return dst;
	} else
		return memcpy(dst, src, n);
}

#else
static __rte_always_inline
void *rte_memcpy(void *dst, const void *src, size_t n)
{
	if (n < 16) {
		rte_memcpy_lt16((uint8_t *)dst, (const uint8_t *)src, n);
		return dst;
	}
	if (n < 64) {
		rte_memcpy_ge16_lt64((uint8_t *)dst, (const uint8_t *)src, n);
		return dst;
	}
	__builtin_prefetch(src, 0, 0);
	__builtin_prefetch(dst, 1, 0);
	if (likely(USE_RTE_MEMCPY(dst, src, n))) {
		rte_memcpy_ge64((uint8_t *)dst, (const uint8_t *)src, n);
		return dst;
	} else
		return memcpy(dst, src, n);
}
#endif /* RTE_CACHE_LINE_SIZE >= 128 */

#else
static inline void
rte_mov16(uint8_t *dst, const uint8_t *src)
{
	memcpy(dst, src, 16);
}

static inline void
rte_mov32(uint8_t *dst, const uint8_t *src)
{
	memcpy(dst, src, 32);
}

static inline void
rte_mov48(uint8_t *dst, const uint8_t *src)
{
	memcpy(dst, src, 48);
}

static inline void
rte_mov64(uint8_t *dst, const uint8_t *src)
{
	memcpy(dst, src, 64);
}

static inline void
rte_mov128(uint8_t *dst, const uint8_t *src)
{
	memcpy(dst, src, 128);
}

static inline void
rte_mov256(uint8_t *dst, const uint8_t *src)
{
	memcpy(dst, src, 256);
}

#define rte_memcpy(d, s, n)	memcpy((d), (s), (n))

#endif /* RTE_ARCH_ARM64_MEMCPY */

#ifdef __cplusplus
}
#endif

#endif /* _RTE_MEMCPY_ARM_64_H_ */

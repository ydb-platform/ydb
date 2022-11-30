/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_MEMCPY_H_
#define _RTE_MEMCPY_H_

/**
 * @file
 *
 * Functions for vectorised implementation of memcpy().
 */

/**
 * Copy 16 bytes from one location to another using optimised
 * instructions. The locations should not overlap.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 */
static inline void
rte_mov16(uint8_t *dst, const uint8_t *src);

/**
 * Copy 32 bytes from one location to another using optimised
 * instructions. The locations should not overlap.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 */
static inline void
rte_mov32(uint8_t *dst, const uint8_t *src);

#ifdef __DOXYGEN__

/**
 * Copy 48 bytes from one location to another using optimised
 * instructions. The locations should not overlap.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 */
static inline void
rte_mov48(uint8_t *dst, const uint8_t *src);

#endif /* __DOXYGEN__ */

/**
 * Copy 64 bytes from one location to another using optimised
 * instructions. The locations should not overlap.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 */
static inline void
rte_mov64(uint8_t *dst, const uint8_t *src);

/**
 * Copy 128 bytes from one location to another using optimised
 * instructions. The locations should not overlap.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 */
static inline void
rte_mov128(uint8_t *dst, const uint8_t *src);

/**
 * Copy 256 bytes from one location to another using optimised
 * instructions. The locations should not overlap.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 */
static inline void
rte_mov256(uint8_t *dst, const uint8_t *src);

#ifdef __DOXYGEN__

/**
 * Copy bytes from one location to another. The locations must not overlap.
 *
 * @note This is implemented as a macro, so it's address should not be taken
 * and care is needed as parameter expressions may be evaluated multiple times.
 *
 * @note For x86 platforms to enable the AVX-512 memcpy implementation, set
 * -DRTE_MEMCPY_AVX512 macro in CFLAGS, or define the RTE_MEMCPY_AVX512 macro
 * explicitly in the source file before including the rte_memcpy header file.
 *
 * @param dst
 *   Pointer to the destination of the data.
 * @param src
 *   Pointer to the source data.
 * @param n
 *   Number of bytes to copy.
 * @return
 *   Pointer to the destination data.
 */
static void *
rte_memcpy(void *dst, const void *src, size_t n);

#endif /* __DOXYGEN__ */

#endif /* _RTE_MEMCPY_H_ */

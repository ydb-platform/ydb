/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2016 6WIND S.A.
 */

#ifndef _RTE_VECT_H_
#define _RTE_VECT_H_

/**
 * @file
 * SIMD vector types and control
 *
 * This file defines types to use vector instructions with generic C code
 * and APIs to enable the code using them.
 */

#include <stdint.h>

#include <rte_compat.h>

/* Unsigned vector types */

/**
 * 64 bits vector size to use with unsigned 8 bits elements.
 *
 * a = (rte_v64u8_t){ a0, a1, a2, a3, a4, a5, a6, a7 }
 */
typedef uint8_t rte_v64u8_t __attribute__((vector_size(8), aligned(8)));

/**
 * 64 bits vector size to use with unsigned 16 bits elements.
 *
 * a = (rte_v64u16_t){ a0, a1, a2, a3 }
 */
typedef uint16_t rte_v64u16_t __attribute__((vector_size(8), aligned(8)));

/**
 * 64 bits vector size to use with unsigned 32 bits elements.
 *
 * a = (rte_v64u32_t){ a0, a1 }
 */
typedef uint32_t rte_v64u32_t __attribute__((vector_size(8), aligned(8)));

/**
 * 128 bits vector size to use with unsigned 8 bits elements.
 *
 * a = (rte_v128u8_t){ a00, a01, a02, a03, a04, a05, a06, a07,
 *                     a08, a09, a10, a11, a12, a13, a14, a15 }
 */
typedef uint8_t rte_v128u8_t __attribute__((vector_size(16), aligned(16)));

/**
 * 128 bits vector size to use with unsigned 16 bits elements.
 *
 * a = (rte_v128u16_t){ a0, a1, a2, a3, a4, a5, a6, a7 }
 */
typedef uint16_t rte_v128u16_t __attribute__((vector_size(16), aligned(16)));

/**
 * 128 bits vector size to use with unsigned 32 bits elements.
 *
 * a = (rte_v128u32_t){ a0, a1, a2, a3 }
 */
typedef uint32_t rte_v128u32_t __attribute__((vector_size(16), aligned(16)));

/**
 * 128 bits vector size to use with unsigned 64 bits elements.
 *
 * a = (rte_v128u64_t){ a0, a1 }
 */
typedef uint64_t rte_v128u64_t __attribute__((vector_size(16), aligned(16)));

/**
 * 256 bits vector size to use with unsigned 8 bits elements.
 *
 * a = (rte_v256u8_t){ a00, a01, a02, a03, a04, a05, a06, a07,
 *                     a08, a09, a10, a11, a12, a13, a14, a15,
 *                     a16, a17, a18, a19, a20, a21, a22, a23,
 *                     a24, a25, a26, a27, a28, a29, a30, a31 }
 */
typedef uint8_t rte_v256u8_t __attribute__((vector_size(32), aligned(32)));

/**
 * 256 bits vector size to use with unsigned 16 bits elements.
 *
 * a = (rte_v256u16_t){ a00, a01, a02, a03, a04, a05, a06, a07,
 *                      a08, a09, a10, a11, a12, a13, a14, a15 }
 */
typedef uint16_t rte_v256u16_t __attribute__((vector_size(32), aligned(32)));

/**
 * 256 bits vector size to use with unsigned 32 bits elements.
 *
 * a = (rte_v256u32_t){ a0, a1, a2, a3, a4, a5, a6, a7 }
 */
typedef uint32_t rte_v256u32_t __attribute__((vector_size(32), aligned(32)));

/**
 * 256 bits vector size to use with unsigned 64 bits elements.
 *
 * a = (rte_v256u64_t){ a0, a1, a2, a3 }
 */
typedef uint64_t rte_v256u64_t __attribute__((vector_size(32), aligned(32)));


/* Signed vector types */

/**
 * 64 bits vector size to use with 8 bits elements.
 *
 * a = (rte_v64s8_t){ a0, a1, a2, a3, a4, a5, a6, a7 }
 */
typedef int8_t rte_v64s8_t __attribute__((vector_size(8), aligned(8)));

/**
 * 64 bits vector size to use with 16 bits elements.
 *
 * a = (rte_v64s16_t){ a0, a1, a2, a3 }
 */
typedef int16_t rte_v64s16_t __attribute__((vector_size(8), aligned(8)));

/**
 * 64 bits vector size to use with 32 bits elements.
 *
 * a = (rte_v64s32_t){ a0, a1 }
 */
typedef int32_t rte_v64s32_t __attribute__((vector_size(8), aligned(8)));

/**
 * 128 bits vector size to use with 8 bits elements.
 *
 * a = (rte_v128s8_t){ a00, a01, a02, a03, a04, a05, a06, a07,
 *                     a08, a09, a10, a11, a12, a13, a14, a15 }
 */
typedef int8_t rte_v128s8_t __attribute__((vector_size(16), aligned(16)));

/**
 * 128 bits vector size to use with 16 bits elements.
 *
 * a = (rte_v128s16_t){ a0, a1, a2, a3, a4, a5, a6, a7 }
 */
typedef int16_t rte_v128s16_t __attribute__((vector_size(16), aligned(16)));

/**
 * 128 bits vector size to use with 32 bits elements.
 *
 * a = (rte_v128s32_t){ a0, a1, a2, a3 }
 */
typedef int32_t rte_v128s32_t __attribute__((vector_size(16), aligned(16)));

/**
 * 128 bits vector size to use with 64 bits elements.
 *
 * a = (rte_v128s64_t){ a1, a2 }
 */
typedef int64_t rte_v128s64_t __attribute__((vector_size(16), aligned(16)));

/**
 * 256 bits vector size to use with 8 bits elements.
 *
 * a = (rte_v256s8_t){ a00, a01, a02, a03, a04, a05, a06, a07,
 *                     a08, a09, a10, a11, a12, a13, a14, a15,
 *                     a16, a17, a18, a19, a20, a21, a22, a23,
 *                     a24, a25, a26, a27, a28, a29, a30, a31 }
 */
typedef int8_t rte_v256s8_t __attribute__((vector_size(32), aligned(32)));

/**
 * 256 bits vector size to use with 16 bits elements.
 *
 * a = (rte_v256s16_t){ a00, a01, a02, a03, a04, a05, a06, a07,
 *                      a08, a09, a10, a11, a12, a13, a14, a15 }
 */
typedef int16_t rte_v256s16_t __attribute__((vector_size(32), aligned(32)));

/**
 * 256 bits vector size to use with 32 bits elements.
 *
 * a = (rte_v256s32_t){ a0, a1, a2, a3, a4, a5, a6, a7 }
 */
typedef int32_t rte_v256s32_t __attribute__((vector_size(32), aligned(32)));

/**
 * 256 bits vector size to use with 64 bits elements.
 *
 * a = (rte_v256s64_t){ a0, a1, a2, a3 }
 */
typedef int64_t rte_v256s64_t __attribute__((vector_size(32), aligned(32)));

/**
 * The max SIMD bitwidth value to limit vector path selection.
 */
enum rte_vect_max_simd {
	RTE_VECT_SIMD_DISABLED = 64,
	/**< Limits path selection to scalar, disables all vector paths. */
	RTE_VECT_SIMD_128 = 128,
	/**< Limits path selection to SSE/NEON/Altivec or below. */
	RTE_VECT_SIMD_256 = 256, /**< Limits path selection to AVX2 or below. */
	RTE_VECT_SIMD_512 = 512, /**< Limits path selection to AVX512 or below. */
	RTE_VECT_SIMD_MAX = INT16_MAX + 1,
	/**<
	 * Disables limiting by max SIMD bitwidth, allows all suitable paths.
	 * This value is used as it is a large number and a power of 2.
	 */
};

/**
 * @warning
 * @b EXPERIMENTAL: this API may change, or be removed, without prior notice
 *
 * Get the supported SIMD bitwidth.
 *
 * @return
 *   uint16_t bitwidth.
 */
__rte_experimental
uint16_t rte_vect_get_max_simd_bitwidth(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change, or be removed, without prior notice
 *
 * Set the supported SIMD bitwidth.
 * This API should only be called once at initialization, before EAL init.
 *
 * @param bitwidth
 *   uint16_t bitwidth.
 * @return
 *   - 0 on success.
 *   - -EINVAL on invalid bitwidth parameter.
 *   - -EPERM if bitwidth is forced.
 */
__rte_experimental
int rte_vect_set_max_simd_bitwidth(uint16_t bitwidth);

#endif /* _RTE_VECT_H_ */

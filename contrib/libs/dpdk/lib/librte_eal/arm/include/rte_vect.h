/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Cavium, Inc
 */

#ifndef _RTE_VECT_ARM_H_
#define _RTE_VECT_ARM_H_

#include <stdint.h>
#include "generic/rte_vect.h"
#include "rte_debug.h"
#include "arm_neon.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RTE_VECT_DEFAULT_SIMD_BITWIDTH RTE_VECT_SIMD_MAX

typedef int32x4_t xmm_t;

#define	XMM_SIZE	(sizeof(xmm_t))
#define	XMM_MASK	(XMM_SIZE - 1)

typedef union rte_xmm {
	xmm_t    x;
	uint8_t  u8[XMM_SIZE / sizeof(uint8_t)];
	uint16_t u16[XMM_SIZE / sizeof(uint16_t)];
	uint32_t u32[XMM_SIZE / sizeof(uint32_t)];
	uint64_t u64[XMM_SIZE / sizeof(uint64_t)];
	double   pd[XMM_SIZE / sizeof(double)];
} __rte_aligned(16) rte_xmm_t;

#if defined(RTE_ARCH_ARM) && defined(RTE_ARCH_32)
/* NEON intrinsic vqtbl1q_u8() is not supported in ARMv7-A(AArch32) */
static __inline uint8x16_t
vqtbl1q_u8(uint8x16_t a, uint8x16_t b)
{
	uint8_t i, pos;
	rte_xmm_t rte_a, rte_b, rte_ret;

	vst1q_u8(rte_a.u8, a);
	vst1q_u8(rte_b.u8, b);

	for (i = 0; i < 16; i++) {
		pos = rte_b.u8[i];
		if (pos < 16)
			rte_ret.u8[i] = rte_a.u8[pos];
		else
			rte_ret.u8[i] = 0;
	}

	return vld1q_u8(rte_ret.u8);
}

static inline uint16_t
vaddvq_u16(uint16x8_t a)
{
	uint32x4_t m = vpaddlq_u16(a);
	uint64x2_t n = vpaddlq_u32(m);
	uint64x1_t o = vget_low_u64(n) + vget_high_u64(n);

	return vget_lane_u32((uint32x2_t)o, 0);
}

#endif

#if (defined(RTE_ARCH_ARM) && defined(RTE_ARCH_32)) || \
(defined(RTE_ARCH_ARM64) && RTE_CC_IS_GNU && (GCC_VERSION < 70000))
/* NEON intrinsic vcopyq_laneq_u32() is not supported in ARMv7-A(AArch32)
 * On AArch64, this intrinsic is supported since GCC version 7.
 */
static inline uint32x4_t
vcopyq_laneq_u32(uint32x4_t a, const int lane_a,
		 uint32x4_t b, const int lane_b)
{
	return vsetq_lane_u32(vgetq_lane_u32(b, lane_b), a, lane_a);
}
#endif

#if defined(RTE_ARCH_ARM64)
#if RTE_CC_IS_GNU && (GCC_VERSION < 70000)

#if (GCC_VERSION < 40900)
typedef uint64_t poly64_t;
typedef uint64x2_t poly64x2_t;
typedef uint8_t poly128_t __attribute__((vector_size(16), aligned(16)));

static inline uint32x4_t
vceqzq_u32(uint32x4_t a)
{
	return (a == 0);
}
#endif

/* NEON intrinsic vreinterpretq_u64_p128() is supported since GCC version 7 */
static inline uint64x2_t
vreinterpretq_u64_p128(poly128_t x)
{
	return (uint64x2_t)x;
}

/* NEON intrinsic vreinterpretq_p64_u64() is supported since GCC version 7 */
static inline poly64x2_t
vreinterpretq_p64_u64(uint64x2_t x)
{
	return (poly64x2_t)x;
}

/* NEON intrinsic vgetq_lane_p64() is supported since GCC version 7 */
static inline poly64_t
vgetq_lane_p64(poly64x2_t x, const int lane)
{
	RTE_ASSERT(lane >= 0 && lane <= 1);

	poly64_t *p = (poly64_t *)&x;

	return p[lane];
}
#endif
#endif

/*
 * If (0 <= index <= 15), then call the ASIMD ext instruction on the
 * 128 bit regs v0 and v1 with the appropriate index.
 *
 * Else returns a zero vector.
 */
static inline uint8x16_t
vextract(uint8x16_t v0, uint8x16_t v1, const int index)
{
	switch (index) {
	case 0: return vextq_u8(v0, v1, 0);
	case 1: return vextq_u8(v0, v1, 1);
	case 2: return vextq_u8(v0, v1, 2);
	case 3: return vextq_u8(v0, v1, 3);
	case 4: return vextq_u8(v0, v1, 4);
	case 5: return vextq_u8(v0, v1, 5);
	case 6: return vextq_u8(v0, v1, 6);
	case 7: return vextq_u8(v0, v1, 7);
	case 8: return vextq_u8(v0, v1, 8);
	case 9: return vextq_u8(v0, v1, 9);
	case 10: return vextq_u8(v0, v1, 10);
	case 11: return vextq_u8(v0, v1, 11);
	case 12: return vextq_u8(v0, v1, 12);
	case 13: return vextq_u8(v0, v1, 13);
	case 14: return vextq_u8(v0, v1, 14);
	case 15: return vextq_u8(v0, v1, 15);
	}
	return vdupq_n_u8(0);
}

/**
 * Shifts right 128 bit register by specified number of bytes
 *
 * Value of shift parameter must be in range 0 - 16
 */
static inline uint64x2_t
vshift_bytes_right(uint64x2_t reg, const unsigned int shift)
{
	return vreinterpretq_u64_u8(vextract(
				vreinterpretq_u8_u64(reg),
				vdupq_n_u8(0),
				shift));
}

/**
 * Shifts left 128 bit register by specified number of bytes
 *
 * Value of shift parameter must be in range 0 - 16
 */
static inline uint64x2_t
vshift_bytes_left(uint64x2_t reg, const unsigned int shift)
{
	return vreinterpretq_u64_u8(vextract(
				vdupq_n_u8(0),
				vreinterpretq_u8_u64(reg),
				16 - shift));
}

#ifdef __cplusplus
}
#endif

#endif

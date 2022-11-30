/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Cavium, Inc
 */
/*
 * Reciprocal divide
 *
 * Used with permission from original authors
 *  Hannes Frederic Sowa and Daniel Borkmann
 *
 * This algorithm is based on the paper "Division by Invariant
 * Integers Using Multiplication" by Torbjörn Granlund and Peter
 * L. Montgomery.
 *
 * The assembler implementation from Agner Fog, which this code is
 * based on, can be found here:
 * http://www.agner.org/optimize/asmlib.zip
 *
 * This optimization for A/B is helpful if the divisor B is mostly
 * runtime invariant. The reciprocal of B is calculated in the
 * slow-path with reciprocal_value(). The fast-path can then just use
 * a much faster multiplication operation with a variable dividend A
 * to calculate the division A/B.
 */

#ifndef _RTE_RECIPROCAL_H_
#define _RTE_RECIPROCAL_H_

#include <stdint.h>

struct rte_reciprocal {
	uint32_t m;
	uint8_t sh1, sh2;
};

struct rte_reciprocal_u64 {
	uint64_t m;
	uint8_t sh1, sh2;
};

static inline uint32_t rte_reciprocal_divide(uint32_t a, struct rte_reciprocal R)
{
	uint32_t t = (uint32_t)(((uint64_t)a * R.m) >> 32);

	return (t + ((a - t) >> R.sh1)) >> R.sh2;
}

static __rte_always_inline uint64_t
mullhi_u64(uint64_t x, uint64_t y)
{
#ifdef __SIZEOF_INT128__
	__uint128_t xl = x;
	__uint128_t rl = xl * y;

	return (rl >> 64);
#else
	uint64_t u0, u1, v0, v1, k, t;
	uint64_t w1, w2;
	uint64_t whi;

	u1 = x >> 32; u0 = x & 0xFFFFFFFF;
	v1 = y >> 32; v0 = y & 0xFFFFFFFF;

	t = u0*v0;
	k = t >> 32;

	t = u1*v0 + k;
	w1 = t & 0xFFFFFFFF;
	w2 = t >> 32;

	t = u0*v1 + w1;
	k = t >> 32;

	whi = u1*v1 + w2 + k;

	return whi;
#endif
}

static __rte_always_inline uint64_t
rte_reciprocal_divide_u64(uint64_t a, const struct rte_reciprocal_u64 *R)
{
	uint64_t t = mullhi_u64(a, R->m);

	return (t + ((a - t) >> R->sh1)) >> R->sh2;
}

struct rte_reciprocal rte_reciprocal_value(uint32_t d);
struct rte_reciprocal_u64 rte_reciprocal_value_u64(uint64_t d);

#endif /* _RTE_RECIPROCAL_H_ */

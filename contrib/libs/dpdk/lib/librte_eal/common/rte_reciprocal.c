#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Cavium, Inc
 * Copyright(c) Hannes Frederic Sowa
 * All rights reserved.
 */

#include <stdio.h>
#include <stdint.h>

#include <rte_common.h>

#include "rte_reciprocal.h"

struct rte_reciprocal rte_reciprocal_value(uint32_t d)
{
	struct rte_reciprocal R;
	uint64_t m;
	int l;

	l = rte_fls_u32(d - 1);
	m = ((1ULL << 32) * ((1ULL << l) - d));
	m /= d;

	++m;
	R.m = m;
	R.sh1 = RTE_MIN(l, 1);
	R.sh2 = RTE_MAX(l - 1, 0);

	return R;
}

/*
 * Code taken from Hacker's Delight:
 * http://www.hackersdelight.org/hdcodetxt/divlu.c.txt
 * License permits inclusion here per:
 * http://www.hackersdelight.org/permissions.htm
 */
static uint64_t
divide_128_div_64_to_64(uint64_t u1, uint64_t u0, uint64_t v, uint64_t *r)
{
	const uint64_t b = (1ULL << 32); /* Number base (16 bits). */
	uint64_t un1, un0,           /* Norm. dividend LSD's. */
		 vn1, vn0,           /* Norm. divisor digits. */
		 q1, q0,             /* Quotient digits. */
		 un64, un21, un10,   /* Dividend digit pairs. */
		 rhat;               /* A remainder. */
	int s;                       /* Shift amount for norm. */

	/* If overflow, set rem. to an impossible value. */
	if (u1 >= v) {
		if (r != NULL)
			*r = (uint64_t) -1;
		return (uint64_t) -1;
	}

	/* Count leading zeros. */
	s = __builtin_clzll(v);
	if (s > 0) {
		v = v << s;
		un64 = (u1 << s) | ((u0 >> (64 - s)) & (-s >> 31));
		un10 = u0 << s;
	} else {

		un64 = u1 | u0;
		un10 = u0;
	}

	vn1 = v >> 32;
	vn0 = v & 0xFFFFFFFF;

	un1 = un10 >> 32;
	un0 = un10 & 0xFFFFFFFF;

	q1 = un64/vn1;
	rhat = un64 - q1*vn1;
again1:
	if (q1 >= b || q1*vn0 > b*rhat + un1) {
		q1 = q1 - 1;
		rhat = rhat + vn1;
		if (rhat < b)
			goto again1;
	}

	un21 = un64*b + un1 - q1*v;

	q0 = un21/vn1;
	rhat = un21 - q0*vn1;
again2:
	if (q0 >= b || q0*vn0 > b*rhat + un0) {
		q0 = q0 - 1;
		rhat = rhat + vn1;
		if (rhat < b)
			goto again2;
	}

	if (r != NULL)
		*r = (un21*b + un0 - q0*v) >> s;
	return q1*b + q0;
}

struct rte_reciprocal_u64
rte_reciprocal_value_u64(uint64_t d)
{
	struct rte_reciprocal_u64 R;
	uint64_t m;
	uint64_t r;
	int l;

	l = 63 - __builtin_clzll(d);

	m = divide_128_div_64_to_64((1ULL << l), 0, d, &r) << 1;
	if (r << 1 < r || r << 1 >= d)
		m++;
	m = (1ULL << l) - d ? m + 1 : 1;
	R.m = m;

	R.sh1 = l > 1 ? 1 : l;
	R.sh2 = (l > 0) ? l : 0;
	R.sh2 -= R.sh2 && (m == 1) ? 1 : 0;

	return R;
}

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Intel Corporation
 */

#ifndef _RTE_TIME_H_
#define _RTE_TIME_H_

#include <stdint.h>
#include <time.h>

#define NSEC_PER_SEC             1000000000L

/**
 * Structure to hold the parameters of a running cycle counter to assist
 * in converting cycles to nanoseconds.
 */
struct rte_timecounter {
	/** Last cycle counter value read. */
	uint64_t cycle_last;
	/** Nanoseconds count. */
	uint64_t nsec;
	/** Bitmask separating nanosecond and sub-nanoseconds. */
	uint64_t nsec_mask;
	/** Sub-nanoseconds count. */
	uint64_t nsec_frac;
	/** Bitmask for two's complement subtraction of non-64 bit counters. */
	uint64_t cc_mask;
	/** Cycle to nanosecond divisor (power of two). */
	uint32_t cc_shift;
};

/**
 * Converts cyclecounter cycles to nanoseconds.
 */
static inline uint64_t
rte_cyclecounter_cycles_to_ns(struct rte_timecounter *tc, uint64_t cycles)
{
	uint64_t ns;

	/* Add fractional nanoseconds. */
	ns = cycles + tc->nsec_frac;
	tc->nsec_frac = ns & tc->nsec_mask;

	/* Shift to get only nanoseconds. */
	return ns >> tc->cc_shift;
}

/**
 * Update the internal nanosecond count in the structure.
 */
static inline uint64_t
rte_timecounter_update(struct rte_timecounter *tc, uint64_t cycle_now)
{
	uint64_t cycle_delta, ns_offset;

	/* Calculate the delta since the last call. */
	if (tc->cycle_last <= cycle_now)
		cycle_delta = (cycle_now - tc->cycle_last) & tc->cc_mask;
	else
		/* Handle cycle counts that have wrapped around . */
		cycle_delta = (~(tc->cycle_last - cycle_now) & tc->cc_mask) + 1;

	/* Convert to nanoseconds. */
	ns_offset = rte_cyclecounter_cycles_to_ns(tc, cycle_delta);

	/* Store current cycle counter for next call. */
	tc->cycle_last = cycle_now;

	/* Update the nanosecond count. */
	tc->nsec += ns_offset;

	return tc->nsec;
}

/**
 * Convert from timespec structure into nanosecond units.
 */
static inline uint64_t
rte_timespec_to_ns(const struct timespec *ts)
{
	return ((uint64_t) ts->tv_sec * NSEC_PER_SEC) + ts->tv_nsec;
}

/**
 * Convert from nanosecond units into timespec structure.
 */
static inline struct timespec
rte_ns_to_timespec(uint64_t nsec)
{
	struct timespec ts = {0, 0};

	if (nsec == 0)
		return ts;

	ts.tv_sec = nsec / NSEC_PER_SEC;
	ts.tv_nsec = nsec % NSEC_PER_SEC;

	return ts;
}

#endif /* _RTE_TIME_H_ */

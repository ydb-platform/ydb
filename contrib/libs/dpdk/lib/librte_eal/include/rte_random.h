/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_RANDOM_H_
#define _RTE_RANDOM_H_

/**
 * @file
 *
 * Pseudo-random Generators in RTE
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#include <rte_compat.h>

/**
 * Seed the pseudo-random generator.
 *
 * The generator is automatically seeded by the EAL init with a timer
 * value. It may need to be re-seeded by the user with a real random
 * value.
 *
 * This function is not multi-thread safe in regards to other
 * rte_srand() calls, nor is it in relation to concurrent rte_rand()
 * calls.
 *
 * @param seedval
 *   The value of the seed.
 */
void
rte_srand(uint64_t seedval);

/**
 * Get a pseudo-random value.
 *
 * The generator is not cryptographically secure.
 *
 * If called from lcore threads, this function is thread-safe.
 *
 * @return
 *   A pseudo-random value between 0 and (1<<64)-1.
 */
uint64_t
rte_rand(void);

/**
 * Generates a pseudo-random number with an upper bound.
 *
 * This function returns an uniformly distributed (unbiased) random
 * number less than a user-specified maximum value.
 *
 * If called from lcore threads, this function is thread-safe.
 *
 * @param upper_bound
 *   The upper bound of the generated number.
 * @return
 *   A pseudo-random value between 0 and (upper_bound-1).
 */
__rte_experimental
uint64_t
rte_rand_max(uint64_t upper_bound);

#ifdef __cplusplus
}
#endif


#endif /* _RTE_RANDOM_H_ */

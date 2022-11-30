/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
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

/**
 * \file
 * CPU set management functions
 */

#ifndef SPDK_CPUSET_H
#define SPDK_CPUSET_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_CPUSET_SIZE 1024

/**
 * List of CPUs.
 */
struct spdk_cpuset {
	char str[SPDK_CPUSET_SIZE / 4 + 1];
	uint8_t cpus[SPDK_CPUSET_SIZE / 8];
};

/**
 * Allocate CPU set object.
 *
 * \return a pointer to the allocated zeroed cpuset on success, or NULL on failure.
 */
struct spdk_cpuset *spdk_cpuset_alloc(void);

/**
 * Free allocated CPU set.
 *
 * \param set CPU set to be freed.
 */
void spdk_cpuset_free(struct spdk_cpuset *set);

/**
 * Compare two CPU sets.
 *
 * \param set1 CPU set1.
 * \param set2 CPU set2.
 *
 * \return true if both CPU sets are equal.
 */
bool spdk_cpuset_equal(const struct spdk_cpuset *set1, const struct spdk_cpuset *set2);

/**
 * Copy the content of CPU set to another.
 *
 * \param dst Destination CPU set
 * \param src Source CPU set
 */
void spdk_cpuset_copy(struct spdk_cpuset *dst, const struct spdk_cpuset *src);

/**
 * Perform AND operation on two CPU sets. The result is stored in dst.
 *
 * \param dst First argument of operation. This value also stores the result of operation.
 * \param src Second argument of operation.
 */
void spdk_cpuset_and(struct spdk_cpuset *dst, const struct spdk_cpuset *src);

/**
 * Perform OR operation on two CPU sets. The result is stored in dst.
 *
 * \param dst First argument of operation. This value also stores the result of operation.
 * \param src Second argument of operation.
 */
void spdk_cpuset_or(struct spdk_cpuset *dst, const struct spdk_cpuset *src);

/**
 * Perform XOR operation on two CPU sets. The result is stored in dst.
 *
 * \param dst First argument of operation. This value also stores the result of operation.
 * \param src Second argument of operation.
 */
void spdk_cpuset_xor(struct spdk_cpuset *dst, const struct spdk_cpuset *src);

/**
 * Negate all CPUs in CPU set.
 *
 * \param set CPU set to be negated. This value also stores the result of operation.
 */
void spdk_cpuset_negate(struct spdk_cpuset *set);

/**
 * Clear all CPUs in CPU set.
 *
 * \param set CPU set to be cleared.
 */
void spdk_cpuset_zero(struct spdk_cpuset *set);

/**
 * Set or clear CPU state in CPU set.
 *
 * \param set CPU set object.
 * \param cpu CPU index to be set or cleared.
 * \param state *true* to set cpu, *false* to clear.
 */
void spdk_cpuset_set_cpu(struct spdk_cpuset *set, uint32_t cpu, bool state);

/**
 * Get the state of CPU in CPU set.
 *
 * \param set CPU set object.
 * \param cpu CPU index.
 *
 * \return the state of selected CPU.
 */
bool spdk_cpuset_get_cpu(const struct spdk_cpuset *set, uint32_t cpu);

/**
 * Get the number of CPUs that are set in CPU set.
 *
 * \param set CPU set object.
 *
 * \return the number of CPUs.
 */
uint32_t spdk_cpuset_count(const struct spdk_cpuset *set);

/**
 * Convert a CPU set to hex string.
 *
 * \param set CPU set.
 *
 * \return a pointer to hexadecimal representation of CPU set. Buffer to store a
 * string is dynamically allocated internally and freed with CPU set object.
 * Memory returned by this function might be changed after subsequent calls to
 * this function so string should be copied by user.
 */
const char *spdk_cpuset_fmt(struct spdk_cpuset *set);

/**
 * Convert a string containing a CPU core mask into a CPU set.
 *
 * \param set CPU set.
 * \param mask String defining CPU set. By default hexadecimal value is used or
 * as CPU list enclosed in square brackets defined as: 'c1[-c2][,c3[-c4],...]'.
 *
 * \return zero if success, non zero if fails.
 */
int spdk_cpuset_parse(struct spdk_cpuset *set, const char *mask);

#ifdef __cplusplus
}
#endif
#endif /* SPDK_CPUSET_H */

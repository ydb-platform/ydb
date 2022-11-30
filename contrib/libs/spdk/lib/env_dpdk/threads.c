#include <contrib/libs/spdk/ndebug.h>
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

#include "env_internal.h"

#include <rte_config.h>
#include <rte_lcore.h>

uint32_t
spdk_env_get_core_count(void)
{
	return rte_lcore_count();
}

uint32_t
spdk_env_get_current_core(void)
{
	return rte_lcore_id();
}

uint32_t
spdk_env_get_first_core(void)
{
	return rte_get_next_lcore(-1, 0, 0);
}

uint32_t
spdk_env_get_last_core(void)
{
	uint32_t i;
	uint32_t last_core = UINT32_MAX;

	SPDK_ENV_FOREACH_CORE(i) {
		last_core = i;
	}

	assert(last_core != UINT32_MAX);

	return last_core;
}

uint32_t
spdk_env_get_next_core(uint32_t prev_core)
{
	unsigned lcore;

	lcore = rte_get_next_lcore(prev_core, 0, 0);
	if (lcore == RTE_MAX_LCORE) {
		return UINT32_MAX;
	}
	return lcore;
}

uint32_t
spdk_env_get_socket_id(uint32_t core)
{
	if (core >= RTE_MAX_LCORE) {
		return SPDK_ENV_SOCKET_ID_ANY;
	}

	return rte_lcore_to_socket_id(core);
}

int
spdk_env_thread_launch_pinned(uint32_t core, thread_start_fn fn, void *arg)
{
	int rc;

	rc = rte_eal_remote_launch(fn, arg, core);

	return rc;
}

void
spdk_env_thread_wait_all(void)
{
	rte_eal_mp_wait_lcore();
}

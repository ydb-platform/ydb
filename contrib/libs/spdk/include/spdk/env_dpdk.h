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

/** \file
 * Encapsulated DPDK specific dependencies
 */

#include "spdk/stdinc.h"

#ifndef SPDK_ENV_DPDK_H
#define SPDK_ENV_DPDK_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize the environment library after DPDK env is already initialized.
 * If DPDK's rte_eal_init is already called, this function must be called
 * instead of spdk_env_init, prior to using any other functions in SPDK
 * env library.
 *
 * \param legacy_mem Indicates whether DPDK was initialized with --legacy-mem
 *                   eal parameter.
 * \return 0 on success, or negative errno on failure.
 */
int spdk_env_dpdk_post_init(bool legacy_mem);

/**
 * Release any resources of the environment library that were alllocated with
 * spdk_env_dpdk_post_init(). After this call, no DPDK function calls may
 * be made. It is expected that common usage of this function is to call it
 * just before terminating the process.
 */
void spdk_env_dpdk_post_fini(void);

/**
 * Check if DPDK was initialized external to the SPDK env_dpdk library.
 *
 * \return true if DPDK was initialized external to the SPDK env_dpdk library.
 * \return false otherwise
 */
bool spdk_env_dpdk_external_init(void);

/**
 * Dump the env allocated memory to the given file.
 *
 * \param file The file object to write to.
 */
void spdk_env_dpdk_dump_mem_stats(FILE *file);

#ifdef __cplusplus
}
#endif

#endif

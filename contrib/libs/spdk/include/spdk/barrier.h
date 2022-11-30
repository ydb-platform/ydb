/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   Copyright (c) 2017, IBM Corporation.
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
 * Memory barriers
 */

#ifndef SPDK_BARRIER_H
#define SPDK_BARRIER_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Compiler memory barrier */
#define spdk_compiler_barrier() __asm volatile("" ::: "memory")

/** Read memory barrier */
#define spdk_rmb()	_spdk_rmb()

/** Write memory barrier */
#define spdk_wmb()	_spdk_wmb()

/** Full read/write memory barrier */
#define spdk_mb()	_spdk_mb()

/** SMP read memory barrier. */
#define spdk_smp_rmb()	_spdk_smp_rmb()

/** SMP write memory barrier. */
#define spdk_smp_wmb()	_spdk_smp_wmb()

/** SMP read/write memory barrier. */
#define spdk_smp_mb()	_spdk_smp_mb()

#ifdef __PPC64__

#define _spdk_rmb()	__asm volatile("sync" ::: "memory")
#define _spdk_wmb()	__asm volatile("sync" ::: "memory")
#define _spdk_mb()	__asm volatile("sync" ::: "memory")
#define _spdk_smp_rmb()	__asm volatile("lwsync" ::: "memory")
#define _spdk_smp_wmb()	__asm volatile("lwsync" ::: "memory")
#define _spdk_smp_mb()	spdk_mb()

#elif defined(__aarch64__)

#define _spdk_rmb()	__asm volatile("dsb ld" ::: "memory")
#define _spdk_wmb()	__asm volatile("dsb st" ::: "memory")
#define _spdk_mb()	__asm volatile("dsb sy" ::: "memory")
#define _spdk_smp_rmb()	__asm volatile("dmb ishld" ::: "memory")
#define _spdk_smp_wmb()	__asm volatile("dmb ishst" ::: "memory")
#define _spdk_smp_mb()	__asm volatile("dmb ish" ::: "memory")

#elif defined(__i386__) || defined(__x86_64__)

#define _spdk_rmb()	__asm volatile("lfence" ::: "memory")
#define _spdk_wmb()	__asm volatile("sfence" ::: "memory")
#define _spdk_mb()	__asm volatile("mfence" ::: "memory")
#define _spdk_smp_rmb()	spdk_compiler_barrier()
#define _spdk_smp_wmb()	spdk_compiler_barrier()
#if defined(__x86_64__)
#define _spdk_smp_mb()	__asm volatile("lock addl $0, -128(%%rsp); " ::: "memory");
#elif defined(__i386__)
#define _spdk_smp_mb()	__asm volatile("lock addl $0, -128(%%esp); " ::: "memory");
#endif

#else

#define _spdk_rmb()
#define _spdk_wmb()
#define _spdk_mb()
#define _spdk_smp_rmb()
#define _spdk_smp_wmb()
#define _spdk_smp_mb()
#error Unknown architecture

#endif

#ifdef __cplusplus
}
#endif

#endif

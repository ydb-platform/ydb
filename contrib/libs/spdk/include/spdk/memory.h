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

#ifndef SPDK_MEMORY_H
#define SPDK_MEMORY_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SHIFT_2MB		21 /* (1 << 21) == 2MB */
#define VALUE_2MB		(1ULL << SHIFT_2MB)
#define MASK_2MB		(VALUE_2MB - 1)

#define SHIFT_4KB		12 /* (1 << 12) == 4KB */
#define VALUE_4KB		(1ULL << SHIFT_4KB)
#define MASK_4KB		(VALUE_4KB - 1)

#define _2MB_OFFSET(ptr)	(((uintptr_t)(ptr)) & MASK_2MB)
#define _2MB_PAGE(ptr)		FLOOR_2MB((uintptr_t)(ptr))
#define FLOOR_2MB(x)		(((uintptr_t)(x)) & ~MASK_2MB)
#define CEIL_2MB(x)		FLOOR_2MB(((uintptr_t)(x)) + VALUE_2MB - 1)

#ifdef __cplusplus
}
#endif

#endif /* SPDK_MEMORY_H */

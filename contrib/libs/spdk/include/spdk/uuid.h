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
 * UUID types and functions
 */

#ifndef SPDK_UUID_H
#define SPDK_UUID_H

#include "spdk/stdinc.h"

#include "spdk/assert.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_uuid {
	union {
		uint8_t raw[16];
	} u;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_uuid) == 16, "Incorrect size");

#define SPDK_UUID_STRING_LEN 37 /* 36 characters + null terminator */

/**
 * Convert UUID in textual format into a spdk_uuid.
 *
 * \param[out] uuid User-provided UUID buffer.
 * \param uuid_str UUID in textual format in C string.
 *
 * \return 0 on success, or negative errno on failure.
 */
int spdk_uuid_parse(struct spdk_uuid *uuid, const char *uuid_str);

/**
 * Convert UUID in spdk_uuid into lowercase textual format.
 *
 * \param uuid_str User-provided string buffer to write the textual format into.
 * \param uuid_str_size Size of uuid_str buffer. Must be at least SPDK_UUID_STRING_LEN.
 * \param uuid UUID to convert to textual format.
 *
 * \return 0 on success, or negative errno on failure.
 */
int spdk_uuid_fmt_lower(char *uuid_str, size_t uuid_str_size, const struct spdk_uuid *uuid);

/**
 * Compare two UUIDs.
 *
 * \param u1 UUID 1.
 * \param u2 UUID 2.
 *
 * \return 0 if u1 == u2, less than 0 if u1 < u2, greater than 0 if u1 > u2.
 */
int spdk_uuid_compare(const struct spdk_uuid *u1, const struct spdk_uuid *u2);

/**
 * Generate a new UUID.
 *
 * \param[out] uuid User-provided UUID buffer to fill.
 */
void spdk_uuid_generate(struct spdk_uuid *uuid);

/**
 * Copy a UUID.
 *
 * \param src Source UUID to copy from.
 * \param dst Destination UUID to store.
 */
void spdk_uuid_copy(struct spdk_uuid *dst, const struct spdk_uuid *src);

#ifdef __cplusplus
}
#endif

#endif

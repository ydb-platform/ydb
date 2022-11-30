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

#include "spdk/uuid.h"

#include <uuid/uuid.h>

SPDK_STATIC_ASSERT(sizeof(struct spdk_uuid) == sizeof(uuid_t), "Size mismatch");

int
spdk_uuid_parse(struct spdk_uuid *uuid, const char *uuid_str)
{
	return uuid_parse(uuid_str, (void *)uuid) == 0 ? 0 : -EINVAL;
}

int
spdk_uuid_fmt_lower(char *uuid_str, size_t uuid_str_size, const struct spdk_uuid *uuid)
{
	if (uuid_str_size < SPDK_UUID_STRING_LEN) {
		return -EINVAL;
	}

	uuid_unparse_lower((void *)uuid, uuid_str);
	return 0;
}

int
spdk_uuid_compare(const struct spdk_uuid *u1, const struct spdk_uuid *u2)
{
	return uuid_compare((void *)u1, (void *)u2);
}

void
spdk_uuid_generate(struct spdk_uuid *uuid)
{
	uuid_generate((void *)uuid);
}

void
spdk_uuid_copy(struct spdk_uuid *dst, const struct spdk_uuid *src)
{
	uuid_copy((void *)dst, (void *)src);
}

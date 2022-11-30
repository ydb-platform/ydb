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

#include "spdk/util.h"

size_t
spdk_iovcpy(struct iovec *siov, size_t siovcnt, struct iovec *diov, size_t diovcnt)
{
	size_t total_sz;
	size_t sidx;
	size_t didx;
	int siov_len;
	uint8_t *siov_base;
	int diov_len;
	uint8_t *diov_base;

	/* d prefix = destination. s prefix = source. */

	assert(diovcnt > 0);
	assert(siovcnt > 0);

	total_sz = 0;
	sidx = 0;
	didx = 0;
	siov_len = siov[0].iov_len;
	siov_base = siov[0].iov_base;
	diov_len = diov[0].iov_len;
	diov_base = diov[0].iov_base;
	while (siov_len > 0 && diov_len > 0) {
		if (siov_len == diov_len) {
			memcpy(diov_base, siov_base, siov_len);
			total_sz += siov_len;

			/* Advance both iovs to the next element */
			sidx++;
			if (sidx == siovcnt) {
				break;
			}

			didx++;
			if (didx == diovcnt) {
				break;
			}

			siov_len = siov[sidx].iov_len;
			siov_base = siov[sidx].iov_base;
			diov_len = diov[didx].iov_len;
			diov_base = diov[didx].iov_base;
		} else if (siov_len < diov_len) {
			memcpy(diov_base, siov_base, siov_len);
			total_sz += siov_len;

			/* Advance only the source to the next element */
			sidx++;
			if (sidx == siovcnt) {
				break;
			}

			diov_base += siov_len;
			diov_len -= siov_len;
			siov_len = siov[sidx].iov_len;
			siov_base = siov[sidx].iov_base;
		} else {
			memcpy(diov_base, siov_base, diov_len);
			total_sz += diov_len;

			/* Advance only the destination to the next element */
			didx++;
			if (didx == diovcnt) {
				break;
			}

			siov_base += diov_len;
			siov_len -= diov_len;
			diov_len = diov[didx].iov_len;
			diov_base = diov[didx].iov_base;
		}
	}

	return total_sz;
}

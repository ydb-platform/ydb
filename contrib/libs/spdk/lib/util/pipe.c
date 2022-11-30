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

#include "spdk/pipe.h"
#include "spdk/util.h"

struct spdk_pipe {
	uint8_t	*buf;
	uint32_t sz;

	uint32_t write;
	uint32_t read;
};

struct spdk_pipe *
spdk_pipe_create(void *buf, uint32_t sz)
{
	struct spdk_pipe *pipe;

	pipe = calloc(1, sizeof(*pipe));
	if (pipe == NULL) {
		return NULL;
	}

	pipe->buf = buf;
	pipe->sz = sz;

	return pipe;
}

void
spdk_pipe_destroy(struct spdk_pipe *pipe)
{
	free(pipe);
}

int
spdk_pipe_writer_get_buffer(struct spdk_pipe *pipe, uint32_t requested_sz, struct iovec *iovs)
{
	uint32_t sz;
	uint32_t read;
	uint32_t write;

	read = pipe->read;
	write = pipe->write;

	if (read <= write) {
		requested_sz = spdk_min(requested_sz, ((read + pipe->sz) - write - 1));

		sz = spdk_min(requested_sz, pipe->sz - write);

		iovs[0].iov_base = (sz == 0) ? NULL : (pipe->buf + write);
		iovs[0].iov_len = sz;

		requested_sz -= sz;

		if (requested_sz > 0) {
			sz = spdk_min(requested_sz, read);

			iovs[1].iov_base = (sz == 0) ? NULL : pipe->buf;
			iovs[1].iov_len = sz;
		} else {
			iovs[1].iov_base = NULL;
			iovs[1].iov_len = 0;
		}
	} else {
		sz = spdk_min(requested_sz, read - write - 1);

		iovs[0].iov_base = (sz == 0) ? NULL : (pipe->buf + write);
		iovs[0].iov_len = sz;
		iovs[1].iov_base = NULL;
		iovs[1].iov_len = 0;
	}

	return iovs[0].iov_len + iovs[1].iov_len;
}

int
spdk_pipe_writer_advance(struct spdk_pipe *pipe, uint32_t requested_sz)
{
	uint32_t sz;
	uint32_t read;
	uint32_t write;

	read = pipe->read;
	write = pipe->write;

	if (requested_sz > pipe->sz - 1) {
		return -EINVAL;
	}

	if (read <= write) {
		if (requested_sz > (read + pipe->sz) - write) {
			return -EINVAL;
		}

		sz = spdk_min(requested_sz, pipe->sz - write);

		write += sz;
		if (write > pipe->sz - 1) {
			write = 0;
		}
		requested_sz -= sz;

		if (requested_sz > 0) {
			if (requested_sz >= read) {
				return -EINVAL;
			}

			write = requested_sz;
		}
	} else {
		if (requested_sz > (read - write - 1)) {
			return -EINVAL;
		}

		write += requested_sz;
	}

	pipe->write = write;

	return 0;
}

uint32_t
spdk_pipe_reader_bytes_available(struct spdk_pipe *pipe)
{
	uint32_t read;
	uint32_t write;

	read = pipe->read;
	write = pipe->write;

	if (read <= write) {
		return write - read;
	}

	return (write + pipe->sz) - read;
}

int
spdk_pipe_reader_get_buffer(struct spdk_pipe *pipe, uint32_t requested_sz, struct iovec *iovs)
{
	uint32_t sz;
	uint32_t read;
	uint32_t write;

	read = pipe->read;
	write = pipe->write;

	if (read <= write) {
		sz = spdk_min(requested_sz, write - read);

		iovs[0].iov_base = (sz == 0) ? NULL : (pipe->buf + read);
		iovs[0].iov_len = sz;
		iovs[1].iov_base = NULL;
		iovs[1].iov_len = 0;
	} else {
		sz = spdk_min(requested_sz, pipe->sz - read);

		iovs[0].iov_base = (sz == 0) ? NULL : (pipe->buf + read);
		iovs[0].iov_len = sz;

		requested_sz -= sz;

		if (requested_sz > 0) {
			sz = spdk_min(requested_sz, write);
			iovs[1].iov_base = (sz == 0) ? NULL : pipe->buf;
			iovs[1].iov_len = sz;
		} else {
			iovs[1].iov_base = NULL;
			iovs[1].iov_len = 0;
		}
	}

	return iovs[0].iov_len + iovs[1].iov_len;
}

int
spdk_pipe_reader_advance(struct spdk_pipe *pipe, uint32_t requested_sz)
{
	uint32_t sz;
	uint32_t read;
	uint32_t write;

	read = pipe->read;
	write = pipe->write;

	if (read <= write) {
		if (requested_sz > (write - read)) {
			return -EINVAL;
		}

		read += requested_sz;
	} else {
		sz = spdk_min(requested_sz, pipe->sz - read);

		read += sz;
		if (read > pipe->sz - 1) {
			read = 0;
		}
		requested_sz -= sz;

		if (requested_sz > 0) {
			if (requested_sz > write) {
				return -EINVAL;
			}

			read = requested_sz;
		}
	}

	pipe->read = read;

	return 0;
}

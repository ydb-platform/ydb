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

#include "spdk/stdinc.h"

#include "spdk/fd.h"

#ifdef __linux__
#include <linux/fs.h>
#endif

static uint64_t
dev_get_size(int fd)
{
#if defined(DIOCGMEDIASIZE) /* FreeBSD */
	off_t size;

	if (ioctl(fd, DIOCGMEDIASIZE, &size) == 0) {
		return size;
	}
#elif defined(__linux__) && defined(BLKGETSIZE64)
	uint64_t size;

	if (ioctl(fd, BLKGETSIZE64, &size) == 0) {
		return size;
	}
#endif

	return 0;
}

uint32_t
spdk_fd_get_blocklen(int fd)
{
#if defined(DKIOCGETBLOCKSIZE) /* FreeBSD */
	uint32_t blocklen;

	if (ioctl(fd, DKIOCGETBLOCKSIZE, &blocklen) == 0) {
		return blocklen;
	}
#elif defined(__linux__) && defined(BLKSSZGET)
	uint32_t blocklen;

	if (ioctl(fd, BLKSSZGET, &blocklen) == 0) {
		return blocklen;
	}
#endif

	return 0;
}

uint64_t
spdk_fd_get_size(int fd)
{
	struct stat st;

	if (fstat(fd, &st) != 0) {
		return 0;
	}

	if (S_ISLNK(st.st_mode)) {
		return 0;
	}

	if (S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode)) {
		return dev_get_size(fd);
	} else if (S_ISREG(st.st_mode)) {
		return st.st_size;
	}

	/* Not REG, CHR or BLK */
	return 0;
}

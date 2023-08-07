#include "../config-host.h"
/* SPDX-License-Identifier: GPL-2.0-or-later */
/*
 * repro-CVE-2020-29373 -- Reproducer for CVE-2020-29373.
 *
 * Copyright (c) 2021 SUSE
 * Author: Nicolai Stange <nstange@suse.de>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "liburing.h"

/*
 * This attempts to make the kernel issue a sendmsg() to
 * path from io_uring's async io_sq_wq_submit_work().
 *
 * Unfortunately, IOSQE_ASYNC is available only from kernel version
 * 5.6 onwards. To still force io_uring to process the request
 * asynchronously from io_sq_wq_submit_work(), queue a couple of
 * auxiliary requests all failing with EAGAIN before. This is
 * implemented by writing repeatedly to an auxiliary O_NONBLOCK
 * AF_UNIX socketpair with a small SO_SNDBUF.
 */
static int try_sendmsg_async(const char * const path)
{
	int snd_sock, r;
	struct io_uring ring;
	char sbuf[16] = {};
	struct iovec siov = { .iov_base = &sbuf, .iov_len = sizeof(sbuf) };
	struct sockaddr_un addr = {};
	struct msghdr msg = {
		.msg_name = &addr,
		.msg_namelen = sizeof(addr),
		.msg_iov = &siov,
		.msg_iovlen = 1,
	};
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;

	snd_sock = socket(AF_UNIX, SOCK_DGRAM, 0);
	if (snd_sock < 0) {
		perror("socket(AF_UNIX)");
		return -1;
	}

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, path);

	r = io_uring_queue_init(512, &ring, 0);
	if (r < 0) {
		fprintf(stderr, "ring setup failed: %d\n", r);
		goto close_iour;
	}

	sqe = io_uring_get_sqe(&ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		r = -EFAULT;
		goto close_iour;
	}

	/* the actual one supposed to fail with -ENOENT. */
	io_uring_prep_sendmsg(sqe, snd_sock, &msg, 0);
	sqe->flags = IOSQE_ASYNC;
	sqe->user_data = 255;

	r = io_uring_submit(&ring);
	if (r != 1) {
		fprintf(stderr, "sqe submit failed: %d\n", r);
		r = -EFAULT;
		goto close_iour;
	}

	r = io_uring_wait_cqe(&ring, &cqe);
	if (r < 0) {
		fprintf(stderr, "wait completion %d\n", r);
		r = -EFAULT;
		goto close_iour;
	}
	if (cqe->user_data != 255) {
		fprintf(stderr, "user data %d\n", r);
		r = -EFAULT;
		goto close_iour;
	}
	if (cqe->res != -ENOENT) {
		r = 3;
		fprintf(stderr,
			"error: cqe %i: res=%i, but expected -ENOENT\n",
			(int)cqe->user_data, (int)cqe->res);
	}
	io_uring_cqe_seen(&ring, cqe);

close_iour:
	io_uring_queue_exit(&ring);
	close(snd_sock);
	return r;
}

int main(int argc, char *argv[])
{
	int r;
	char tmpdir[] = "/tmp/tmp.XXXXXX";
	int rcv_sock;
	struct sockaddr_un addr = {};
	pid_t c;
	int wstatus;

	if (!mkdtemp(tmpdir)) {
		perror("mkdtemp()");
		return 1;
	}

	rcv_sock = socket(AF_UNIX, SOCK_DGRAM, 0);
	if (rcv_sock < 0) {
		perror("socket(AF_UNIX)");
		r = 1;
		goto rmtmpdir;
	}

	addr.sun_family = AF_UNIX;
	snprintf(addr.sun_path, sizeof(addr.sun_path), "%s/sock", tmpdir);

	r = bind(rcv_sock, (struct sockaddr *)&addr,
		 sizeof(addr));
	if (r < 0) {
		perror("bind()");
		close(rcv_sock);
		r = 1;
		goto rmtmpdir;
	}

	c = fork();
	if (!c) {
		close(rcv_sock);

		r = chroot(tmpdir);
		if (r) {
			if (errno == EPERM) {
				fprintf(stderr, "chroot not allowed, skip\n");
				return 0;
			}

			perror("chroot()");
			return 1;
		}

		r = try_sendmsg_async(addr.sun_path);
		if (r < 0) {
			/* system call failure */
			r = 1;
		} else if (r) {
			/* test case failure */
			r += 1;
		}
		return r;
	}

	if (waitpid(c, &wstatus, 0) == (pid_t)-1) {
		perror("waitpid()");
		r = 1;
		goto rmsock;
	}
	if (!WIFEXITED(wstatus)) {
		fprintf(stderr, "child got terminated\n");
		r = 1;
		goto rmsock;
	}
	r = WEXITSTATUS(wstatus);
	if (r)
		fprintf(stderr, "error: Test failed\n");
rmsock:
	close(rcv_sock);
	unlink(addr.sun_path);
rmtmpdir:
	rmdir(tmpdir);
	return r;
}

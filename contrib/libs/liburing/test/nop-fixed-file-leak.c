#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test for a struct file reference leak in IORING_OP_NOP.
 *
 * When an SQE carries IOSQE_FIXED_FILE (which sets REQ_F_FIXED_FILE at
 * prep time) together with IORING_NOP_FILE but *without*
 * IORING_NOP_FIXED_FILE, io_nop() takes a normal file reference via
 * io_file_get_normal(), but io_put_file() won't drop the ref as
 * REQ_F_FIXED_FILE is set.
 *
 * Reported-by: syzbot+2cd473471e77bda12b0e@syzkaller.appspotmail.com
 * Fixes: a85f31052bce ("io_uring/nop: add support to fixed and normal files")
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <string.h>

#include "liburing.h"
#include "helpers.h"

#ifndef IORING_NOP_FILE
#define IORING_NOP_FILE		(1U << 1)
#endif
#ifndef IORING_NOP_FIXED_FILE
#define IORING_NOP_FIXED_FILE	(1U << 2)
#endif

int main(int argc, char *argv[])
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int pipefd[2];
	char c = 'x';
	ssize_t n;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	/* don't die on the EPIPE write in the non-buggy case */
	signal(SIGPIPE, SIG_IGN);

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (pipe(pipefd) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FILE;
	sqe->fd = pipefd[0];
	sqe->flags |= IOSQE_FIXED_FILE;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	/*
	 * Drop our own reference on the read end. If the kernel leaked the
	 * NOP's reference, the read end stays alive and the write below
	 * succeeds. If correct, the read end is gone and we get -EPIPE.
	 */
	close(pipefd[0]);

	n = write(pipefd[1], &c, 1);
	if (n == 1) {
		fprintf(stderr, "BUG: write to pipe with closed read end "
			"succeeded -- NOP leaked a file reference\n");
		ret = T_EXIT_FAIL;
	} else if (n < 0 && errno == EPIPE) {
		ret = T_EXIT_PASS;
	} else {
		fprintf(stderr, "unexpected write result: n=%zd errno=%d\n",
			n, errno);
		ret = T_EXIT_FAIL;
	}

	close(pipefd[1]);
	io_uring_queue_exit(&ring);
	return ret;
}

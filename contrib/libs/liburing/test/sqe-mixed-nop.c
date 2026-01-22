#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: run various nop tests
 *
 */
#include <stdio.h>

#include "liburing.h"
#include "helpers.h"
#include "test.h"

static int seq;

static int test_single_nop(struct io_uring *ring, bool sqe128)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	if (sqe128)
		sqe = io_uring_get_sqe128(ring);
	else
		sqe = io_uring_get_sqe(ring);

	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	if (sqe128)
		io_uring_prep_nop128(sqe);
	else
		io_uring_prep_nop(sqe);

	sqe->user_data = ++seq;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
	} else if (cqe->res != 0) {
		fprintf(stderr, "Completion error:%d\n", cqe->res);
	} else if (cqe->user_data != seq) {
		fprintf(stderr, "Unexpected user_data: %ld\n", (long) cqe->user_data);
	} else {
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_PASS;
	}
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_SQE_MIXED);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* alternate big and little sqe's */
	for (i = 0; i < 32; i++) {
		ret = test_single_nop(&ring, i & 1);
		if (ret != T_EXIT_PASS)
			break;
	}

	io_uring_queue_exit(&ring);
	return ret;
}

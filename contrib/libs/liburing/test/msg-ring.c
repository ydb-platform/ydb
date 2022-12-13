#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test ring messaging command
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

static int no_msg;

static int test_own(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	io_uring_prep_msg_ring(sqe, ring->ring_fd, 0x10, 0x1234, 0);
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "wait completion %d\n", ret);
			goto err;
		}
		switch (cqe->user_data) {
		case 1:
			if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP) {
				no_msg = 1;
				return 0;
			}
			if (cqe->res != 0) {
				fprintf(stderr, "cqe res %d\n", cqe->res);
				return -1;
			}
			break;
		case 0x1234:
			if (cqe->res != 0x10) {
				fprintf(stderr, "invalid len %x\n", cqe->res);
				return -1;
			}
			break;
		default:
			fprintf(stderr, "Invalid user_data\n");
			return -1;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

static void *wait_cqe_fn(void *data)
{
	struct io_uring *ring = data;
	struct io_uring_cqe *cqe;
	int ret;

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe %d\n", ret);
		goto err;
	}

	if (cqe->user_data != 0x5aa5) {
		fprintf(stderr, "user_data %llx\n", (long long) cqe->user_data);
		goto err;
	}
	if (cqe->res != 0x20) {
		fprintf(stderr, "len %x\n", cqe->res);
		goto err;
	}

	return NULL;
err:
	return (void *) (unsigned long) 1;
}

static int test_remote(struct io_uring *ring, struct io_uring *target)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		goto err;
	}

	io_uring_prep_msg_ring(sqe, target->ring_fd, 0x20, 0x5aa5, 0);
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res != 0) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return -1;
	}
	if (cqe->user_data != 1) {
		fprintf(stderr, "user_data %llx\n", (long long) cqe->user_data);
		return -1;
	}

	io_uring_cqe_seen(ring, cqe);
	return 0;
err:
	return 1;
}

static int test_invalid(struct io_uring *ring, bool fixed)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, fd = 1;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return 1;
	}

	if (fixed) {
		ret = io_uring_register_files(ring, &fd, 1);
		if (ret) {
			fprintf(stderr, "file register %d\n", ret);
			return 1;
		}
		io_uring_prep_msg_ring(sqe, 0, 0, 0x8989, 0);
		sqe->flags |= IOSQE_FIXED_FILE;
	} else {
		io_uring_prep_msg_ring(sqe, 1, 0, 0x8989, 0);
	}

	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		fprintf(stderr, "sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res != -EBADFD) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return -1;
	}

	io_uring_cqe_seen(ring, cqe);
	if (fixed)
		io_uring_unregister_files(ring);
	return 0;
err:
	if (fixed)
		io_uring_unregister_files(ring);
	return 1;
}

int main(int argc, char *argv[])
{
	struct io_uring ring, ring2, pring;
	pthread_t thread;
	void *tret;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(8, &ring2, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(8, &pring, IORING_SETUP_IOPOLL);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_own(&ring);
	if (ret) {
		fprintf(stderr, "test_own failed\n");
		return ret;
	}
	if (no_msg) {
		fprintf(stdout, "Skipped\n");
		return T_EXIT_SKIP;
	}
	ret = test_own(&pring);
	if (ret) {
		fprintf(stderr, "test_own iopoll failed\n");
		return ret;
	}

	ret = test_invalid(&ring, 0);
	if (ret) {
		fprintf(stderr, "test_invalid failed\n");
		return ret;
	}

	for (i = 0; i < 2; i++) {
		ret = test_invalid(&ring, 1);
		if (ret) {
			fprintf(stderr, "test_invalid fixed failed\n");
			return ret;
		}
	}

	pthread_create(&thread, NULL, wait_cqe_fn, &ring2);

	ret = test_remote(&ring, &ring2);
	if (ret) {
		fprintf(stderr, "test_remote failed\n");
		return ret;
	}

	pthread_join(thread, &tret);

	return T_EXIT_PASS;
}

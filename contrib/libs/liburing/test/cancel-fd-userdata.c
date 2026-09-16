#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: Test IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_USERDATA
 *
 * Tests that combining CANCEL_FD and CANCEL_USERDATA correctly matches
 * requests by both file descriptor and user_data. These share a union
 * in the kernel (io_cancel_data.data / io_cancel_data.file), so this
 * exercises that the union is handled correctly.
 */
#include <stdio.h>
#include <unistd.h>
#include <poll.h>

#include "liburing.h"
#include "helpers.h"

static int cancel_all(struct io_uring *ring, int fd, int fixed, int nr)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int i, ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_cancel(sqe, 0, IORING_ASYNC_CANCEL_ALL);
	sqe->cancel_flags |= IORING_ASYNC_CANCEL_FD;
	if (fixed)
		sqe->cancel_flags |= IORING_ASYNC_CANCEL_FD_FIXED;
	sqe->fd = fd;
	sqe->user_data = 200;

	ret = io_uring_submit(ring);
	if (ret != 1)
		return 1;

	for (i = 0; i < nr + 1; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret)
			return 1;
		io_uring_cqe_seen(ring, cqe);
	}
	return 0;
}

/*
 * Submit multiple polls on the same fd with different user_data values.
 * Cancel with CANCEL_FD | CANCEL_USERDATA targeting a specific user_data.
 * Only the request matching both fd AND user_data should be canceled.
 */
static int test_cancel_fd_userdata(struct io_uring *ring, int *fd, int fixed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, i, __fd = fd[0];
	int target_ud = 2;
	int canceled = 0;

	if (fixed) {
		ret = io_uring_register_files(ring, fd, 2);
		if (ret) {
			fprintf(stderr, "file register: %d\n", ret);
			return T_EXIT_FAIL;
		}
		__fd = 0;
	}

	/* Submit 4 polls on the same fd, with user_data 1..4 */
	for (i = 0; i < 4; i++) {
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "get sqe failed\n");
			return T_EXIT_FAIL;
		}
		io_uring_prep_poll_add(sqe, __fd, POLLIN);
		sqe->user_data = i + 1;
		if (fixed)
			sqe->flags |= IOSQE_FIXED_FILE;
	}

	ret = io_uring_submit(ring);
	if (ret != 4) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Cancel only the request with user_data == target_ud on this fd */
	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_prep_cancel(sqe, 0, 0);
	sqe->cancel_flags = IORING_ASYNC_CANCEL_FD |
			    IORING_ASYNC_CANCEL_USERDATA;
	if (fixed)
		sqe->cancel_flags |= IORING_ASYNC_CANCEL_FD_FIXED;
	sqe->fd = __fd;
	sqe->addr = target_ud;
	sqe->user_data = 100;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "cancel submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/*
	 * Get the cancel CQE first. If the kernel doesn't support these
	 * cancel flags, it will return -EINVAL and we skip the test.
	 */
	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/*
	 * The cancel CQE may arrive before or after the canceled poll CQE.
	 * If we got the cancel result first, check it. If we got a poll
	 * completion first, consume it and get the cancel result next.
	 */
	if (cqe->user_data == 100) {
		if (cqe->res == -EINVAL) {
			io_uring_cqe_seen(ring, cqe);
			cancel_all(ring, __fd, fixed, 4);
			if (fixed)
				io_uring_unregister_files(ring);
			return T_EXIT_SKIP;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "cancel failed: %d\n", cqe->res);
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);

		/* Now get the canceled poll CQE */
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data != target_ud || cqe->res != -ECANCELED) {
			fprintf(stderr, "unexpected ud=%lu res=%d\n",
				(unsigned long)cqe->user_data, cqe->res);
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
		canceled++;
	} else if (cqe->user_data == target_ud) {
		if (cqe->res != -ECANCELED) {
			fprintf(stderr, "poll ud=%d res=%d\n",
				target_ud, cqe->res);
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_FAIL;
		}
		canceled++;
		io_uring_cqe_seen(ring, cqe);

		/* Now get the cancel result CQE */
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data != 100 || cqe->res < 0) {
			fprintf(stderr, "cancel ud=%lu res=%d\n",
				(unsigned long)cqe->user_data, cqe->res);
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	} else {
		fprintf(stderr, "unexpected user_data %lu res %d\n",
			(unsigned long)cqe->user_data, cqe->res);
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_FAIL;
	}

	if (canceled != 1) {
		fprintf(stderr, "expected 1 canceled, got %d\n", canceled);
		return T_EXIT_FAIL;
	}

	/* The other 3 polls should still be pending - cancel them */
	if (cancel_all(ring, __fd, fixed, 3)) {
		fprintf(stderr, "cleanup failed\n");
		return T_EXIT_FAIL;
	}

	if (fixed)
		io_uring_unregister_files(ring);

	return T_EXIT_PASS;
}

/*
 * Submit polls on two different fds with the same user_data.
 * Cancel with CANCEL_FD | CANCEL_USERDATA should only match the
 * request on the right fd with the right user_data.
 */
static int test_cancel_fd_userdata_two_fds(struct io_uring *ring, int *fd1,
					   int *fd2)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, i;
	int canceled = 0;

	/* Poll on fd1 with user_data=1 */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_poll_add(sqe, fd1[0], POLLIN);
	sqe->user_data = 1;

	/* Poll on fd2 with user_data=1 (same user_data, different fd) */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_poll_add(sqe, fd2[0], POLLIN);
	sqe->user_data = 1;

	/* Poll on fd1 with user_data=2 (same fd, different user_data) */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_poll_add(sqe, fd1[0], POLLIN);
	sqe->user_data = 2;

	ret = io_uring_submit(ring);
	if (ret != 3) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/*
	 * Cancel: fd=fd1[0], user_data=1. Should only cancel the first
	 * poll (fd1, ud=1), not the second (fd2, ud=1) or third (fd1, ud=2).
	 */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_cancel(sqe, 0, 0);
	sqe->cancel_flags = IORING_ASYNC_CANCEL_FD |
			    IORING_ASYNC_CANCEL_USERDATA;
	sqe->fd = fd1[0];
	sqe->addr = 1;
	sqe->user_data = 100;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "cancel submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->user_data == 100) {
			if (cqe->res < 0) {
				fprintf(stderr, "cancel res: %d\n", cqe->res);
				io_uring_cqe_seen(ring, cqe);
				return T_EXIT_FAIL;
			}
		} else if (cqe->user_data == 1) {
			if (cqe->res != -ECANCELED) {
				fprintf(stderr, "poll res: %d\n", cqe->res);
				io_uring_cqe_seen(ring, cqe);
				return T_EXIT_FAIL;
			}
			canceled++;
		} else {
			fprintf(stderr, "unexpected ud=%lu res=%d\n",
				(unsigned long)cqe->user_data, cqe->res);
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	if (canceled != 1) {
		fprintf(stderr, "expected 1 cancel, got %d\n", canceled);
		return T_EXIT_FAIL;
	}

	/* Clean up: cancel all remaining */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_cancel(sqe, 0, IORING_ASYNC_CANCEL_ANY |
				     IORING_ASYNC_CANCEL_ALL);
	sqe->user_data = 200;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "cleanup submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 3; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "cleanup wait: %d\n", ret);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret, fd[2], fd2[2];

	if (argc > 1)
		return T_EXIT_SKIP;

	if (pipe(fd) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_cancel_fd_userdata(&ring, fd, 0);
	if (ret == T_EXIT_SKIP)
		return T_EXIT_SKIP;
	if (ret) {
		fprintf(stderr, "test normal fd failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_cancel_fd_userdata(&ring, fd, 1);
	if (ret == T_EXIT_SKIP)
		return T_EXIT_SKIP;
	if (ret) {
		fprintf(stderr, "test fixed fd failed\n");
		return T_EXIT_FAIL;
	}

	if (pipe(fd2) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = test_cancel_fd_userdata_two_fds(&ring, fd, fd2);
	if (ret) {
		fprintf(stderr, "test two fds failed\n");
		return ret;
	}

	close(fd2[0]);
	close(fd2[1]);
	close(fd[0]);
	close(fd[1]);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

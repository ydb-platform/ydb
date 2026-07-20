#include "../config-host.h"
// SPDX-License-Identifier: MIT
/*
 * Test pipe creation with specific fixed file slots (not auto-alloc).
 *
 * Bug: __io_fixed_fd_install() returns 0 for non-alloc success, but
 * io_pipe_fixed() used this return value as the slot index for both
 * the fds reported to userspace and error cleanup. This resulted in:
 * - fds always reporting {0, 0} instead of the actual slot indices
 * - error cleanup removing slot 0 instead of the correct slots
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "helpers.h"
#include "liburing.h"

static int no_pipe;

/*
 * Create a pipe at specific fixed slots, verify the returned fds match
 * the requested slots, and verify communication works through them.
 */
static int test_specific_slots(int slot)
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	char src[32], dst[32];
	int ret, fds[2];
	int i;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return 1;
	}

	ret = io_uring_register_files_sparse(&ring, 20);
	if (ret) {
		if (ret == -EINVAL) {
			no_pipe = 1;
			io_uring_queue_exit(&ring);
			return 0;
		}
		fprintf(stderr, "register_files_sparse: %d\n", ret);
		goto fail;
	}

	fds[0] = fds[1] = -1;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_pipe_direct(sqe, fds, 0, slot);
	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		goto fail;
	}
	if (cqe->res) {
		if (cqe->res == -EINVAL) {
			no_pipe = 1;
			io_uring_queue_exit(&ring);
			return 0;
		}
		fprintf(stderr, "pipe cqe res: %d\n", cqe->res);
		goto fail;
	}
	io_uring_cqe_seen(&ring, cqe);

	/* Verify returned fds are the correct 0-based slot indices */
	if (fds[0] != slot) {
		fprintf(stderr, "fds[0]=%d, expected %d\n", fds[0], slot);
		goto fail;
	}
	if (fds[1] != slot + 1) {
		fprintf(stderr, "fds[1]=%d, expected %d\n", fds[1], slot + 1);
		goto fail;
	}

	/* Verify pipe works through the fixed file slots */
	memset(src, 0x5a, sizeof(src));
	memset(dst, 0, sizeof(dst));

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_write(sqe, fds[1], src, sizeof(src), 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	sqe->user_data = 1;
	io_uring_submit(&ring);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, fds[0], dst, sizeof(dst), 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	sqe->user_data = 2;
	io_uring_submit(&ring);

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "io wait: %d\n", ret);
			goto fail;
		}
		if (cqe->res != sizeof(src)) {
			fprintf(stderr, "ud=%d res=%d\n",
				(int)cqe->user_data, cqe->res);
			goto fail;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	if (memcmp(src, dst, sizeof(src))) {
		fprintf(stderr, "data mismatch\n");
		goto fail;
	}

	io_uring_queue_exit(&ring);
	return 0;
fail:
	io_uring_queue_exit(&ring);
	return 1;
}

/*
 * Verify that creating a pipe at specific fixed slots doesn't accidentally
 * clobber slot 0. Without the fix, fds would be {0, 0} and error cleanup
 * would remove slot 0 instead of the actual pipe slots.
 */
static int test_no_clobber_slot0(void)
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fds[2], pipe_fds[2];
	int sentinel_fd;
	char buf;

	if (pipe(pipe_fds)) {
		fprintf(stderr, "pipe\n");
		return 1;
	}

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return 1;
	}

	ret = io_uring_register_files_sparse(&ring, 20);
	if (ret) {
		fprintf(stderr, "register_files_sparse: %d\n", ret);
		goto err;
	}

	/* Put a sentinel pipe-read-end at slot 0 */
	sentinel_fd = pipe_fds[0];
	ret = io_uring_register_files_update(&ring, 0, &sentinel_fd, 1);
	if (ret != 1) {
		fprintf(stderr, "sentinel install: %d\n", ret);
		goto err;
	}

	/* Create pipe at specific slots 5 and 6 */
	fds[0] = fds[1] = -1;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_pipe_direct(sqe, fds, 0, 5);
	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		goto err;
	}
	if (cqe->res) {
		fprintf(stderr, "pipe cqe res: %d\n", cqe->res);
		goto err;
	}
	io_uring_cqe_seen(&ring, cqe);

	/* Verify slot 0 sentinel is still intact by reading through it */
	ret = write(pipe_fds[1], "x", 1);
	if (ret != 1) {
		fprintf(stderr, "sentinel write: %d\n", ret);
		goto err;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_read(sqe, 0, &buf, 1, 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	io_uring_submit(&ring);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait sentinel: %d\n", ret);
		goto err;
	}
	if (cqe->res != 1) {
		fprintf(stderr, "sentinel read res=%d, slot 0 may have been clobbered\n",
			cqe->res);
		goto err;
	}
	io_uring_cqe_seen(&ring, cqe);

	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return 0;
err:
	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return 1;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_specific_slots(5);
	if (ret) {
		fprintf(stderr, "test_specific_slots(5) failed\n");
		return T_EXIT_FAIL;
	}
	if (no_pipe)
		return T_EXIT_SKIP;

	ret = test_specific_slots(0);
	if (ret) {
		fprintf(stderr, "test_specific_slots(0) failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_no_clobber_slot0();
	if (ret) {
		fprintf(stderr, "test_no_clobber_slot0 failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

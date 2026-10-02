#include "../config-host.h"
// SPDX-License-Identifier: MIT
/*
 * Test that auto-allocated fixed file slots stay within the configured
 * alloc range, even after explicit operations on slots outside the range
 * corrupt alloc_hint.
 *
 * Bug: io_file_bitmap_clear() and io_file_bitmap_set() unconditionally
 * update alloc_hint to the bit position. If a file is installed/removed
 * at a slot outside the configured alloc range, alloc_hint can point
 * outside the range, causing subsequent auto-allocations to escape it.
 */
#include <stdio.h>
#include <unistd.h>

#include "helpers.h"
#include "liburing.h"

static int no_sparse;

/*
 * Auto-alloc a file into the fixed table using IORING_FILE_INDEX_ALLOC.
 * On success, the kernel writes back the allocated slot index into *fd
 * (via put_user). Returns cqe->res (count on success, negative on error).
 */
static int file_update_alloc(struct io_uring *ring, int *fd)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_files_update(sqe, fd, 1, IORING_FILE_INDEX_ALLOC);

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		return -1;
	}
	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

/*
 * Install a file at a slot below the alloc range, then remove it.
 * This corrupts alloc_hint to point below the range. Verify that
 * subsequent auto-alloc still returns a slot within the range.
 */
static int test_hint_below_range(void)
{
	struct io_uring ring;
	int pipe_fds[2];
	int roff = 10, rlen = 10;
	int ret, fd;

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
		if (ret == -EINVAL) {
			no_sparse = 1;
			goto err;
		}
		fprintf(stderr, "register_files_sparse: %d\n", ret);
		goto err;
	}

	ret = io_uring_register_file_alloc_range(&ring, roff, rlen);
	if (ret) {
		fprintf(stderr, "register_file_alloc_range: %d\n", ret);
		goto err;
	}

	/* Install a file at slot 2, outside the alloc range [10, 20) */
	fd = pipe_fds[0];
	ret = io_uring_register_files_update(&ring, 2, &fd, 1);
	if (ret != 1) {
		fprintf(stderr, "install at slot 2: %d\n", ret);
		goto err;
	}

	/* Remove it — corrupts alloc_hint to point at slot 2 */
	fd = -1;
	ret = io_uring_register_files_update(&ring, 2, &fd, 1);
	if (ret != 1) {
		fprintf(stderr, "remove at slot 2: %d\n", ret);
		goto err;
	}

	/* Auto-alloc should stay within [10, 20), not land at slot 2 */
	fd = pipe_fds[1];
	ret = file_update_alloc(&ring, &fd);
	if (ret != 1) {
		fprintf(stderr, "file_update_alloc: %d\n", ret);
		goto err;
	}

	if (fd < roff || fd >= roff + rlen) {
		fprintf(stderr, "alloc returned slot %d, expected [%d, %d)\n",
			fd, roff, roff + rlen);
		goto err;
	}

	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
err:
	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return no_sparse ? T_EXIT_SKIP : T_EXIT_FAIL;
}

/*
 * Install a file at a slot above the alloc range end. The bitmap_set
 * pushes alloc_hint above file_alloc_end. Verify auto-alloc still
 * stays within range.
 */
static int test_hint_above_range(void)
{
	struct io_uring ring;
	int pipe_fds[2];
	int roff = 0, rlen = 10;
	int ret, fd;

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

	ret = io_uring_register_file_alloc_range(&ring, roff, rlen);
	if (ret) {
		fprintf(stderr, "register_file_alloc_range: %d\n", ret);
		goto err;
	}

	/* Install at slot 15, above range [0, 10) — alloc_hint = 16 */
	fd = pipe_fds[0];
	ret = io_uring_register_files_update(&ring, 15, &fd, 1);
	if (ret != 1) {
		fprintf(stderr, "install at slot 15: %d\n", ret);
		goto err;
	}

	/* Auto-alloc should stay within [0, 10), not search [0, 16) */
	fd = pipe_fds[1];
	ret = file_update_alloc(&ring, &fd);
	if (ret != 1) {
		fprintf(stderr, "file_update_alloc: %d\n", ret);
		goto err;
	}

	if (fd < roff || fd >= roff + rlen) {
		fprintf(stderr, "alloc returned slot %d, expected [%d, %d)\n",
			fd, roff, roff + rlen);
		goto err;
	}

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

	ret = test_hint_below_range();
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret) {
		fprintf(stderr, "test_hint_below_range failed\n");
		return T_EXIT_FAIL;
	}
	if (no_sparse)
		return T_EXIT_SKIP;

	ret = test_hint_above_range();
	if (ret) {
		fprintf(stderr, "test_hint_above_range failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

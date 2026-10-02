#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test SQE_MIXED physical SQE boundary validation with sq_array
 *
 * Verify that 128-byte operations are correctly rejected when sq_array
 * remaps them to the last physical SQE slot, preventing a 64-byte OOB
 * read past the SQE array.
 */
#include <stdio.h>
#include <string.h>

#include "liburing.h"
#include "helpers.h"
#include "test.h"

#define NENTRIES	4

/*
 * Positive test: NOP128 at a valid physical position should succeed.
 */
static int test_valid_position(void)
{
	struct io_uring ring;
	struct io_uring_params p = { .flags = IORING_SETUP_SQE_MIXED };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret;

	ret = t_io_uring_init_sqarray(NENTRIES, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_nop(sqe);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe128(&ring);
	if (!sqe) {
		fprintf(stderr, "get_sqe128 failed\n");
		goto fail;
	}
	io_uring_prep_nop128(sqe);
	sqe->user_data = 2;

	ret = io_uring_submit(&ring);
	if (ret < 0) {
		fprintf(stderr, "submit: %d\n", ret);
		goto fail;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		goto fail;
	}
	io_uring_cqe_seen(&ring, cqe);

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		goto fail;
	}
	if (cqe->user_data == 2 && cqe->res != 0) {
		fprintf(stderr, "NOP128 at valid position failed: %d\n",
			cqe->res);
		io_uring_cqe_seen(&ring, cqe);
		goto fail;
	}
	io_uring_cqe_seen(&ring, cqe);

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
fail:
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Negative test: NOP128 at the last physical SQE slot via sq_array remap
 * must be rejected. Without the kernel fix, this triggers a 64-byte OOB
 * read in io_uring_cmd_sqe_copy().
 */
static int test_oob_boundary(void)
{
	struct io_uring ring;
	struct io_uring_params p = { .flags = IORING_SETUP_SQE_MIXED };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	unsigned mask;
	int ret, i, found;

	ret = t_io_uring_init_sqarray(NENTRIES, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	mask = *ring.sq.kring_entries - 1;

	/* Advance internal tail: NOP (1) + NOP128 (2) = 3 slots */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_nop(sqe);
	sqe->user_data = 1;

	sqe = io_uring_get_sqe128(&ring);
	if (!sqe) {
		fprintf(stderr, "get_sqe128 failed\n");
		goto fail;
	}

	/*
	 * Override: remap logical position 1 to last physical slot.
	 * Prep NOP128 there instead of the position get_sqe128 returned.
	 */
	ring.sq.array[1] = mask;
	memset(&ring.sq.sqes[mask], 0, sizeof(struct io_uring_sqe));
	io_uring_prep_nop128(&ring.sq.sqes[mask]);
	ring.sq.sqes[mask].user_data = 2;

	ret = io_uring_submit(&ring);
	if (ret < 0) {
		fprintf(stderr, "submit: %d\n", ret);
		goto fail;
	}

	found = 0;
	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret)
			break;
		if (cqe->user_data == 2) {
			if (cqe->res != -EINVAL) {
				fprintf(stderr,
					"NOP128 at last slot: expected -EINVAL, got %d\n",
					cqe->res);
				io_uring_cqe_seen(&ring, cqe);
				goto fail;
			}
			found = 1;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	if (!found) {
		fprintf(stderr, "no CQE for NOP128 boundary test\n");
		goto fail;
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
fail:
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_valid_position();
	if (ret == T_EXIT_SKIP)
		return T_EXIT_SKIP;
	if (ret) {
		fprintf(stderr, "test_valid_position failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_oob_boundary();
	if (ret) {
		fprintf(stderr, "test_oob_boundary failed\n");
		return ret;
	}

	return T_EXIT_PASS;
}

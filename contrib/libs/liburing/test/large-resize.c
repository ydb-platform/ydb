#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test ring resizing with fixed large sqes/cqes, and with
 *		mixed mode sqes/cqes.
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "liburing.h"
#include "helpers.h"

static int test_cqe32_resize(void)
{
	struct io_uring ring;
	struct io_uring_params p = { }, new_p = { };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	p.flags = IORING_SETUP_CQE32 | IORING_SETUP_DEFER_TASKRUN |
		  IORING_SETUP_SINGLE_ISSUER;
	p.sq_entries = 8;
	p.cq_entries = 8;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "queue_init: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 4; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_nop(sqe);
		sqe->user_data = 0xAAAA0000ULL + i;
	}

	ret = io_uring_submit(&ring);
	if (ret != 4) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Resize the ring */
	new_p.sq_entries = 8;
	new_p.cq_entries = 16;
	new_p.flags = IORING_SETUP_CQSIZE;

	ret = io_uring_resize_rings(&ring, &new_p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "resize failed: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 4; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %s\n", strerror(-ret));
			return T_EXIT_FAIL;
		}

		if (cqe->user_data != (0xAAAA0000ULL + i)) {
			fprintf(stderr, "  *** CQE32 CORRUPTION DETECTED ***\n");
			return T_EXIT_FAIL;
		}

		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static int test_cqe_mixed_resize(void)
{
	struct io_uring ring;
	struct io_uring_params p = { }, new_p = { };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	/* Setup ring with CQE_MIXED, DEFER_TASKRUN, SINGLE_ISSUER */
	p.flags = IORING_SETUP_CQE_MIXED | IORING_SETUP_DEFER_TASKRUN |
		  IORING_SETUP_SINGLE_ISSUER;
	p.sq_entries = 8;
	p.cq_entries = 8;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 4; i++) {
		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			fprintf(stderr, "get_sqe failed\n");
			return T_EXIT_FAIL;
		}

		if (i % 2 == 0) {
			io_uring_prep_nop(sqe);
			sqe->user_data = 0xBBBB0000ULL + i;
		} else {
			io_uring_prep_nop(sqe);  /* Still NOP but different pattern */
			sqe->user_data = 0xCCCC0000ULL + i;
		}
	}

	ret = io_uring_submit(&ring);
	if (ret != 4) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Resize the ring */
	new_p.sq_entries = 8;
	new_p.cq_entries = 16;
	new_p.flags = IORING_SETUP_CQSIZE;

	ret = io_uring_resize_rings(&ring, &new_p);
	if (ret) {
		fprintf(stderr, "resize failed: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	/* Reap and verify CQEs */
	for (i = 0; i < 4; i++) {
		uint64_t expected;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %s\n", strerror(-ret));
			return T_EXIT_FAIL;
		}

		expected = (i % 2 == 0) ? (0xBBBB0000ULL + i) : (0xCCCC0000ULL + i);
		if (cqe->user_data != expected) {
			fprintf(stderr, "  *** CQE_MIXED CORRUPTION DETECTED ***\n");
			return T_EXIT_FAIL;
		}

		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static int test_ring_wrapping(void)
{
	struct io_uring ring;
	struct io_uring_params p = { }, new_p = { };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i, consumed_count;
	int remaining_expected[] = {6, 7};
	int remaining_count = 0;

	/* Setup small ring to force wrapping */
	p.flags = IORING_SETUP_CQE32 | IORING_SETUP_DEFER_TASKRUN |
		  IORING_SETUP_SINGLE_ISSUER;
	p.sq_entries = 4;
	p.cq_entries = 4;

	ret = io_uring_queue_init_params(4, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "queue_init: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	/* Submit more entries to force head/tail wrapping */
	consumed_count = 0;
	for (i = 0; i < 8; i++) {
		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			fprintf(stderr, "get_sqe failed at %d\n", i);
			return T_EXIT_FAIL;
		}
		io_uring_prep_nop(sqe);
		sqe->user_data = 0xDDDD0000ULL + i;

		ret = io_uring_submit(&ring);
		if (ret != 1) {
			fprintf(stderr, "submit failed: %d\n", ret);
			return T_EXIT_FAIL;
		}

		/* Consume some to create wrap scenario */
		if (i >= 2) {
			uint64_t expected;

			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait_cqe: %s\n", strerror(-ret));
				return T_EXIT_FAIL;
			}

			/* Verify early consumed CQE */
			expected = 0xDDDD0000ULL + consumed_count;

			if (cqe->user_data != expected) {
				fprintf(stderr, "  *** EARLY CQE CORRUPTION DETECTED ***\n");
				return T_EXIT_FAIL;
			}

			io_uring_cqe_seen(&ring, cqe);
			consumed_count++;
		}
	}

	/* Now resize with pending wrapped entries */
	new_p.sq_entries = 4;
	new_p.cq_entries = 16;

	ret = io_uring_resize_rings(&ring, &new_p);
	if (ret) {
		fprintf(stderr, "resize failed: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	/* Consume remaining entries (should be operations 6 and 7) */
	while (true) {
		uint64_t expected;

		ret = io_uring_peek_cqe(&ring, &cqe);
		if (ret == -EAGAIN)
			break;
		if (ret) {
			fprintf(stderr, "peek_cqe: %s\n", strerror(-ret));
			return T_EXIT_FAIL;
		}

		if (remaining_count >= 2) {
			printf("  *** TOO MANY REMAINING CQES ***\n");
			return T_EXIT_FAIL;
		}

		expected = 0xDDDD0000ULL + remaining_expected[remaining_count];
		if (cqe->user_data != expected) {
			fprintf(stderr, "  *** REMAINING CQE CORRUPTION DETECTED ***\n");
			return T_EXIT_FAIL;
		}

		io_uring_cqe_seen(&ring, cqe);
		remaining_count++;
	}

	if (remaining_count != 2) {
		fprintf(stderr, "  *** WRONG NUMBER OF REMAINING CQES: got %d, expected 2 ***\n", remaining_count);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static int test_sqe128_resize(void)
{
	struct io_uring ring;
	struct io_uring_params p = { }, new_p = { };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	p.flags = IORING_SETUP_SQE128 | IORING_SETUP_DEFER_TASKRUN |
		  IORING_SETUP_SINGLE_ISSUER;
	p.sq_entries = 8;
	p.cq_entries = 8;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "queue_init: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	/* Prepare 4 large SQEs but don't submit them yet */
	for (i = 0; i < 4; i++) {
		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			fprintf(stderr, "get_sqe failed\n");
			return 1;
		}
		io_uring_prep_nop(sqe);
		sqe->user_data = 0xEEEE0000ULL + i;

		/* Fill some extended SQE data to ensure 128-byte copy works */
		if (ring.sq.ring_ptr) {
			/* Access extended part of SQE128 if available */
			memset(sqe->cmd, 0x55 + i, sizeof(sqe->cmd));
		}
	}

	__io_uring_flush_sq(&ring);

	/* Resize the ring while SQEs are pending */
	new_p.sq_entries = 16;
	new_p.cq_entries = 16;
	new_p.flags = IORING_SETUP_CQSIZE;

	ret = io_uring_resize_rings(&ring, &new_p);
	if (ret) {
		fprintf(stderr, "resize failed: %s\n", strerror(-ret));
		return 1;
	}

	/* Now submit the pending SQEs */
	ret = io_uring_submit(&ring);
	if (ret != 4) {
		fprintf(stderr, "submit after resize: %d (expected 4)\n", ret);
		return T_EXIT_FAIL;
	}

	/* Reap and verify CQEs */
	for (i = 0; i < 4; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %s\n", strerror(-ret));
			return T_EXIT_FAIL;
		}

		if (cqe->user_data != (0xEEEE0000ULL + i)) {
			printf("  *** SQE128 CORRUPTION DETECTED ***\n");
			return T_EXIT_FAIL;
		}

		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

static int test_sqe_mixed_resize(void)
{
	struct io_uring ring;
	struct io_uring_params p = { }, new_p = { };
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	p.flags = IORING_SETUP_SQE_MIXED | IORING_SETUP_DEFER_TASKRUN |
		  IORING_SETUP_SINGLE_ISSUER;
	p.sq_entries = 8;
	p.cq_entries = 8;

	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		return T_EXIT_FAIL;
	}

	/* Prepare mix of regular and large SQEs but don't submit them yet */
	for (i = 0; i < 4; i++) {
		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			fprintf(stderr, "get_sqe failed\n");
			return T_EXIT_FAIL;
		}

		/* Alternate between regular NOPs and operations that might use large SQEs */
		if (i % 2 == 0) {
			io_uring_prep_nop(sqe);
			sqe->user_data = 0xFFFF0000ULL + i;
		} else {
			io_uring_prep_nop(sqe);  /* Still NOP but different pattern */
			sqe->user_data = 0x11110000ULL + i;
			/* Fill extended data for mixed SQE */
			memset(sqe->cmd, 0xAA + i, sizeof(sqe->cmd));
		}
	}

	__io_uring_flush_sq(&ring);

	/* Resize the ring while mixed SQEs are pending */
	new_p.sq_entries = 16;
	new_p.cq_entries = 16;
	new_p.flags = IORING_SETUP_CQSIZE;

	ret = io_uring_resize_rings(&ring, &new_p);
	if (ret) {
		fprintf(stderr, "resize failed: %s\n", strerror(-ret));
		return T_EXIT_FAIL;
	}

	/* Now submit the pending mixed SQEs */
	ret = io_uring_submit(&ring);
	if (ret != 4) {
		fprintf(stderr, "submit after resize: %d (expected 4)\n", ret);
		return T_EXIT_FAIL;
	}

	/* Reap and verify CQEs */
	for (i = 0; i < 4; i++) {
		uint64_t expected;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %s\n", strerror(-ret));
			return T_EXIT_FAIL;
		}

		expected = (i % 2 == 0) ? (0xFFFF0000ULL + i) : (0x11110000ULL + i);
		if (cqe->user_data != expected) {
			fprintf(stderr, "  *** SQE_MIXED CORRUPTION DETECTED ***\n");
			return T_EXIT_FAIL;
		}

		io_uring_cqe_seen(&ring, cqe);
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

int main(void)
{
	int ret;

	ret = test_cqe32_resize();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "cqe32_resize failed\n");
		return T_EXIT_FAIL;
	} else if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	}

	ret = test_cqe_mixed_resize();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "cqe_mixed_resize failed\n");
		return T_EXIT_FAIL;
	} else if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	}

	ret = test_sqe128_resize();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "sqe128_resize failed\n");
		return T_EXIT_FAIL;
	} else if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	}

	ret = test_ring_wrapping();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "ring_wrapping failed\n");
		return T_EXIT_FAIL;
	} else if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	}

	ret = test_sqe_mixed_resize();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "sqe_mixed_resize failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

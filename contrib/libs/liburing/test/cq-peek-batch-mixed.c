#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test CQ peek-batch on rings setup with IORING_SETUP_CQE_MIXED,
 * where CQEs are variably sized and the kernel may post skip entries to pad
 * the CQ ring at wrap time.
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "liburing.h"
#include "helpers.h"

static int queue_nops(struct io_uring *ring, int n, int cqe32, int offset)
{
	struct io_uring_sqe *sqe;
	int i, ret;

	for (i = 0; i < n; i++) {
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			fprintf(stderr, "get sqe failed\n");
			return 1;
		}

		io_uring_prep_nop(sqe);
		sqe->user_data = i + offset;
		if (cqe32) {
			sqe->nop_flags = IORING_NOP_CQE32;
			sqe->off = i + offset + 1000;
			sqe->addr = i + offset + 2000;
		}
	}

	ret = io_uring_submit(ring);
	if (ret != n) {
		fprintf(stderr, "submitted %d, wanted %d\n", ret, n);
		return 1;
	}

	return 0;
}

static int check_cqe(struct io_uring_cqe *cqe, __u64 user_data, int cqe32)
{
	if (cqe->flags & IORING_CQE_F_SKIP) {
		fprintf(stderr, "skip CQE returned to application\n");
		return 1;
	}
	if (cqe->user_data != user_data) {
		fprintf(stderr, "got user_data %ld, expected %ld\n",
			(long) cqe->user_data, (long) user_data);
		return 1;
	}
	if (cqe32 != !!(cqe->flags & IORING_CQE_F_32)) {
		fprintf(stderr, "bad CQE_F_32 flag for user_data %ld\n",
			(long) cqe->user_data);
		return 1;
	}
	if (cqe32) {
		if (cqe->big_cqe[0] != user_data + 1000 ||
		    cqe->big_cqe[1] != user_data + 2000) {
			fprintf(stderr, "bad big_cqe data for user_data %ld\n",
				(long) cqe->user_data);
			return 1;
		}
	}
	return 0;
}

static void advance_cqes(struct io_uring *ring, struct io_uring_cqe **cqes,
			 unsigned nr)
{
	unsigned i, slots = 0;

	for (i = 0; i < nr; i++)
		slots += io_uring_cqe_nr(cqes[i]);

	io_uring_cq_advance(ring, slots);
}

#define CHECK_BATCH(ring, got, cqes, count, expected) do {\
	got = io_uring_peek_batch_cqe((ring), cqes, count);\
	if (got != expected) {\
		fprintf(stderr, "got %d CQEs, expected %d\n", got, expected);\
		goto err;\
	}\
} while(0)

int main(int argc, char *argv[])
{
	struct io_uring_params p = { };
	struct io_uring_cqe *cqes[16];
	struct io_uring ring;
	unsigned got;
	int ret, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	p.flags = IORING_SETUP_CQE_MIXED;
	ret = io_uring_queue_init_params(8, &ring, &p);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	/* the wrap arithmetic below assumes 16 CQ ring slots */
	if (ring.cq.ring_entries != 16) {
		io_uring_queue_exit(&ring);
		return T_EXIT_SKIP;
	}

	CHECK_BATCH(&ring, got, cqes, 16, 0);

	/*
	 * Mix of 16b and 32b CQEs, 8 slots in total. The batch must return
	 * pointers to the CQE starts, not one pointer per slot.
	 */
	if (queue_nops(&ring, 2, 0, 1))
		goto err;
	if (queue_nops(&ring, 2, 1, 3))
		goto err;
	if (queue_nops(&ring, 2, 0, 5))
		goto err;

	/* limited count batch, ending on a 32b CQE */
	CHECK_BATCH(&ring, got, cqes, 3, 3);

	CHECK_BATCH(&ring, got, cqes, 16, 6);
	for (i = 0; i < 6; i++) {
		int cqe32 = i == 2 || i == 3;

		if (check_cqe(cqes[i], i + 1, cqe32))
			goto err;
	}
	advance_cqes(&ring, cqes, got);

	/*
	 * CQ head is now at slot 8 of 16. Consume 3 more plain CQEs to move
	 * it to slot 11, then fill slots 11-14 with plain CQEs and post a
	 * 32b CQE. That doesn't fit in the single slot left before the ring
	 * wraps, so the kernel posts a skip entry in slot 15 and the 32b CQE
	 * in slots 0-1. Top it off with a plain CQE in slot 2.
	 */
	if (queue_nops(&ring, 3, 0, 10))
		goto err;
	CHECK_BATCH(&ring, got, cqes, 16, 3);
	advance_cqes(&ring, cqes, got);

	if (queue_nops(&ring, 4, 0, 20))
		goto err;
	if (queue_nops(&ring, 1, 1, 24))
		goto err;
	if (queue_nops(&ring, 1, 0, 25))
		goto err;

	/* the batch must stop at the skip entry in slot 15 */
	CHECK_BATCH(&ring, got, cqes, 16, 4);
	for (i = 0; i < 4; i++) {
		if (check_cqe(cqes[i], i + 20, 0))
			goto err;
	}
	advance_cqes(&ring, cqes, got);

	/* skip entry now at the head, must be consumed and not returned */
	CHECK_BATCH(&ring, got, cqes, 16, 2);
	if (check_cqe(cqes[0], 24, 1))
		goto err;
	if (check_cqe(cqes[1], 25, 0))
		goto err;
	advance_cqes(&ring, cqes, got);

	CHECK_BATCH(&ring, got, cqes, 16, 0);
	if (io_uring_cq_ready(&ring)) {
		fprintf(stderr, "CQ ring not empty\n");
		goto err;
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
err:
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

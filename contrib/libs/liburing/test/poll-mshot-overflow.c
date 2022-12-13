#include "../config-host.h"
// SPDX-License-Identifier: MIT

#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <poll.h>
#include <sys/wait.h>

#include "liburing.h"
#include "helpers.h"

int check_final_cqe(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	int count = 0;
	bool signalled_no_more = false;

	while (!io_uring_peek_cqe(ring, &cqe)) {
		if (cqe->user_data == 1) {
			count++;
			if (signalled_no_more) {
				fprintf(stderr, "signalled no more!\n");
				return T_EXIT_FAIL;
			}
			if (!(cqe->flags & IORING_CQE_F_MORE))
				signalled_no_more = true;
		} else if (cqe->user_data != 3) {
			fprintf(stderr, "%d: got unexpected %d\n", count, (int)cqe->user_data);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	if (!count) {
		fprintf(stderr, "no cqe\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

static int test(bool defer_taskrun)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	int pipe1[2];
	int ret, i;

	if (pipe(pipe1) != 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	struct io_uring_params params = {
		/* cheat using SINGLE_ISSUER existence to know if this behaviour
		 * is updated
		 */
		.flags = IORING_SETUP_CQSIZE | IORING_SETUP_SINGLE_ISSUER,
		.cq_entries = 2
	};

	if (defer_taskrun)
		params.flags |= IORING_SETUP_SINGLE_ISSUER |
				IORING_SETUP_DEFER_TASKRUN;

	ret = io_uring_queue_init_params(2, &ring, &params);
	if (ret)
		return T_EXIT_SKIP;

	sqe = io_uring_get_sqe(&ring);
	if (!sqe) {
		fprintf(stderr, "get sqe failed\n");
		return T_EXIT_FAIL;
	}
	io_uring_prep_poll_multishot(sqe, pipe1[0], POLLIN);
	io_uring_sqe_set_data64(sqe, 1);

	if (io_uring_cq_ready(&ring)) {
		fprintf(stderr, "unexpected cqe\n");
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 2; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_nop(sqe);
		io_uring_sqe_set_data64(sqe, 2);
		io_uring_submit(&ring);
	}

	do {
		errno = 0;
		ret = write(pipe1[1], "foo", 3);
	} while (ret == -1 && errno == EINTR);

	if (ret <= 0) {
		fprintf(stderr, "write failed: %d\n", errno);
		return T_EXIT_FAIL;
	}

	/* should have 2 cqe + 1 overflow now, so take out two cqes */
	for (i = 0; i < 2; i++) {
		if (io_uring_peek_cqe(&ring, &cqe)) {
			fprintf(stderr, "unexpectedly no cqe\n");
			return T_EXIT_FAIL;
		}
		if (cqe->user_data != 2) {
			fprintf(stderr, "unexpected user_data\n");
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	/* make sure everything is processed */
	io_uring_get_events(&ring);

	/* now remove the poll */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_remove(sqe, 1);
	io_uring_sqe_set_data64(sqe, 3);
	ret = io_uring_submit(&ring);

	if (ret != 1) {
		fprintf(stderr, "bad poll remove\n");
		return T_EXIT_FAIL;
	}

	ret = check_final_cqe(&ring);

	close(pipe1[0]);
	close(pipe1[1]);
	io_uring_queue_exit(&ring);

	return ret;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(false);
	if (ret != T_EXIT_PASS) {
		fprintf(stderr, "%s: test(false) failed\n", argv[0]);
		return ret;
	}

	if (t_probe_defer_taskrun()) {
		ret = test(true);
		if (ret != T_EXIT_PASS) {
			fprintf(stderr, "%s: test(true) failed\n", argv[0]);
			return ret;
		}
	}

	return ret;
}

#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test multishot POLL_ADD on an eventfd that is also the
 *		CQ notification eventfd of the same ring. Triggering the poll
 *		posts a CQE, which notifies the eventfd, which re-enters the
 *		poll wake path via EPOLL_URING_WAKE. The multishot poll must
 *		keep delivering events for subsequent wakeups; a regression
 *		here leaves the poll request permanently stuck with
 *		IORING_CQE_F_MORE set but no further CQEs.
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <poll.h>
#include <sys/eventfd.h>

#include "liburing.h"
#include "helpers.h"

#define NR_LOOPS	2

static int test(void)
{
	struct __kernel_timespec ts;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	int ret, evfd, i;
	uint64_t val;

	evfd = eventfd(0, EFD_NONBLOCK);
	if (evfd < 0) {
		perror("eventfd");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		close(evfd);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_eventfd(&ring, evfd);
	if (ret) {
		fprintf(stderr, "register_eventfd: %d\n", ret);
		close(evfd);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_multishot(sqe, evfd, POLLIN);
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		goto err;
	}

	for (i = 0; i < NR_LOOPS; i++) {
		val = 1;
		ret = write(evfd, &val, sizeof(val));
		if (ret != sizeof(val)) {
			perror("write");
			goto err;
		}
		ret = read(evfd, &val, sizeof(val));
		if (ret != sizeof(val)) {
			perror("read");
			goto err;
		}

		ts.tv_sec = 1;
		ts.tv_nsec = 0;
		ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
		if (ret == -ETIME) {
			fprintf(stderr, "poll stuck: no CQE after iteration %d\n", i);
			goto err;
		}
		if (ret < 0) {
			fprintf(stderr, "wait_cqe: %d\n", ret);
			goto err;
		}
		if (cqe->user_data != 1) {
			fprintf(stderr, "unexpected user_data %llx\n",
				(unsigned long long) cqe->user_data);
			io_uring_cqe_seen(&ring, cqe);
			goto err;
		}
		if (cqe->res < 0) {
			fprintf(stderr, "cqe res: %d\n", cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto err;
		}
		if (!i) {
			if (!(cqe->flags & IORING_CQE_F_MORE)) {
				fprintf(stderr, "IORING_CQE_F_MORE not set\n");
				goto err;
			}
		} else {
			if (cqe->flags & IORING_CQE_F_MORE) {
				fprintf(stderr, "IORING_CQE_F_MORE set\n");
				goto err;
			}
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	close(evfd);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
err:
	close(evfd);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	if (argc > 1)
		return T_EXIT_SKIP;

	return test();
}

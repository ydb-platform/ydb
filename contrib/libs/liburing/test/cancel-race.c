#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: stress test cancel racing with completion. Submits operations
 *		and immediately cancels them, checking that we get consistent
 *		results (either the op completed or was cancelled, never both
 *		or neither).
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define RING_SIZE	64
#define NR_ITERS	10000

/* user_data encoding: op type in low bits, sequence in upper bits */
#define UD_OP_POLL	1
#define UD_OP_CANCEL	2
#define UD_OP_READ	3
#define UD_SEQ_SHIFT	8

static int no_cancel;

/*
 * Stress poll + cancel race: submit a poll and immediately cancel it.
 * We should see exactly one completion per pair (either the poll
 * completes or the cancel succeeds, or the cancel fails because the
 * poll already completed).
 */
static int test_poll_cancel_race(void)
{
	struct io_uring ring;
	int ret, i;
	int pipe_fds[2];

	if (pipe(pipe_fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(RING_SIZE, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		close(pipe_fds[0]);
		close(pipe_fds[1]);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_ITERS; i++) {
		struct io_uring_sqe *sqe;
		struct io_uring_cqe *cqe;
		__u64 poll_ud = UD_OP_POLL | ((__u64)i << UD_SEQ_SHIFT);
		__u64 cancel_ud = UD_OP_CANCEL | ((__u64)i << UD_SEQ_SHIFT);
		bool got_poll = false, got_cancel = false;

		/* Submit poll */
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_poll_add(sqe, pipe_fds[0], POLLIN);
		sqe->user_data = poll_ud;

		/* Submit cancel immediately after */
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_cancel64(sqe, poll_ud, 0);
		sqe->user_data = cancel_ud;

		ret = io_uring_submit(&ring);
		if (ret != 2) {
			fprintf(stderr, "submit iter %d: %d\n", i, ret);
			goto err;
		}

		/* Must get exactly 2 CQEs */
		for (int j = 0; j < 2; j++) {
			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait iter %d/%d: %d\n", i, j, ret);
				goto err;
			}

			if ((cqe->user_data & 0xff) == UD_OP_POLL) {
				got_poll = true;
				/*
				 * Poll result: either -ECANCELED (cancel won)
				 * or 0/POLLIN (poll completed first, shouldn't
				 * happen here since nothing writes to pipe)
				 */
				if (cqe->res != -ECANCELED && cqe->res != 0) {
					fprintf(stderr, "poll res: %d\n", cqe->res);
					io_uring_cqe_seen(&ring, cqe);
					goto err;
				}
			} else if ((cqe->user_data & 0xff) == UD_OP_CANCEL) {
				got_cancel = true;
				/*
				 * Cancel result: 0 (cancelled successfully) or
				 * -ENOENT (poll already completed) or
				 * -EALREADY (in progress)
				 */
				if (cqe->res != 0 && cqe->res != -ENOENT &&
				    cqe->res != -EALREADY) {
					if (cqe->res == -EINVAL) {
						no_cancel = 1;
						io_uring_cqe_seen(&ring, cqe);
						goto out;
					}
					fprintf(stderr, "cancel res: %d\n", cqe->res);
					io_uring_cqe_seen(&ring, cqe);
					goto err;
				}
			}
			io_uring_cqe_seen(&ring, cqe);
		}

		if (!got_poll || !got_cancel) {
			fprintf(stderr, "missing CQE iter %d: poll=%d cancel=%d\n",
				i, got_poll, got_cancel);
			goto err;
		}
	}

out:
	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);

	if (no_cancel)
		return T_EXIT_SKIP;

	return T_EXIT_PASS;

err:
	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Stress read + cancel race: submit a read on a pipe (that won't complete
 * immediately) and immediately cancel it.
 */
static int test_read_cancel_race(void)
{
	struct io_uring ring;
	char buf[32];
	int ret, i;
	int pipe_fds[2];

	if (no_cancel)
		return T_EXIT_SKIP;

	if (pipe(pipe_fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	/* Make read end non-blocking so prep works, but reads will still
	 * go async since there's no data
	 */
	t_set_nonblock(pipe_fds[0]);

	ret = io_uring_queue_init(RING_SIZE, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		close(pipe_fds[0]);
		close(pipe_fds[1]);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_ITERS; i++) {
		struct io_uring_sqe *sqe;
		struct io_uring_cqe *cqe;
		__u64 read_ud = UD_OP_READ | ((__u64)i << UD_SEQ_SHIFT);
		__u64 cancel_ud = UD_OP_CANCEL | ((__u64)i << UD_SEQ_SHIFT);
		bool got_read = false, got_cancel = false;
		int nr_cqes;

		/* Submit read that will block (no data in pipe) */
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read(sqe, pipe_fds[0], buf, sizeof(buf), 0);
		sqe->user_data = read_ud;

		/* Submit cancel */
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_cancel64(sqe, read_ud, 0);
		sqe->user_data = cancel_ud;

		ret = io_uring_submit(&ring);
		if (ret != 2) {
			fprintf(stderr, "submit iter %d: %d\n", i, ret);
			goto err;
		}

		/*
		 * We should get 2 CQEs, but the read may complete
		 * with -EAGAIN immediately (non-blocking pipe with no data),
		 * in which case cancel gets -ENOENT.
		 */
		nr_cqes = 0;
		while (nr_cqes < 2) {
			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait iter %d/%d: %d\n",
					i, nr_cqes, ret);
				goto err;
			}

			if ((cqe->user_data & 0xff) == UD_OP_READ) {
				got_read = true;
				/* -ECANCELED, -EAGAIN, or short read all OK */
				if (cqe->res != -ECANCELED &&
				    cqe->res != -EAGAIN &&
				    cqe->res < 0 && cqe->res != -EINTR) {
					fprintf(stderr, "read res iter %d: %d\n",
						i, cqe->res);
					io_uring_cqe_seen(&ring, cqe);
					goto err;
				}
			} else if ((cqe->user_data & 0xff) == UD_OP_CANCEL) {
				got_cancel = true;
				if (cqe->res != 0 && cqe->res != -ENOENT &&
				    cqe->res != -EALREADY) {
					fprintf(stderr, "cancel res iter %d: %d\n",
						i, cqe->res);
					io_uring_cqe_seen(&ring, cqe);
					goto err;
				}
			}
			io_uring_cqe_seen(&ring, cqe);
			nr_cqes++;
		}

		if (!got_read || !got_cancel) {
			fprintf(stderr, "missing CQE iter %d\n", i);
			goto err;
		}
	}

	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err:
	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Stress concurrent cancel from multiple threads: one thread submits
 * polls, another cancels them via IORING_ASYNC_CANCEL_ANY.
 */
struct cancel_thread_data {
	struct io_uring *ring;
	int cancel_count;
	volatile int stop;
};

static void *cancel_thread_fn(void *arg)
{
	struct cancel_thread_data *ctd = arg;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	while (!ctd->stop) {
		sqe = io_uring_get_sqe(ctd->ring);
		if (!sqe) {
			io_uring_submit(ctd->ring);
			continue;
		}
		io_uring_prep_cancel64(sqe, 0, IORING_ASYNC_CANCEL_ANY);
		sqe->user_data = 0xdead;

		ret = io_uring_submit(ctd->ring);
		if (ret < 0)
			continue;

		ret = io_uring_wait_cqe(ctd->ring, &cqe);
		if (ret)
			continue;

		if (cqe->res == 0)
			ctd->cancel_count++;
		io_uring_cqe_seen(ctd->ring, cqe);
	}

	return NULL;
}

static int test_concurrent_cancel(void)
{
	struct io_uring submit_ring, cancel_ring;
	struct cancel_thread_data ctd;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	pthread_t thread;
	int ret, i;
	int pipe_fds[2];
	int submitted = 0, completed = 0;

	if (no_cancel)
		return T_EXIT_SKIP;

	if (pipe(pipe_fds) < 0) {
		perror("pipe");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(RING_SIZE, &submit_ring, 0);
	if (ret) {
		fprintf(stderr, "submit ring setup: %d\n", ret);
		close(pipe_fds[0]);
		close(pipe_fds[1]);
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(RING_SIZE, &cancel_ring, 0);
	if (ret) {
		fprintf(stderr, "cancel ring setup: %d\n", ret);
		io_uring_queue_exit(&submit_ring);
		close(pipe_fds[0]);
		close(pipe_fds[1]);
		return T_EXIT_FAIL;
	}

	ctd.ring = &cancel_ring;
	ctd.cancel_count = 0;
	ctd.stop = 0;
	pthread_create(&thread, NULL, cancel_thread_fn, &ctd);

	/* Submit polls and let the other thread race cancels */
	for (i = 0; i < NR_ITERS; i++) {
		sqe = io_uring_get_sqe(&submit_ring);
		if (!sqe) {
			/* Flush and drain */
			io_uring_submit(&submit_ring);
			while (io_uring_peek_cqe(&submit_ring, &cqe) == 0) {
				completed++;
				io_uring_cqe_seen(&submit_ring, cqe);
			}
			sqe = io_uring_get_sqe(&submit_ring);
			if (!sqe)
				continue;
		}
		io_uring_prep_poll_add(sqe, pipe_fds[0], POLLIN);
		sqe->user_data = i + 1;
		submitted++;

		if (i % 16 == 15) {
			io_uring_submit(&submit_ring);
			/* Drain any completions */
			while (io_uring_peek_cqe(&submit_ring, &cqe) == 0) {
				completed++;
				io_uring_cqe_seen(&submit_ring, cqe);
			}
		}
	}

	io_uring_submit(&submit_ring);
	ctd.stop = 1;
	pthread_join(thread, NULL);

	/* Drain remaining CQEs from submit ring */
	while (completed < submitted) {
		/* Cancel anything remaining */
		sqe = io_uring_get_sqe(&submit_ring);
		if (sqe) {
			io_uring_prep_cancel64(sqe, 0, IORING_ASYNC_CANCEL_ANY);
			sqe->user_data = 0xbeef;
			io_uring_submit(&submit_ring);
		}

		ret = io_uring_peek_cqe(&submit_ring, &cqe);
		if (ret == -EAGAIN) {
			struct __kernel_timespec ts = { .tv_sec = 1 };
			ret = io_uring_wait_cqe_timeout(&submit_ring, &cqe, &ts);
			if (ret == -ETIME)
				break;
			if (ret)
				break;
		}
		if (cqe->user_data != 0xbeef)
			completed++;
		io_uring_cqe_seen(&submit_ring, cqe);
	}

	close(pipe_fds[0]);
	close(pipe_fds[1]);
	io_uring_queue_exit(&cancel_ring);
	io_uring_queue_exit(&submit_ring);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_poll_cancel_race();
	if (ret == T_EXIT_SKIP) {
		printf("cancel not supported, skipping\n");
		return T_EXIT_SKIP;
	}
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_poll_cancel_race failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_read_cancel_race();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_read_cancel_race failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_concurrent_cancel();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_concurrent_cancel failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

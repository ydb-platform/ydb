#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: stress test multishot accept under rapid connection churn.
 *		Submits a multishot accept and has client threads rapidly
 *		connecting and disconnecting, verifying all connections are
 *		properly accepted.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define NR_CONNS	200
#define NR_BURST	32
#define NR_BURST_ROUNDS	4
#define NR_RECONNECTS	1000
#define MSHOT_UDATA	1000
#define SEND_BYTE	0xa5

static int no_mshot_accept;

struct stress_ctx {
	pthread_barrier_t barrier;
	struct sockaddr_in addr;
	int nr_conns;
};

struct burst_ctx {
	pthread_barrier_t barrier;
	pthread_barrier_t round_barrier;
	struct sockaddr_in addr;
	int nr_burst;
	int nr_rounds;
};

struct reconnect_ctx {
	pthread_barrier_t barrier;
	struct sockaddr_in addr;
	int nr_reconnects;
};

static int create_listen_sock(struct sockaddr_in *addr)
{
	int fd, ret, val = 1;

	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
	if (fd < 0) {
		perror("socket");
		return -1;
	}

	setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	addr->sin_family = AF_INET;
	addr->sin_addr.s_addr = inet_addr("127.0.0.1");
	ret = t_bind_ephemeral_port(fd, addr);
	if (ret) {
		close(fd);
		return -1;
	}

	ret = listen(fd, 256);
	if (ret < 0) {
		perror("listen");
		close(fd);
		return -1;
	}

	return fd;
}

static int arm_mshot_accept(struct io_uring *ring, int listen_fd)
{
	struct io_uring_sqe *sqe;

	sqe = io_uring_get_sqe(ring);
	if (!sqe)
		return -1;
	io_uring_prep_multishot_accept(sqe, listen_fd, NULL, NULL, 0);
	io_uring_sqe_set_data64(sqe, MSHOT_UDATA);
	return io_uring_submit(ring);
}

/*
 * Client thread for stress test: rapid connect + close
 */
static void *stress_client_fn(void *data)
{
	struct stress_ctx *ctx = data;
	int i;

	pthread_barrier_wait(&ctx->barrier);

	for (i = 0; i < ctx->nr_conns; i++) {
		int fd;

		fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (fd < 0)
			break;
		if (connect(fd, (struct sockaddr *)&ctx->addr,
			    sizeof(ctx->addr)) < 0) {
			close(fd);
			break;
		}
		close(fd);
	}

	return NULL;
}

/*
 * Stress test: multishot accept with NR_CONNS rapid connections
 */
static int test_accept_mshot_stress(void)
{
	struct io_uring ring;
	struct stress_ctx ctx;
	pthread_t thread;
	int listen_fd, ret, accepted = 0;
	struct __kernel_timespec ts = { .tv_sec = 5 };

	ret = io_uring_queue_init(512, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	listen_fd = create_listen_sock(&ctx.addr);
	if (listen_fd < 0) {
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	ctx.nr_conns = NR_CONNS;
	pthread_barrier_init(&ctx.barrier, NULL, 2);

	ret = arm_mshot_accept(&ring, listen_fd);
	if (ret < 0) {
		fprintf(stderr, "arm accept: %d\n", ret);
		goto err;
	}

	pthread_create(&thread, NULL, stress_client_fn, &ctx);
	pthread_barrier_wait(&ctx.barrier);

	while (accepted < NR_CONNS) {
		struct io_uring_cqe *cqe;

		ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
		if (ret == -ETIME)
			break;
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			goto err_thread;
		}

		if (cqe->res == -EINVAL) {
			no_mshot_accept = 1;
			io_uring_cqe_seen(&ring, cqe);
			pthread_join(thread, NULL);
			close(listen_fd);
			io_uring_queue_exit(&ring);
			return T_EXIT_SKIP;
		}

		if (cqe->res < 0) {
			fprintf(stderr, "accept res: %d\n", cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto err_thread;
		}

		close(cqe->res);
		accepted++;

		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			io_uring_cqe_seen(&ring, cqe);
			arm_mshot_accept(&ring, listen_fd);
			continue;
		}

		io_uring_cqe_seen(&ring, cqe);
	}

	pthread_join(thread, NULL);

	if (accepted < NR_CONNS) {
		fprintf(stderr, "stress: accepted %d, expected %d\n",
			accepted, NR_CONNS);
		goto err;
	}

	close(listen_fd);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_thread:
	pthread_join(thread, NULL);
err:
	close(listen_fd);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Client thread for burst test: open NR_BURST connections, wait for
 * server to drain, close all, repeat.
 */
static void *burst_client_fn(void *data)
{
	struct burst_ctx *ctx = data;
	int round;

	pthread_barrier_wait(&ctx->barrier);

	for (round = 0; round < ctx->nr_rounds; round++) {
		int fds[NR_BURST];
		int i;

		for (i = 0; i < ctx->nr_burst; i++) {
			fds[i] = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
			if (fds[i] < 0)
				break;
			if (connect(fds[i], (struct sockaddr *)&ctx->addr,
				    sizeof(ctx->addr)) < 0) {
				close(fds[i]);
				fds[i] = -1;
				break;
			}
		}

		/* Signal server: all connections are open */
		pthread_barrier_wait(&ctx->round_barrier);
		/* Wait for server to drain */
		pthread_barrier_wait(&ctx->round_barrier);

		for (i = 0; i < ctx->nr_burst; i++) {
			if (fds[i] >= 0)
				close(fds[i]);
		}
	}

	return NULL;
}

/*
 * Burst test: client opens NR_BURST connections at once, server drains,
 * client closes, repeat for NR_BURST_ROUNDS.
 */
static int test_accept_mshot_burst(void)
{
	struct io_uring ring;
	struct burst_ctx ctx;
	pthread_t thread;
	int listen_fd, ret, total = 0;
	struct __kernel_timespec ts = { .tv_sec = 5 };

	if (no_mshot_accept)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(256, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	listen_fd = create_listen_sock(&ctx.addr);
	if (listen_fd < 0) {
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	ctx.nr_burst = NR_BURST;
	ctx.nr_rounds = NR_BURST_ROUNDS;
	pthread_barrier_init(&ctx.barrier, NULL, 2);
	pthread_barrier_init(&ctx.round_barrier, NULL, 2);

	ret = arm_mshot_accept(&ring, listen_fd);
	if (ret < 0)
		goto err;

	pthread_create(&thread, NULL, burst_client_fn, &ctx);
	pthread_barrier_wait(&ctx.barrier);

	for (int round = 0; round < NR_BURST_ROUNDS; round++) {
		int round_accepted = 0;

		/* Wait for client to open all connections */
		pthread_barrier_wait(&ctx.round_barrier);

		while (round_accepted < NR_BURST) {
			struct io_uring_cqe *cqe;

			ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
			if (ret == -ETIME) {
				fprintf(stderr, "burst round %d timeout at %d\n",
					round, round_accepted);
				goto err_thread;
			}
			if (ret) {
				fprintf(stderr, "wait: %d\n", ret);
				goto err_thread;
			}

			if (cqe->res < 0) {
				fprintf(stderr, "accept res: %d\n", cqe->res);
				io_uring_cqe_seen(&ring, cqe);
				goto err_thread;
			}

			close(cqe->res);
			round_accepted++;
			total++;

			if (!(cqe->flags & IORING_CQE_F_MORE)) {
				io_uring_cqe_seen(&ring, cqe);
				arm_mshot_accept(&ring, listen_fd);
				continue;
			}
			io_uring_cqe_seen(&ring, cqe);
		}

		/* Signal client: done draining */
		pthread_barrier_wait(&ctx.round_barrier);
	}

	pthread_join(thread, NULL);

	if (total != NR_BURST * NR_BURST_ROUNDS) {
		fprintf(stderr, "burst: accepted %d, expected %d\n",
			total, NR_BURST * NR_BURST_ROUNDS);
		goto err;
	}

	close(listen_fd);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_thread:
	pthread_join(thread, NULL);
err:
	close(listen_fd);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Client thread for reconnect test: connect, send 1 byte, close, repeat.
 */
static void *reconnect_client_fn(void *data)
{
	struct reconnect_ctx *ctx = data;
	unsigned char byte = SEND_BYTE;
	int i;

	pthread_barrier_wait(&ctx->barrier);

	for (i = 0; i < ctx->nr_reconnects; i++) {
		int fd;

		fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (fd < 0)
			break;
		if (connect(fd, (struct sockaddr *)&ctx->addr,
			    sizeof(ctx->addr)) < 0) {
			close(fd);
			break;
		}
		send(fd, &byte, 1, 0);
		close(fd);
	}

	return NULL;
}

/*
 * Reconnect test: client connects, sends 1 byte, closes, reconnects.
 * Server accepts each and reads the byte.
 */
static int test_accept_mshot_reconnect(void)
{
	struct io_uring ring;
	struct reconnect_ctx ctx;
	pthread_t thread;
	int listen_fd, ret, accepted = 0;
	struct __kernel_timespec ts = { .tv_sec = 5 };

	if (no_mshot_accept)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(512, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	listen_fd = create_listen_sock(&ctx.addr);
	if (listen_fd < 0) {
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	ctx.nr_reconnects = NR_RECONNECTS;
	pthread_barrier_init(&ctx.barrier, NULL, 2);

	ret = arm_mshot_accept(&ring, listen_fd);
	if (ret < 0)
		goto err;

	pthread_create(&thread, NULL, reconnect_client_fn, &ctx);
	pthread_barrier_wait(&ctx.barrier);

	while (accepted < NR_RECONNECTS) {
		struct io_uring_cqe *cqe;
		unsigned char buf;
		int conn_fd;

		ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
		if (ret == -ETIME)
			break;
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			goto err_thread;
		}

		if (cqe->res < 0) {
			fprintf(stderr, "accept res: %d\n", cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto err_thread;
		}

		conn_fd = cqe->res;

		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			io_uring_cqe_seen(&ring, cqe);
			arm_mshot_accept(&ring, listen_fd);
		} else {
			io_uring_cqe_seen(&ring, cqe);
		}

		/* Read the byte the client sent */
		ret = read(conn_fd, &buf, 1);
		if (ret == 1 && buf != SEND_BYTE) {
			fprintf(stderr, "bad byte: 0x%x\n", buf);
			close(conn_fd);
			goto err_thread;
		}

		close(conn_fd);
		accepted++;
	}

	pthread_join(thread, NULL);

	if (accepted < NR_RECONNECTS) {
		fprintf(stderr, "reconnect: accepted %d, expected %d\n",
			accepted, NR_RECONNECTS);
		goto err;
	}

	close(listen_fd);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_thread:
	pthread_join(thread, NULL);
err:
	close(listen_fd);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_accept_mshot_stress();
	if (ret == T_EXIT_SKIP) {
		printf("Multishot accept not supported, skipping\n");
		return T_EXIT_SKIP;
	}
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_accept_mshot_stress failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_accept_mshot_burst();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_accept_mshot_burst failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_accept_mshot_reconnect();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_accept_mshot_reconnect failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

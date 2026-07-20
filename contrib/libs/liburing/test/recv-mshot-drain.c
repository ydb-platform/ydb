#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: stress test multishot recv with buffer ring exhaustion and
 *		refill. Verifies that multishot recv handles the case where
 *		the buffer ring runs dry during active receives, and that
 *		data is correctly received after refilling buffers.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		1
#define BUF_SIZE	128
/* Intentionally small to force exhaustion */
#define NR_BUFS		4
#define NR_SENDS	4096
#define SEND_SIZE	64

struct thread_data {
	pthread_barrier_t connect_barrier;
	int port;
};

static void provide_buffers(struct io_uring_buf_ring *br, void *base,
			    int nr_bufs, int buf_size, int start_bid)
{
	int i;

	for (i = 0; i < nr_bufs; i++) {
		void *addr = base + i * buf_size;
		io_uring_buf_ring_add(br, addr, buf_size, start_bid + i,
				      io_uring_buf_ring_mask(nr_bufs), i);
	}
	io_uring_buf_ring_advance(br, nr_bufs);
}

static void *send_thread(void *arg)
{
	struct thread_data *td = arg;
	struct sockaddr_in saddr;
	char buf[SEND_SIZE];
	int fd, ret, i;

	memset(buf, 0xaa, sizeof(buf));

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		perror("socket");
		return (void *)(intptr_t)1;
	}

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(td->port);
	inet_pton(AF_INET, "127.0.0.1", &saddr.sin_addr);

	pthread_barrier_wait(&td->connect_barrier);

	ret = connect(fd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("connect");
		close(fd);
		return (void *)(intptr_t)1;
	}

	/* Send more data than we have buffers for */
	for (i = 0; i < NR_SENDS; i++) {
		/* Fill pattern: iteration number */
		memset(buf, i & 0xff, sizeof(buf));
		ret = send(fd, buf, sizeof(buf), 0);
		if (ret < 0) {
			if (errno == EPIPE)
				break;
			perror("send");
			close(fd);
			return (void *)(intptr_t)1;
		}
		/* Small delay to spread sends over time */
		if (i % 8 == 7)
			usleep(1000);
	}

	close(fd);
	return NULL;
}

static int test_recv_drain_refill(void)
{
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct thread_data td;
	struct sockaddr_in saddr;
	pthread_t thread;
	void *buf_base;
	int sockfd, connfd, ret, val;
	int total_recv = 0;
	socklen_t socklen;
	int port;

	ret = io_uring_queue_init(32, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Allocate buffer ring */
	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL || ret == -ENOENT) {
			io_uring_queue_exit(&ring);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "buf ring setup: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	buf_base = malloc(NR_BUFS * BUF_SIZE);
	if (!buf_base) {
		fprintf(stderr, "malloc\n");
		goto err;
	}

	provide_buffers(br, buf_base, NR_BUFS, BUF_SIZE, 0);

	/* Setup listening socket */
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		perror("socket");
		goto err;
	}

	val = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = 0;

	ret = bind(sockfd, (struct sockaddr *)&saddr, sizeof(saddr));
	if (ret < 0) {
		perror("bind");
		close(sockfd);
		goto err;
	}

	socklen = sizeof(saddr);
	getsockname(sockfd, (struct sockaddr *)&saddr, &socklen);
	port = ntohs(saddr.sin_port);

	ret = listen(sockfd, 1);
	if (ret < 0) {
		perror("listen");
		close(sockfd);
		goto err;
	}

	/* Start sender thread */
	td.port = port;
	pthread_barrier_init(&td.connect_barrier, NULL, 2);
	pthread_create(&thread, NULL, send_thread, &td);
	pthread_barrier_wait(&td.connect_barrier);

	socklen = sizeof(saddr);
	connfd = accept(sockfd, (struct sockaddr *)&saddr, &socklen);
	if (connfd < 0) {
		perror("accept");
		close(sockfd);
		goto err;
	}

	/* Submit multishot recv */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recv_multishot(sqe, connfd, NULL, 0, 0);
	sqe->buf_group = BGID;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit recv: %d\n", ret);
		goto err_conn;
	}

	/*
	 * Process CQEs. The multishot will eventually run out of buffers.
	 * When it does (no IORING_CQE_F_MORE), we refill and re-arm.
	 */
	while (1) {
		struct __kernel_timespec ts = { .tv_sec = 2 };

		ret = io_uring_wait_cqe_timeout(&ring, &cqe, &ts);
		if (ret == -ETIME)
			break;
		if (ret) {
			fprintf(stderr, "wait cqe: %d\n", ret);
			goto err_conn;
		}

		if (cqe->res == -ENOBUFS) {
			/* Buffer ring exhausted - refill and re-arm */
			io_uring_cqe_seen(&ring, cqe);
			provide_buffers(br, buf_base, NR_BUFS, BUF_SIZE, 0);

			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_recv_multishot(sqe, connfd, NULL, 0, 0);
			sqe->buf_group = BGID;
			sqe->flags |= IOSQE_BUFFER_SELECT;
			sqe->user_data = 1;

			ret = io_uring_submit(&ring);
			if (ret != 1) {
				fprintf(stderr, "rearm submit: %d\n", ret);
				goto err_conn;
			}
			continue;
		}

		if (cqe->res == 0) {
			/* EOF */
			io_uring_cqe_seen(&ring, cqe);
			break;
		}

		if (cqe->res < 0) {
			fprintf(stderr, "recv error: %d\n", cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto err_conn;
		}

		total_recv += cqe->res;

		/* Return buffer if we got one */
		if (cqe->flags & IORING_CQE_F_BUFFER) {
			int bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
			void *addr = buf_base + bid * BUF_SIZE;
			io_uring_buf_ring_add(br, addr, BUF_SIZE, bid,
					      io_uring_buf_ring_mask(NR_BUFS), 0);
			io_uring_buf_ring_advance(br, 1);
		}

		/* If no F_MORE, multishot terminated - re-arm */
		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			io_uring_cqe_seen(&ring, cqe);

			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_recv_multishot(sqe, connfd, NULL, 0, 0);
			sqe->buf_group = BGID;
			sqe->flags |= IOSQE_BUFFER_SELECT;
			sqe->user_data = 1;

			ret = io_uring_submit(&ring);
			if (ret != 1) {
				fprintf(stderr, "rearm submit: %d\n", ret);
				goto err_conn;
			}
			continue;
		}

		io_uring_cqe_seen(&ring, cqe);
	}

	pthread_join(thread, NULL);

	if (total_recv != NR_SENDS * SEND_SIZE) {
		fprintf(stderr, "recv %d bytes, expected %d\n",
			total_recv, NR_SENDS * SEND_SIZE);
		goto err_conn;
	}

	close(connfd);
	close(sockfd);
	free(buf_base);
	io_uring_free_buf_ring(&ring, br, NR_BUFS, BGID);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_conn:
	close(connfd);
	close(sockfd);
err:
	free(buf_base);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_recv_drain_refill();
	if (ret == T_EXIT_SKIP) {
		printf("Buffer rings not supported, skipping\n");
		return T_EXIT_SKIP;
	}
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_recv_drain_refill failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

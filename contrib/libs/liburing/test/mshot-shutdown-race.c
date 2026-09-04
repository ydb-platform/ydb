#include "../config-host.h"
// SPDX-License-Identifier: MIT
/*
 * Description: test AF_UNIX multishot handling of a send immediately
 *		followed by a shutdown of the socket, testing that neither
 *		of those events even if they both arrive before the kernel
 *		can react to the first one. Based on a reproducer from a
 *		bug report here:
 *
 *		https://github.com/axboe/liburing/issues/1549
 */
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#define DUMMY_SEND_SIZE	123
#define BUF_COUNT	4
#define BUF_SIZE	256
#define BUF_GRP		0
#define ITERATIONS	10000

struct thread_data {
	pthread_t thread;
	pthread_barrier_t barrier;
	int server_socket;
	int client_socket;
	int nr_sends;
	int stop;
};

static bool use_af_inet;
static int received_bytes;

#define CHECK(x)	do {					\
	if (!(x)) {						\
		fprintf(stderr, "Error: " #x "\n");		\
		_exit(1);					\
	}							\
} while (0)

static void *client_thread(void *arg)
{
	static char buf[DUMMY_SEND_SIZE];
	struct thread_data *td = arg;
	int res;

	while (!td->stop) {
		int j;

		pthread_barrier_wait(&td->barrier);

		for (j = 0; j < td->nr_sends; j++) {
			res = send(td->client_socket, buf, sizeof(buf), 0);
			CHECK(res == sizeof(buf));
		}

		res = shutdown(td->client_socket, SHUT_WR);
		CHECK(res == 0);
	}

	return NULL;
}

static void create_sockets(struct thread_data *td)
{
	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = 0,
		.sin_addr = { .s_addr = htonl(INADDR_LOOPBACK) },
	};
	socklen_t addr_len = sizeof(addr);
	int listen_fd, res;

	if (!use_af_inet) {
		int sockets[2];

		CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
		td->client_socket = sockets[0];
		td->server_socket = sockets[1];
		return;
	}

	listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	CHECK(listen_fd > 0);

	res = bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr));
	CHECK(res == 0);

	res = getsockname(listen_fd, (struct sockaddr *)&addr, &addr_len);
	CHECK(res == 0);

	res = listen(listen_fd, 1);
	CHECK(res == 0);

	td->client_socket = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	CHECK(td->client_socket > 0);

	res = connect(td->client_socket, (struct sockaddr *)&addr, sizeof(addr));
	CHECK(res == 0);

	td->server_socket = accept4(listen_fd, NULL, NULL, SOCK_CLOEXEC);
	CHECK(td->server_socket > 0);
	close(listen_fd);
}

static int last_received_bytes;

static void sig_alrm(int sig)
{
	if (received_bytes == last_received_bytes) {
		fprintf(stderr, "Seems stuck, exit\n");
		exit(1);
	}
	last_received_bytes = received_bytes;
	alarm(5);
}

int main(int argc, char *argv[])
{
	static char buffer_bytes[BUF_COUNT * BUF_SIZE];
	struct io_uring_buf_ring *ring;
	struct sigaction act = { };
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring iouring;
	struct thread_data td;
	bool received_eof;
	uint16_t buffer_id;
	int err, i;

	if (argc > 1)
		return T_EXIT_SKIP;

	act.sa_handler = sig_alrm;
	act.sa_flags = SA_RESTART;
	sigaction(SIGALRM, &act, NULL);
	alarm(5);

	use_af_inet = !!getenv("TEST_USE_INET");

	CHECK(io_uring_queue_init(8, &iouring, 0) >= 0);

	pthread_barrier_init(&td.barrier, NULL, 2);
	td.stop = 0;

	pthread_create(&td.thread, NULL, client_thread, &td);
	pthread_detach(td.thread);

	ring = io_uring_setup_buf_ring(&iouring, BUF_COUNT, BUF_GRP, 0, &err);
	CHECK(err == 0);

	for (i = 0; i < BUF_COUNT; i++) {
		io_uring_buf_ring_add(ring, &buffer_bytes[BUF_SIZE * i],
					BUF_SIZE, (uint16_t)i,
					BUF_COUNT - 1, i);
	}
	io_uring_buf_ring_advance(ring, BUF_COUNT);

	for (i = 0; i < ITERATIONS; i++) {
		int expected_bytes;

		td.nr_sends = (i & 1) + 1;
		expected_bytes = td.nr_sends * DUMMY_SEND_SIZE;

		create_sockets(&td);

		sqe = io_uring_get_sqe(&iouring);
		CHECK(sqe);
		io_uring_prep_recv_multishot(sqe, td.server_socket, NULL, 0, 0);
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = BUF_GRP;

		io_uring_submit(&iouring);

		pthread_barrier_wait(&td.barrier);

		received_bytes = 0;
		received_eof = false;

		while (!received_eof) {
			cqe = NULL;
			do {
				io_uring_wait_cqe(&iouring, &cqe);
			} while (!cqe);

			if (cqe->res > 0) {
				CHECK(cqe->flags & IORING_CQE_F_MORE);
				CHECK(cqe->flags & IORING_CQE_F_BUFFER);

				buffer_id = cqe->flags >> 16;

				io_uring_buf_ring_add(ring,
					&buffer_bytes[BUF_SIZE * buffer_id],
					BUF_SIZE, buffer_id,
					BUF_COUNT - 1, buffer_id);
				io_uring_buf_ring_advance(ring, 1);

				received_bytes += cqe->res;
			} else if (cqe->res == 0) {
				CHECK(!received_eof);
				received_eof = true;

				CHECK(!(cqe->flags & IORING_CQE_F_MORE));
				CHECK(!(cqe->flags & IORING_CQE_F_BUFFER));
			} else {
				CHECK(!"Unexpected recv");
			}

			io_uring_cqe_seen(&iouring, cqe);
		}

		CHECK(received_bytes == expected_bytes);

		close(td.client_socket);
		close(td.server_socket);
	}

	td.stop = 1;
	return T_EXIT_PASS;
}

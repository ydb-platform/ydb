#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: verify that a single buffer ring shared across multiple
 *		concurrent multishot recv operations works correctly.
 *		Tests buffer distribution, data integrity, and exhaustion
 *		handling with refill across multiple streams.
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

#define NR_STREAMS	4
#define BGID		1
#define BUF_SIZE	256
#define NR_BUFS_BASIC	32
#define NR_BUFS_SMALL	8
#define NR_SENDS	32
#define NR_SENDS_EXHAUST 64
#define NR_SENDS_BIG	128
#define NR_SENDS_SMALL	8
#define SEND_SIZE	128

#define UDATA_STREAM(id)	(((__u64)(id) + 1) << 32)
#define UDATA_GET_STREAM(ud)	((int)(((ud) >> 32) - 1))

struct stream {
	int send_fd;
	int recv_fd;
	int nr_sends;
	int bytes_sent;
	int bytes_recv;
};

struct sender_ctx {
	pthread_barrier_t *barrier;
	struct stream *stream;
	int stream_id;
};

struct test_ctx {
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	void *buf_base;
	int nr_bufs;
	struct stream streams[NR_STREAMS];
};

static int no_buf_ring;
static int no_mshot_recv;

static int setup_streams(struct test_ctx *ctx)
{
	int i;

	for (i = 0; i < NR_STREAMS; i++) {
		int fds[2];
		int ret;

		ret = t_create_socket_pair(fds, true);
		if (ret) {
			fprintf(stderr, "socket pair %d: %d\n", i, ret);
			/* cleanup already created pairs */
			while (--i >= 0) {
				close(ctx->streams[i].recv_fd);
				close(ctx->streams[i].send_fd);
			}
			return -1;
		}
		ctx->streams[i].recv_fd = fds[0];
		ctx->streams[i].send_fd = fds[1];
		ctx->streams[i].bytes_sent = 0;
		ctx->streams[i].bytes_recv = 0;
	}

	return 0;
}

static void close_streams(struct test_ctx *ctx)
{
	int i;

	for (i = 0; i < NR_STREAMS; i++) {
		close(ctx->streams[i].recv_fd);
		close(ctx->streams[i].send_fd);
	}
}

static int setup_buf_ring(struct test_ctx *ctx, int nr_bufs)
{
	int ret, i;

	ctx->nr_bufs = nr_bufs;
	ctx->buf_base = malloc(nr_bufs * BUF_SIZE);
	if (!ctx->buf_base)
		return -ENOMEM;

	ctx->br = io_uring_setup_buf_ring(&ctx->ring, nr_bufs, BGID, 0, &ret);
	if (!ctx->br) {
		free(ctx->buf_base);
		ctx->buf_base = NULL;
		if (ret == -EINVAL || ret == -ENOENT)
			return ret;
		fprintf(stderr, "buf ring setup: %d\n", ret);
		return ret;
	}

	for (i = 0; i < nr_bufs; i++) {
		io_uring_buf_ring_add(ctx->br, ctx->buf_base + i * BUF_SIZE,
				      BUF_SIZE, i,
				      io_uring_buf_ring_mask(nr_bufs), i);
	}
	io_uring_buf_ring_advance(ctx->br, nr_bufs);

	return 0;
}

static void cleanup_buf_ring(struct test_ctx *ctx)
{
	if (ctx->br) {
		io_uring_free_buf_ring(&ctx->ring, ctx->br, ctx->nr_bufs, BGID);
		ctx->br = NULL;
	}
	free(ctx->buf_base);
	ctx->buf_base = NULL;
}

static void recycle_buffer(struct test_ctx *ctx, int bid)
{
	io_uring_buf_ring_add(ctx->br,
			      ctx->buf_base + bid * BUF_SIZE,
			      BUF_SIZE, bid,
			      io_uring_buf_ring_mask(ctx->nr_bufs), 0);
	io_uring_buf_ring_advance(ctx->br, 1);
}

static void arm_recv(struct io_uring *ring, int fd, int stream_id)
{
	struct io_uring_sqe *sqe;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_recv_multishot(sqe, fd, NULL, 0, 0);
	sqe->buf_group = BGID;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	io_uring_sqe_set_data64(sqe, UDATA_STREAM(stream_id));
}

static void *sender_fn(void *data)
{
	struct sender_ctx *sc = data;
	struct stream *s = sc->stream;
	char buf[SEND_SIZE];
	int i;

	memset(buf, sc->stream_id & 0xff, sizeof(buf));

	pthread_barrier_wait(sc->barrier);

	for (i = 0; i < s->nr_sends; i++) {
		int ret = send(s->send_fd, buf, sizeof(buf), 0);

		if (ret < 0)
			break;
		s->bytes_sent += ret;
	}

	shutdown(s->send_fd, SHUT_WR);
	return NULL;
}

/*
 * Run a shared buffer ring test with the given parameters.
 */
static int run_shared_test(int nr_bufs, int *sends_per_stream)
{
	struct test_ctx ctx = { };
	struct sender_ctx senders[NR_STREAMS];
	pthread_t threads[NR_STREAMS];
	pthread_barrier_t barrier;
	struct __kernel_timespec ts = { .tv_sec = 5 };
	int ret, i, done = 0;

	ret = io_uring_queue_init(128, &ctx.ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = setup_streams(&ctx);
	if (ret) {
		io_uring_queue_exit(&ctx.ring);
		return T_EXIT_FAIL;
	}

	ret = setup_buf_ring(&ctx, nr_bufs);
	if (ret == -EINVAL || ret == -ENOENT) {
		no_buf_ring = 1;
		close_streams(&ctx);
		io_uring_queue_exit(&ctx.ring);
		return T_EXIT_SKIP;
	}
	if (ret) {
		close_streams(&ctx);
		io_uring_queue_exit(&ctx.ring);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_STREAMS; i++)
		ctx.streams[i].nr_sends = sends_per_stream[i];

	pthread_barrier_init(&barrier, NULL, NR_STREAMS + 1);

	for (i = 0; i < NR_STREAMS; i++) {
		senders[i].barrier = &barrier;
		senders[i].stream = &ctx.streams[i];
		senders[i].stream_id = i;
		pthread_create(&threads[i], NULL, sender_fn, &senders[i]);
	}

	/* Arm multishot recv on all streams */
	for (i = 0; i < NR_STREAMS; i++)
		arm_recv(&ctx.ring, ctx.streams[i].recv_fd, i);

	ret = io_uring_submit(&ctx.ring);
	if (ret != NR_STREAMS) {
		fprintf(stderr, "submit: %d\n", ret);
		goto err;
	}

	/* Release all senders */
	pthread_barrier_wait(&barrier);

	while (done < NR_STREAMS) {
		struct io_uring_cqe *cqe;
		int sid, bid;

		ret = io_uring_wait_cqe_timeout(&ctx.ring, &cqe, &ts);
		if (ret == -ETIME) {
			fprintf(stderr, "timeout: %d streams done\n", done);
			goto err;
		}
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			goto err;
		}

		sid = UDATA_GET_STREAM(cqe->user_data);
		if (sid < 0 || sid >= NR_STREAMS) {
			fprintf(stderr, "bad user_data: 0x%llx\n",
				(unsigned long long)cqe->user_data);
			io_uring_cqe_seen(&ctx.ring, cqe);
			goto err;
		}

		/* Multishot recv not supported */
		if (cqe->res == -EINVAL) {
			no_mshot_recv = 1;
			io_uring_cqe_seen(&ctx.ring, cqe);
			goto skip;
		}

		/* EOF */
		if (cqe->res == 0) {
			done++;
			io_uring_cqe_seen(&ctx.ring, cqe);
			continue;
		}

		/* Buffer exhaustion */
		if (cqe->res == -ENOBUFS) {
			io_uring_cqe_seen(&ctx.ring, cqe);
			arm_recv(&ctx.ring, ctx.streams[sid].recv_fd, sid);
			io_uring_submit(&ctx.ring);
			continue;
		}

		if (cqe->res < 0) {
			fprintf(stderr, "recv stream %d: %d\n", sid, cqe->res);
			io_uring_cqe_seen(&ctx.ring, cqe);
			goto err;
		}

		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "no buffer flag stream %d\n", sid);
			io_uring_cqe_seen(&ctx.ring, cqe);
			goto err;
		}

		bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
		if (bid >= nr_bufs) {
			fprintf(stderr, "bad bid %d (max %d)\n", bid, nr_bufs);
			io_uring_cqe_seen(&ctx.ring, cqe);
			goto err;
		}

		/* Verify data integrity */
		{
			unsigned char *data = ctx.buf_base + bid * BUF_SIZE;
			int j;

			for (j = 0; j < cqe->res; j++) {
				if (data[j] != (unsigned char)(sid & 0xff)) {
					fprintf(stderr,
						"data mismatch stream %d byte %d: "
						"got 0x%x expected 0x%x\n",
						sid, j, data[j],
						(unsigned char)(sid & 0xff));
					io_uring_cqe_seen(&ctx.ring, cqe);
					goto err;
				}
			}
		}

		ctx.streams[sid].bytes_recv += cqe->res;
		recycle_buffer(&ctx, bid);

		if (!(cqe->flags & IORING_CQE_F_MORE)) {
			io_uring_cqe_seen(&ctx.ring, cqe);
			arm_recv(&ctx.ring, ctx.streams[sid].recv_fd, sid);
			io_uring_submit(&ctx.ring);
			continue;
		}

		io_uring_cqe_seen(&ctx.ring, cqe);
	}

	for (i = 0; i < NR_STREAMS; i++)
		pthread_join(threads[i], NULL);

	/* Verify all bytes received */
	for (i = 0; i < NR_STREAMS; i++) {
		if (ctx.streams[i].bytes_recv != ctx.streams[i].bytes_sent) {
			fprintf(stderr,
				"stream %d: recv %d bytes, sent %d bytes\n",
				i, ctx.streams[i].bytes_recv,
				ctx.streams[i].bytes_sent);
			goto cleanup_fail;
		}
	}

	cleanup_buf_ring(&ctx);
	close_streams(&ctx);
	io_uring_queue_exit(&ctx.ring);
	return T_EXIT_PASS;

skip:
	for (i = 0; i < NR_STREAMS; i++)
		pthread_join(threads[i], NULL);
	cleanup_buf_ring(&ctx);
	close_streams(&ctx);
	io_uring_queue_exit(&ctx.ring);
	return T_EXIT_SKIP;

err:
	for (i = 0; i < NR_STREAMS; i++)
		pthread_join(threads[i], NULL);
cleanup_fail:
	cleanup_buf_ring(&ctx);
	close_streams(&ctx);
	io_uring_queue_exit(&ctx.ring);
	return T_EXIT_FAIL;
}

/*
 * Basic test: 4 streams sharing 32 buffers, moderate load
 */
static int test_shared_basic(void)
{
	int sends[NR_STREAMS];
	int i;

	for (i = 0; i < NR_STREAMS; i++)
		sends[i] = NR_SENDS;

	return run_shared_test(NR_BUFS_BASIC, sends);
}

/*
 * Exhaustion test: 4 streams sharing only 8 buffers under heavy load
 */
static int test_shared_exhaust(void)
{
	int sends[NR_STREAMS];
	int i;

	if (no_buf_ring || no_mshot_recv)
		return T_EXIT_SKIP;

	for (i = 0; i < NR_STREAMS; i++)
		sends[i] = NR_SENDS_EXHAUST;

	return run_shared_test(NR_BUFS_SMALL, sends);
}

/*
 * Uneven test: one heavy stream (128 sends), three light streams (8 sends)
 */
static int test_shared_uneven(void)
{
	int sends[NR_STREAMS];
	int i;

	if (no_buf_ring || no_mshot_recv)
		return T_EXIT_SKIP;

	sends[0] = NR_SENDS_BIG;
	for (i = 1; i < NR_STREAMS; i++)
		sends[i] = NR_SENDS_SMALL;

	return run_shared_test(NR_BUFS_BASIC, sends);
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_shared_basic();
	if (ret == T_EXIT_SKIP) {
		printf("Buffer rings or multishot recv not supported, skipping\n");
		return T_EXIT_SKIP;
	}
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_shared_basic failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_shared_exhaust();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_shared_exhaust failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_shared_uneven();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_shared_uneven failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

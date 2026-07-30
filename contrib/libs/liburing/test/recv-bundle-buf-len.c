#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: Test that a provided buffer ring descriptor's len is not
 *		persistently corrupted by a non-incremental bundle operation
 *		that truncates the head buffer.
 *
 *		1) A bundle recv that fails with -EAGAIN (no data available)
 *		   must NOT leave the head buffer's len shrunk to the
 *		   requested size. A later operation using the same buffer
 *		   group must still see the full buffer length.
 *
 *		2) A bundle recv that succeeds while truncating the buffer
 *		   (sqe->len < buf->len) must post a completion that matches
 *		   the actual transfer length, not the original buffer len,
 *		   and must consume the buffer correctly.
 *
 *		Based on the reproducer from Federico Brasili:
 *		https://lore.kernel.org/io-uring/CAAEr8jbY60noGj1fw_k91UJRBkyiRVoS6=nLhZ7Svwidjn4CAA@mail.gmail.com/
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		1
#define NR_BUFS		2
#define BUF_SIZE	4096
#define SHORT_LEN	32

static int no_buf_ring, no_bundle;

struct test_ctx {
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	void *buf_mem;
};

static void *buf_addr(struct test_ctx *t, int bid)
{
	return (char *) t->buf_mem + bid * BUF_SIZE;
}

/*
 * Set up a ring + a non-incremental provided buffer ring with NR_BUFS
 * buffers of BUF_SIZE each, fully published to the kernel.
 */
static int setup(struct test_ctx *t)
{
	struct io_uring_params p = { };
	int ret, i, mask;

	ret = io_uring_queue_init_params(8, &t->ring, &p);
	if (ret < 0) {
		fprintf(stderr, "queue init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (!(p.features & IORING_FEAT_RECVSEND_BUNDLE)) {
		no_bundle = 1;
		io_uring_queue_exit(&t->ring);
		return T_EXIT_SKIP;
	}

	if (posix_memalign(&t->buf_mem, 4096, NR_BUFS * BUF_SIZE)) {
		perror("posix_memalign");
		io_uring_queue_exit(&t->ring);
		return T_EXIT_FAIL;
	}

	t->br = io_uring_setup_buf_ring(&t->ring, NR_BUFS, BGID, 0, &ret);
	if (!t->br) {
		if (ret == -EINVAL) {
			no_buf_ring = 1;
			free(t->buf_mem);
			io_uring_queue_exit(&t->ring);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "buf ring setup: %d\n", ret);
		free(t->buf_mem);
		io_uring_queue_exit(&t->ring);
		return T_EXIT_FAIL;
	}

	mask = io_uring_buf_ring_mask(NR_BUFS);
	for (i = 0; i < NR_BUFS; i++)
		io_uring_buf_ring_add(t->br, buf_addr(t, i), BUF_SIZE, i, mask, i);
	io_uring_buf_ring_advance(t->br, NR_BUFS);

	return T_EXIT_PASS;
}

static void teardown(struct test_ctx *t)
{
	io_uring_free_buf_ring(&t->ring, t->br, NR_BUFS, BGID);
	io_uring_queue_exit(&t->ring);
	free(t->buf_mem);
}

static int submit_wait(struct test_ctx *t, struct io_uring_cqe **cqe)
{
	int ret;

	ret = io_uring_submit(&t->ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return -1;
	}
	ret = io_uring_wait_cqe(&t->ring, cqe);
	if (ret < 0) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		return -1;
	}
	return 0;
}

/*
 * Case 1: a failed (-EAGAIN) bundle recv on an empty socket must not shrink
 * the head buffer's len. We verify both the raw descriptor and that a later
 * unrelated READ using the same group sees the full buffer.
 */
static int test_eagain_no_corrupt(void)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct test_ctx t;
	int ret, sv[2], pfd[2], bid;
	char *pipe_buf;

	ret = setup(&t);
	if (ret != T_EXIT_PASS)
		return ret;

	if (socketpair(AF_UNIX, SOCK_DGRAM, 0, sv) < 0) {
		perror("socketpair");
		teardown(&t);
		return T_EXIT_FAIL;
	}

	/* empty socket, MSG_DONTWAIT -> -EAGAIN, bundle, len=1 */
	sqe = io_uring_get_sqe(&t.ring);
	io_uring_prep_recv(sqe, sv[0], NULL, 1, MSG_DONTWAIT);
	sqe->ioprio |= IORING_RECVSEND_BUNDLE;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_group = BGID;

	if (submit_wait(&t, &cqe))
		goto fail;

	if (cqe->res == -EINVAL) {
		no_bundle = 1;
		io_uring_cqe_seen(&t.ring, cqe);
		goto skip;
	}
	if (cqe->res != -EAGAIN) {
		fprintf(stderr, "case1: expected -EAGAIN, got %d\n", cqe->res);
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	io_uring_cqe_seen(&t.ring, cqe);

	/* the failed recv must not have consumed or shrunk buffer 0 */
	if (t.br->bufs[0].len != BUF_SIZE) {
		fprintf(stderr, "case1: head buf len corrupted: %u (want %u)\n",
			t.br->bufs[0].len, BUF_SIZE);
		goto fail;
	}

	/*
	 * User-visible impact: an unrelated READ from a pipe using the same
	 * buffer group must consume the full buffer, not the poisoned len.
	 */
	if (pipe(pfd) < 0) {
		perror("pipe");
		goto fail;
	}
	pipe_buf = malloc(BUF_SIZE);
	memset(pipe_buf, 0x5a, BUF_SIZE);
	if (write(pfd[1], pipe_buf, BUF_SIZE) != BUF_SIZE) {
		perror("write pipe");
		free(pipe_buf);
		close(pfd[0]);
		close(pfd[1]);
		goto fail;
	}
	free(pipe_buf);

	sqe = io_uring_get_sqe(&t.ring);
	io_uring_prep_read(sqe, pfd[0], NULL, BUF_SIZE, 0);
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_group = BGID;

	ret = submit_wait(&t, &cqe);
	close(pfd[0]);
	close(pfd[1]);
	if (ret)
		goto fail;

	if (cqe->res != BUF_SIZE) {
		fprintf(stderr, "case1: read consumed poisoned len: res=%d "
			"(want %d)\n", cqe->res, BUF_SIZE);
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
		fprintf(stderr, "case1: read did not select a buffer\n");
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
	if (bid != 0) {
		fprintf(stderr, "case1: read used wrong bid %d\n", bid);
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	io_uring_cqe_seen(&t.ring, cqe);

	close(sv[0]);
	close(sv[1]);
	teardown(&t);
	return T_EXIT_PASS;
skip:
	close(sv[0]);
	close(sv[1]);
	teardown(&t);
	return T_EXIT_SKIP;
fail:
	close(sv[0]);
	close(sv[1]);
	teardown(&t);
	return T_EXIT_FAIL;
}

/*
 * Case 2: a successful bundle recv that truncates the buffer (len=32 on a
 * 4096 buffer) must post a completion matching the actual transfer length
 * (32), not the buffer's original len, and must read only the truncated
 * amount even though more data is queued.
 */
static int test_success_trim(void)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct test_ctx t;
	char snd[64], *got;
	int ret, sv[2], bid, i;

	ret = setup(&t);
	if (ret != T_EXIT_PASS)
		return ret;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
		perror("socketpair");
		teardown(&t);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < (int) sizeof(snd); i++)
		snd[i] = 'A' + (i % 26);
	if (write(sv[1], snd, sizeof(snd)) != sizeof(snd)) {
		perror("write sock");
		goto fail;
	}

	/* 64 bytes available, ask for 32: trim to 32, expect res == 32 */
	sqe = io_uring_get_sqe(&t.ring);
	io_uring_prep_recv(sqe, sv[0], NULL, SHORT_LEN, MSG_DONTWAIT);
	sqe->ioprio |= IORING_RECVSEND_BUNDLE;
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_group = BGID;

	if (submit_wait(&t, &cqe))
		goto fail;

	if (cqe->res == -EINVAL) {
		no_bundle = 1;
		io_uring_cqe_seen(&t.ring, cqe);
		goto skip;
	}
	if (cqe->res != SHORT_LEN) {
		fprintf(stderr, "case2: expected res=%d, got %d\n",
			SHORT_LEN, cqe->res);
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
		fprintf(stderr, "case2: recv did not select a buffer\n");
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
	if (bid != 0) {
		fprintf(stderr, "case2: recv used wrong bid %d\n", bid);
		io_uring_cqe_seen(&t.ring, cqe);
		goto fail;
	}
	io_uring_cqe_seen(&t.ring, cqe);

	/* the 32 bytes landed in buffer 0 and match the head of the stream */
	got = buf_addr(&t, 0);
	if (memcmp(got, snd, SHORT_LEN)) {
		fprintf(stderr, "case2: received data mismatch\n");
		goto fail;
	}

	close(sv[0]);
	close(sv[1]);
	teardown(&t);
	return T_EXIT_PASS;
skip:
	close(sv[0]);
	close(sv[1]);
	teardown(&t);
	return T_EXIT_SKIP;
fail:
	close(sv[0]);
	close(sv[1]);
	teardown(&t);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_eagain_no_corrupt();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_eagain_no_corrupt failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_ring || no_bundle)
		return T_EXIT_SKIP;

	ret = test_success_trim();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_success_trim failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

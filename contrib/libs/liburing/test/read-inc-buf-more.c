#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test that IORING_CQE_F_BUF_MORE is correctly set for
 *		incremental provided buffer rings (IOU_PBUF_RING_INC).
 *
 * Two bugs existed:
 * 1) BUF_MORE was never set for non-pollable files (regular files),
 *    because the early buffer commit path discarded the information.
 * 2) BUF_MORE was not set at EOF (zero-length read), even though
 *    the buffer still had remaining space.
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"
#include "helpers.h"

#define BUF_BGID	1
#define BUF_BID		0
#define BUF_SIZE	256
#define READ_SIZE	32

static int no_buf_ring_inc;

static int do_read(struct io_uring *ring, int fd)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fd, NULL, READ_SIZE, -1);
	sqe->flags = IOSQE_BUFFER_SELECT;
	sqe->buf_group = BUF_BGID;

	io_uring_submit(ring);

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return -1;
	}

	ret = cqe->res;
	if (ret < 0) {
		fprintf(stderr, "read error: %d\n", ret);
		io_uring_cqe_seen(ring, cqe);
		return -1;
	}

	if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
		fprintf(stderr, "no buffer flag set\n");
		io_uring_cqe_seen(ring, cqe);
		return -1;
	}

	/*
	 * If we got data and the buffer still has space, BUF_MORE must be
	 * set. If we got EOF (res=0), BUF_MORE must also be set because the
	 * buffer was not consumed. In both cases, the 256 byte buffer has
	 * room left after a <= 32 byte read.
	 */
	if (!(cqe->flags & IORING_CQE_F_BUF_MORE)) {
		fprintf(stderr, "BUF_MORE not set, res=%d flags=0x%x\n",
			cqe->res, cqe->flags);
		io_uring_cqe_seen(ring, cqe);
		return -1;
	}

	io_uring_cqe_seen(ring, cqe);
	return ret;
}

/*
 * Test BUF_MORE with a pipe (pollable fd). Exercises the normal locked
 * commit path, and tests EOF handling.
 */
static int test_pipe(void)
{
	struct io_uring_buf_ring *br;
	struct io_uring ring;
	int ret, fds[2];
	char *buf;

	ret = io_uring_queue_init(64, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return 1;
	}

	if (pipe(fds) < 0) {
		perror("pipe");
		return 1;
	}

	if (posix_memalign((void **) &buf, 4096, BUF_SIZE))
		return 1;

	br = io_uring_setup_buf_ring(&ring, 1, BUF_BGID, IOU_PBUF_RING_INC,
				     &ret);
	if (!br) {
		if (ret == -EINVAL) {
			no_buf_ring_inc = 1;
			free(buf);
			close(fds[0]);
			close(fds[1]);
			io_uring_queue_exit(&ring);
			return 0;
		}
		fprintf(stderr, "buffer ring register failed: %d\n", ret);
		return 1;
	}

	io_uring_buf_ring_add(br, buf, BUF_SIZE, BUF_BID, 0, 0);
	io_uring_buf_ring_advance(br, 1);

	memset(buf, 0, BUF_SIZE);

	/* Write some data and read it - BUF_MORE should be set */
	ret = write(fds[1], "hello world!", 12);
	if (ret != 12) {
		perror("write");
		return 1;
	}

	ret = do_read(&ring, fds[0]);
	if (ret < 0) {
		fprintf(stderr, "pipe data read failed\n");
		return 1;
	}
	if (ret != 12) {
		fprintf(stderr, "pipe short read: %d\n", ret);
		return 1;
	}

	/* Close write end, read should get EOF - BUF_MORE should be set
	 * because the buffer still has space */
	close(fds[1]);

	ret = do_read(&ring, fds[0]);
	if (ret < 0) {
		fprintf(stderr, "pipe EOF read failed\n");
		return 1;
	}
	if (ret != 0) {
		fprintf(stderr, "expected EOF, got %d\n", ret);
		return 1;
	}

	io_uring_free_buf_ring(&ring, br, 1, BUF_BGID);
	io_uring_queue_exit(&ring);
	free(buf);
	close(fds[0]);
	return 0;
}

/*
 * Test BUF_MORE with a regular file (non-pollable fd). Exercises the
 * early commit path where io_should_commit() returns true.
 */
static int test_file(void)
{
	struct io_uring_buf_ring *br;
	struct io_uring ring;
	char fname[64];
	int ret, fd, i;
	char *buf;

	sprintf(fname, ".read-inc-buf-more.%d", getpid());

	fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		perror("open");
		return 1;
	}
	for (i = 0; i < 4; i++) {
		char tmp[READ_SIZE];

		memset(tmp, 'a' + i, sizeof(tmp));
		ret = write(fd, tmp, sizeof(tmp));
		if (ret != sizeof(tmp)) {
			perror("write");
			unlink(fname);
			return 1;
		}
	}
	close(fd);

	fd = open(fname, O_RDONLY);
	if (fd < 0) {
		perror("open read");
		unlink(fname);
		return 1;
	}

	ret = io_uring_queue_init(64, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		unlink(fname);
		return 1;
	}

	if (posix_memalign((void **) &buf, 4096, BUF_SIZE)) {
		unlink(fname);
		return 1;
	}

	br = io_uring_setup_buf_ring(&ring, 1, BUF_BGID, IOU_PBUF_RING_INC,
				     &ret);
	if (!br) {
		if (ret == -EINVAL) {
			no_buf_ring_inc = 1;
			free(buf);
			close(fd);
			io_uring_queue_exit(&ring);
			unlink(fname);
			return 0;
		}
		fprintf(stderr, "buffer ring register failed: %d\n", ret);
		unlink(fname);
		return 1;
	}

	io_uring_buf_ring_add(br, buf, BUF_SIZE, BUF_BID, 0, 0);
	io_uring_buf_ring_advance(br, 1);

	memset(buf, 0, BUF_SIZE);

	/* Read 4 chunks - each should have BUF_MORE since buffer is 256
	 * bytes and each read is 32 bytes */
	for (i = 0; i < 4; i++) {
		ret = do_read(&ring, fd);
		if (ret < 0) {
			fprintf(stderr, "file read %d failed\n", i);
			goto err;
		}
		if (ret != READ_SIZE) {
			fprintf(stderr, "file short read %d: %d\n", i, ret);
			goto err;
		}
	}

	io_uring_free_buf_ring(&ring, br, 1, BUF_BGID);
	io_uring_queue_exit(&ring);
	free(buf);
	close(fd);
	unlink(fname);
	return 0;
err:
	unlink(fname);
	return 1;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_pipe();
	if (ret) {
		fprintf(stderr, "test_pipe failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_ring_inc)
		return T_EXIT_SKIP;

	ret = test_file();
	if (ret) {
		fprintf(stderr, "test_file failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

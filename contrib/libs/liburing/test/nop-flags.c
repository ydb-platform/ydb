#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test NOP flags (IORING_NOP_FILE, IORING_NOP_FIXED_FILE,
 *		IORING_NOP_FIXED_BUFFER, IORING_NOP_TW, IORING_NOP_CQE32)
 *
 * These flags allow NOP to exercise file lookups, buffer lookups, and
 * task_work completion paths without real I/O, useful for targeted testing.
 */
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"
#include "helpers.h"

#ifndef IORING_NOP_INJECT_RESULT
#define IORING_NOP_INJECT_RESULT	(1U << 0)
#endif
#ifndef IORING_NOP_FILE
#define IORING_NOP_FILE			(1U << 1)
#endif
#ifndef IORING_NOP_FIXED_FILE
#define IORING_NOP_FIXED_FILE		(1U << 2)
#endif
#ifndef IORING_NOP_FIXED_BUFFER
#define IORING_NOP_FIXED_BUFFER		(1U << 3)
#endif
#ifndef IORING_NOP_TW
#define IORING_NOP_TW			(1U << 4)
#endif
#ifndef IORING_NOP_CQE32
#define IORING_NOP_CQE32		(1U << 5)
#endif

static int no_nop_flags;
static int no_nop_file;

/*
 * Detect NOP flags support using INJECT_RESULT. A kernel that supports
 * NOP flags will return the injected value; one that doesn't will
 * return 0 (plain NOP) or -EINVAL.
 */
static int test_nop_detect(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_INJECT_RESULT;
	sqe->len = 42;
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit detect: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait detect: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (cqe->res != 42) {
		/* Kernel doesn't support NOP flags */
		no_nop_flags = 1;
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_SKIP;
	}

	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

/*
 * Detect IORING_NOP_FILE support by using it with a bad fd.
 * If supported, returns -EBADF. If not, returns 0 (NOP succeeds,
 * kernel ignores the FILE flag).
 */
static int test_nop_file_bad_fd(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	if (no_nop_flags)
		return T_EXIT_SKIP;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FILE;
	sqe->fd = 9999; /* invalid fd */
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (cqe->res == 0) {
		/* Kernel doesn't support IORING_NOP_FILE */
		no_nop_file = 1;
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_SKIP;
	}

	if (cqe->res != -EBADF && cqe->res != -EINVAL) {
		fprintf(stderr, "nop bad fd res: %d (expected -EBADF)\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

/*
 * Test IORING_NOP_FILE - NOP with a normal fd lookup
 */
static int test_nop_file(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fd;

	if (no_nop_flags || no_nop_file)
		return T_EXIT_SKIP;

	fd = open("/dev/null", O_RDWR);
	if (fd < 0) {
		perror("open /dev/null");
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FILE;
	sqe->fd = fd;
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		close(fd);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		close(fd);
		return T_EXIT_FAIL;
	}

	if (cqe->res != 0) {
		if (cqe->res == -EINVAL) {
			no_nop_file = 1;
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "nop file res: %d\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		close(fd);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	close(fd);
	return T_EXIT_PASS;
}

/*
 * Test IORING_NOP_FIXED_FILE - NOP with a fixed file lookup
 */
static int test_nop_fixed_file(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, fd;

	if (no_nop_flags || no_nop_file)
		return T_EXIT_SKIP;

	fd = open("/dev/null", O_RDWR);
	if (fd < 0) {
		perror("open /dev/null");
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files(ring, &fd, 1);
	if (ret) {
		fprintf(stderr, "register files: %d\n", ret);
		close(fd);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FILE | IORING_NOP_FIXED_FILE;
	sqe->fd = 0; /* fixed file index */
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		goto err;
	}

	if (cqe->res != 0) {
		fprintf(stderr, "nop fixed file res: %d\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		goto err;
	}

	io_uring_cqe_seen(ring, cqe);

	/* Test with invalid fixed file index */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FILE | IORING_NOP_FIXED_FILE;
	sqe->fd = 999; /* invalid index */
	sqe->user_data = 2;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit bad index: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait bad index: %d\n", ret);
		goto err;
	}

	if (cqe->res != -EBADF) {
		fprintf(stderr, "nop bad fixed file res: %d (expected -EBADF)\n",
			cqe->res);
		io_uring_cqe_seen(ring, cqe);
		goto err;
	}

	io_uring_cqe_seen(ring, cqe);
	io_uring_unregister_files(ring);
	close(fd);
	return T_EXIT_PASS;
err:
	io_uring_unregister_files(ring);
	close(fd);
	return T_EXIT_FAIL;
}

/*
 * Test IORING_NOP_FIXED_BUFFER - NOP with a registered buffer lookup
 */
static int test_nop_fixed_buffer(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	char buf[4096];
	struct iovec iov = { .iov_base = buf, .iov_len = sizeof(buf) };
	int ret;

	if (no_nop_flags)
		return T_EXIT_SKIP;

	ret = io_uring_register_buffers(ring, &iov, 1);
	if (ret) {
		fprintf(stderr, "register buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Valid buffer index */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FIXED_BUFFER;
	sqe->buf_index = 0;
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		goto err;
	}

	if (cqe->res != 0) {
		if (cqe->res == -EINVAL) {
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "nop fixed buffer res: %d\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		goto err;
	}
	io_uring_cqe_seen(ring, cqe);

	/* Invalid buffer index */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FIXED_BUFFER;
	sqe->buf_index = 999;
	sqe->user_data = 2;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit bad buf: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait bad buf: %d\n", ret);
		goto err;
	}

	if (cqe->res == 0) {
		/* Kernel doesn't support IORING_NOP_FIXED_BUFFER */
		io_uring_cqe_seen(ring, cqe);
		io_uring_unregister_buffers(ring);
		return T_EXIT_SKIP;
	}
	if (cqe->res != -EFAULT) {
		fprintf(stderr, "nop bad buffer res: %d (expected -EFAULT)\n",
			cqe->res);
		io_uring_cqe_seen(ring, cqe);
		goto err;
	}

	io_uring_cqe_seen(ring, cqe);
	io_uring_unregister_buffers(ring);
	return T_EXIT_PASS;
err:
	io_uring_unregister_buffers(ring);
	return T_EXIT_FAIL;
}

/*
 * Test IORING_NOP_TW - NOP completing via task_work
 */
static int test_nop_tw(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret, i;

	if (no_nop_flags)
		return T_EXIT_SKIP;

	/* Submit several NOPs via task_work path */
	for (i = 0; i < 8; i++) {
		sqe = io_uring_get_sqe(ring);
		io_uring_prep_nop(sqe);
		sqe->nop_flags = IORING_NOP_TW;
		sqe->user_data = i + 1;
	}

	ret = io_uring_submit(ring);
	if (ret != 8) {
		fprintf(stderr, "submit tw: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < 8; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait tw: %d\n", ret);
			return T_EXIT_FAIL;
		}
		if (cqe->res != 0) {
			if (cqe->res == -EINVAL) {
				io_uring_cqe_seen(ring, cqe);
				return T_EXIT_SKIP;
			}
			fprintf(stderr, "nop tw res: %d\n", cqe->res);
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_FAIL;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return T_EXIT_PASS;
}

/*
 * Test IORING_NOP_TW combined with IORING_NOP_INJECT_RESULT
 */
static int test_nop_tw_inject(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	if (no_nop_flags)
		return T_EXIT_SKIP;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_TW | IORING_NOP_INJECT_RESULT;
	sqe->len = 42;
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (cqe->res != 42) {
		if (cqe->res == -EINVAL) {
			io_uring_cqe_seen(ring, cqe);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "nop tw inject res: %d (expected 42)\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(ring, cqe);
	return T_EXIT_PASS;
}

/*
 * Test IORING_NOP_CQE32 - NOP with extra1/extra2 in big CQE
 */
static int test_nop_cqe32(void)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	if (no_nop_flags)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_CQE32);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup cqe32: %d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_CQE32;
	sqe->off = 0xdeadbeef12345678ULL; /* extra1 */
	sqe->addr = 0xabcdef0011223344ULL; /* extra2 */
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit cqe32: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe32: %d\n", ret);
		goto err;
	}

	if (cqe->res != 0) {
		if (cqe->res == -EINVAL)
			goto skip;
		fprintf(stderr, "nop cqe32 res: %d\n", cqe->res);
		io_uring_cqe_seen(&ring, cqe);
		goto err;
	}

	if (cqe->big_cqe[0] != 0xdeadbeef12345678ULL) {
		fprintf(stderr, "cqe32 extra1: 0x%llx (expected 0xdeadbeef12345678)\n",
			(unsigned long long) cqe->big_cqe[0]);
		io_uring_cqe_seen(&ring, cqe);
		goto err;
	}

	if (cqe->big_cqe[1] != 0xabcdef0011223344ULL) {
		fprintf(stderr, "cqe32 extra2: 0x%llx (expected 0xabcdef0011223344)\n",
			(unsigned long long) cqe->big_cqe[1]);
		io_uring_cqe_seen(&ring, cqe);
		goto err;
	}

	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
err:
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
skip:
	io_uring_queue_exit(&ring);
	return T_EXIT_SKIP;
}

/*
 * Test IORING_NOP_CQE32 requires CQE32 or CQE_MIXED ring
 */
static int test_nop_cqe32_no_ring_support(void)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	if (no_nop_flags)
		return T_EXIT_SKIP;

	/* Ring without CQE32 */
	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_CQE32;
	sqe->off = 1;
	sqe->addr = 2;
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	if (cqe->res != -EINVAL) {
		fprintf(stderr, "nop cqe32 no support res: %d (expected -EINVAL)\n",
			cqe->res);
		io_uring_cqe_seen(&ring, cqe);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

/*
 * Test combined flags: FILE + FIXED_FILE + FIXED_BUFFER + TW
 */
static int test_nop_combined(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	char buf[4096];
	struct iovec iov = { .iov_base = buf, .iov_len = sizeof(buf) };
	int ret, fd;

	if (no_nop_flags || no_nop_file)
		return T_EXIT_SKIP;

	fd = open("/dev/null", O_RDWR);
	if (fd < 0) {
		perror("open /dev/null");
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files(ring, &fd, 1);
	if (ret) {
		fprintf(stderr, "register files: %d\n", ret);
		close(fd);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_buffers(ring, &iov, 1);
	if (ret) {
		fprintf(stderr, "register buffers: %d\n", ret);
		io_uring_unregister_files(ring);
		close(fd);
		return T_EXIT_FAIL;
	}

	/* All flags combined */
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	sqe->nop_flags = IORING_NOP_FILE | IORING_NOP_FIXED_FILE |
			 IORING_NOP_FIXED_BUFFER | IORING_NOP_TW;
	sqe->fd = 0;
	sqe->buf_index = 0;
	sqe->user_data = 1;

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit combined: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait combined: %d\n", ret);
		goto err;
	}

	if (cqe->res != 0) {
		fprintf(stderr, "nop combined res: %d\n", cqe->res);
		io_uring_cqe_seen(ring, cqe);
		goto err;
	}

	io_uring_cqe_seen(ring, cqe);
	io_uring_unregister_buffers(ring);
	io_uring_unregister_files(ring);
	close(fd);
	return T_EXIT_PASS;
err:
	io_uring_unregister_buffers(ring);
	io_uring_unregister_files(ring);
	close(fd);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, IORING_SETUP_SUBMIT_ALL);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_nop_detect(&ring);
	if (ret == T_EXIT_SKIP) {
		printf("NOP flags not supported, skipping\n");
		io_uring_queue_exit(&ring);
		return T_EXIT_SKIP;
	} else if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_detect failed\n");
		return T_EXIT_FAIL;
	}

	/* Use bad fd test to detect NOP_FILE support */
	ret = test_nop_file_bad_fd(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_file_bad_fd failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_nop_file(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_file failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_nop_fixed_file(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_fixed_file failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_nop_fixed_buffer(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_fixed_buffer failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_nop_tw(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_tw failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_nop_tw_inject(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_tw_inject failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);

	ret = test_nop_cqe32();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_cqe32 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_nop_cqe32_no_ring_support();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_cqe32_no_ring_support failed\n");
		return T_EXIT_FAIL;
	}

	/* New ring for combined test since we need fresh file/buffer tables */
	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = test_nop_combined(&ring);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_nop_combined failed\n");
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

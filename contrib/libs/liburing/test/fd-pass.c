#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: run various fixed file fd passing tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"
#include "helpers.h"

#define FSIZE	128
#define PAT	0x9a

static int no_fd_pass;

static int verify_fixed_read(struct io_uring *ring, int fixed_fd, int fail)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	unsigned char buf[FSIZE];
	int i;
	
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_read(sqe, fixed_fd, buf, FSIZE, 0);
	sqe->flags |= IOSQE_FIXED_FILE;
	io_uring_submit(ring);

	io_uring_wait_cqe(ring, &cqe);
	if (cqe->res != FSIZE) {
		if (fail && cqe->res == -EBADF)
			return 0;
		fprintf(stderr, "Read: %d\n", cqe->res);
		return 1;
	}
	io_uring_cqe_seen(ring, cqe);

	for (i = 0; i < FSIZE; i++) {
		if (buf[i] != PAT) {
			fprintf(stderr, "got %x, wanted %x\n", buf[i], PAT);
			return 1;
		}
	}

	return 0;
}

static int test(const char *filename)
{
	struct io_uring sring, dring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	ret = io_uring_queue_init(8, &sring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(8, &dring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_files_sparse(&sring, 8);
	if (ret) {
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		fprintf(stderr, "register files failed %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_register_files_sparse(&dring, 8);
	if (ret) {
		fprintf(stderr, "register files failed %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* open direct descriptor */
	sqe = io_uring_get_sqe(&sring);
	io_uring_prep_openat_direct(sqe, AT_FDCWD, filename, 0, 0644, 0);
	io_uring_submit(&sring);
	ret = io_uring_wait_cqe(&sring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe failed %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res) {
		fprintf(stderr, "cqe res %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&sring, cqe);

	/* verify data is sane for source ring */
	if (verify_fixed_read(&sring, 0, 0))
		return T_EXIT_FAIL;

	/* send direct descriptor to destination ring */
	sqe = io_uring_get_sqe(&sring);
	io_uring_prep_msg_ring(sqe, dring.ring_fd, 0, 0x89, 0);
	sqe->addr = 1;
	sqe->addr3 = 0;
	sqe->file_index = 1;
	io_uring_submit(&sring);

	ret = io_uring_wait_cqe(&sring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe failed %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res) {
		if (cqe->res == -EINVAL && !no_fd_pass) {
			no_fd_pass = 1;
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "msg_ring failed %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&sring, cqe);

	/* get posted completion for the passing */
	ret = io_uring_wait_cqe(&dring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe failed %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->user_data != 0x89) {
		fprintf(stderr, "bad user_data %ld\n", (long) cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&dring, cqe);

	/* now verify we can read the sane data from the destination ring */
	if (verify_fixed_read(&dring, 0, 0))
		return T_EXIT_FAIL;

	/* close descriptor in source ring */
	sqe = io_uring_get_sqe(&sring);
	io_uring_prep_close_direct(sqe, 0);
	io_uring_submit(&sring);

	ret = io_uring_wait_cqe(&sring, &cqe);
	if (ret) {
		fprintf(stderr, "wait cqe failed %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res) {
		fprintf(stderr, "direct close failed %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&sring, cqe);

	/* check that source ring fails after close */
	if (verify_fixed_read(&sring, 0, 1))
		return T_EXIT_FAIL;

	/* check we can still read from destination ring */
	if (verify_fixed_read(&dring, 0, 0))
		return T_EXIT_FAIL;

	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	char fname[80];
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	sprintf(fname, ".fd-pass.%d", getpid());
	t_create_file_pattern(fname, FSIZE, PAT);

	ret = test(fname);
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test failed\n");
		ret = T_EXIT_FAIL;
	}

	unlink(fname);
	return ret;
}

#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test buffer cloning between rings
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/uio.h>

#include "liburing.h"
#include "helpers.h"

#define NR_VECS		64
#define BUF_SIZE	8192

static int no_buf_clone;

static int test(int reg_src, int reg_dst)
{
	struct iovec vecs[NR_VECS];
	struct io_uring src, dst;
	int ret, i;

	ret = io_uring_queue_init(1, &src, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(1, &dst, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	if (reg_src) {
		ret = io_uring_register_ring_fd(&src);
		if (ret < 0) {
			if (ret == -EINVAL)
				return T_EXIT_SKIP;
			fprintf(stderr, "register ring: %d\n", ret);
			return T_EXIT_FAIL;
		}
	}
	if (reg_dst) {
		ret = io_uring_register_ring_fd(&dst);
		if (ret < 0) {
			if (ret == -EINVAL)
				return T_EXIT_SKIP;
			fprintf(stderr, "register ring: %d\n", ret);
			return T_EXIT_FAIL;
		}
	}

	/* test fail with no buffers in src */
	ret = io_uring_clone_buffers(&dst, &src);
	if (ret == -EINVAL) {
		/* no buffer copy support */
		no_buf_clone = true;
		return T_EXIT_SKIP;
	} else if (ret != -ENXIO) {
		fprintf(stderr, "empty copy: %d\n", ret);
		return T_EXIT_FAIL;
	}

	for (i = 0; i < NR_VECS; i++) {
		if (posix_memalign(&vecs[i].iov_base, 4096, BUF_SIZE))
			return T_EXIT_FAIL;
		vecs[i].iov_len = BUF_SIZE;
	}

	ret = io_uring_register_buffers(&src, vecs, NR_VECS);
	if (ret < 0) {
		if (ret == -ENOMEM)
			return T_EXIT_SKIP;
		return T_EXIT_FAIL;
	}

	/* copy should work now */
	ret = io_uring_clone_buffers(&dst, &src);
	if (ret) {
		fprintf(stderr, "buffer copy: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* try copy again, should get -EBUSY */
	ret = io_uring_clone_buffers(&dst, &src);
	if (ret != -EBUSY) {
		fprintf(stderr, "busy copy: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "dst unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret != -ENXIO) {
		fprintf(stderr, "dst unregister empty buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "src unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_buffers(&dst, vecs, NR_VECS);
	if (ret < 0) {
		fprintf(stderr, "register buffers dst; %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_clone_buffers(&src, &dst);
	if (ret) {
		fprintf(stderr, "buffer copy reverse: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "dst unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret != -ENXIO) {
		fprintf(stderr, "dst unregister empty buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "src unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&src);
	io_uring_queue_exit(&dst);

	for (i = 0; i < NR_VECS; i++)
		free(vecs[i].iov_base);

	return T_EXIT_PASS;
}

static int test_dummy(void)
{
	struct iovec vec = { };
	struct io_uring src, dst;
	int ret;

	ret = io_uring_queue_init(1, &src, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_queue_init(1, &dst, 0);
	if (ret) {
		fprintf(stderr, "ring_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_register_buffers(&src, &vec, 1);
	if (ret < 0) {
		fprintf(stderr, "failed to register dummy buffer: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_clone_buffers(&dst, &src);
	if (ret) {
		fprintf(stderr, "clone dummy buf: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&src);
	if (ret) {
		fprintf(stderr, "rsc unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_unregister_buffers(&dst);
	if (ret) {
		fprintf(stderr, "dst unregister buffers: %d\n", ret);
		return T_EXIT_FAIL;
	}

	io_uring_queue_exit(&src);
	io_uring_queue_exit(&dst);

	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test(0, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 0 0 failed\n");
		return T_EXIT_FAIL;
	}
	if (no_buf_clone)
		return T_EXIT_SKIP;

	ret = test(0, 1);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 0 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 0);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 1 0 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test(1, 1);
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test 1 1 failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_dummy();
	if (ret == T_EXIT_SKIP) {
		return T_EXIT_SKIP;
	} else if (ret != T_EXIT_PASS) {
		fprintf(stderr, "test_dummy failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

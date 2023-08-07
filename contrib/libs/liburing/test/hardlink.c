#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test io_uring linkat handling
 */
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"


static int do_linkat(struct io_uring *ring, const char *oldname, const char *newname)
{
	int ret;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		fprintf(stderr, "sqe get failed\n");
		goto err;
	}
	io_uring_prep_linkat(sqe, AT_FDCWD, oldname, AT_FDCWD, newname, 0);

	ret = io_uring_submit(ring);
	if (ret != 1) {
		fprintf(stderr, "submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqes(ring, &cqe, 1, 0, 0);
	if (ret) {
		fprintf(stderr, "wait_cqe failed: %d\n", ret);
		goto err;
	}
	ret = cqe->res;
	io_uring_cqe_seen(ring, cqe);
	return ret;
err:
	return 1;
}

static int files_linked_ok(const char* fn1, const char *fn2)
{
	struct stat s1, s2;

	if (stat(fn1, &s1)) {
		fprintf(stderr, "stat(%s): %s\n", fn1, strerror(errno));
		return 0;
	}
	if (stat(fn2, &s2)) {
		fprintf(stderr, "stat(%s): %s\n", fn2, strerror(errno));
		return 0;
	}
	if (s1.st_dev != s2.st_dev || s1.st_ino != s2.st_ino) {
		fprintf(stderr, "linked files have different device / inode numbers\n");
		return 0;
	}
	if (s1.st_nlink != 2 || s2.st_nlink != 2) {
		fprintf(stderr, "linked files have unexpected links count\n");
		return 0;
	}
	return 1;
}

int main(int argc, char *argv[])
{
	static const char target[] = "io_uring-linkat-test-target";
	static const char linkname[] = "io_uring-linkat-test-link";
	int ret;
	struct io_uring ring;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue init failed: %d\n", ret);
		return ret;
	}

	ret = open(target, O_CREAT | O_RDWR | O_EXCL, 0600);
	if (ret < 0) {
		perror("open");
		goto err;
	}
	if (write(ret, "linktest", 8) != 8) {
		close(ret);
		goto err1;
	}
	close(ret);

	ret = do_linkat(&ring, target, linkname);
	if (ret < 0) {
		if (ret == -EBADF || ret == -EINVAL) {
			fprintf(stdout, "linkat not supported, skipping\n");
			goto skip;
		}
		fprintf(stderr, "linkat: %s\n", strerror(-ret));
		goto err1;
	} else if (ret) {
		goto err1;
	}

	if (!files_linked_ok(linkname, target))
		goto err2;

	ret = do_linkat(&ring, target, linkname);
	if (ret != -EEXIST) {
		fprintf(stderr, "test_linkat linkname already exists failed: %d\n", ret);
		goto err2;
	}

	ret = do_linkat(&ring, target, "surely/this/does/not/exist");
	if (ret != -ENOENT) {
		fprintf(stderr, "test_linkat no parent failed: %d\n", ret);
		goto err2;
	}

	unlinkat(AT_FDCWD, linkname, 0);
	unlinkat(AT_FDCWD, target, 0);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
skip:
	unlinkat(AT_FDCWD, linkname, 0);
	unlinkat(AT_FDCWD, target, 0);
	io_uring_queue_exit(&ring);
	return T_EXIT_SKIP;
err2:
	unlinkat(AT_FDCWD, linkname, 0);
err1:
	unlinkat(AT_FDCWD, target, 0);
err:
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

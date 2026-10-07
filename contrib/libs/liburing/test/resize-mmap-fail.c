#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test ring resize failure handling when mmap fails.
 * Ensures the previous ring state remains valid and usable on failure.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/resource.h>

#include "liburing.h"
#include "helpers.h"

static long get_vm_size_pages(void)
{
	FILE *f = fopen("/proc/self/statm", "r");
	if (!f)
		return -1;
	long pages = 0;
	if (fscanf(f, "%ld", &pages) != 1)
		pages = -1;
	fclose(f);
	return pages;
}

int main(int argc, char *argv[])
{
	struct io_uring_params p = { };
	struct io_uring ring;
	struct rlimit old_rlim, new_rlim;
	long pages, page_size;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	/* Initialize small ring */
	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup failed: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Verify original ring works before any resize */
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	if (!sqe) {
		fprintf(stderr, "Failed to get sqe\n");
		return T_EXIT_FAIL;
	}
	io_uring_prep_nop(sqe);
	ret = io_uring_submit_and_wait(&ring, 1);
	if (ret < 0) {
		fprintf(stderr, "submit failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	struct io_uring_cqe *cqe;
	ret = io_uring_peek_cqe(&ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "peek cqe failed: %d\n", ret);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	/* Get virtual memory size and pagesize */
	pages = get_vm_size_pages();
	page_size = sysconf(_SC_PAGESIZE);
	if (pages < 0 || page_size < 0) {
		fprintf(stderr, "Failed to get VM size or page size\n");
		return T_EXIT_SKIP;
	}

	/* Get old resource limit */
	if (getrlimit(RLIMIT_AS, &old_rlim) < 0) {
		perror("getrlimit");
		return T_EXIT_FAIL;
	}

	/* Set tight virtual memory limit to current usage + 2 pages */
	new_rlim.rlim_cur = (pages + 2) * page_size;
	new_rlim.rlim_max = old_rlim.rlim_max;
	if (setrlimit(RLIMIT_AS, &new_rlim) < 0) {
		perror("setrlimit");
		return T_EXIT_FAIL;
	}

	/* Attempt resize to a larger size which is guaranteed to exceed the memory limit */
	memset(&p, 0, sizeof(p));
	p.sq_entries = 64;
	p.cq_entries = 128;
	ret = io_uring_resize_rings(&ring, &p);

	/* Restore old limit immediately so standard calls are safe */
	setrlimit(RLIMIT_AS, &old_rlim);

	/* We expect resize to fail with -ENOMEM due to our tight memory limit */
	if (ret >= 0) {
		fprintf(stderr, "Resize unexpectedly succeeded under memory pressure\n");
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	/* Ensure the old ring is STILL fully functional and did not crash */
	sqe = io_uring_get_sqe(&ring);
	if (!sqe) {
		fprintf(stderr, "Failed to get sqe after failed resize (ring is broken)\n");
		return T_EXIT_FAIL;
	}
	io_uring_prep_nop(sqe);
	ret = io_uring_submit_and_wait(&ring, 1);
	if (ret < 0) {
		fprintf(stderr, "Submit failed after failed resize (ring is broken): %d\n", ret);
		return T_EXIT_FAIL;
	}
	ret = io_uring_peek_cqe(&ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "Peek cqe failed after failed resize: %d\n", ret);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;
}

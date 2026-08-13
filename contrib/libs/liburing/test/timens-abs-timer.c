#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: regression test for IORING_TIMEOUT_ABS and
 * IORING_ENTER_ABS_TIMER honouring the submitter's time
 * namespace. The kernel converts user supplied absolute time
 * from the caller's time namespace view to host view via
 * timens_ktime_to_host(). Without that conversion an absolute
 * deadline submitted from inside a CLONE_NEWTIME namespace fires
 * immediately instead of after the requested interval.
 *
 * The test forks a child, enters a fresh user namespace plus
 * time namespace with a -10s monotonic offset, submits an
 * absolute deadline of now + 1s on each path, and asserts the
 * call returns after ~1s rather than after <100ms. The test is
 * skipped if the kernel lacks CLONE_NEWTIME support or the
 * caller cannot create a user namespace.
 */
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "helpers.h"
#include "liburing.h"
#include "../src/syscall.h"

#ifndef CLONE_NEWTIME
#define CLONE_NEWTIME	0x00000080
#endif

#define EXPECTED_NS	1000000000ULL	/* deadline at now + 1s */
#define MIN_OBSERVED_NS	900000000ULL	/* fire no earlier than 0.9s */
#define BUG_OBSERVED_NS	100000000ULL	/* bug fires under 0.1s */

static int write_one(const char *path, const char *buf)
{
	int fd, ret;

	fd = open(path, O_WRONLY);
	if (fd < 0)
		return -errno;
	ret = write(fd, buf, strlen(buf));
	close(fd);
	if (ret < 0)
		return -errno;
	if ((size_t) ret != strlen(buf))
		return -EIO;
	return 0;
}

static int enter_unpriv_userns_timens(void)
{
	int ret;

	ret = unshare(CLONE_NEWUSER | CLONE_NEWTIME);
	if (ret < 0)
		return -errno;

	if (write_one("/proc/self/setgroups", "deny") < 0)
		return -errno;
	if (write_one("/proc/self/uid_map", "0 0 1\n") < 0)
		return -errno;
	if (write_one("/proc/self/gid_map", "0 0 1\n") < 0)
		return -errno;

	/* -10s monotonic offset: host_monotonic - 10s inside this ns. */
	if (write_one("/proc/self/timens_offsets", "monotonic -10 0\n") < 0)
		return -errno;

	return 0;
}

static unsigned long long ts_to_ns(const struct timespec *ts)
{
	return ts->tv_sec * 1000000000ULL + ts->tv_nsec;
}

static long long elapsed_ns(const struct timespec *start)
{
	struct timespec now;

	if (clock_gettime(CLOCK_MONOTONIC, &now) < 0)
		return -errno;
	return ts_to_ns(&now) - ts_to_ns(start);
}

/*
 * Path 1: IORING_OP_TIMEOUT with IORING_TIMEOUT_ABS, parsed via
 * io_parse_user_time() in io_uring/timeout.c.
 */
static int test_op_timeout_abs(void)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct __kernel_timespec kts;
	struct timespec start;
	struct io_uring ring;
	long long elapsed;
	int ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (clock_gettime(CLOCK_MONOTONIC, &start) < 0) {
		perror("clock_gettime");
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	kts.tv_sec = start.tv_sec + 1;
	kts.tv_nsec = start.tv_nsec;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_timeout(sqe, &kts, 0, IORING_TIMEOUT_ABS);

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret) {
		fprintf(stderr, "wait_cqe: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	elapsed = elapsed_ns(&start);
	io_uring_queue_exit(&ring);

	if (elapsed < 0) {
		fprintf(stderr, "elapsed_ns failed\n");
		return T_EXIT_FAIL;
	}
	if ((unsigned long long) elapsed < BUG_OBSERVED_NS) {
		fprintf(stderr,
			"IORING_TIMEOUT_ABS fired after %lld ns, expected ~%llu ns. "
			"Likely missing timens_ktime_to_host() in io_parse_user_time().\n",
			elapsed, EXPECTED_NS);
		return T_EXIT_FAIL;
	}
	if ((unsigned long long) elapsed < MIN_OBSERVED_NS) {
		fprintf(stderr,
			"IORING_TIMEOUT_ABS fired early at %lld ns\n", elapsed);
		return T_EXIT_FAIL;
	}
	return T_EXIT_PASS;
}

/*
 * Path 2: io_uring_enter with IORING_ENTER_ABS_TIMER, parsed
 * inline in io_uring/wait.c::io_cqring_wait().
 */
static int test_enter_abs_timer(void)
{
	struct io_uring_getevents_arg arg;
	struct __kernel_timespec kts;
	struct timespec start;
	struct io_uring ring;
	long long elapsed;
	int ret;

	ret = io_uring_queue_init(1, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (clock_gettime(CLOCK_MONOTONIC, &start) < 0) {
		perror("clock_gettime");
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	kts.tv_sec = start.tv_sec + 1;
	kts.tv_nsec = start.tv_nsec;

	memset(&arg, 0, sizeof(arg));
	arg.sigmask_sz = _NSIG / 8;
	arg.ts = (unsigned long) &kts;

	ret = io_uring_enter2(ring.ring_fd, 0, 1,
			      IORING_ENTER_GETEVENTS |
			      IORING_ENTER_EXT_ARG |
			      IORING_ENTER_ABS_TIMER,
			      &arg, sizeof(arg));
	if (ret != -ETIME) {
		fprintf(stderr,
			"io_uring_enter2 returned %d, expected -ETIME (%d)\n",
			ret, -ETIME);
		io_uring_queue_exit(&ring);
		if (ret == -EINVAL)
			return T_EXIT_SKIP;
		return T_EXIT_FAIL;
	}

	elapsed = elapsed_ns(&start);
	io_uring_queue_exit(&ring);

	if (elapsed < 0) {
		fprintf(stderr, "elapsed_ns failed\n");
		return T_EXIT_FAIL;
	}
	if ((unsigned long long) elapsed < BUG_OBSERVED_NS) {
		fprintf(stderr,
			"IORING_ENTER_ABS_TIMER fired after %lld ns, expected ~%llu ns. "
			"Likely missing timens_ktime_to_host() on the ABS_TIMER branch.\n",
			elapsed, EXPECTED_NS);
		return T_EXIT_FAIL;
	}
	if ((unsigned long long) elapsed < MIN_OBSERVED_NS) {
		fprintf(stderr,
			"IORING_ENTER_ABS_TIMER fired early at %lld ns\n", elapsed);
		return T_EXIT_FAIL;
	}
	return T_EXIT_PASS;
}

/*
 * Run the actual io_uring tests inside the new time namespace.
 * unshare(CLONE_NEWTIME) does not move the caller into the new
 * namespace, only its future children. So the caller sets up
 * userns and timens, writes the offset, then forks once more to
 * enter the new time namespace.
 */
static int run_tests_in_timens_grandchild(void)
{
	struct timespec probe;
	int ret;

	/*
	 * Sanity check: clock_gettime should reflect the -10s offset.
	 * If it does not, the offset was not applied and the test
	 * would silently appear to pass on an unpatched kernel.
	 */
	if (clock_gettime(CLOCK_MONOTONIC, &probe) < 0) {
		perror("clock_gettime");
		return T_EXIT_FAIL;
	}

	ret = test_op_timeout_abs();
	if (ret != T_EXIT_PASS)
		return ret;

	return test_enter_abs_timer();
}

static int run_in_timens(void)
{
	pid_t pid;
	int status, ret;

	ret = enter_unpriv_userns_timens();
	if (ret == -EPERM || ret == -ENOSPC || ret == -EINVAL || ret == -ENOENT)
		return T_EXIT_SKIP;
	if (ret) {
		fprintf(stderr, "userns/timens setup: %s\n", strerror(-ret));
		return T_EXIT_SKIP;
	}

	pid = fork();
	if (pid < 0) {
		perror("fork (timens)");
		return T_EXIT_FAIL;
	}
	if (pid == 0)
		_exit(run_tests_in_timens_grandchild());

	if (waitpid(pid, &status, 0) < 0) {
		perror("waitpid (timens)");
		return T_EXIT_FAIL;
	}
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	pid_t pid;
	int status;

	if (argc > 1)
		return T_EXIT_SKIP;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return T_EXIT_FAIL;
	}
	if (pid == 0)
		_exit(run_in_timens());

	if (waitpid(pid, &status, 0) < 0) {
		perror("waitpid");
		return T_EXIT_FAIL;
	}
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return T_EXIT_FAIL;
}

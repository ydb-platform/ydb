#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: Test having a buffer group with legacy buffers, where those
 *		are consumed until the group is empty. Then when empty, the
 *		group is converted to a ring provided one, and finally the
 *		original buffer from the legacy group is recycled.
 */
#include <errno.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		0x444
#define BID0		0
#define RING_ENTRIES	1024
#define LEGACY_BUF_LEN	0x1000
#define TRY_ITERS	800000
#define POST_REG_ITERS	300000

static volatile sig_atomic_t g_stop;

struct shared_state {
	volatile int stop;
	volatile int reg_ok;
	volatile int reg_err;
	volatile int reg_no_br;
};

static void on_alarm(int sig)
{
	g_stop = 1;
}

static void pin_cpu(int cpu)
{
	cpu_set_t set;

	CPU_ZERO(&set);
	CPU_SET(cpu, &set);
	sched_setaffinity(0, sizeof(set), &set);
}

static int sender_loop(int sfd, struct shared_state *sh)
{
	pin_cpu(1);

	while (!sh->stop && !g_stop) {
		ssize_t n = send(sfd, "", 0, MSG_DONTWAIT);

		if (n < 0 && errno == EAGAIN)
			sched_yield();
	}
	return 0;
}

static int reg_loop(int ring_fd, struct io_uring_buf_reg *reg,
		    struct shared_state *sh)
{
	pin_cpu(1);

	while (!sh->stop && !g_stop) {
		int ret;

		ret = io_uring_register(ring_fd, IORING_REGISTER_PBUF_RING,
					reg, 1);
		if (ret == 0) {
			sh->reg_ok = 1;
			break;
		}

		if (ret == -EINVAL) {
			sh->reg_no_br = 1;
			break;
		}
		if (ret != -EEXIST && ret != -EBUSY && ret != -EINVAL)
			sh->reg_err = -ret;
	}

	while (!sh->stop && !g_stop)
		usleep(1000);

	return 0;
}

static int run_poc(void)
{
	struct io_uring ring;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring_buf_reg reg;
	struct io_uring_buf_ring *br = NULL;
	struct shared_state *sh = NULL;
	char *legacy_buf = NULL;
	char *ring_data = NULL;
	size_t ring_bytes;
	pid_t regger = -1, sender = -1;
	int sv[2] = { -1, -1 }, ret, i, st, reg_seen_iter = -1;

	pin_cpu(0);

	sh = mmap(NULL, 4096, PROT_READ | PROT_WRITE,
		  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (sh == MAP_FAILED) {
		perror("mmap sh");
		return T_EXIT_FAIL;
	}
	memset(sh, 0, 4096);

	legacy_buf = mmap(NULL, LEGACY_BUF_LEN, PROT_READ | PROT_WRITE,
			  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (legacy_buf == MAP_FAILED) {
		perror("mmap legacy_buf");
		return T_EXIT_FAIL;
	}
	memset(legacy_buf, 0x41, LEGACY_BUF_LEN);

	ring_data = mmap(NULL, LEGACY_BUF_LEN, PROT_READ | PROT_WRITE,
			 MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (ring_data == MAP_FAILED) {
		perror("mmap ring_data");
		return T_EXIT_FAIL;
	}
	memset(ring_data, 0x42, LEGACY_BUF_LEN);

	ring_bytes = (sizeof(struct io_uring_buf_ring) +
		      RING_ENTRIES * sizeof(struct io_uring_buf) + 4095) & ~4095UL;
	br = mmap(NULL, ring_bytes, PROT_READ | PROT_WRITE,
		  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (br == MAP_FAILED) {
		perror("mmap br");
		return T_EXIT_FAIL;
	}

	memset(br, 0, ring_bytes);
	for (i = 0; i < RING_ENTRIES; i++) {
		br->bufs[i].addr = (uint64_t)(uintptr_t)ring_data;
		br->bufs[i].len = LEGACY_BUF_LEN;
		br->bufs[i].bid = (uint16_t)i;
	}
	br->tail = RING_ENTRIES;

	ret = socketpair(AF_UNIX, SOCK_DGRAM, 0, sv);
	if (ret < 0) {
		perror("socketpair");
		return T_EXIT_FAIL;
	}

	ret = io_uring_queue_init(2, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "ring_init=%d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Provide a single legacy buffer for BGID */
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_provide_buffers(sqe, legacy_buf, LEGACY_BUF_LEN, 1,
				      BGID, BID0);
	ret = io_uring_submit_and_wait(&ring, 1);
	if (ret < 0) {
		fprintf(stderr, "submit_and_wait: %d\n", ret);
		return T_EXIT_FAIL;
	}

	ret = io_uring_wait_cqe(&ring, &cqe);
	if (ret < 0) {
		fprintf(stderr, "wait_cqe=%d\n", ret);
		return T_EXIT_FAIL;
	}
	if (cqe->res < 0) {
		io_uring_cqe_seen(&ring, cqe);
		return T_EXIT_FAIL;
	}
	io_uring_cqe_seen(&ring, cqe);

	memset(&reg, 0, sizeof(reg));
	reg.ring_addr = (uint64_t)(uintptr_t)br;
	reg.ring_entries = RING_ENTRIES;
	reg.bgid = BGID;

	sender = fork();
	if (sender < 0)
		return T_EXIT_FAIL;
	if (sender == 0)
		_exit(sender_loop(sv[1], sh));

	regger = fork();
	if (regger < 0)
		return T_EXIT_FAIL;
	if (regger == 0)
		_exit(reg_loop(ring.ring_fd, &reg, sh));

	for (i = 0; i < TRY_ITERS; i++) {
		if (g_stop)
			break;
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_recv(sqe, sv[0], NULL, 1, 0);
		sqe->flags = IOSQE_ASYNC | IOSQE_BUFFER_SELECT;
		sqe->buf_group = BGID;

		ret = io_uring_submit_and_wait(&ring, 1);
		if (ret < 0) {
			fprintf(stderr, "wait_cqe %d\n", ret);
			break;
		}

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "wait_cqe=%d at %u\n", ret, i);
			break;
		}

		io_uring_cqe_seen(&ring, cqe);

		if (sh->reg_no_br)
			break;
		if (sh->reg_ok && reg_seen_iter < 0)
			reg_seen_iter = (int)i;

		if (reg_seen_iter >= 0 &&
		    (int)i - reg_seen_iter > POST_REG_ITERS)
			break;
	}

	sh->stop = 1;
	if (sender > 0)
		waitpid(sender, &st, 0);
	if (regger > 0)
		waitpid(regger, &st, 0);

	io_uring_queue_exit(&ring);
	close(sv[0]);
	close(sv[1]);
	return T_EXIT_PASS;
}

int main(int argc, char *argv[])
{
	if (argc > 1)
		return T_EXIT_SKIP;

	signal(SIGALRM, on_alarm);
	alarm(30);

	return run_poc();
}

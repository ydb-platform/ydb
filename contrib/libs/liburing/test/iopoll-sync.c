#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test uring_cmds for files that don't support iopoll
 * on IORING_SETUP_IOPOLL rings
 */

#include <liburing.h>
#include <stdio.h>
#include <sys/socket.h>

#include "helpers.h"

int main(void)
{
	int sockfd;
	int level = SOL_SOCKET;
	int optname = SO_REUSEADDR;
	int optval1, optval2, optval3;
	struct io_uring ring;
	int ret;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe_iter cqe_iter;
	struct io_uring_cqe *cqe;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		fprintf(stderr, "socket() failed: %m\n");
		return T_EXIT_SKIP;
	}

	optval1 = 0;
	if (setsockopt(sockfd, level, optname, &optval1, sizeof(optval1)) < 0) {
		fprintf(stderr, "setsockopt() failed: %m\n");
		return T_EXIT_SKIP;
	}

	ret = t_create_ring(3, &ring, IORING_SETUP_IOPOLL);
	if (ret == T_SETUP_SKIP) {
		fprintf(stderr, "IORING_SETUP_IOPOLL not supported\n");
		return T_EXIT_SKIP;
	}
	if (ret)
		return T_EXIT_FAIL;

	optval1 = 123;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_GETSOCKOPT, sockfd,
			       level, optname, &optval1, sizeof(optval1));
	sqe->flags |= IOSQE_IO_LINK;
	sqe->user_data = 1;

	optval2 = 1;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_SETSOCKOPT, sockfd,
			       level, optname, &optval2, sizeof(optval2));
	sqe->flags |= IOSQE_IO_LINK;
	sqe->user_data = 2;

	optval3 = 123;
	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_cmd_sock(sqe, SOCKET_URING_OP_GETSOCKOPT, sockfd,
			       level, optname, &optval3, sizeof(optval3));
	sqe->user_data = 3;

	ret = io_uring_submit(&ring);
	if (ret != 3) {
		fprintf(stderr, "io_uring_submit() returned %d\n", ret);
		return T_EXIT_FAIL;
	}

	cqe_iter = io_uring_cqe_iter_init(&ring);
	if (!io_uring_cqe_iter_next(&cqe_iter, &cqe)) {
		fprintf(stderr, "No CQE available\n");
		return T_EXIT_FAIL;
	}
	if (cqe->user_data != 1) {
		fprintf(stderr, "No CQE for user_data 1\n");
		return T_EXIT_FAIL;
	}
	if (cqe->res == -EOPNOTSUPP || cqe->res == -EINVAL)
		return T_EXIT_SKIP;
	if (cqe->res != sizeof(optval1)) {
		fprintf(stderr, "GETSOCKOPT returned %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (optval1 != 0) {
		fprintf(stderr, "optval %d != 0\n", optval1);
		return T_EXIT_FAIL;
	}

	if (!io_uring_cqe_iter_next(&cqe_iter, &cqe)) {
		fprintf(stderr, "Only 1 CQE available\n");
		return T_EXIT_FAIL;
	}
	if (cqe->user_data != 2) {
		fprintf(stderr, "No CQE for user_data 2\n");
		return T_EXIT_FAIL;
	}
	if (cqe->res) {
		fprintf(stderr, "SETSOCKOPT returned %d\n", cqe->res);
		return T_EXIT_FAIL;
	}

	if (!io_uring_cqe_iter_next(&cqe_iter, &cqe)) {
		fprintf(stderr, "Only 2 CQEs available\n");
		return T_EXIT_FAIL;
	}
	if (cqe->user_data != 3) {
		fprintf(stderr, "No CQE for user_data 3\n");
		return T_EXIT_FAIL;
	}
	if (cqe->res != sizeof(optval3)) {
		fprintf(stderr, "GETSOCKOPT returned %d\n", cqe->res);
		return T_EXIT_FAIL;
	}
	if (optval3 != 1) {
		fprintf(stderr, "optval %d != 1\n", optval3);
		return T_EXIT_FAIL;
	}

	if (io_uring_cqe_iter_next(&cqe_iter, &cqe)) {
		fprintf(stderr, "More than 3 CQEs available");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

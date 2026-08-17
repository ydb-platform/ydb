#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: test per-task io_uring restrictions
 *
 * Per-task restrictions are registered via io_uring_register(2) with fd=-1
 * and IORING_REGISTER_RESTRICTIONS opcode. Once registered, they apply to
 * all rings created by that task and are inherited across fork.
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/wait.h>

#include "liburing.h"
#include "helpers.h"
#include "../src/syscall.h"

static int register_task_restrictions(struct io_uring_restriction *res,
				      unsigned int nr_res)
{
	struct {
		__u16 flags;
		__u16 nr_res;
		__u32 resv[3];
		struct io_uring_restriction restrictions[];
	} *arg;
	size_t sz;
	int ret;

	sz = sizeof(*arg) + nr_res * sizeof(struct io_uring_restriction);
	arg = calloc(1, sz);
	if (!arg)
		return -ENOMEM;

	arg->nr_res = nr_res;
	memcpy(arg->restrictions, res, nr_res * sizeof(*res));

	ret = __sys_io_uring_register(-1, IORING_REGISTER_RESTRICTIONS, arg, 1);
	free(arg);
	return ret;
}

/*
 * Test that per-task restrictions restrict SQE ops on newly created rings
 */
static int test_task_restrict_sqe_op(void)
{
	struct io_uring_restriction res[1];
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	/* Allow only NOP */
	res[0].opcode = IORING_RESTRICTION_SQE_OP;
	res[0].sqe_op = IORING_OP_NOP;

	/*
	 * Task restrictions need to be tested in a child process since
	 * once set they can't be removed for the current task.
	 */
	pid_t pid = fork();
	if (pid < 0) {
		perror("fork");
		return T_EXIT_FAIL;
	}

	if (pid == 0) {
		/* Child: set no_new_privs (required like seccomp) */
		prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);

		/* Register task restriction, then create ring */
		ret = register_task_restrictions(res, 1);
		if (ret == -EINVAL || ret == -EBADF) {
			/* Kernel doesn't support per-task restrictions */
			_exit(T_EXIT_SKIP);
		}
		if (ret) {
			fprintf(stderr, "register task restrictions: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		/* Create a new ring - should inherit task restrictions */
		ret = io_uring_queue_init(8, &ring, 0);
		if (ret) {
			fprintf(stderr, "ring setup failed: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		/* NOP should be allowed */
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_nop(sqe);
		sqe->user_data = 1;

		/* Read should be denied */
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read(sqe, 0, NULL, 0, 0);
		sqe->user_data = 2;

		ret = io_uring_submit(&ring);
		if (ret != 2) {
			fprintf(stderr, "submit: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		for (int i = 0; i < 2; i++) {
			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait: %d\n", ret);
				_exit(T_EXIT_FAIL);
			}

			switch (cqe->user_data) {
			case 1: /* nop - should succeed */
				if (cqe->res != 0) {
					fprintf(stderr, "nop res: %d\n", cqe->res);
					_exit(T_EXIT_FAIL);
				}
				break;
			case 2: /* read - should be denied */
				if (cqe->res != -EACCES) {
					fprintf(stderr, "read res: %d (expected -EACCES)\n",
						cqe->res);
					_exit(T_EXIT_FAIL);
				}
				break;
			}
			io_uring_cqe_seen(&ring, cqe);
		}

		io_uring_queue_exit(&ring);
		_exit(T_EXIT_PASS);
	}

	/* Parent: wait for child */
	int status;
	waitpid(pid, &status, 0);
	if (!WIFEXITED(status))
		return T_EXIT_FAIL;
	return WEXITSTATUS(status);
}

/*
 * Test that per-task restrictions are inherited across fork
 */
static int test_task_restrict_fork_inherit(void)
{
	struct io_uring_restriction res[2];
	int ret;

	/* Allow only NOP and WRITE */
	res[0].opcode = IORING_RESTRICTION_SQE_OP;
	res[0].sqe_op = IORING_OP_NOP;
	res[1].opcode = IORING_RESTRICTION_SQE_OP;
	res[1].sqe_op = IORING_OP_WRITE;

	pid_t pid = fork();
	if (pid < 0) {
		perror("fork");
		return T_EXIT_FAIL;
	}

	if (pid == 0) {
		/* Child: set no_new_privs (required like seccomp) */
		prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);

		/* Register task restriction then fork again */
		ret = register_task_restrictions(res, 2);
		if (ret == -EINVAL) {
			_exit(T_EXIT_SKIP);
		}
		if (ret) {
			fprintf(stderr, "register task restrictions: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		/* Fork a grandchild - restrictions should be inherited */
		pid_t gpid = fork();
		if (gpid < 0) {
			perror("fork");
			_exit(T_EXIT_FAIL);
		}

		if (gpid == 0) {
			/* Grandchild: create ring and verify restrictions */
			struct io_uring ring;
			struct io_uring_sqe *sqe;
			struct io_uring_cqe *cqe;

			ret = io_uring_queue_init(8, &ring, 0);
			if (ret) {
				fprintf(stderr, "grandchild ring setup: %d\n", ret);
				_exit(T_EXIT_FAIL);
			}

			/* NOP should be allowed */
			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_nop(sqe);
			sqe->user_data = 1;

			/* Read should be denied */
			sqe = io_uring_get_sqe(&ring);
			io_uring_prep_read(sqe, 0, NULL, 0, 0);
			sqe->user_data = 2;

			ret = io_uring_submit(&ring);
			if (ret != 2) {
				fprintf(stderr, "grandchild submit: %d\n", ret);
				_exit(T_EXIT_FAIL);
			}

			for (int i = 0; i < 2; i++) {
				ret = io_uring_wait_cqe(&ring, &cqe);
				if (ret) {
					fprintf(stderr, "grandchild wait: %d\n", ret);
					_exit(T_EXIT_FAIL);
				}

				switch (cqe->user_data) {
				case 1:
					if (cqe->res != 0) {
						fprintf(stderr, "grandchild nop: %d\n",
							cqe->res);
						_exit(T_EXIT_FAIL);
					}
					break;
				case 2:
					if (cqe->res != -EACCES) {
						fprintf(stderr, "grandchild read: %d\n",
							cqe->res);
						_exit(T_EXIT_FAIL);
					}
					break;
				}
				io_uring_cqe_seen(&ring, cqe);
			}

			io_uring_queue_exit(&ring);
			_exit(T_EXIT_PASS);
		}

		int status;
		waitpid(gpid, &status, 0);
		if (!WIFEXITED(status))
			_exit(T_EXIT_FAIL);
		_exit(WEXITSTATUS(status));
	}

	int status;
	waitpid(pid, &status, 0);
	if (!WIFEXITED(status))
		return T_EXIT_FAIL;
	return WEXITSTATUS(status);
}

/*
 * Test that registering task restrictions twice fails
 */
static int test_task_restrict_double_register(void)
{
	struct io_uring_restriction res[1];
	int ret;

	res[0].opcode = IORING_RESTRICTION_SQE_OP;
	res[0].sqe_op = IORING_OP_NOP;

	pid_t pid = fork();
	if (pid < 0) {
		perror("fork");
		return T_EXIT_FAIL;
	}

	if (pid == 0) {
		prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);

		ret = register_task_restrictions(res, 1);
		if (ret == -EINVAL) {
			_exit(T_EXIT_SKIP);
		}
		if (ret) {
			fprintf(stderr, "first register: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		/* Second registration should fail with -EPERM */
		ret = register_task_restrictions(res, 1);
		if (ret != -EPERM) {
			fprintf(stderr, "second register: %d (expected -EPERM)\n", ret);
			_exit(T_EXIT_FAIL);
		}

		_exit(T_EXIT_PASS);
	}

	int status;
	waitpid(pid, &status, 0);
	if (!WIFEXITED(status))
		return T_EXIT_FAIL;
	return WEXITSTATUS(status);
}

/*
 * Test per-task register op restrictions
 */
static int test_task_restrict_register_op(void)
{
	struct io_uring_restriction res[1];
	int ret;

	/* Allow only IORING_REGISTER_FILES */
	res[0].opcode = IORING_RESTRICTION_REGISTER_OP;
	res[0].register_op = IORING_REGISTER_FILES;

	pid_t pid = fork();
	if (pid < 0) {
		perror("fork");
		return T_EXIT_FAIL;
	}

	if (pid == 0) {
		prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);

		ret = register_task_restrictions(res, 1);
		if (ret == -EINVAL) {
			_exit(T_EXIT_SKIP);
		}
		if (ret) {
			fprintf(stderr, "register task restrictions: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		struct io_uring ring;
		int pipe1[2];

		if (pipe(pipe1) != 0) {
			perror("pipe");
			_exit(T_EXIT_FAIL);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret) {
			fprintf(stderr, "ring setup: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		/* Register files should be allowed */
		ret = io_uring_register_files(&ring, pipe1, 2);
		if (ret) {
			fprintf(stderr, "register files: %d\n", ret);
			_exit(T_EXIT_FAIL);
		}

		/* Register buffers should be denied */
		uint64_t ptr;
		struct iovec vec = { .iov_base = &ptr, .iov_len = sizeof(ptr) };
		ret = io_uring_register_buffers(&ring, &vec, 1);
		if (ret != -EACCES) {
			fprintf(stderr, "register buffers: %d (expected -EACCES)\n", ret);
			_exit(T_EXIT_FAIL);
		}

		io_uring_queue_exit(&ring);
		close(pipe1[0]);
		close(pipe1[1]);
		_exit(T_EXIT_PASS);
	}

	int status;
	waitpid(pid, &status, 0);
	if (!WIFEXITED(status))
		return T_EXIT_FAIL;
	return WEXITSTATUS(status);
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_task_restrict_sqe_op();
	if (ret == T_EXIT_SKIP) {
		printf("Per-task restrictions not supported, skipping\n");
		return T_EXIT_SKIP;
	} else if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_task_restrict_sqe_op failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_task_restrict_fork_inherit();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_task_restrict_fork_inherit failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_task_restrict_double_register();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_task_restrict_double_register failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_task_restrict_register_op();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_task_restrict_register_op failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: stress test for buffer ring drain and refill. Creates a
 *		small buffer ring, submits many reads that consume all
 *		buffers, verifies correct behavior when buffers are
 *		exhausted, then refills and continues. Tests both read
 *		and recv paths with buffer rings under pressure.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		1
#define BUF_SIZE	256
#define NR_BUFS		8
#define NR_ROUNDS	10000
#define FILE_SIZE	(BUF_SIZE * NR_BUFS * NR_ROUNDS)

static void provide_buffers(struct io_uring_buf_ring *br, void *base,
			    int nr_bufs, int buf_size)
{
	int i;

	for (i = 0; i < nr_bufs; i++) {
		void *addr = base + i * buf_size;
		io_uring_buf_ring_add(br, addr, buf_size, i,
				      io_uring_buf_ring_mask(nr_bufs), i);
	}
	io_uring_buf_ring_advance(br, nr_bufs);
}

/*
 * Test: submit reads consuming all buffers, refill, repeat.
 * Verifies data integrity through the whole process.
 */
static int test_read_drain_refill(void)
{
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	void *buf_base;
	char *file_data;
	const char *fname = ".buf-ring-stress-tmp";
	int fd, ret, round;
	int total_read = 0;

	ret = io_uring_queue_init(32, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	/* Setup buffer ring */
	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL || ret == -ENOENT) {
			io_uring_queue_exit(&ring);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "buf ring setup: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	buf_base = malloc(NR_BUFS * BUF_SIZE);
	if (!buf_base)
		goto err;

	/* Create test file with known pattern */
	file_data = malloc(FILE_SIZE);
	if (!file_data)
		goto err;

	for (int i = 0; i < FILE_SIZE; i++)
		file_data[i] = (char)(i & 0xff);

	fd = open(fname, O_CREAT | O_WRONLY | O_TRUNC, 0644);
	if (fd < 0) {
		perror("open write");
		free(file_data);
		goto err;
	}
	if (write(fd, file_data, FILE_SIZE) != FILE_SIZE) {
		perror("write");
		close(fd);
		free(file_data);
		goto err;
	}
	close(fd);

	fd = open(fname, O_RDONLY);
	if (fd < 0) {
		perror("open read");
		free(file_data);
		goto err;
	}

	for (round = 0; round < NR_ROUNDS; round++) {
		int reads_submitted = 0;
		int reads_completed = 0;

		/* Provide fresh buffers for this round */
		provide_buffers(br, buf_base, NR_BUFS, BUF_SIZE);

		/* Submit NR_BUFS reads using buffer selection */
		for (int i = 0; i < NR_BUFS; i++) {
			sqe = io_uring_get_sqe(&ring);
			if (!sqe) {
				fprintf(stderr, "get sqe round %d/%d\n", round, i);
				goto err_file;
			}
			io_uring_prep_read(sqe, fd, NULL, BUF_SIZE, total_read + i * BUF_SIZE);
			sqe->flags |= IOSQE_BUFFER_SELECT;
			sqe->buf_group = BGID;
			sqe->user_data = (round << 16) | i;
			reads_submitted++;
		}

		ret = io_uring_submit(&ring);
		if (ret != reads_submitted) {
			fprintf(stderr, "submit round %d: %d\n", round, ret);
			goto err_file;
		}

		/* Reap all completions */
		while (reads_completed < reads_submitted) {
			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret) {
				fprintf(stderr, "wait round %d: %d\n", round, ret);
				goto err_file;
			}

			if (cqe->res < 0) {
				fprintf(stderr, "read error round %d: %d\n",
					round, cqe->res);
				io_uring_cqe_seen(&ring, cqe);
				goto err_file;
			}

			if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
				fprintf(stderr, "no buffer flag round %d\n", round);
				io_uring_cqe_seen(&ring, cqe);
				goto err_file;
			}

			int bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
			if (bid >= NR_BUFS) {
				fprintf(stderr, "bad bid %d round %d\n", bid, round);
				io_uring_cqe_seen(&ring, cqe);
				goto err_file;
			}

			/* Verify data integrity */
			void *got = buf_base + bid * BUF_SIZE;
			int idx = cqe->user_data & 0xffff;
			int off = total_read + idx * BUF_SIZE;
			if (cqe->res > 0 && memcmp(got, file_data + off, cqe->res) != 0) {
				fprintf(stderr, "data mismatch round %d bid %d\n",
					round, bid);
				io_uring_cqe_seen(&ring, cqe);
				goto err_file;
			}

			io_uring_cqe_seen(&ring, cqe);
			reads_completed++;
		}

		total_read += NR_BUFS * BUF_SIZE;
	}

	if (total_read != FILE_SIZE) {
		fprintf(stderr, "total read %d, expected %d\n", total_read, FILE_SIZE);
		goto err_file;
	}

	close(fd);
	unlink(fname);
	free(file_data);
	free(buf_base);
	io_uring_free_buf_ring(&ring, br, NR_BUFS, BGID);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_file:
	close(fd);
	unlink(fname);
	free(file_data);
err:
	free(buf_base);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Test: submit more reads than available buffers. Excess reads should
 * fail with -ENOBUFS. Then refill and retry.
 */
static int test_read_overflow_refill(void)
{
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	void *buf_base;
	const char *fname = ".buf-ring-overflow-tmp";
	int fd, ret;
	int nr_success = 0, nr_enobufs = 0;

	ret = io_uring_queue_init(32, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL || ret == -ENOENT) {
			io_uring_queue_exit(&ring);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "buf ring setup: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	buf_base = malloc(NR_BUFS * BUF_SIZE);
	if (!buf_base)
		goto err;

	/* Create test file */
	t_create_file(fname, NR_BUFS * BUF_SIZE * 4);
	fd = open(fname, O_RDONLY);
	if (fd < 0) {
		perror("open");
		goto err;
	}

	/* Provide only NR_BUFS buffers */
	provide_buffers(br, buf_base, NR_BUFS, BUF_SIZE);

	/* Submit more reads than we have buffers */
	for (int i = 0; i < NR_BUFS * 2; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read(sqe, fd, NULL, BUF_SIZE, i * BUF_SIZE);
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = BGID;
		sqe->user_data = i;
	}

	ret = io_uring_submit(&ring);
	if (ret != NR_BUFS * 2) {
		fprintf(stderr, "submit: %d\n", ret);
		goto err_file;
	}

	/* Collect all completions */
	for (int i = 0; i < NR_BUFS * 2; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait: %d\n", ret);
			goto err_file;
		}

		if (cqe->res == -ENOBUFS) {
			nr_enobufs++;
		} else if (cqe->res > 0) {
			nr_success++;
		} else {
			fprintf(stderr, "unexpected res %d for ud %llu\n",
				cqe->res, (unsigned long long)cqe->user_data);
			io_uring_cqe_seen(&ring, cqe);
			goto err_file;
		}
		io_uring_cqe_seen(&ring, cqe);
	}

	/* We should have gotten exactly NR_BUFS successes */
	if (nr_success != NR_BUFS) {
		fprintf(stderr, "expected %d success, got %d\n", NR_BUFS, nr_success);
		goto err_file;
	}

	if (nr_enobufs != NR_BUFS) {
		fprintf(stderr, "expected %d ENOBUFS, got %d\n", NR_BUFS, nr_enobufs);
		goto err_file;
	}

	/* Now refill and do another round - should work fine */
	provide_buffers(br, buf_base, NR_BUFS, BUF_SIZE);

	nr_success = 0;
	for (int i = 0; i < NR_BUFS; i++) {
		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read(sqe, fd, NULL, BUF_SIZE, i * BUF_SIZE);
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = BGID;
		sqe->user_data = 100 + i;
	}

	ret = io_uring_submit(&ring);
	if (ret != NR_BUFS) {
		fprintf(stderr, "refill submit: %d\n", ret);
		goto err_file;
	}

	for (int i = 0; i < NR_BUFS; i++) {
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "refill wait: %d\n", ret);
			goto err_file;
		}

		if (cqe->res <= 0) {
			fprintf(stderr, "refill read res: %d\n", cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto err_file;
		}
		nr_success++;
		io_uring_cqe_seen(&ring, cqe);
	}

	if (nr_success != NR_BUFS) {
		fprintf(stderr, "refill success: %d\n", nr_success);
		goto err_file;
	}

	close(fd);
	unlink(fname);
	free(buf_base);
	io_uring_free_buf_ring(&ring, br, NR_BUFS, BGID);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_file:
	close(fd);
	unlink(fname);
err:
	free(buf_base);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

/*
 * Test: rapid drain/refill cycles with concurrent reads
 */
static int test_rapid_drain_refill(void)
{
	struct io_uring ring;
	struct io_uring_buf_ring *br;
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	void *buf_base;
	const char *fname = ".buf-ring-rapid-tmp";
	int fd, ret;
	int total_success = 0;

	ret = io_uring_queue_init(64, &ring, 0);
	if (ret) {
		fprintf(stderr, "ring setup: %d\n", ret);
		return T_EXIT_FAIL;
	}

	br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
	if (!br) {
		if (ret == -EINVAL || ret == -ENOENT) {
			io_uring_queue_exit(&ring);
			return T_EXIT_SKIP;
		}
		fprintf(stderr, "buf ring setup: %d\n", ret);
		io_uring_queue_exit(&ring);
		return T_EXIT_FAIL;
	}

	buf_base = malloc(NR_BUFS * BUF_SIZE);
	if (!buf_base)
		goto err;

	t_create_file(fname, FILE_SIZE);
	fd = open(fname, O_RDONLY);
	if (fd < 0) {
		perror("open");
		goto err;
	}

	/*
	 * Do many rapid cycles: provide 1 buffer, submit 1 read,
	 * reap, repeat. This tests the fast path of single-buffer
	 * provide/consume cycles.
	 */
	for (int i = 0; i < NR_ROUNDS * NR_BUFS; i++) {
		/* Provide exactly 1 buffer */
		void *addr = buf_base + (i % NR_BUFS) * BUF_SIZE;
		io_uring_buf_ring_add(br, addr, BUF_SIZE, i % NR_BUFS,
				      io_uring_buf_ring_mask(NR_BUFS), 0);
		io_uring_buf_ring_advance(br, 1);

		sqe = io_uring_get_sqe(&ring);
		io_uring_prep_read(sqe, fd, NULL, BUF_SIZE,
				   (i * BUF_SIZE) % FILE_SIZE);
		sqe->flags |= IOSQE_BUFFER_SELECT;
		sqe->buf_group = BGID;
		sqe->user_data = i;

		ret = io_uring_submit(&ring);
		if (ret != 1) {
			fprintf(stderr, "rapid submit %d: %d\n", i, ret);
			goto err_file;
		}

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "rapid wait %d: %d\n", i, ret);
			goto err_file;
		}

		if (cqe->res <= 0) {
			fprintf(stderr, "rapid read %d res: %d\n", i, cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto err_file;
		}

		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "rapid read %d: no buffer flag\n", i);
			io_uring_cqe_seen(&ring, cqe);
			goto err_file;
		}

		total_success++;
		io_uring_cqe_seen(&ring, cqe);
	}

	if (total_success != NR_ROUNDS * NR_BUFS) {
		fprintf(stderr, "rapid: %d success, expected %d\n",
			total_success, NR_ROUNDS * NR_BUFS);
		goto err_file;
	}

	close(fd);
	unlink(fname);
	free(buf_base);
	io_uring_free_buf_ring(&ring, br, NR_BUFS, BGID);
	io_uring_queue_exit(&ring);
	return T_EXIT_PASS;

err_file:
	close(fd);
	unlink(fname);
err:
	free(buf_base);
	io_uring_queue_exit(&ring);
	return T_EXIT_FAIL;
}

int main(int argc, char *argv[])
{
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	ret = test_read_drain_refill();
	if (ret == T_EXIT_SKIP) {
		printf("Buffer rings not supported, skipping\n");
		return T_EXIT_SKIP;
	}
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_read_drain_refill failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_read_overflow_refill();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_read_overflow_refill failed\n");
		return T_EXIT_FAIL;
	}

	ret = test_rapid_drain_refill();
	if (ret == T_EXIT_FAIL) {
		fprintf(stderr, "test_rapid_drain_refill failed\n");
		return T_EXIT_FAIL;
	}

	return T_EXIT_PASS;
}

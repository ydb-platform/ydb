#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Description: Verify recvmsg multishot with IOU_PBUF_RING_INC does not
 *		return spurious -EFAULT (BADADDR) when the tail of a
 *		partially-consumed buffer is smaller than the msghdr header
 *		the kernel needs to place (sizeof(io_uring_recvmsg_out) +
 *		namelen + controllen).
 *
 *		The kernel must retire the too-small tail and advance to
 *		the next ring entry rather than refusing the recv.
 *
 *		Also validates the in-buffer layout of each CQE via the
 *		io_uring_recvmsg_* helpers, so a bug that returns a valid
 *		bid but mis-positions the payload is caught.
 *
 *		See https://github.com/axboe/liburing/issues/1433
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "liburing.h"
#include "helpers.h"

#define BGID		1
#define NR_BUFS		4
#define BUF_SIZE	1024
#define QD		8

/*
 * hdr reserved per CQE by recvmsg multishot:
 *   sizeof(io_uring_recvmsg_out) + namelen + controllen
 *   = 16 + sizeof(struct sockaddr_in) + 0 = 32
 *
 * Two send sizes exercise both buffer-advance paths:
 *
 * LARGE_SZ=480: two CQEs of (32+480)=512 consume bid=0 exactly. The
 *   kernel advances to bid=1 via the normal head++ path, no retire
 *   needed.
 *
 * SMALL_SZ=305: three CQEs of (32+305)=337 in a 1024-byte buffer total
 *   1011, leaving a 13-byte tail. 13 < 32, so the 4th send into that
 *   buffer must retire the tail and advance to the next ring entry.
 *   Pre-fix this manifests as -EFAULT; post-fix the CQE succeeds with
 *   a new buffer id.
 *
 * Send sequence: 2 LARGE + 7 SMALL. Expected bid per CQE:
 *   { 0, 0, 1, 1, 1, 2, 2, 2, 3 }
 * — one natural transition (0->1) and two retire transitions (1->2
 * and 2->3).
 */
#define LARGE_SZ	480
#define NR_LARGE	2
#define SMALL_SZ	305
#define NR_SMALL	7
#define NR_SENDS	(NR_LARGE + NR_SMALL)

static const int expected_bids[NR_SENDS] = { 0, 0, 1, 1, 1, 2, 2, 2, 3 };

static int no_buf_ring, no_recv_mshot;

static int setup_buf_ring(struct io_uring *ring, void **buf_mem,
			  struct io_uring_buf_ring **out_br)
{
	struct io_uring_buf_ring *br;
	struct io_uring_buf_reg reg = { };
	size_t total = NR_BUFS * BUF_SIZE;
	int page_size = sysconf(_SC_PAGESIZE);
	size_t ring_size;
	void *mem;
	int ret, i;

	ring_size = NR_BUFS * sizeof(struct io_uring_buf);
	ring_size = (ring_size + page_size - 1) & ~(page_size - 1);

	mem = mmap(NULL, total, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (mem == MAP_FAILED)
		return -1;
	*buf_mem = mem;

	br = mmap(NULL, ring_size, PROT_READ | PROT_WRITE,
		  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (br == MAP_FAILED) {
		munmap(mem, total);
		return -1;
	}
	*out_br = br;

	io_uring_buf_ring_init(br);
	reg.ring_addr = (unsigned long) br;
	reg.ring_entries = NR_BUFS;
	reg.bgid = BGID;
	reg.min_left = 32;

	ret = io_uring_register_buf_ring(ring, &reg, IOU_PBUF_RING_INC);
	if (ret) {
		if (ret == -EINVAL) {
			no_buf_ring = 1;
			return 0;
		}
		fprintf(stderr, "register_buf_ring: %d\n", ret);
		return -1;
	}

	for (i = 0; i < NR_BUFS; i++) {
		io_uring_buf_ring_add(br, (char *)mem + i * BUF_SIZE, BUF_SIZE,
				      i, io_uring_buf_ring_mask(NR_BUFS), i);
	}
	io_uring_buf_ring_advance(br, NR_BUFS);
	return 0;
}

static int test(void)
{
	struct io_uring ring;
	struct io_uring_buf_ring *br = NULL;
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct msghdr msg;
	struct sockaddr_in name;
	size_t expected_payload = NR_LARGE * LARGE_SZ + NR_SMALL * SMALL_SZ;
	uint8_t stream[NR_LARGE * LARGE_SZ + NR_SMALL * SMALL_SZ];
	size_t bid_offset[NR_BUFS] = { 0 };
	size_t stream_cursor = 0;
	size_t sent_offset = 0;
	void *buf_mem = NULL;
	int ret, fds[2];
	int i, seen_bids = 0;
	int last_bid = -1;
	int ret_val = T_EXIT_FAIL;

	for (i = 0; i < (int) expected_payload; i++)
		stream[i] = (uint8_t)(i & 0xff);

	ret = io_uring_queue_init(QD, &ring, 0);
	if (ret) {
		fprintf(stderr, "queue_init: %d\n", ret);
		return T_EXIT_FAIL;
	}

	if (setup_buf_ring(&ring, &buf_mem, &br))
		goto out;
	if (no_buf_ring) {
		ret_val = T_EXIT_SKIP;
		goto out;
	}

	ret = t_create_socket_pair(fds, true);
	if (ret) {
		fprintf(stderr, "socket_pair: %d\n", ret);
		goto out;
	}

	memset(&msg, 0, sizeof(msg));
	msg.msg_name = &name;
	msg.msg_namelen = sizeof(name);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recvmsg_multishot(sqe, fds[0], &msg, 0);
	sqe->flags |= IOSQE_BUFFER_SELECT;
	sqe->buf_group = BGID;
	sqe->user_data = 1;

	ret = io_uring_submit(&ring);
	if (ret != 1) {
		fprintf(stderr, "submit: %d\n", ret);
		goto out_close;
	}

	for (i = 0; i < NR_SENDS; i++) {
		int hdr = sizeof(struct io_uring_recvmsg_out) + sizeof(name);
		int send_sz = (i < NR_LARGE) ? LARGE_SZ : SMALL_SZ;
		struct io_uring_recvmsg_out *o;
		uint8_t *cqe_buf, *pdata;
		unsigned int plen;
		int bid;

		if (write(fds[1], stream + sent_offset, send_sz) != send_sz) {
			perror("write");
			goto out_close;
		}
		sent_offset += send_sz;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret) {
			fprintf(stderr, "wait_cqe: %d\n", ret);
			goto out_close;
		}

		if (cqe->res == -EINVAL || cqe->res == -ENOTSUP) {
			no_recv_mshot = 1;
			io_uring_cqe_seen(&ring, cqe);
			ret_val = T_EXIT_SKIP;
			goto out_close;
		}
		if (cqe->res < 0) {
			fprintf(stderr,
				"send %d: recvmsg multishot failed: %s (res=%d)\n",
				i, strerror(-cqe->res), cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}
		if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
			fprintf(stderr, "send %d: CQE missing buffer id\n", i);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}
		if (cqe->res < hdr) {
			fprintf(stderr, "send %d: short CQE res=%d (< hdr %d)\n",
				i, cqe->res, hdr);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}

		bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
		if (bid != expected_bids[i]) {
			fprintf(stderr,
				"send %d: bid=%d, expected %d\n",
				i, bid, expected_bids[i]);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}
		if (bid != last_bid) {
			seen_bids++;
			last_bid = bid;
		}

		cqe_buf = (uint8_t *)buf_mem + bid * BUF_SIZE + bid_offset[bid];
		o = io_uring_recvmsg_validate(cqe_buf, cqe->res, &msg);
		if (!o) {
			fprintf(stderr,
				"send %d: recvmsg_validate returned NULL (res=%d)\n",
				i, cqe->res);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}
		if (o->controllen != 0) {
			fprintf(stderr,
				"send %d: unexpected controllen=%u\n",
				i, o->controllen);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}

		plen = io_uring_recvmsg_payload_length(o, cqe->res, &msg);
		pdata = io_uring_recvmsg_payload(o, &msg);

		if (plen != (unsigned int)(cqe->res - hdr)) {
			fprintf(stderr,
				"send %d: payload_length=%u, expected %d\n",
				i, plen, cqe->res - hdr);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}
		if (memcmp(pdata, stream + stream_cursor, plen) != 0) {
			fprintf(stderr,
				"send %d: payload content mismatch at cursor %zu, plen %u\n",
				i, stream_cursor, plen);
			io_uring_cqe_seen(&ring, cqe);
			goto out_close;
		}

		stream_cursor += plen;
		bid_offset[bid] += cqe->res;

		io_uring_cqe_seen(&ring, cqe);
	}

	if (stream_cursor != expected_payload) {
		fprintf(stderr, "payload mismatch: got %zu, expected %zu\n",
			stream_cursor, expected_payload);
		goto out_close;
	}

	/*
	 * Four bids expected: 0 (natural advance), 1 (first retire
	 * target), 2 (second retire target), 3 (third retire target).
	 * Anything else means the retire path didn't behave as planned.
	 */
	if (seen_bids != 4) {
		fprintf(stderr, "expected 4 distinct bids, saw %d\n",
			seen_bids);
		goto out_close;
	}

	ret_val = T_EXIT_PASS;

out_close:
	close(fds[0]);
	close(fds[1]);
out:
	io_uring_queue_exit(&ring);
	if (buf_mem)
		munmap(buf_mem, NR_BUFS * BUF_SIZE);
	if (br) {
		size_t ring_size = NR_BUFS * sizeof(struct io_uring_buf);
		int page_size = sysconf(_SC_PAGESIZE);

		ring_size = (ring_size + page_size - 1) & ~(page_size - 1);
		munmap(br, ring_size);
	}
	return ret_val;
}

int main(int argc, char *argv[])
{
	if (argc > 1)
		return T_EXIT_SKIP;

	return test();
}

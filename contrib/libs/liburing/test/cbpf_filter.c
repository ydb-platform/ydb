#include "../config-host.h"
/* SPDX-License-Identifier: MIT */
/*
 * Test classic BPF (cBPF) filtering for io_uring operations.
 *
 * This test demonstrates using cBPF filters to restrict io_uring operations.
 * Unlike eBPF which requires a separate compiled program, cBPF filters can
 * be defined inline as an array of sock_filter instructions.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <linux/filter.h>
#include <netinet/in.h>
#include <sys/un.h>

#include "liburing.h"
#include "liburing/io_uring/bpf_filter.h"
#include "helpers.h"

#ifndef RESOLVE_IN_ROOT
#define RESOLVE_IN_ROOT	0x10
#endif

/*
 * cBPF filter context layout (struct io_uring_bpf_ctx):
 *   offset 0:  user_data (u64)
 *   offset 8:  opcode (u8)
 *   offset 9:  sqe_flags (u8)
 *   offset 10: pdu_size (u8)
 *   offset 11: pad[5]
 *   offset 16: union (socket: family/type/protocol at 16/20/24)
 *                    (open: flags/mode/resolve at 16/24/32 - all u64)
 */
#define CTX_OFF_USER_DATA	0
#define CTX_OFF_OPCODE		8
#define CTX_OFF_SQE_FLAGS	9
#define CTX_OFF_SOCKET_FAMILY	16
#define CTX_OFF_SOCKET_TYPE	20
#define CTX_OFF_SOCKET_PROTO	24
#define CTX_OFF_OPEN_FLAGS	16	/* u64, use low 32 bits */
#define CTX_OFF_OPEN_MODE	24	/* u64 */
#define CTX_OFF_OPEN_RESOLVE	32	/* u64, use low 32 bits */
/*
 * connect: family @16 (u32), port @20 (__be16) + 2 pad,
 *          v4_addr @24 (__be32) / v6_addr @24 (u8[16]).
 * pdu_size = 24 (one __u32 + one __be16 + 2 pad + 16 bytes).
 * v6_addr is 16 bytes, accessed as four 4-byte words at offsets 24,
 * 28, 32, 36 via BPF_LD|BPF_W|BPF_ABS.
 */
#define CTX_OFF_CONNECT_FAMILY		16
#define CTX_OFF_CONNECT_PORT		20
#define CTX_OFF_CONNECT_V4_ADDR		24
#define CTX_OFF_CONNECT_V6_ADDR_W0	24	/* v6 bytes  0-3 */
#define CTX_OFF_CONNECT_V6_ADDR_W1	28	/* v6 bytes  4-7 */
#define CTX_OFF_CONNECT_V6_ADDR_W2	32	/* v6 bytes  8-11 */
#define CTX_OFF_CONNECT_V6_ADDR_W3	36	/* v6 bytes 12-15 */
#define CONNECT_PDU_SIZE		24

/*
 * Compile-time __be16 swap. htons() is a function call and is not
 * usable in static initializers like BPF_JUMP K constants.
 */
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
# define CT_HTONS(x)	((__u16)(x))
#else
# define CT_HTONS(x)	((__u16)((((x) & 0xff) << 8) | (((x) >> 8) & 0xff)))
#endif

/*
 * Compile-time K-constant for matching the __be16 port field via a
 * BPF_LD|BPF_W|BPF_ABS load at CTX_OFF_CONNECT_PORT. The kernel
 * populator writes port (__be16) at offset 20 with 2 zero pad bytes
 * at offset 22-23, and bpf_prog_run reads in native host byte order.
 * On LE the port lands in the low 16 bits; on BE the port lands in
 * the high 16 bits. Pad bytes are guaranteed zero by the framework's
 * memset, so no AND-mask is required.
 */
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
# define CT_PORT_K(p)	((__u32)(p) << 16)
#else
# define CT_PORT_K(p)	((__u32)CT_HTONS(p))
#endif

/*
 * Compile-time K-constant for matching a 4-byte address slice (one v4
 * address, one dword of a v6 address, or a /N subnet mask/base) via a
 * BPF_LD|BPF_W|BPF_ABS load. Pass the bytes in their on-the-wire
 * (network byte order) order; the macro emits the host-order u32 that
 * the BPF interpreter will see after loading those bytes.
 */
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
# define CT_ADDR_K(a, b, c, d) \
	(((__u32)(a) << 24) | ((__u32)(b) << 16) | ((__u32)(c) << 8) | (__u32)(d))
#else
# define CT_ADDR_K(a, b, c, d) \
	(((__u32)(d) << 24) | ((__u32)(c) << 16) | ((__u32)(b) << 8) | (__u32)(a))
#endif

/*
 * Simple cBPF filter that allows all operations.
 * Returns 1 (non-zero) to allow.
 */
static struct sock_filter allow_all_filter[] = {
	/* return 1 (allow) */
	BPF_STMT(BPF_RET | BPF_K, 1),
};

/*
 * Simple cBPF filter that denies all operations.
 * Returns 0 to deny.
 */
static struct sock_filter deny_all_filter[] = {
	/* return 0 (deny) */
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that only allows AF_INET sockets (denies AF_INET6, etc).
 * Checks the socket family field in the context.
 */
static struct sock_filter allow_inet_only_filter[] = {
	/* Load socket family (32-bit at offset 16) */
	BPF_STMT(BPF_LD | BPF_W | BPF_ABS, CTX_OFF_SOCKET_FAMILY),
	/* Jump if family == AF_INET (2), allow; else deny */
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 1),
	/* Allow: return 1 */
	BPF_STMT(BPF_RET | BPF_K, 1),
	/* Deny: return 0 */
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that only allows TCP sockets (SOCK_STREAM).
 */
static struct sock_filter allow_tcp_only_filter[] = {
	/* Load socket type (32-bit at offset 20) */
	BPF_STMT(BPF_LD | BPF_W | BPF_ABS, CTX_OFF_SOCKET_TYPE),
	/* Mask off SOCK_CLOEXEC/SOCK_NONBLOCK flags */
	BPF_STMT(BPF_ALU | BPF_AND | BPF_K, 0xf),
	/* Jump if type == SOCK_STREAM (1), allow; else deny */
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SOCK_STREAM, 0, 1),
	/* Allow: return 1 */
	BPF_STMT(BPF_RET | BPF_K, 1),
	/* Deny: return 0 */
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that denies O_CREAT flag for openat operations.
 * Checks the flags field in the open context.
 */
static struct sock_filter deny_o_creat_filter[] = {
	/* Load open flags (low 32 bits at offset 16) */
	BPF_STMT(BPF_LD | BPF_W | BPF_ABS, CTX_OFF_OPEN_FLAGS),
	/* Check if O_CREAT bit is set */
	BPF_STMT(BPF_ALU | BPF_AND | BPF_K, O_CREAT),
	/* If result is non-zero (O_CREAT set), deny */
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, 0),
	/* Deny: return 0 */
	BPF_STMT(BPF_RET | BPF_K, 0),
	/* Allow: return 1 */
	BPF_STMT(BPF_RET | BPF_K, 1),
};

/*
 * cBPF filter that denies RESOLVE_IN_ROOT flag for openat2 operations.
 * Checks the resolve field in the open context.
 */
static struct sock_filter deny_resolve_in_root_filter[] = {
	/* Load resolve flags (low 32 bits at offset 32) */
	BPF_STMT(BPF_LD | BPF_W | BPF_ABS, CTX_OFF_OPEN_RESOLVE),
	/* Check if RESOLVE_IN_ROOT bit is set */
	BPF_STMT(BPF_ALU | BPF_AND | BPF_K, RESOLVE_IN_ROOT),
	/* If result is non-zero (RESOLVE_IN_ROOT set), deny */
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, 0),
	/* Deny: return 0 */
	BPF_STMT(BPF_RET | BPF_K, 0),
	/* Allow: return 1 */
	BPF_STMT(BPF_RET | BPF_K, 1),
};

/*
 * cBPF filter that allows only AF_INET CONNECTs and denies everything
 * else (a family-whitelist of AF_INET).
 */
static struct sock_filter connect_allow_family_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that denies AF_UNIX CONNECTs and allows everything else
 * (a family-blacklist of AF_UNIX).
 */
static struct sock_filter connect_deny_family_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_UNIX, 1, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * Deny AF_INET CONNECTs to 127.0.0.127 and allow the rest. The test
 * address is byte-palindromic, so the K constant is endian-symmetric
 * and CT_ADDR_K() is not needed here.
 */
static struct sock_filter connect_deny_v4_addr_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 2),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V4_ADDR),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0x7f00007f, 1, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * Deny AF_INET CONNECTs to port 22 and allow the rest. Non-AF_INET
 * traffic falls through to allow. Matches the port via CT_PORT_K().
 */
static struct sock_filter connect_deny_port_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 3),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_PORT),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_PORT_K(22), 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
};

/*
 * cBPF filter that denies AF_INET CONNECTs outright. Used by the
 * stale-cache test: poisons the async msghdr with valid
 * AF_INET state, then submits a short-len CONNECT and verifies the
 * second one does NOT inherit AF_INET. When the framework zero-fill
 * remains intact (the populator returns early via the addr_len
 * guard), the filter sees family=0, falls through to allow, and the
 * kernel net path returns -EINVAL for the short addr_len.
 */
static struct sock_filter connect_deny_inet_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
};

/*
 * cBPF filter that allows only AF_INET CONNECTs to 127.0.0.1 and
 * denies everything else (a v4-address whitelist).
 */
static struct sock_filter connect_allow_v4_addr_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 3),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V4_ADDR),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(127, 0, 0, 1), 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * Deny AF_INET6 CONNECTs to 2001:db8::dead and allow the rest.
 * Walks the v6 address as four 4-byte word loads at offsets 24, 28,
 * 32, 36.
 */
static struct sock_filter connect_deny_v6_addr_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET6, 0, 8),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W0),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(0x20, 0x01, 0x0d, 0xb8), 0, 6),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W1),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 4),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W2),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 2),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W3),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(0, 0, 0xde, 0xad), 1, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * Allow only AF_INET6 CONNECTs to ::1 and deny everything else. Walks
 * the v6 address as four 4-byte word loads at offsets 24, 28, 32, 36.
 */
static struct sock_filter connect_allow_v6_addr_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET6, 0, 9),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W0),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 7),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W1),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 5),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W2),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, 0, 0, 3),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W3),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(0, 0, 0, 1), 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that allows only AF_INET CONNECTs to port 80 and denies
 * everything else (a port whitelist).
 */
static struct sock_filter connect_allow_port_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 3),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_PORT),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_PORT_K(80), 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * Deny AF_INET CONNECTs in 127.42.0.0/24 and allow the rest. CIDR
 * matching via load-mask-compare on the v4 address.
 */
static struct sock_filter connect_deny_v4_subnet_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 3),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V4_ADDR),
	BPF_STMT(BPF_ALU | BPF_AND | BPF_K, CT_ADDR_K(0xff, 0xff, 0xff, 0x00)),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(127, 42, 0, 0), 1, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that allows only AF_INET CONNECTs in the 127.0.0.0/24
 * subnet and denies everything else (a v4 subnet whitelist).
 */
static struct sock_filter connect_allow_v4_subnet_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 0, 4),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V4_ADDR),
	BPF_STMT(BPF_ALU | BPF_AND | BPF_K, CT_ADDR_K(0xff, 0xff, 0xff, 0x00)),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(127, 0, 0, 0), 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that denies AF_INET6 CONNECTs in the 2001:db8::/32
 * subnet and allows everything else. /32 falls on a word boundary, so
 * an exact-match JEQ on the first v6 word suffices.
 */
static struct sock_filter connect_deny_v6_subnet_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET6, 0, 2),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W0),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(0x20, 0x01, 0x0d, 0xb8), 1, 0),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/*
 * cBPF filter that allows only AF_INET6 CONNECTs in the fe80::/16
 * subnet (link-local) and denies everything else. /16 falls within
 * the first v6 word, so we AND-mask the first 16 bits and compare.
 */
static struct sock_filter connect_allow_v6_subnet_filter[] = {
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_FAMILY),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET6, 0, 4),
	BPF_STMT(BPF_LD  | BPF_W   | BPF_ABS, CTX_OFF_CONNECT_V6_ADDR_W0),
	BPF_STMT(BPF_ALU | BPF_AND | BPF_K, CT_ADDR_K(0xff, 0xff, 0x00, 0x00)),
	BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, CT_ADDR_K(0xfe, 0x80, 0x00, 0x00), 0, 1),
	BPF_STMT(BPF_RET | BPF_K, 1),
	BPF_STMT(BPF_RET | BPF_K, 0),
};

/* Register a BPF filter on a task */
static int register_bpf_filter(struct sock_filter *filter, unsigned int len,
			       __u32 opcode, __u8 pdu_size, int deny_rest)
{
	struct io_uring_bpf bpf = {
		.cmd_type = IO_URING_BPF_CMD_FILTER,
		.filter = {
			.opcode = opcode,
			.flags = deny_rest ? IO_URING_BPF_FILTER_DENY_REST : 0,
			.filter_len = len,
			.filter_ptr = (unsigned long) (uintptr_t) filter,
			.pdu_size = pdu_size,
		},
	};

	return io_uring_register_bpf_filter_task(&bpf);
}

/* Register a BPF filter on a ring */
static int register_bpf_filter_ring(struct io_uring *ring,
				    struct sock_filter *filter, unsigned int len,
				    __u32 opcode, __u8 pdu_size, int deny_rest)
{
	struct io_uring_bpf bpf = {
		.cmd_type = IO_URING_BPF_CMD_FILTER,
		.filter = {
			.opcode = opcode,
			.flags = deny_rest ? IO_URING_BPF_FILTER_DENY_REST : 0,
			.filter_len = len,
			.filter_ptr = (unsigned long) (uintptr_t) filter,
			.pdu_size = pdu_size,
		},
	};

	return io_uring_register_bpf_filter(ring, &bpf);
}

/* Test NOP operation */
static int test_nop(struct io_uring *ring, const char *desc, int should_succeed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("FAIL (get_sqe)\n");
		return -1;
	}

	io_uring_prep_nop(sqe);
	sqe->user_data = 0x1234;

	ret = io_uring_submit(ring);
	if (ret < 0) {
		printf("FAIL (submit: %s)\n", strerror(-ret));
		return ret;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("FAIL (wait: %s)\n", strerror(-ret));
		return ret;
	}

	if (should_succeed) {
		if (cqe->res >= 0) {
			ret = 0;
		} else {
			printf("FAIL (expected success, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		}
	} else {
		if (cqe->res == -EACCES) {
			ret = 0;
		} else {
			printf("FAIL (expected -EACCES, got %d)\n", cqe->res);
			ret = -1;
		}
	}

	if (ret)
		fprintf(stderr, "%s: %s: failed\n", __FUNCTION__, desc);
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

/* Test socket operation */
static int test_socket(struct io_uring *ring, int family, int type,
		       const char *desc, int should_succeed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_socket(sqe, family, type, 0, 0);
	sqe->user_data = 0x5678;

	ret = io_uring_submit(ring);
	if (ret < 0) {
		printf("FAIL (submit: %s)\n", strerror(-ret));
		return ret;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("FAIL (wait: %s)\n", strerror(-ret));
		return ret;
	}

	if (should_succeed) {
		if (cqe->res >= 0) {
			close(cqe->res);
			ret = 0;
		} else {
			printf("FAIL (expected success, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		}
	} else {
		if (cqe->res == -EACCES) {
			ret = 0;
		} else if (cqe->res < 0) {
			printf("FAIL (expected -EACCES, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		} else {
			printf("FAIL (expected denial, got fd=%d)\n", cqe->res);
			close(cqe->res);
			ret = -1;
		}
	}

	if (ret)
		fprintf(stderr, "%s: %s: failed\n", __FUNCTION__, desc);
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

/* Test openat operation */
static int test_openat(struct io_uring *ring, const char *path, int flags,
		       mode_t mode, const char *desc, int should_succeed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_openat(sqe, AT_FDCWD, path, flags, mode);
	sqe->user_data = 0xabcd;

	ret = io_uring_submit(ring);
	if (ret < 0) {
		printf("FAIL (submit: %s)\n", strerror(-ret));
		return ret;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("FAIL (wait: %s)\n", strerror(-ret));
		return ret;
	}

	if (should_succeed) {
		if (cqe->res >= 0) {
			close(cqe->res);
			ret = 0;
		} else {
			printf("FAIL (expected success, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		}
	} else {
		if (cqe->res == -EACCES) {
			ret = 0;
		} else if (cqe->res < 0) {
			printf("FAIL (expected -EACCES, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		} else {
			printf("FAIL (expected denial, got fd=%d)\n", cqe->res);
			close(cqe->res);
			ret = -1;
		}
	}

	if (ret)
		fprintf(stderr, "%s: %s: failed\n", __FUNCTION__, desc);
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

/* Test openat2 operation */
static int test_openat2(struct io_uring *ring, const char *path,
			struct open_how *how, const char *desc,
			int should_succeed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_openat2(sqe, AT_FDCWD, path, how);
	sqe->user_data = 0xef01;

	ret = io_uring_submit(ring);
	if (ret < 0) {
		printf("FAIL (submit: %s)\n", strerror(-ret));
		return ret;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("FAIL (wait: %s)\n", strerror(-ret));
		return ret;
	}

	if (should_succeed) {
		if (cqe->res >= 0) {
			close(cqe->res);
			ret = 0;
		} else {
			printf("FAIL (expected success, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		}
	} else {
		if (cqe->res == -EACCES) {
			ret = 0;
		} else if (cqe->res < 0) {
			printf("FAIL (expected -EACCES, got %s)\n",
			       strerror(-cqe->res));
			ret = -1;
		} else {
			printf("FAIL (expected denial, got fd=%d)\n", cqe->res);
			close(cqe->res);
			ret = -1;
		}
	}

	if (ret)
		fprintf(stderr, "%s: %s: failed\n", __FUNCTION__, desc);
	io_uring_cqe_seen(ring, cqe);
	return ret;
}

/*
 * Submit an IORING_OP_CONNECT to @sa/@slen. should_succeed == 1 means
 * the filter must allow the op through (cqe->res != -EACCES); the
 * connect itself may still fail, typically with -ECONNREFUSED on
 * closed loopback ports. Any non--EACCES result means the kernel net
 * path ran. should_succeed == 0 means the filter must deny
 * (cqe->res == -EACCES). The socket fd is consumed.
 */
static int test_connect(struct io_uring *ring, const struct sockaddr *sa,
			socklen_t slen, const char *desc, int should_succeed)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	int fd, ret;

	fd = socket(sa->sa_family, SOCK_STREAM, 0);
	if (fd < 0) {
		printf("FAIL (socket: %s)\n", strerror(errno));
		return -1;
	}

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_connect(sqe, fd, sa, slen);
	sqe->user_data = 0x9abc;

	ret = io_uring_submit(ring);
	if (ret < 0) {
		printf("FAIL (submit: %s)\n", strerror(-ret));
		close(fd);
		return ret;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("FAIL (wait: %s)\n", strerror(-ret));
		close(fd);
		return ret;
	}

	ret = 0;
	if (should_succeed && cqe->res == -EACCES) {
		printf("FAIL (expected allow, got -EACCES)\n");
		ret = -1;
	} else if (!should_succeed && cqe->res != -EACCES) {
		printf("FAIL (expected -EACCES, got %s)\n",
		       strerror(cqe->res < 0 ? -cqe->res : 0));
		ret = -1;
	}
	if (ret)
		fprintf(stderr, "%s: %s: failed\n", __FUNCTION__, desc);
	io_uring_cqe_seen(ring, cqe);
	close(fd);
	return ret;
}

static int test_deny_nop(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	/* Fork to get fresh task restrictions */
	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* Child process */
		ret = register_bpf_filter(deny_all_filter,
					  sizeof(deny_all_filter) / sizeof(deny_all_filter[0]),
					  IORING_OP_NOP, 0, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed\n");
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_nop(&ring, "NOP should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	/* Parent waits for child */
	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_allow_inet_only(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	/* Fork to get fresh task restrictions */
	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* Child process */
		ret = register_bpf_filter(allow_inet_only_filter,
					   sizeof(allow_inet_only_filter) / sizeof(allow_inet_only_filter[0]),
					   IORING_OP_SOCKET, 12, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed\n");
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_socket(&ring, AF_INET, SOCK_STREAM,
				"AF_INET TCP should succeed", 1) != 0)
			failed++;

		if (test_socket(&ring, AF_INET6, SOCK_STREAM,
				"AF_INET6 TCP should be denied", 0) != 0)
			failed++;

		if (test_socket(&ring, AF_UNIX, SOCK_STREAM,
				"AF_UNIX should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	/* Parent waits for child */
	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_allow_tcp_only(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		ret = register_bpf_filter(allow_tcp_only_filter,
					   sizeof(allow_tcp_only_filter) / sizeof(allow_tcp_only_filter[0]),
					   IORING_OP_SOCKET, 12, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed\n");
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_socket(&ring, AF_INET, SOCK_STREAM,
				"TCP should succeed", 1) != 0)
			failed++;

		if (test_socket(&ring, AF_INET, SOCK_DGRAM,
				"UDP should be denied", 0) != 0)
			failed++;

		if (test_socket(&ring, AF_INET6, SOCK_STREAM,
				"IPv6 TCP should succeed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_deny_rest(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* Register allow filter for NOP with DENY_REST flag */
		ret = register_bpf_filter(allow_all_filter,
					   sizeof(allow_all_filter) / sizeof(allow_all_filter[0]),
					   IORING_OP_NOP, 0,
					   1);  /* deny_rest = true */
		if (ret < 0) {
			fprintf(stderr, "Child: register failed\n");
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_nop(&ring, "NOP should succeed", 1) != 0)
			failed++;

		if (test_socket(&ring, AF_INET, SOCK_STREAM,
				"Socket should be denied (DENY_REST)", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test denying O_CREAT flag for IORING_OP_OPENAT.
 * Verifies the operation works before filter installation,
 * then fails with -EACCES after.
 */
static int test_deny_openat_creat(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;
	char tmpfile[] = "/tmp/cbpf_test_XXXXXX";
	int tmpfd;

	/* Create a temp file path we can use for testing */
	tmpfd = mkstemp(tmpfile);
	if (tmpfd < 0) {
		perror("mkstemp");
		return 1;
	}
	close(tmpfd);
	unlink(tmpfile);

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* Test that O_CREAT works BEFORE installing filter */
		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_openat(&ring, tmpfile, O_CREAT | O_RDWR, 0644,
				"O_CREAT should succeed before filter", 1) != 0)
			failed++;

		/* Clean up created file */
		unlink(tmpfile);

		/* Test that regular open (no O_CREAT) works */
		if (test_openat(&ring, "/dev/null", O_RDONLY, 0,
				"regular open should succeed before filter", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);

		/* Now install the O_CREAT deny filter */
		ret = register_bpf_filter(deny_o_creat_filter,
					  sizeof(deny_o_creat_filter) / sizeof(deny_o_creat_filter[0]),
					  IORING_OP_OPENAT, 24, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		/* Create new ring after filter is installed */
		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init 2 failed\n");
			exit(1);
		}

		/* Test that O_CREAT is now denied */
		if (test_openat(&ring, tmpfile, O_CREAT | O_RDWR, 0644,
				"O_CREAT should be denied after filter", 0) != 0)
			failed++;

		/* Test that regular open still works */
		if (test_openat(&ring, "/dev/null", O_RDONLY, 0,
				"regular open should still succeed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test denying RESOLVE_IN_ROOT flag for IORING_OP_OPENAT2.
 * Verifies the operation works before filter installation,
 * then fails with -EACCES after.
 *
 * Note: RESOLVE_IN_ROOT requires a relative path since it treats dfd as root.
 * We use "." with O_DIRECTORY to test this.
 */
static int test_deny_openat2_resolve_in_root(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;
	struct open_how how_with_resolve = {
		.flags = O_RDONLY | O_DIRECTORY,
		.mode = 0,
		.resolve = RESOLVE_IN_ROOT,
	};
	struct open_how how_normal = {
		.flags = O_RDONLY | O_DIRECTORY,
		.mode = 0,
		.resolve = 0,
	};

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* Test that RESOLVE_IN_ROOT works BEFORE installing filter */
		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_openat2(&ring, ".", &how_with_resolve,
				 "RESOLVE_IN_ROOT should succeed before filter", 1) != 0)
			failed++;

		/* Test that normal openat2 works */
		if (test_openat2(&ring, ".", &how_normal,
				 "normal openat2 should succeed before filter", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);

		/* Now install the RESOLVE_IN_ROOT deny filter */
		ret = register_bpf_filter(deny_resolve_in_root_filter,
					  sizeof(deny_resolve_in_root_filter) / sizeof(deny_resolve_in_root_filter[0]),
					  IORING_OP_OPENAT2, 24, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		/* Create new ring after filter is installed */
		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init 2 failed\n");
			exit(1);
		}

		/* Test that RESOLVE_IN_ROOT is now denied */
		if (test_openat2(&ring, ".", &how_with_resolve,
				 "RESOLVE_IN_ROOT should be denied after filter", 0) != 0)
			failed++;

		/* Test that normal openat2 still works */
		if (test_openat2(&ring, ".", &how_normal,
				 "normal openat2 should still succeed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_allow_family(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in v4 = {
			.sin_family = AF_INET,
			.sin_port = htons(1),
		};
		struct sockaddr_in6 v6 = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(1),
		};
		struct sockaddr_un un = { .sun_family = AF_UNIX };

		v4.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		v6.sin6_addr = in6addr_loopback;
		strncpy(un.sun_path, "/tmp/cbpf_filter_no_such_socket",
			sizeof(un.sun_path) - 1);

		ret = register_bpf_filter(connect_allow_family_filter,
					  sizeof(connect_allow_family_filter) / sizeof(connect_allow_family_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&v4, sizeof(v4),
				 "AF_INET should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&v6, sizeof(v6),
				 "AF_INET6 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&un, sizeof(un),
				 "AF_UNIX should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_deny_v4_addr(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in banned = {
			.sin_family = AF_INET,
			.sin_port = htons(1),
		};
		struct sockaddr_in other = {
			.sin_family = AF_INET,
			.sin_port = htons(1),
		};

		banned.sin_addr.s_addr = htonl(0x7f00007f);
		other.sin_addr.s_addr  = htonl(INADDR_LOOPBACK);

		ret = register_bpf_filter(connect_deny_v4_addr_filter,
					  sizeof(connect_deny_v4_addr_filter) / sizeof(connect_deny_v4_addr_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&banned, sizeof(banned),
				 "127.0.0.127 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&other, sizeof(other),
				 "127.0.0.1 should be allowed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_deny_port(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in ssh = {
			.sin_family = AF_INET,
			.sin_port = htons(22),
		};
		struct sockaddr_in http = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};

		ssh.sin_addr.s_addr  = htonl(INADDR_LOOPBACK);
		http.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

		ret = register_bpf_filter(connect_deny_port_filter,
					  sizeof(connect_deny_port_filter) / sizeof(connect_deny_port_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&ssh, sizeof(ssh),
				 "port 22 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&http, sizeof(http),
				 "port 80 should be allowed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test for io_connect_bpf_populate's addr_len handling.
 * Two kernel-side mechanisms cooperate: the framework's caller-side
 * memset in io_uring_populate_bpf_ctx() zero-fills bctx before the
 * populator runs, and the populator returns early when addr_len does
 * not cover the family discriminator (sizeof(sa_family_t)) so the
 * zero-fill stays intact. Step 1 poisons iomsg->addr with a denied
 * AF_INET CONNECT. Step 2 submits CONNECT with addr_len=1: the
 * filter must see family=0 and fall through to the kernel net path,
 * which returns -EINVAL for the sub-minimum addr_len. If the
 * populator read the stale AF_INET cache instead, the filter would
 * deny with -EACCES -- the failure mode this test catches.
 */
static int test_connect_stale_addr_len(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in sa = {
			.sin_family = AF_INET,
			.sin_port = htons(1),
		};
		struct io_uring_sqe *sqe;
		struct io_uring_cqe *cqe;
		int fd;

		sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

		ret = register_bpf_filter(connect_deny_inet_filter,
					  sizeof(connect_deny_inet_filter) / sizeof(connect_deny_inet_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		/*
		 * Step 1: poison iomsg->addr by submitting a fully-formed
		 * AF_INET CONNECT. The submit path's move_addr_to_kernel()
		 * copies the user sockaddr into the async msghdr before the
		 * filter runs; the filter then denies based on the populated
		 * family, leaving the AF_INET state cached in iomsg->addr.
		 */
		fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0) {
			perror("stale: socket step1");
			exit(1);
		}
		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			fprintf(stderr, "stale: get_sqe step1 failed\n");
			close(fd);
			exit(1);
		}
		io_uring_prep_connect(sqe, fd, (struct sockaddr *)&sa,
				      sizeof(sa));
		ret = io_uring_submit(&ring);
		if (ret < 0) {
			fprintf(stderr, "stale: submit step1: %s\n",
				strerror(-ret));
			close(fd);
			exit(1);
		}
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "stale: wait step1: %s\n",
				strerror(-ret));
			close(fd);
			exit(1);
		}
		if (cqe->res != -EACCES) {
			fprintf(stderr, "stale: poison expected -EACCES, got %d\n",
				cqe->res);
			failed++;
		}
		io_uring_cqe_seen(&ring, cqe);
		close(fd);

		/*
		 * Step 2: short-len CONNECT. Without the guard, this would
		 * reuse stale AF_INET from step 1 and be denied with
		 * -EACCES. With the guard, the filter sees family=0, allows
		 * the op through, and the kernel net path rejects the
		 * sub-minimum addr_len with -EINVAL -- which is the
		 * specific result we assert.
		 */
		fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0) {
			perror("stale: socket step2");
			exit(1);
		}
		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			fprintf(stderr, "stale: get_sqe step2 failed\n");
			close(fd);
			exit(1);
		}
		io_uring_prep_connect(sqe, fd, (struct sockaddr *)&sa, 1);
		ret = io_uring_submit(&ring);
		if (ret < 0) {
			fprintf(stderr, "stale: submit step2: %s\n",
				strerror(-ret));
			close(fd);
			exit(1);
		}
		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0) {
			fprintf(stderr, "stale: wait step2: %s\n",
				strerror(-ret));
			close(fd);
			exit(1);
		}
		if (cqe->res != -EINVAL) {
			fprintf(stderr, "stale: short-len expected -EINVAL, got %d\n",
				cqe->res);
			failed++;
		}
		io_uring_cqe_seen(&ring, cqe);
		close(fd);

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_deny_family(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in v4 = {
			.sin_family = AF_INET,
			.sin_port = htons(1),
		};
		struct sockaddr_in6 v6 = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(1),
		};
		struct sockaddr_un un = { .sun_family = AF_UNIX };

		v4.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		v6.sin6_addr = in6addr_loopback;
		strncpy(un.sun_path, "/tmp/cbpf_filter_no_such_socket",
			sizeof(un.sun_path) - 1);

		ret = register_bpf_filter(connect_deny_family_filter,
					  sizeof(connect_deny_family_filter) / sizeof(connect_deny_family_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&v4, sizeof(v4),
				 "AF_INET should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&v6, sizeof(v6),
				 "AF_INET6 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&un, sizeof(un),
				 "AF_UNIX should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_allow_v4_addr(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in allowed = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in denied_a = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in denied_b = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};

		allowed.sin_addr.s_addr  = htonl(INADDR_LOOPBACK);
		denied_a.sin_addr.s_addr = htonl(0x7f00007f);
		denied_b.sin_addr.s_addr = htonl(0x7f000002);

		ret = register_bpf_filter(connect_allow_v4_addr_filter,
					  sizeof(connect_allow_v4_addr_filter) / sizeof(connect_allow_v4_addr_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&allowed, sizeof(allowed),
				 "127.0.0.1 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&denied_a, sizeof(denied_a),
				 "127.0.0.127 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&denied_b, sizeof(denied_b),
				 "127.0.0.2 should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test blacklisting the v6 address 2001:db8::dead for
 * IORING_OP_CONNECT. Other v6 addresses (including those sharing the
 * 2001:db8::/32 prefix) are allowed. Non-AF_INET6 sockaddrs fall
 * through to allow as well, since this is purely a v6-address
 * blacklist.
 */
static int test_connect_deny_v6_addr(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in6 banned = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 other_lo = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 other_doc = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};

		/* 2001:db8::dead -- banned */
		banned.sin6_addr.s6_addr[0]  = 0x20;
		banned.sin6_addr.s6_addr[1]  = 0x01;
		banned.sin6_addr.s6_addr[2]  = 0x0d;
		banned.sin6_addr.s6_addr[3]  = 0xb8;
		banned.sin6_addr.s6_addr[14] = 0xde;
		banned.sin6_addr.s6_addr[15] = 0xad;
		/* ::1 -- loopback, outside the banned exact address */
		other_lo.sin6_addr = in6addr_loopback;
		/* 2001:db8::1 -- same /32 prefix, different exact addr */
		other_doc.sin6_addr.s6_addr[0]  = 0x20;
		other_doc.sin6_addr.s6_addr[1]  = 0x01;
		other_doc.sin6_addr.s6_addr[2]  = 0x0d;
		other_doc.sin6_addr.s6_addr[3]  = 0xb8;
		other_doc.sin6_addr.s6_addr[15] = 0x01;

		ret = register_bpf_filter(connect_deny_v6_addr_filter,
					  sizeof(connect_deny_v6_addr_filter) / sizeof(connect_deny_v6_addr_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&banned, sizeof(banned),
				 "2001:db8::dead should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&other_lo, sizeof(other_lo),
				 "::1 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&other_doc, sizeof(other_doc),
				 "2001:db8::1 should be allowed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_allow_v6_addr(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in6 allowed = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 denied_lo = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 denied_doc = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};

		allowed.sin6_addr = in6addr_loopback;
		/* ::2 -- test target */
		denied_lo.sin6_addr.s6_addr[15] = 0x02;
		/* 2001:db8::1 -- test target */
		denied_doc.sin6_addr.s6_addr[0]  = 0x20;
		denied_doc.sin6_addr.s6_addr[1]  = 0x01;
		denied_doc.sin6_addr.s6_addr[2]  = 0x0d;
		denied_doc.sin6_addr.s6_addr[3]  = 0xb8;
		denied_doc.sin6_addr.s6_addr[15] = 0x01;

		ret = register_bpf_filter(connect_allow_v6_addr_filter,
					  sizeof(connect_allow_v6_addr_filter) / sizeof(connect_allow_v6_addr_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&allowed, sizeof(allowed),
				 "::1 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&denied_lo, sizeof(denied_lo),
				 "::2 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&denied_doc, sizeof(denied_doc),
				 "2001:db8::1 should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_allow_port(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in allowed = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in denied_ssh = {
			.sin_family = AF_INET,
			.sin_port = htons(22),
		};
		struct sockaddr_in denied_https = {
			.sin_family = AF_INET,
			.sin_port = htons(443),
		};

		allowed.sin_addr.s_addr     = htonl(INADDR_LOOPBACK);
		denied_ssh.sin_addr.s_addr  = htonl(INADDR_LOOPBACK);
		denied_https.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

		ret = register_bpf_filter(connect_allow_port_filter,
					  sizeof(connect_allow_port_filter) / sizeof(connect_allow_port_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&allowed, sizeof(allowed),
				 "port 80 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&denied_ssh, sizeof(denied_ssh),
				 "port 22 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&denied_https, sizeof(denied_https),
				 "port 443 should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_deny_v4_subnet(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in in_subnet_a = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in in_subnet_b = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in out_subnet = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};

		in_subnet_a.sin_addr.s_addr = htonl(0x7f2a0001);  /* 127.42.0.1 */
		in_subnet_b.sin_addr.s_addr = htonl(0x7f2a0063);  /* 127.42.0.99 */
		out_subnet.sin_addr.s_addr  = htonl(INADDR_LOOPBACK);

		ret = register_bpf_filter(connect_deny_v4_subnet_filter,
					  sizeof(connect_deny_v4_subnet_filter) / sizeof(connect_deny_v4_subnet_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&in_subnet_a, sizeof(in_subnet_a),
				 "127.42.0.1 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&in_subnet_b, sizeof(in_subnet_b),
				 "127.42.0.99 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&out_subnet, sizeof(out_subnet),
				 "127.0.0.1 should be allowed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_allow_v4_subnet(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in in_subnet_a = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in in_subnet_b = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};
		struct sockaddr_in out_subnet = {
			.sin_family = AF_INET,
			.sin_port = htons(80),
		};

		in_subnet_a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);   /* 127.0.0.1 */
		in_subnet_b.sin_addr.s_addr = htonl(0x7f000063);        /* 127.0.0.99 */
		out_subnet.sin_addr.s_addr  = htonl(0x7f2a0001);        /* 127.42.0.1 */

		ret = register_bpf_filter(connect_allow_v4_subnet_filter,
					  sizeof(connect_allow_v4_subnet_filter) / sizeof(connect_allow_v4_subnet_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&in_subnet_a, sizeof(in_subnet_a),
				 "127.0.0.1 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&in_subnet_b, sizeof(in_subnet_b),
				 "127.0.0.99 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&out_subnet, sizeof(out_subnet),
				 "127.42.0.1 should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_deny_v6_subnet(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in6 in_subnet_a = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 in_subnet_b = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 out_subnet = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};

		/* 2001:db8::1 */
		in_subnet_a.sin6_addr.s6_addr[0]  = 0x20;
		in_subnet_a.sin6_addr.s6_addr[1]  = 0x01;
		in_subnet_a.sin6_addr.s6_addr[2]  = 0x0d;
		in_subnet_a.sin6_addr.s6_addr[3]  = 0xb8;
		in_subnet_a.sin6_addr.s6_addr[15] = 0x01;
		/* 2001:db8:dead::1 -- same /32 prefix, different remainder */
		in_subnet_b.sin6_addr.s6_addr[0]  = 0x20;
		in_subnet_b.sin6_addr.s6_addr[1]  = 0x01;
		in_subnet_b.sin6_addr.s6_addr[2]  = 0x0d;
		in_subnet_b.sin6_addr.s6_addr[3]  = 0xb8;
		in_subnet_b.sin6_addr.s6_addr[4]  = 0xde;
		in_subnet_b.sin6_addr.s6_addr[5]  = 0xad;
		in_subnet_b.sin6_addr.s6_addr[15] = 0x01;
		/* ::1 -- loopback, outside the /32 */
		out_subnet.sin6_addr = in6addr_loopback;

		ret = register_bpf_filter(connect_deny_v6_subnet_filter,
					  sizeof(connect_deny_v6_subnet_filter) / sizeof(connect_deny_v6_subnet_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&in_subnet_a, sizeof(in_subnet_a),
				 "2001:db8::1 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&in_subnet_b, sizeof(in_subnet_b),
				 "2001:db8:dead::1 should be denied", 0) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&out_subnet, sizeof(out_subnet),
				 "::1 should be allowed", 1) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

static int test_connect_allow_v6_subnet(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		struct sockaddr_in6 in_subnet_a = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 in_subnet_b = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};
		struct sockaddr_in6 out_subnet = {
			.sin6_family = AF_INET6,
			.sin6_port = htons(80),
		};

		/* fe80::1 */
		in_subnet_a.sin6_addr.s6_addr[0]  = 0xfe;
		in_subnet_a.sin6_addr.s6_addr[1]  = 0x80;
		in_subnet_a.sin6_addr.s6_addr[15] = 0x01;
		/* fe80:cafe::beef -- same /16 prefix */
		in_subnet_b.sin6_addr.s6_addr[0]  = 0xfe;
		in_subnet_b.sin6_addr.s6_addr[1]  = 0x80;
		in_subnet_b.sin6_addr.s6_addr[2]  = 0xca;
		in_subnet_b.sin6_addr.s6_addr[3]  = 0xfe;
		in_subnet_b.sin6_addr.s6_addr[14] = 0xbe;
		in_subnet_b.sin6_addr.s6_addr[15] = 0xef;
		/* 2001:db8::1 -- documentation prefix, outside fe80::/16 */
		out_subnet.sin6_addr.s6_addr[0]  = 0x20;
		out_subnet.sin6_addr.s6_addr[1]  = 0x01;
		out_subnet.sin6_addr.s6_addr[2]  = 0x0d;
		out_subnet.sin6_addr.s6_addr[3]  = 0xb8;
		out_subnet.sin6_addr.s6_addr[15] = 0x01;

		ret = register_bpf_filter(connect_allow_v6_subnet_filter,
					  sizeof(connect_allow_v6_subnet_filter) / sizeof(connect_allow_v6_subnet_filter[0]),
					  IORING_OP_CONNECT, CONNECT_PDU_SIZE, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: register failed: %s\n",
				strerror(-ret));
			exit(ret == -EINVAL ? 0 : 1);
		}

		ret = io_uring_queue_init(8, &ring, 0);
		if (ret < 0) {
			fprintf(stderr, "Child: queue_init failed\n");
			exit(1);
		}

		if (test_connect(&ring, (struct sockaddr *)&in_subnet_a, sizeof(in_subnet_a),
				 "fe80::1 should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&in_subnet_b, sizeof(in_subnet_b),
				 "fe80:cafe::beef should be allowed", 1) != 0)
			failed++;
		if (test_connect(&ring, (struct sockaddr *)&out_subnet, sizeof(out_subnet),
				 "2001:db8::1 should be denied", 0) != 0)
			failed++;

		io_uring_queue_exit(&ring);
		exit(failed);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Ring-level filter tests - these test filters registered on a specific ring
 * rather than on the task. Ring filters don't require forking.
 */

static int test_deny_nop_ring(void)
{
	struct io_uring ring;
	int ret, failed = 0;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "queue_init failed: %s\n", strerror(-ret));
		return 1;
	}

	ret = register_bpf_filter_ring(&ring, deny_all_filter,
				       ARRAY_SIZE(deny_all_filter),
				       IORING_OP_NOP, 0, 0);
	if (ret < 0) {
		fprintf(stderr, "register failed: %s\n", strerror(-ret));
		io_uring_queue_exit(&ring);
		return ret == -EINVAL ? 0 : 1;
	}

	if (test_nop(&ring, "NOP should be denied (ring)", 0) != 0)
		failed++;

	io_uring_queue_exit(&ring);
	return failed;
}

static int test_allow_inet_only_ring(void)
{
	struct io_uring ring;
	int ret, failed = 0;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "queue_init failed: %s\n", strerror(-ret));
		return 1;
	}

	ret = register_bpf_filter_ring(&ring, allow_inet_only_filter,
				       ARRAY_SIZE(allow_inet_only_filter),
				       IORING_OP_SOCKET, 12, 0);
	if (ret < 0) {
		fprintf(stderr, "register failed: %s\n", strerror(-ret));
		io_uring_queue_exit(&ring);
		return ret == -EINVAL ? 0 : 1;
	}

	if (test_socket(&ring, AF_INET, SOCK_STREAM,
			"AF_INET TCP should succeed (ring)", 1) != 0)
		failed++;

	if (test_socket(&ring, AF_INET6, SOCK_STREAM,
			"AF_INET6 TCP should be denied (ring)", 0) != 0)
		failed++;

	if (test_socket(&ring, AF_UNIX, SOCK_STREAM,
			"AF_UNIX should be denied (ring)", 0) != 0)
		failed++;

	io_uring_queue_exit(&ring);
	return failed;
}

static int test_allow_tcp_only_ring(void)
{
	struct io_uring ring;
	int ret, failed = 0;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "queue_init failed: %s\n", strerror(-ret));
		return 1;
	}

	ret = register_bpf_filter_ring(&ring, allow_tcp_only_filter,
				       ARRAY_SIZE(allow_tcp_only_filter),
				       IORING_OP_SOCKET, 12, 0);
	if (ret < 0) {
		fprintf(stderr, "register failed: %s\n", strerror(-ret));
		io_uring_queue_exit(&ring);
		return ret == -EINVAL ? 0 : 1;
	}

	if (test_socket(&ring, AF_INET, SOCK_STREAM,
			"TCP should succeed (ring)", 1) != 0)
		failed++;

	if (test_socket(&ring, AF_INET, SOCK_DGRAM,
			"UDP should be denied (ring)", 0) != 0)
		failed++;

	if (test_socket(&ring, AF_INET6, SOCK_STREAM,
			"IPv6 TCP should succeed (ring)", 1) != 0)
		failed++;

	io_uring_queue_exit(&ring);
	return failed;
}

static int test_deny_rest_ring(void)
{
	struct io_uring ring;
	int ret, failed = 0;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "queue_init failed: %s\n", strerror(-ret));
		return 1;
	}

	/* Register allow filter for NOP with DENY_REST flag */
	ret = register_bpf_filter_ring(&ring, allow_all_filter,
				       ARRAY_SIZE(allow_all_filter),
				       IORING_OP_NOP, 0, 1);
	if (ret < 0) {
		fprintf(stderr, "register failed: %s\n", strerror(-ret));
		io_uring_queue_exit(&ring);
		return ret == -EINVAL ? 0 : 1;
	}

	if (test_nop(&ring, "NOP should succeed (ring)", 1) != 0)
		failed++;

	if (test_socket(&ring, AF_INET, SOCK_STREAM,
			"Socket should be denied DENY_REST (ring)", 0) != 0)
		failed++;

	io_uring_queue_exit(&ring);
	return failed;
}

/*
 * Test pdu_size validation for filter registration.
 *
 * IORING_OP_SOCKET has a kernel pdu_size of 12 (3x __u32). Test:
 * 1) pdu_size too big (24) - should fail with -EMSGSIZE
 * 2) pdu_size too small (8) without strict - should succeed, kernel
 *    writes back actual pdu_size (12)
 * 3) pdu_size too small (8) with IO_URING_BPF_FILTER_SZ_STRICT -
 *    should fail with -EMSGSIZE, kernel writes back actual pdu_size (12)
 */
static int test_pdu_size_ring(void)
{
	struct io_uring ring;
	struct io_uring_bpf bpf;
	int ret, failed = 0;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "queue_init failed: %s\n", strerror(-ret));
		return 1;
	}

	/* Test 1: pdu_size too big, should fail with -EMSGSIZE */
	memset(&bpf, 0, sizeof(bpf));
	bpf.cmd_type = IO_URING_BPF_CMD_FILTER;
	bpf.filter.opcode = IORING_OP_SOCKET;
	bpf.filter.filter_len = ARRAY_SIZE(allow_all_filter);
	bpf.filter.filter_ptr = (unsigned long) (uintptr_t) allow_all_filter;
	bpf.filter.pdu_size = 24;

	ret = io_uring_register_bpf_filter(&ring, &bpf);
	if (ret != -EMSGSIZE) {
		fprintf(stderr, "pdu too big: expected -EMSGSIZE, got %d\n",
			ret);
		failed++;
	} else if (bpf.filter.pdu_size != 12) {
		fprintf(stderr, "pdu too big: expected writeback 12, got %u\n",
			bpf.filter.pdu_size);
		failed++;
	}

	/* Test 2: pdu_size smaller without strict, should succeed */
	memset(&bpf, 0, sizeof(bpf));
	bpf.cmd_type = IO_URING_BPF_CMD_FILTER;
	bpf.filter.opcode = IORING_OP_SOCKET;
	bpf.filter.filter_len = ARRAY_SIZE(allow_all_filter);
	bpf.filter.filter_ptr = (unsigned long) (uintptr_t) allow_all_filter;
	bpf.filter.pdu_size = 8;

	ret = io_uring_register_bpf_filter(&ring, &bpf);
	if (ret) {
		fprintf(stderr, "pdu smaller no strict: expected success, "
			"got %d\n", ret);
		failed++;
	} else if (bpf.filter.pdu_size != 12) {
		fprintf(stderr, "pdu smaller no strict: expected writeback "
			"12, got %u\n", bpf.filter.pdu_size);
		failed++;
	}

	io_uring_queue_exit(&ring);

	/*
	 * Test 3: pdu_size smaller with strict, should fail.
	 * Use a fresh ring since test 2 registered a filter.
	 */
	ret = io_uring_queue_init(8, &ring, 0);
	if (ret < 0) {
		fprintf(stderr, "queue_init 2 failed: %s\n", strerror(-ret));
		return 1;
	}

	memset(&bpf, 0, sizeof(bpf));
	bpf.cmd_type = IO_URING_BPF_CMD_FILTER;
	bpf.filter.opcode = IORING_OP_SOCKET;
	bpf.filter.flags = IO_URING_BPF_FILTER_SZ_STRICT;
	bpf.filter.filter_len = ARRAY_SIZE(allow_all_filter);
	bpf.filter.filter_ptr = (unsigned long) (uintptr_t) allow_all_filter;
	bpf.filter.pdu_size = 8;

	ret = io_uring_register_bpf_filter(&ring, &bpf);
	if (ret != -EMSGSIZE) {
		fprintf(stderr, "pdu smaller strict: expected -EMSGSIZE, "
			"got %d\n", ret);
		failed++;
	} else if (bpf.filter.pdu_size != 12) {
		fprintf(stderr, "pdu smaller strict: expected writeback 12, "
			"got %u\n", bpf.filter.pdu_size);
		failed++;
	}

	io_uring_queue_exit(&ring);
	return failed;
}

/*
 * Test that child processes inherit parent's restrictions.
 * Parent registers a filter, forks, child verifies the restriction applies.
 */
static int test_inherit_restrictions(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* First child: register deny filter for NOP, then fork */
		ret = register_bpf_filter(deny_all_filter,
					  sizeof(deny_all_filter) / sizeof(deny_all_filter[0]),
					  IORING_OP_NOP, 0, 0);
		if (ret < 0) {
			fprintf(stderr, "Child1: register failed: %s\n",
				strerror(-ret));
			exit(1);
		}

		/* Fork grandchild to test inheritance */
		pid_t grandchild = fork();
		if (grandchild < 0) {
			perror("fork grandchild");
			exit(1);
		}

		if (grandchild == 0) {
			/* Grandchild: should inherit parent's NOP denial */
			ret = io_uring_queue_init(8, &ring, 0);
			if (ret < 0) {
				fprintf(stderr, "Grandchild: queue_init failed\n");
				exit(1);
			}

			/* NOP should be denied due to inherited restriction */
			if (test_nop(&ring, "inherited NOP denial", 0) != 0)
				failed++;

			io_uring_queue_exit(&ring);
			exit(failed);
		}

		waitpid(grandchild, &status, 0);
		exit(WIFEXITED(status) ? WEXITSTATUS(status) : 1);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test that child can add new restrictions on top of inherited ones.
 * Parent allows only AF_INET, child adds TCP-only filter.
 * Result: only AF_INET + TCP should be allowed.
 */
static int test_stack_restrictions(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* First child: register AF_INET only filter */
		ret = register_bpf_filter(allow_inet_only_filter,
					  sizeof(allow_inet_only_filter) / sizeof(allow_inet_only_filter[0]),
					  IORING_OP_SOCKET, 12, 0);
		if (ret < 0) {
			fprintf(stderr, "Child1: register failed: %s\n",
				strerror(-ret));
			exit(1);
		}

		/* Fork grandchild to add more restrictions */
		pid_t grandchild = fork();
		if (grandchild < 0) {
			perror("fork grandchild");
			exit(1);
		}

		if (grandchild == 0) {
			/* Grandchild: add TCP-only filter on top */
			ret = register_bpf_filter(allow_tcp_only_filter,
						  sizeof(allow_tcp_only_filter) / sizeof(allow_tcp_only_filter[0]),
						  IORING_OP_SOCKET, 12, 0);
			if (ret < 0) {
				fprintf(stderr, "Grandchild: register failed: %s\n",
					strerror(-ret));
				exit(1);
			}

			ret = io_uring_queue_init(8, &ring, 0);
			if (ret < 0) {
				fprintf(stderr, "Grandchild: queue_init failed\n");
				exit(1);
			}

			/* AF_INET + TCP: allowed by both filters */
			if (test_socket(&ring, AF_INET, SOCK_STREAM,
					"AF_INET TCP (both filters allow)", 1) != 0)
				failed++;

			/* AF_INET + UDP: allowed by parent, denied by child */
			if (test_socket(&ring, AF_INET, SOCK_DGRAM,
					"AF_INET UDP (child denies)", 0) != 0)
				failed++;

			/* AF_INET6 + TCP: denied by parent, allowed by child */
			if (test_socket(&ring, AF_INET6, SOCK_STREAM,
					"AF_INET6 TCP (parent denies)", 0) != 0)
				failed++;

			io_uring_queue_exit(&ring);
			exit(failed);
		}

		waitpid(grandchild, &status, 0);
		exit(WIFEXITED(status) ? WEXITSTATUS(status) : 1);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test that child cannot loosen parent's restrictions.
 * Parent denies NOP, child tries to allow it - should still be denied.
 */
static int test_cannot_loosen_restrictions(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* First child: deny NOP */
		ret = register_bpf_filter(deny_all_filter,
					  sizeof(deny_all_filter) / sizeof(deny_all_filter[0]),
					  IORING_OP_NOP, 0, 0);
		if (ret < 0) {
			fprintf(stderr, "Child1: register failed: %s\n",
				strerror(-ret));
			exit(1);
		}

		/* Fork grandchild that tries to allow NOP */
		pid_t grandchild = fork();
		if (grandchild < 0) {
			perror("fork grandchild");
			exit(1);
		}

		if (grandchild == 0) {
			/* Grandchild: try to allow NOP (inherits no_new_privs) */
			ret = register_bpf_filter(allow_all_filter,
						  sizeof(allow_all_filter) / sizeof(allow_all_filter[0]),
						  IORING_OP_NOP, 0, 0);
			if (ret < 0) {
				fprintf(stderr, "Grandchild: register failed: %s\n",
					strerror(-ret));
				exit(1);
			}

			ret = io_uring_queue_init(8, &ring, 0);
			if (ret < 0) {
				fprintf(stderr, "Grandchild: queue_init failed\n");
				exit(1);
			}

			/*
			 * NOP should still be denied - child's allow filter
			 * runs first (returns 1), but parent's deny filter
			 * is stacked and runs second (returns 0).
			 */
			if (test_nop(&ring, "NOP still denied (can't loosen)", 0) != 0)
				failed++;

			io_uring_queue_exit(&ring);
			exit(failed);
		}

		waitpid(grandchild, &status, 0);
		exit(WIFEXITED(status) ? WEXITSTATUS(status) : 1);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test multi-level inheritance (parent -> child -> grandchild -> great-grandchild).
 * Each level adds more restrictions.
 */
static int test_multi_level_inherit(void)
{
	struct io_uring ring;
	int ret, failed = 0;
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		/* Level 1: allow only AF_INET for sockets */
		ret = register_bpf_filter(allow_inet_only_filter,
					  sizeof(allow_inet_only_filter) / sizeof(allow_inet_only_filter[0]),
					  IORING_OP_SOCKET, 12, 0);
		if (ret < 0) {
			fprintf(stderr, "Level1: register failed\n");
			exit(1);
		}

		pid_t level2 = fork();
		if (level2 < 0)
			exit(1);

		if (level2 == 0) {
			/* Level 2: add TCP-only restriction */
			ret = register_bpf_filter(allow_tcp_only_filter,
						  sizeof(allow_tcp_only_filter) / sizeof(allow_tcp_only_filter[0]),
						  IORING_OP_SOCKET, 12, 0);
			if (ret < 0) {
				fprintf(stderr, "Level2: register failed\n");
				exit(1);
			}

			pid_t level3 = fork();
			if (level3 < 0)
				exit(1);

			if (level3 == 0) {
				/* Level 3: allow NOP with DENY_REST */
				ret = register_bpf_filter(allow_all_filter,
							  sizeof(allow_all_filter) / sizeof(allow_all_filter[0]),
							  IORING_OP_NOP, 0, 1);
				if (ret < 0) {
					fprintf(stderr, "Level3: register failed\n");
					exit(1);
				}

				ret = io_uring_queue_init(8, &ring, 0);
				if (ret < 0) {
					fprintf(stderr, "Level3: queue_init failed\n");
					exit(1);
				}

				/* NOP: allowed (explicit filter) */
				if (test_nop(&ring, "NOP allowed", 1) != 0)
					failed++;

				/*
				 * Socket still governed by inherited filters.
				 * DENY_REST only denies opcodes with no filter
				 * in the entire chain - ancestors have socket
				 * filters so those still apply.
				 *
				 * AF_INET + TCP: allowed by both ancestors
				 */
				if (test_socket(&ring, AF_INET, SOCK_STREAM,
						"AF_INET TCP (inherited filters)", 1) != 0)
					failed++;

				/* AF_INET + UDP: denied by Level 2's TCP filter */
				if (test_socket(&ring, AF_INET, SOCK_DGRAM,
						"AF_INET UDP (denied by L2)", 0) != 0)
					failed++;

				/* AF_INET6 + TCP: denied by Level 1's AF_INET filter */
				if (test_socket(&ring, AF_INET6, SOCK_STREAM,
						"AF_INET6 TCP (denied by L1)", 0) != 0)
					failed++;

				io_uring_queue_exit(&ring);
				exit(failed);
			}

			waitpid(level3, &status, 0);
			exit(WIFEXITED(status) ? WEXITSTATUS(status) : 1);
		}

		waitpid(level2, &status, 0);
		exit(WIFEXITED(status) ? WEXITSTATUS(status) : 1);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	return 1;
}

/*
 * Test that registering a filter without no_new_privs returns -EACCES.
 * This must be called before prctl(PR_SET_NO_NEW_PRIVS) in main().
 */
static int test_no_new_privs_required(void)
{
	struct io_uring_bpf io_bpf = {
		.cmd_type = IO_URING_BPF_CMD_FILTER,
		.filter = {
			.opcode = IORING_OP_NOP,
			.flags = 0,
			.filter_len = sizeof(allow_all_filter) / sizeof(allow_all_filter[0]),
			.filter_ptr = (unsigned long)allow_all_filter,
		},
	};
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0) {
		perror("fork");
		return 1;
	}

	if (pid == 0) {
		int ret;

		/* Try to register without no_new_privs - should fail with EACCES */
		ret = io_uring_register(-1, IORING_REGISTER_BPF_FILTER,
					&io_bpf, 1);
		if (ret == -EACCES) {
			if (!geteuid())
				exit(1);
			exit(0);  /* Expected */
		} else if (!ret) {
			if (!geteuid())
				exit(0);
			exit(1);
		}
		if (ret == -EINVAL || ret == -ENOSYS)
			exit(2);  /* Not supported */
		fprintf(stderr, "Expected -EACCES, got %d\n", ret);
		exit(1);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status)) {
		int code = WEXITSTATUS(status);
		if (code == 0)
			return 0;  /* Test passed */
		if (code == 2)
			return -1;  /* Not supported, skip */
	}
	fprintf(stderr, "test_no_new_privs_required failed\n");
	return 1;
}

static int probe_bpf_filter_support(void)
{
	struct io_uring_bpf io_bpf = {
		.cmd_type = IO_URING_BPF_CMD_FILTER,
		.filter = {
			.opcode = IORING_OP_NOP,
			.flags = 0,
			.filter_len = sizeof(allow_all_filter) / sizeof(allow_all_filter[0]),
			.filter_ptr = (unsigned long)allow_all_filter,
		},
	};
	pid_t pid;
	int status;

	/* Fork so we don't pollute the main process */
	pid = fork();
	if (pid < 0)
		return -1;

	if (pid == 0) {
		int ret;

		ret = io_uring_register(-1, IORING_REGISTER_BPF_FILTER,
					&io_bpf, 1);
		exit(ret < 0 ? -ret : 0);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status)) {
		int code = WEXITSTATUS(status);
		if (code == EINVAL || code == ENOSYS)
			return -1;  /* Not supported */
		return 0;  /* Supported (or other error we'll catch later) */
	}
	return -1;
}

static int probe_connect_filter_support(void)
{
	struct io_uring_bpf io_bpf = {
		.cmd_type = IO_URING_BPF_CMD_FILTER,
		.filter = {
			.opcode = IORING_OP_CONNECT,
			.flags = 0,
			.filter_len = sizeof(allow_all_filter) / sizeof(allow_all_filter[0]),
			.filter_ptr = (unsigned long)allow_all_filter,
			.pdu_size = CONNECT_PDU_SIZE,
		},
	};
	pid_t pid;
	int status;

	pid = fork();
	if (pid < 0)
		return -1;

	if (pid == 0) {
		int ret;

		ret = io_uring_register(-1, IORING_REGISTER_BPF_FILTER,
					&io_bpf, 1);
		exit(ret < 0 ? -ret : 0);
	}

	waitpid(pid, &status, 0);
	if (WIFEXITED(status)) {
		int code = WEXITSTATUS(status);
		if (code == EMSGSIZE)
			return -1;  /* No populator for IORING_OP_CONNECT */
		return 0;  /* Populator present (or unrelated error we will catch later) */
	}
	return -1;
}

int main(int argc, char *argv[])
{
	int total_failed = 0;
	int ret;

	if (argc > 1)
		return T_EXIT_SKIP;

	if (probe_bpf_filter_support() < 0)
		return T_EXIT_SKIP;

	/*
	 * Test that filter registration fails without no_new_privs.
	 * Must run before we call prctl() below.
	 */
	ret = test_no_new_privs_required();
	if (ret < 0)
		return T_EXIT_SKIP;
	if (ret > 0)
		total_failed++;

	/*
	 * Must set no_new_privs to register BPF filters without CAP_SYS_ADMIN.
	 * This is inherited by all child processes.
	 */
	if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0) {
		perror("prctl");
		return T_EXIT_SKIP;
	}

	/* Task-level filter tests */
	total_failed += test_deny_nop();
	total_failed += test_allow_inet_only();
	total_failed += test_allow_tcp_only();
	total_failed += test_deny_rest();

	/* Task-level openat/openat2 filter tests */
	total_failed += test_deny_openat_creat();
	total_failed += test_deny_openat2_resolve_in_root();

	/* Task-level connect filter tests */
	/*
	 * Probe whether the kernel exposes a filter populator for
	 * IORING_OP_CONNECT.
	 */
	if (probe_connect_filter_support() == 0) {
		total_failed += test_connect_allow_family();
		total_failed += test_connect_deny_family();
		total_failed += test_connect_deny_v4_addr();
		total_failed += test_connect_deny_port();
		total_failed += test_connect_stale_addr_len();
		total_failed += test_connect_allow_v4_addr();
		total_failed += test_connect_deny_v6_addr();
		total_failed += test_connect_allow_v6_addr();
		total_failed += test_connect_allow_port();
		total_failed += test_connect_deny_v4_subnet();
		total_failed += test_connect_allow_v4_subnet();
		total_failed += test_connect_deny_v6_subnet();
		total_failed += test_connect_allow_v6_subnet();
	}

	/* Ring-level filter tests */
	total_failed += test_deny_nop_ring();
	total_failed += test_allow_inet_only_ring();
	total_failed += test_allow_tcp_only_ring();
	total_failed += test_deny_rest_ring();
	total_failed += test_pdu_size_ring();

	/* Per-task inheritance tests */
	total_failed += test_inherit_restrictions();
	total_failed += test_stack_restrictions();
	total_failed += test_cannot_loosen_restrictions();
	total_failed += test_multi_level_inherit();

	return total_failed;
}

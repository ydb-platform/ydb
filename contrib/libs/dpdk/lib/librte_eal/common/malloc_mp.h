/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2018 Intel Corporation
 */

#ifndef MALLOC_MP_H
#define MALLOC_MP_H

#include <stdbool.h>
#include <stdint.h>

#include <rte_common.h>
#include <rte_random.h>
#include <rte_spinlock.h>
#include <rte_tailq.h>

/* forward declarations */
struct malloc_heap;
struct rte_memseg;

/* multiprocess synchronization structures for malloc */
enum malloc_req_type {
	REQ_TYPE_ALLOC,     /**< ask primary to allocate */
	REQ_TYPE_FREE,      /**< ask primary to free */
	REQ_TYPE_SYNC       /**< ask secondary to synchronize its memory map */
};

enum malloc_req_result {
	REQ_RESULT_SUCCESS,
	REQ_RESULT_FAIL
};

struct malloc_req_alloc {
	struct malloc_heap *heap;
	uint64_t page_sz;
	size_t elt_size;
	int socket;
	unsigned int flags;
	size_t align;
	size_t bound;
	bool contig;
};

struct malloc_req_free {
	RTE_STD_C11
	union {
		void *addr;
		uint64_t addr_64;
	};
	uint64_t len;
};

struct malloc_mp_req {
	enum malloc_req_type t;
	RTE_STD_C11
	union {
		struct malloc_req_alloc alloc_req;
		struct malloc_req_free free_req;
	};
	uint64_t id; /**< not to be populated by caller */
	enum malloc_req_result result;
};

int
register_mp_requests(void);

int
request_to_primary(struct malloc_mp_req *req);

/* synchronous memory map sync request */
int
request_sync(void);

/* functions from malloc_heap exposed here */
int
malloc_heap_free_pages(void *aligned_start, size_t aligned_len);

struct malloc_elem *
alloc_pages_on_heap(struct malloc_heap *heap, uint64_t pg_sz, size_t elt_size,
		int socket, unsigned int flags, size_t align, size_t bound,
		bool contig, struct rte_memseg **ms, int n_segs);

void
rollback_expand_heap(struct rte_memseg **ms, int n_segs,
		struct malloc_elem *elem, void *map_addr, size_t map_len);

#endif /* MALLOC_MP_H */

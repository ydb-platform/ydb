/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef MALLOC_ELEM_H_
#define MALLOC_ELEM_H_

#include <stdbool.h>

#define MIN_DATA_SIZE (RTE_CACHE_LINE_SIZE)

/* dummy definition of struct so we can use pointers to it in malloc_elem struct */
struct malloc_heap;

enum elem_state {
	ELEM_FREE = 0,
	ELEM_BUSY,
	ELEM_PAD  /* element is a padding-only header */
};

struct malloc_elem {
	struct malloc_heap *heap;
	struct malloc_elem *volatile prev;
	/**< points to prev elem in memseg */
	struct malloc_elem *volatile next;
	/**< points to next elem in memseg */
	LIST_ENTRY(malloc_elem) free_list;
	/**< list of free elements in heap */
	struct rte_memseg_list *msl;
	volatile enum elem_state state;
	uint32_t pad;
	size_t size;
	struct malloc_elem *orig_elem;
	size_t orig_size;
#ifdef RTE_MALLOC_DEBUG
	uint64_t header_cookie;         /* Cookie marking start of data */
	                                /* trailer cookie at start + size */
#endif
} __rte_cache_aligned;

#ifndef RTE_MALLOC_DEBUG
static const unsigned MALLOC_ELEM_TRAILER_LEN = 0;

/* dummy function - just check if pointer is non-null */
static inline int
malloc_elem_cookies_ok(const struct malloc_elem *elem){ return elem != NULL; }

/* dummy function - no header if malloc_debug is not enabled */
static inline void
set_header(struct malloc_elem *elem __rte_unused){ }

/* dummy function - no trailer if malloc_debug is not enabled */
static inline void
set_trailer(struct malloc_elem *elem __rte_unused){ }


#else
static const unsigned MALLOC_ELEM_TRAILER_LEN = RTE_CACHE_LINE_SIZE;

#define MALLOC_HEADER_COOKIE   0xbadbadbadadd2e55ULL /**< Header cookie. */
#define MALLOC_TRAILER_COOKIE  0xadd2e55badbadbadULL /**< Trailer cookie.*/

/* define macros to make referencing the header and trailer cookies easier */
#define MALLOC_ELEM_TRAILER(elem) (*((uint64_t*)RTE_PTR_ADD(elem, \
		elem->size - MALLOC_ELEM_TRAILER_LEN)))
#define MALLOC_ELEM_HEADER(elem) (elem->header_cookie)

static inline void
set_header(struct malloc_elem *elem)
{
	if (elem != NULL)
		MALLOC_ELEM_HEADER(elem) = MALLOC_HEADER_COOKIE;
}

static inline void
set_trailer(struct malloc_elem *elem)
{
	if (elem != NULL)
		MALLOC_ELEM_TRAILER(elem) = MALLOC_TRAILER_COOKIE;
}

/* check that the header and trailer cookies are set correctly */
static inline int
malloc_elem_cookies_ok(const struct malloc_elem *elem)
{
	return elem != NULL &&
			MALLOC_ELEM_HEADER(elem) == MALLOC_HEADER_COOKIE &&
			MALLOC_ELEM_TRAILER(elem) == MALLOC_TRAILER_COOKIE;
}

#endif

static const unsigned MALLOC_ELEM_HEADER_LEN = sizeof(struct malloc_elem);
#define MALLOC_ELEM_OVERHEAD (MALLOC_ELEM_HEADER_LEN + MALLOC_ELEM_TRAILER_LEN)

/*
 * Given a pointer to the start of a memory block returned by malloc, get
 * the actual malloc_elem header for that block.
 */
static inline struct malloc_elem *
malloc_elem_from_data(const void *data)
{
	if (data == NULL)
		return NULL;

	struct malloc_elem *elem = RTE_PTR_SUB(data, MALLOC_ELEM_HEADER_LEN);
	if (!malloc_elem_cookies_ok(elem))
		return NULL;
	return elem->state != ELEM_PAD ? elem:  RTE_PTR_SUB(elem, elem->pad);
}

/*
 * initialise a malloc_elem header
 */
void
malloc_elem_init(struct malloc_elem *elem,
		struct malloc_heap *heap,
		struct rte_memseg_list *msl,
		size_t size,
		struct malloc_elem *orig_elem,
		size_t orig_size);

void
malloc_elem_insert(struct malloc_elem *elem);

/*
 * return true if the current malloc_elem can hold a block of data
 * of the requested size and with the requested alignment
 */
int
malloc_elem_can_hold(struct malloc_elem *elem, size_t size,
		unsigned int align, size_t bound, bool contig);

/*
 * reserve a block of data in an existing malloc_elem. If the malloc_elem
 * is much larger than the data block requested, we split the element in two.
 */
struct malloc_elem *
malloc_elem_alloc(struct malloc_elem *elem, size_t size,
		unsigned int align, size_t bound, bool contig);

/*
 * free a malloc_elem block by adding it to the free list. If the
 * blocks either immediately before or immediately after newly freed block
 * are also free, the blocks are merged together.
 */
struct malloc_elem *
malloc_elem_free(struct malloc_elem *elem);

struct malloc_elem *
malloc_elem_join_adjacent_free(struct malloc_elem *elem);

/*
 * attempt to resize a malloc_elem by expanding into any free space
 * immediately after it in memory.
 */
int
malloc_elem_resize(struct malloc_elem *elem, size_t size);

void
malloc_elem_hide_region(struct malloc_elem *elem, void *start, size_t len);

void
malloc_elem_free_list_remove(struct malloc_elem *elem);

/*
 * dump contents of malloc elem to a file.
 */
void
malloc_elem_dump(const struct malloc_elem *elem, FILE *f);

/*
 * Given an element size, compute its freelist index.
 */
size_t
malloc_elem_free_list_index(size_t size);

/*
 * Add element to its heap's free list.
 */
void
malloc_elem_free_list_insert(struct malloc_elem *elem);

/*
 * Find biggest IOVA-contiguous zone within an element with specified alignment.
 */
size_t
malloc_elem_find_max_iova_contig(struct malloc_elem *elem, size_t align);

#endif /* MALLOC_ELEM_H_ */

/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright(c) 2016 6WIND S.A.
 */

#ifndef _RTE_MEMPOOL_H_
#define _RTE_MEMPOOL_H_

/**
 * @file
 * RTE Mempool.
 *
 * A memory pool is an allocator of fixed-size object. It is
 * identified by its name, and uses a ring to store free objects. It
 * provides some other optional services, like a per-core object
 * cache, and an alignment helper to ensure that objects are padded
 * to spread them equally on all RAM channels, ranks, and so on.
 *
 * Objects owned by a mempool should never be added in another
 * mempool. When an object is freed using rte_mempool_put() or
 * equivalent, the object data is not modified; the user can save some
 * meta-data in the object data and retrieve them when allocating a
 * new object.
 *
 * Note: the mempool implementation is not preemptible. An lcore must not be
 * interrupted by another task that uses the same mempool (because it uses a
 * ring which is not preemptible). Also, usual mempool functions like
 * rte_mempool_get() or rte_mempool_put() are designed to be called from an EAL
 * thread due to the internal per-lcore cache. Due to the lack of caching,
 * rte_mempool_get() or rte_mempool_put() performance will suffer when called
 * by unregistered non-EAL threads. Instead, unregistered non-EAL threads
 * should call rte_mempool_generic_get() or rte_mempool_generic_put() with a
 * user cache created with rte_mempool_cache_create().
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/queue.h>

#include <rte_config.h>
#include <rte_spinlock.h>
#include <rte_log.h>
#include <rte_debug.h>
#include <rte_lcore.h>
#include <rte_memory.h>
#include <rte_branch_prediction.h>
#include <rte_ring.h>
#include <rte_memcpy.h>
#include <rte_common.h>

#include "rte_mempool_trace_fp.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RTE_MEMPOOL_HEADER_COOKIE1  0xbadbadbadadd2e55ULL /**< Header cookie. */
#define RTE_MEMPOOL_HEADER_COOKIE2  0xf2eef2eedadd2e55ULL /**< Header cookie. */
#define RTE_MEMPOOL_TRAILER_COOKIE  0xadd2e55badbadbadULL /**< Trailer cookie.*/

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
/**
 * A structure that stores the mempool statistics (per-lcore).
 */
struct rte_mempool_debug_stats {
	uint64_t put_bulk;         /**< Number of puts. */
	uint64_t put_objs;         /**< Number of objects successfully put. */
	uint64_t get_success_bulk; /**< Successful allocation number. */
	uint64_t get_success_objs; /**< Objects successfully allocated. */
	uint64_t get_fail_bulk;    /**< Failed allocation number. */
	uint64_t get_fail_objs;    /**< Objects that failed to be allocated. */
	/** Successful allocation number of contiguous blocks. */
	uint64_t get_success_blks;
	/** Failed allocation number of contiguous blocks. */
	uint64_t get_fail_blks;
} __rte_cache_aligned;
#endif

/**
 * A structure that stores a per-core object cache.
 */
struct rte_mempool_cache {
	uint32_t size;	      /**< Size of the cache */
	uint32_t flushthresh; /**< Threshold before we flush excess elements */
	uint32_t len;	      /**< Current cache count */
	/*
	 * Cache is allocated to this size to allow it to overflow in certain
	 * cases to avoid needless emptying of cache.
	 */
	void *objs[RTE_MEMPOOL_CACHE_MAX_SIZE * 3]; /**< Cache objects */
} __rte_cache_aligned;

/**
 * A structure that stores the size of mempool elements.
 */
struct rte_mempool_objsz {
	uint32_t elt_size;     /**< Size of an element. */
	uint32_t header_size;  /**< Size of header (before elt). */
	uint32_t trailer_size; /**< Size of trailer (after elt). */
	uint32_t total_size;
	/**< Total size of an object (header + elt + trailer). */
};

/**< Maximum length of a memory pool's name. */
#define RTE_MEMPOOL_NAMESIZE (RTE_RING_NAMESIZE - \
			      sizeof(RTE_MEMPOOL_MZ_PREFIX) + 1)
#define RTE_MEMPOOL_MZ_PREFIX "MP_"

/* "MP_<name>" */
#define	RTE_MEMPOOL_MZ_FORMAT	RTE_MEMPOOL_MZ_PREFIX "%s"

#define	MEMPOOL_PG_SHIFT_MAX	(sizeof(uintptr_t) * CHAR_BIT - 1)

/** Mempool over one chunk of physically continuous memory */
#define	MEMPOOL_PG_NUM_DEFAULT	1

#ifndef RTE_MEMPOOL_ALIGN
/**
 * Alignment of elements inside mempool.
 */
#define RTE_MEMPOOL_ALIGN	RTE_CACHE_LINE_SIZE
#endif

#define RTE_MEMPOOL_ALIGN_MASK	(RTE_MEMPOOL_ALIGN - 1)

/**
 * Mempool object header structure
 *
 * Each object stored in mempools are prefixed by this header structure,
 * it allows to retrieve the mempool pointer from the object and to
 * iterate on all objects attached to a mempool. When debug is enabled,
 * a cookie is also added in this structure preventing corruptions and
 * double-frees.
 */
struct rte_mempool_objhdr {
	STAILQ_ENTRY(rte_mempool_objhdr) next; /**< Next in list. */
	struct rte_mempool *mp;          /**< The mempool owning the object. */
	rte_iova_t iova;                 /**< IO address of the object. */
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	uint64_t cookie;                 /**< Debug cookie. */
#endif
};

/**
 * A list of object headers type
 */
STAILQ_HEAD(rte_mempool_objhdr_list, rte_mempool_objhdr);

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG

/**
 * Mempool object trailer structure
 *
 * In debug mode, each object stored in mempools are suffixed by this
 * trailer structure containing a cookie preventing memory corruptions.
 */
struct rte_mempool_objtlr {
	uint64_t cookie;                 /**< Debug cookie. */
};

#endif

/**
 * A list of memory where objects are stored
 */
STAILQ_HEAD(rte_mempool_memhdr_list, rte_mempool_memhdr);

/**
 * Callback used to free a memory chunk
 */
typedef void (rte_mempool_memchunk_free_cb_t)(struct rte_mempool_memhdr *memhdr,
	void *opaque);

/**
 * Mempool objects memory header structure
 *
 * The memory chunks where objects are stored. Each chunk is virtually
 * and physically contiguous.
 */
struct rte_mempool_memhdr {
	STAILQ_ENTRY(rte_mempool_memhdr) next; /**< Next in list. */
	struct rte_mempool *mp;  /**< The mempool owning the chunk */
	void *addr;              /**< Virtual address of the chunk */
	rte_iova_t iova;         /**< IO address of the chunk */
	size_t len;              /**< length of the chunk */
	rte_mempool_memchunk_free_cb_t *free_cb; /**< Free callback */
	void *opaque;            /**< Argument passed to the free callback */
};

/**
 * Additional information about the mempool
 *
 * The structure is cache-line aligned to avoid ABI breakages in
 * a number of cases when something small is added.
 */
struct rte_mempool_info {
	/** Number of objects in the contiguous block */
	unsigned int contig_block_size;
} __rte_cache_aligned;

/**
 * The RTE mempool structure.
 */
struct rte_mempool {
	/*
	 * Note: this field kept the RTE_MEMZONE_NAMESIZE size due to ABI
	 * compatibility requirements, it could be changed to
	 * RTE_MEMPOOL_NAMESIZE next time the ABI changes
	 */
	char name[RTE_MEMZONE_NAMESIZE]; /**< Name of mempool. */
	RTE_STD_C11
	union {
		void *pool_data;         /**< Ring or pool to store objects. */
		uint64_t pool_id;        /**< External mempool identifier. */
	};
	void *pool_config;               /**< optional args for ops alloc. */
	const struct rte_memzone *mz;    /**< Memzone where pool is alloc'd. */
	unsigned int flags;              /**< Flags of the mempool. */
	int socket_id;                   /**< Socket id passed at create. */
	uint32_t size;                   /**< Max size of the mempool. */
	uint32_t cache_size;
	/**< Size of per-lcore default local cache. */

	uint32_t elt_size;               /**< Size of an element. */
	uint32_t header_size;            /**< Size of header (before elt). */
	uint32_t trailer_size;           /**< Size of trailer (after elt). */

	unsigned private_data_size;      /**< Size of private data. */
	/**
	 * Index into rte_mempool_ops_table array of mempool ops
	 * structs, which contain callback function pointers.
	 * We're using an index here rather than pointers to the callbacks
	 * to facilitate any secondary processes that may want to use
	 * this mempool.
	 */
	int32_t ops_index;

	struct rte_mempool_cache *local_cache; /**< Per-lcore local cache */

	uint32_t populated_size;         /**< Number of populated objects. */
	struct rte_mempool_objhdr_list elt_list; /**< List of objects in pool */
	uint32_t nb_mem_chunks;          /**< Number of memory chunks */
	struct rte_mempool_memhdr_list mem_list; /**< List of memory chunks */

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	/** Per-lcore statistics. */
	struct rte_mempool_debug_stats stats[RTE_MAX_LCORE];
#endif
}  __rte_cache_aligned;

#define MEMPOOL_F_NO_SPREAD      0x0001
		/**< Spreading among memory channels not required. */
#define MEMPOOL_F_NO_CACHE_ALIGN 0x0002 /**< Do not align objs on cache lines.*/
#define MEMPOOL_F_SP_PUT         0x0004 /**< Default put is "single-producer".*/
#define MEMPOOL_F_SC_GET         0x0008 /**< Default get is "single-consumer".*/
#define MEMPOOL_F_POOL_CREATED   0x0010 /**< Internal: pool is created. */
#define MEMPOOL_F_NO_IOVA_CONTIG 0x0020 /**< Don't need IOVA contiguous objs. */

/**
 * @internal When debug is enabled, store some statistics.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param name
 *   Name of the statistics field to increment in the memory pool.
 * @param n
 *   Number to add to the object-oriented statistics.
 */
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
#define __MEMPOOL_STAT_ADD(mp, name, n) do {                    \
		unsigned __lcore_id = rte_lcore_id();           \
		if (__lcore_id < RTE_MAX_LCORE) {               \
			mp->stats[__lcore_id].name##_objs += n;	\
			mp->stats[__lcore_id].name##_bulk += 1;	\
		}                                               \
	} while(0)
#define __MEMPOOL_CONTIG_BLOCKS_STAT_ADD(mp, name, n) do {                    \
		unsigned int __lcore_id = rte_lcore_id();       \
		if (__lcore_id < RTE_MAX_LCORE) {               \
			mp->stats[__lcore_id].name##_blks += n;	\
			mp->stats[__lcore_id].name##_bulk += 1;	\
		}                                               \
	} while (0)
#else
#define __MEMPOOL_STAT_ADD(mp, name, n) do {} while(0)
#define __MEMPOOL_CONTIG_BLOCKS_STAT_ADD(mp, name, n) do {} while (0)
#endif

/**
 * Calculate the size of the mempool header.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param cs
 *   Size of the per-lcore cache.
 */
#define MEMPOOL_HEADER_SIZE(mp, cs) \
	(sizeof(*(mp)) + (((cs) == 0) ? 0 : \
	(sizeof(struct rte_mempool_cache) * RTE_MAX_LCORE)))

/* return the header of a mempool object (internal) */
static inline struct rte_mempool_objhdr *__mempool_get_header(void *obj)
{
	return (struct rte_mempool_objhdr *)RTE_PTR_SUB(obj,
		sizeof(struct rte_mempool_objhdr));
}

/**
 * Return a pointer to the mempool owning this object.
 *
 * @param obj
 *   An object that is owned by a pool. If this is not the case,
 *   the behavior is undefined.
 * @return
 *   A pointer to the mempool structure.
 */
static inline struct rte_mempool *rte_mempool_from_obj(void *obj)
{
	struct rte_mempool_objhdr *hdr = __mempool_get_header(obj);
	return hdr->mp;
}

/* return the trailer of a mempool object (internal) */
static inline struct rte_mempool_objtlr *__mempool_get_trailer(void *obj)
{
	struct rte_mempool *mp = rte_mempool_from_obj(obj);
	return (struct rte_mempool_objtlr *)RTE_PTR_ADD(obj, mp->elt_size);
}

/**
 * @internal Check and update cookies or panic.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param obj_table_const
 *   Pointer to a table of void * pointers (objects).
 * @param n
 *   Index of object in object table.
 * @param free
 *   - 0: object is supposed to be allocated, mark it as free
 *   - 1: object is supposed to be free, mark it as allocated
 *   - 2: just check that cookie is valid (free or allocated)
 */
void rte_mempool_check_cookies(const struct rte_mempool *mp,
	void * const *obj_table_const, unsigned n, int free);

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
#define __mempool_check_cookies(mp, obj_table_const, n, free) \
	rte_mempool_check_cookies(mp, obj_table_const, n, free)
#else
#define __mempool_check_cookies(mp, obj_table_const, n, free) do {} while(0)
#endif /* RTE_LIBRTE_MEMPOOL_DEBUG */

/**
 * @internal Check contiguous object blocks and update cookies or panic.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param first_obj_table_const
 *   Pointer to a table of void * pointers (first object of the contiguous
 *   object blocks).
 * @param n
 *   Number of contiguous object blocks.
 * @param free
 *   - 0: object is supposed to be allocated, mark it as free
 *   - 1: object is supposed to be free, mark it as allocated
 *   - 2: just check that cookie is valid (free or allocated)
 */
void rte_mempool_contig_blocks_check_cookies(const struct rte_mempool *mp,
	void * const *first_obj_table_const, unsigned int n, int free);

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
#define __mempool_contig_blocks_check_cookies(mp, first_obj_table_const, n, \
					      free) \
	rte_mempool_contig_blocks_check_cookies(mp, first_obj_table_const, n, \
						free)
#else
#define __mempool_contig_blocks_check_cookies(mp, first_obj_table_const, n, \
					      free) \
	do {} while (0)
#endif /* RTE_LIBRTE_MEMPOOL_DEBUG */

#define RTE_MEMPOOL_OPS_NAMESIZE 32 /**< Max length of ops struct name. */

/**
 * Prototype for implementation specific data provisioning function.
 *
 * The function should provide the implementation specific memory for
 * use by the other mempool ops functions in a given mempool ops struct.
 * E.g. the default ops provides an instance of the rte_ring for this purpose.
 * it will most likely point to a different type of data structure, and
 * will be transparent to the application programmer.
 * This function should set mp->pool_data.
 */
typedef int (*rte_mempool_alloc_t)(struct rte_mempool *mp);

/**
 * Free the opaque private data pointed to by mp->pool_data pointer.
 */
typedef void (*rte_mempool_free_t)(struct rte_mempool *mp);

/**
 * Enqueue an object into the external pool.
 */
typedef int (*rte_mempool_enqueue_t)(struct rte_mempool *mp,
		void * const *obj_table, unsigned int n);

/**
 * Dequeue an object from the external pool.
 */
typedef int (*rte_mempool_dequeue_t)(struct rte_mempool *mp,
		void **obj_table, unsigned int n);

/**
 * Dequeue a number of contiguous object blocks from the external pool.
 */
typedef int (*rte_mempool_dequeue_contig_blocks_t)(struct rte_mempool *mp,
		 void **first_obj_table, unsigned int n);

/**
 * Return the number of available objects in the external pool.
 */
typedef unsigned (*rte_mempool_get_count)(const struct rte_mempool *mp);

/**
 * Calculate memory size required to store given number of objects.
 *
 * If mempool objects are not required to be IOVA-contiguous
 * (the flag MEMPOOL_F_NO_IOVA_CONTIG is set), min_chunk_size defines
 * virtually contiguous chunk size. Otherwise, if mempool objects must
 * be IOVA-contiguous (the flag MEMPOOL_F_NO_IOVA_CONTIG is clear),
 * min_chunk_size defines IOVA-contiguous chunk size.
 *
 * @param[in] mp
 *   Pointer to the memory pool.
 * @param[in] obj_num
 *   Number of objects.
 * @param[in] pg_shift
 *   LOG2 of the physical pages size. If set to 0, ignore page boundaries.
 * @param[out] min_chunk_size
 *   Location for minimum size of the memory chunk which may be used to
 *   store memory pool objects.
 * @param[out] align
 *   Location for required memory chunk alignment.
 * @return
 *   Required memory size.
 */
typedef ssize_t (*rte_mempool_calc_mem_size_t)(const struct rte_mempool *mp,
		uint32_t obj_num,  uint32_t pg_shift,
		size_t *min_chunk_size, size_t *align);

/**
 * @internal Helper to calculate memory size required to store given
 * number of objects.
 *
 * This function is internal to mempool library and mempool drivers.
 *
 * If page boundaries may be ignored, it is just a product of total
 * object size including header and trailer and number of objects.
 * Otherwise, it is a number of pages required to store given number of
 * objects without crossing page boundary.
 *
 * Note that if object size is bigger than page size, then it assumes
 * that pages are grouped in subsets of physically continuous pages big
 * enough to store at least one object.
 *
 * Minimum size of memory chunk is the total element size.
 * Required memory chunk alignment is the cache line size.
 *
 * @param[in] mp
 *   A pointer to the mempool structure.
 * @param[in] obj_num
 *   Number of objects to be added in mempool.
 * @param[in] pg_shift
 *   LOG2 of the physical pages size. If set to 0, ignore page boundaries.
 * @param[in] chunk_reserve
 *   Amount of memory that must be reserved at the beginning of each page,
 *   or at the beginning of the memory area if pg_shift is 0.
 * @param[out] min_chunk_size
 *   Location for minimum size of the memory chunk which may be used to
 *   store memory pool objects.
 * @param[out] align
 *   Location for required memory chunk alignment.
 * @return
 *   Required memory size.
 */
ssize_t rte_mempool_op_calc_mem_size_helper(const struct rte_mempool *mp,
		uint32_t obj_num, uint32_t pg_shift, size_t chunk_reserve,
		size_t *min_chunk_size, size_t *align);

/**
 * Default way to calculate memory size required to store given number of
 * objects.
 *
 * Equivalent to rte_mempool_op_calc_mem_size_helper(mp, obj_num, pg_shift,
 * 0, min_chunk_size, align).
 */
ssize_t rte_mempool_op_calc_mem_size_default(const struct rte_mempool *mp,
		uint32_t obj_num, uint32_t pg_shift,
		size_t *min_chunk_size, size_t *align);

/**
 * Function to be called for each populated object.
 *
 * @param[in] mp
 *   A pointer to the mempool structure.
 * @param[in] opaque
 *   An opaque pointer passed to iterator.
 * @param[in] vaddr
 *   Object virtual address.
 * @param[in] iova
 *   Input/output virtual address of the object or RTE_BAD_IOVA.
 */
typedef void (rte_mempool_populate_obj_cb_t)(struct rte_mempool *mp,
		void *opaque, void *vaddr, rte_iova_t iova);

/**
 * Populate memory pool objects using provided memory chunk.
 *
 * Populated objects should be enqueued to the pool, e.g. using
 * rte_mempool_ops_enqueue_bulk().
 *
 * If the given IO address is unknown (iova = RTE_BAD_IOVA),
 * the chunk doesn't need to be physically contiguous (only virtually),
 * and allocated objects may span two pages.
 *
 * @param[in] mp
 *   A pointer to the mempool structure.
 * @param[in] max_objs
 *   Maximum number of objects to be populated.
 * @param[in] vaddr
 *   The virtual address of memory that should be used to store objects.
 * @param[in] iova
 *   The IO address
 * @param[in] len
 *   The length of memory in bytes.
 * @param[in] obj_cb
 *   Callback function to be executed for each populated object.
 * @param[in] obj_cb_arg
 *   An opaque pointer passed to the callback function.
 * @return
 *   The number of objects added on success.
 *   On error, no objects are populated and a negative errno is returned.
 */
typedef int (*rte_mempool_populate_t)(struct rte_mempool *mp,
		unsigned int max_objs,
		void *vaddr, rte_iova_t iova, size_t len,
		rte_mempool_populate_obj_cb_t *obj_cb, void *obj_cb_arg);

/**
 * Align objects on addresses multiple of total_elt_sz.
 */
#define RTE_MEMPOOL_POPULATE_F_ALIGN_OBJ 0x0001

/**
 * @internal Helper to populate memory pool object using provided memory
 * chunk: just slice objects one by one, taking care of not
 * crossing page boundaries.
 *
 * If RTE_MEMPOOL_POPULATE_F_ALIGN_OBJ is set in flags, the addresses
 * of object headers will be aligned on a multiple of total_elt_sz.
 * This feature is used by octeontx hardware.
 *
 * This function is internal to mempool library and mempool drivers.
 *
 * @param[in] mp
 *   A pointer to the mempool structure.
 * @param[in] flags
 *   Logical OR of following flags:
 *   - RTE_MEMPOOL_POPULATE_F_ALIGN_OBJ: align objects on addresses
 *     multiple of total_elt_sz.
 * @param[in] max_objs
 *   Maximum number of objects to be added in mempool.
 * @param[in] vaddr
 *   The virtual address of memory that should be used to store objects.
 * @param[in] iova
 *   The IO address corresponding to vaddr, or RTE_BAD_IOVA.
 * @param[in] len
 *   The length of memory in bytes.
 * @param[in] obj_cb
 *   Callback function to be executed for each populated object.
 * @param[in] obj_cb_arg
 *   An opaque pointer passed to the callback function.
 * @return
 *   The number of objects added in mempool.
 */
int rte_mempool_op_populate_helper(struct rte_mempool *mp,
		unsigned int flags, unsigned int max_objs,
		void *vaddr, rte_iova_t iova, size_t len,
		rte_mempool_populate_obj_cb_t *obj_cb, void *obj_cb_arg);

/**
 * Default way to populate memory pool object using provided memory chunk.
 *
 * Equivalent to rte_mempool_op_populate_helper(mp, 0, max_objs, vaddr, iova,
 * len, obj_cb, obj_cb_arg).
 */
int rte_mempool_op_populate_default(struct rte_mempool *mp,
		unsigned int max_objs,
		void *vaddr, rte_iova_t iova, size_t len,
		rte_mempool_populate_obj_cb_t *obj_cb, void *obj_cb_arg);

/**
 * Get some additional information about a mempool.
 */
typedef int (*rte_mempool_get_info_t)(const struct rte_mempool *mp,
		struct rte_mempool_info *info);


/** Structure defining mempool operations structure */
struct rte_mempool_ops {
	char name[RTE_MEMPOOL_OPS_NAMESIZE]; /**< Name of mempool ops struct. */
	rte_mempool_alloc_t alloc;       /**< Allocate private data. */
	rte_mempool_free_t free;         /**< Free the external pool. */
	rte_mempool_enqueue_t enqueue;   /**< Enqueue an object. */
	rte_mempool_dequeue_t dequeue;   /**< Dequeue an object. */
	rte_mempool_get_count get_count; /**< Get qty of available objs. */
	/**
	 * Optional callback to calculate memory size required to
	 * store specified number of objects.
	 */
	rte_mempool_calc_mem_size_t calc_mem_size;
	/**
	 * Optional callback to populate mempool objects using
	 * provided memory chunk.
	 */
	rte_mempool_populate_t populate;
	/**
	 * Get mempool info
	 */
	rte_mempool_get_info_t get_info;
	/**
	 * Dequeue a number of contiguous object blocks.
	 */
	rte_mempool_dequeue_contig_blocks_t dequeue_contig_blocks;
} __rte_cache_aligned;

#define RTE_MEMPOOL_MAX_OPS_IDX 16  /**< Max registered ops structs */

/**
 * Structure storing the table of registered ops structs, each of which contain
 * the function pointers for the mempool ops functions.
 * Each process has its own storage for this ops struct array so that
 * the mempools can be shared across primary and secondary processes.
 * The indices used to access the array are valid across processes, whereas
 * any function pointers stored directly in the mempool struct would not be.
 * This results in us simply having "ops_index" in the mempool struct.
 */
struct rte_mempool_ops_table {
	rte_spinlock_t sl;     /**< Spinlock for add/delete. */
	uint32_t num_ops;      /**< Number of used ops structs in the table. */
	/**
	 * Storage for all possible ops structs.
	 */
	struct rte_mempool_ops ops[RTE_MEMPOOL_MAX_OPS_IDX];
} __rte_cache_aligned;

/** Array of registered ops structs. */
extern struct rte_mempool_ops_table rte_mempool_ops_table;

/**
 * @internal Get the mempool ops struct from its index.
 *
 * @param ops_index
 *   The index of the ops struct in the ops struct table. It must be a valid
 *   index: (0 <= idx < num_ops).
 * @return
 *   The pointer to the ops struct in the table.
 */
static inline struct rte_mempool_ops *
rte_mempool_get_ops(int ops_index)
{
	RTE_VERIFY((ops_index >= 0) && (ops_index < RTE_MEMPOOL_MAX_OPS_IDX));

	return &rte_mempool_ops_table.ops[ops_index];
}

/**
 * @internal Wrapper for mempool_ops alloc callback.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @return
 *   - 0: Success; successfully allocated mempool pool_data.
 *   - <0: Error; code of alloc function.
 */
int
rte_mempool_ops_alloc(struct rte_mempool *mp);

/**
 * @internal Wrapper for mempool_ops dequeue callback.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param obj_table
 *   Pointer to a table of void * pointers (objects).
 * @param n
 *   Number of objects to get.
 * @return
 *   - 0: Success; got n objects.
 *   - <0: Error; code of dequeue function.
 */
static inline int
rte_mempool_ops_dequeue_bulk(struct rte_mempool *mp,
		void **obj_table, unsigned n)
{
	struct rte_mempool_ops *ops;

	rte_mempool_trace_ops_dequeue_bulk(mp, obj_table, n);
	ops = rte_mempool_get_ops(mp->ops_index);
	return ops->dequeue(mp, obj_table, n);
}

/**
 * @internal Wrapper for mempool_ops dequeue_contig_blocks callback.
 *
 * @param[in] mp
 *   Pointer to the memory pool.
 * @param[out] first_obj_table
 *   Pointer to a table of void * pointers (first objects).
 * @param[in] n
 *   Number of blocks to get.
 * @return
 *   - 0: Success; got n objects.
 *   - <0: Error; code of dequeue function.
 */
static inline int
rte_mempool_ops_dequeue_contig_blocks(struct rte_mempool *mp,
		void **first_obj_table, unsigned int n)
{
	struct rte_mempool_ops *ops;

	ops = rte_mempool_get_ops(mp->ops_index);
	RTE_ASSERT(ops->dequeue_contig_blocks != NULL);
	rte_mempool_trace_ops_dequeue_contig_blocks(mp, first_obj_table, n);
	return ops->dequeue_contig_blocks(mp, first_obj_table, n);
}

/**
 * @internal wrapper for mempool_ops enqueue callback.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param obj_table
 *   Pointer to a table of void * pointers (objects).
 * @param n
 *   Number of objects to put.
 * @return
 *   - 0: Success; n objects supplied.
 *   - <0: Error; code of enqueue function.
 */
static inline int
rte_mempool_ops_enqueue_bulk(struct rte_mempool *mp, void * const *obj_table,
		unsigned n)
{
	struct rte_mempool_ops *ops;

	rte_mempool_trace_ops_enqueue_bulk(mp, obj_table, n);
	ops = rte_mempool_get_ops(mp->ops_index);
	return ops->enqueue(mp, obj_table, n);
}

/**
 * @internal wrapper for mempool_ops get_count callback.
 *
 * @param mp
 *   Pointer to the memory pool.
 * @return
 *   The number of available objects in the external pool.
 */
unsigned
rte_mempool_ops_get_count(const struct rte_mempool *mp);

/**
 * @internal wrapper for mempool_ops calc_mem_size callback.
 * API to calculate size of memory required to store specified number of
 * object.
 *
 * @param[in] mp
 *   Pointer to the memory pool.
 * @param[in] obj_num
 *   Number of objects.
 * @param[in] pg_shift
 *   LOG2 of the physical pages size. If set to 0, ignore page boundaries.
 * @param[out] min_chunk_size
 *   Location for minimum size of the memory chunk which may be used to
 *   store memory pool objects.
 * @param[out] align
 *   Location for required memory chunk alignment.
 * @return
 *   Required memory size aligned at page boundary.
 */
ssize_t rte_mempool_ops_calc_mem_size(const struct rte_mempool *mp,
				      uint32_t obj_num, uint32_t pg_shift,
				      size_t *min_chunk_size, size_t *align);

/**
 * @internal wrapper for mempool_ops populate callback.
 *
 * Populate memory pool objects using provided memory chunk.
 *
 * @param[in] mp
 *   A pointer to the mempool structure.
 * @param[in] max_objs
 *   Maximum number of objects to be populated.
 * @param[in] vaddr
 *   The virtual address of memory that should be used to store objects.
 * @param[in] iova
 *   The IO address
 * @param[in] len
 *   The length of memory in bytes.
 * @param[in] obj_cb
 *   Callback function to be executed for each populated object.
 * @param[in] obj_cb_arg
 *   An opaque pointer passed to the callback function.
 * @return
 *   The number of objects added on success.
 *   On error, no objects are populated and a negative errno is returned.
 */
int rte_mempool_ops_populate(struct rte_mempool *mp, unsigned int max_objs,
			     void *vaddr, rte_iova_t iova, size_t len,
			     rte_mempool_populate_obj_cb_t *obj_cb,
			     void *obj_cb_arg);

/**
 * Wrapper for mempool_ops get_info callback.
 *
 * @param[in] mp
 *   Pointer to the memory pool.
 * @param[out] info
 *   Pointer to the rte_mempool_info structure
 * @return
 *   - 0: Success; The mempool driver supports retrieving supplementary
 *        mempool information
 *   - -ENOTSUP - doesn't support get_info ops (valid case).
 */
int rte_mempool_ops_get_info(const struct rte_mempool *mp,
			 struct rte_mempool_info *info);

/**
 * @internal wrapper for mempool_ops free callback.
 *
 * @param mp
 *   Pointer to the memory pool.
 */
void
rte_mempool_ops_free(struct rte_mempool *mp);

/**
 * Set the ops of a mempool.
 *
 * This can only be done on a mempool that is not populated, i.e. just after
 * a call to rte_mempool_create_empty().
 *
 * @param mp
 *   Pointer to the memory pool.
 * @param name
 *   Name of the ops structure to use for this mempool.
 * @param pool_config
 *   Opaque data that can be passed by the application to the ops functions.
 * @return
 *   - 0: Success; the mempool is now using the requested ops functions.
 *   - -EINVAL - Invalid ops struct name provided.
 *   - -EEXIST - mempool already has an ops struct assigned.
 */
int
rte_mempool_set_ops_byname(struct rte_mempool *mp, const char *name,
		void *pool_config);

/**
 * Register mempool operations.
 *
 * @param ops
 *   Pointer to an ops structure to register.
 * @return
 *   - >=0: Success; return the index of the ops struct in the table.
 *   - -EINVAL - some missing callbacks while registering ops struct.
 *   - -ENOSPC - the maximum number of ops structs has been reached.
 */
int rte_mempool_register_ops(const struct rte_mempool_ops *ops);

/**
 * Macro to statically register the ops of a mempool handler.
 * Note that the rte_mempool_register_ops fails silently here when
 * more than RTE_MEMPOOL_MAX_OPS_IDX is registered.
 */
#define MEMPOOL_REGISTER_OPS(ops)				\
	RTE_INIT(mp_hdlr_init_##ops)				\
	{							\
		rte_mempool_register_ops(&ops);			\
	}

/**
 * An object callback function for mempool.
 *
 * Used by rte_mempool_create() and rte_mempool_obj_iter().
 */
typedef void (rte_mempool_obj_cb_t)(struct rte_mempool *mp,
		void *opaque, void *obj, unsigned obj_idx);
typedef rte_mempool_obj_cb_t rte_mempool_obj_ctor_t; /* compat */

/**
 * A memory callback function for mempool.
 *
 * Used by rte_mempool_mem_iter().
 */
typedef void (rte_mempool_mem_cb_t)(struct rte_mempool *mp,
		void *opaque, struct rte_mempool_memhdr *memhdr,
		unsigned mem_idx);

/**
 * A mempool constructor callback function.
 *
 * Arguments are the mempool and the opaque pointer given by the user in
 * rte_mempool_create().
 */
typedef void (rte_mempool_ctor_t)(struct rte_mempool *, void *);

/**
 * Create a new mempool named *name* in memory.
 *
 * This function uses ``rte_memzone_reserve()`` to allocate memory. The
 * pool contains n elements of elt_size. Its size is set to n.
 *
 * @param name
 *   The name of the mempool.
 * @param n
 *   The number of elements in the mempool. The optimum size (in terms of
 *   memory usage) for a mempool is when n is a power of two minus one:
 *   n = (2^q - 1).
 * @param elt_size
 *   The size of each element.
 * @param cache_size
 *   If cache_size is non-zero, the rte_mempool library will try to
 *   limit the accesses to the common lockless pool, by maintaining a
 *   per-lcore object cache. This argument must be lower or equal to
 *   RTE_MEMPOOL_CACHE_MAX_SIZE and n / 1.5. It is advised to choose
 *   cache_size to have "n modulo cache_size == 0": if this is
 *   not the case, some elements will always stay in the pool and will
 *   never be used. The access to the per-lcore table is of course
 *   faster than the multi-producer/consumer pool. The cache can be
 *   disabled if the cache_size argument is set to 0; it can be useful to
 *   avoid losing objects in cache.
 * @param private_data_size
 *   The size of the private data appended after the mempool
 *   structure. This is useful for storing some private data after the
 *   mempool structure, as is done for rte_mbuf_pool for example.
 * @param mp_init
 *   A function pointer that is called for initialization of the pool,
 *   before object initialization. The user can initialize the private
 *   data in this function if needed. This parameter can be NULL if
 *   not needed.
 * @param mp_init_arg
 *   An opaque pointer to data that can be used in the mempool
 *   constructor function.
 * @param obj_init
 *   A function pointer that is called for each object at
 *   initialization of the pool. The user can set some meta data in
 *   objects if needed. This parameter can be NULL if not needed.
 *   The obj_init() function takes the mempool pointer, the init_arg,
 *   the object pointer and the object number as parameters.
 * @param obj_init_arg
 *   An opaque pointer to data that can be used as an argument for
 *   each call to the object constructor function.
 * @param socket_id
 *   The *socket_id* argument is the socket identifier in the case of
 *   NUMA. The value can be *SOCKET_ID_ANY* if there is no NUMA
 *   constraint for the reserved zone.
 * @param flags
 *   The *flags* arguments is an OR of following flags:
 *   - MEMPOOL_F_NO_SPREAD: By default, objects addresses are spread
 *     between channels in RAM: the pool allocator will add padding
 *     between objects depending on the hardware configuration. See
 *     Memory alignment constraints for details. If this flag is set,
 *     the allocator will just align them to a cache line.
 *   - MEMPOOL_F_NO_CACHE_ALIGN: By default, the returned objects are
 *     cache-aligned. This flag removes this constraint, and no
 *     padding will be present between objects. This flag implies
 *     MEMPOOL_F_NO_SPREAD.
 *   - MEMPOOL_F_SP_PUT: If this flag is set, the default behavior
 *     when using rte_mempool_put() or rte_mempool_put_bulk() is
 *     "single-producer". Otherwise, it is "multi-producers".
 *   - MEMPOOL_F_SC_GET: If this flag is set, the default behavior
 *     when using rte_mempool_get() or rte_mempool_get_bulk() is
 *     "single-consumer". Otherwise, it is "multi-consumers".
 *   - MEMPOOL_F_NO_IOVA_CONTIG: If set, allocated objects won't
 *     necessarily be contiguous in IO memory.
 * @return
 *   The pointer to the new allocated mempool, on success. NULL on error
 *   with rte_errno set appropriately. Possible rte_errno values include:
 *    - E_RTE_NO_CONFIG - function could not get pointer to rte_config structure
 *    - E_RTE_SECONDARY - function was called from a secondary process instance
 *    - EINVAL - cache size provided is too large
 *    - ENOSPC - the maximum number of memzones has already been allocated
 *    - EEXIST - a memzone with the same name already exists
 *    - ENOMEM - no appropriate memory area found in which to create memzone
 */
struct rte_mempool *
rte_mempool_create(const char *name, unsigned n, unsigned elt_size,
		   unsigned cache_size, unsigned private_data_size,
		   rte_mempool_ctor_t *mp_init, void *mp_init_arg,
		   rte_mempool_obj_cb_t *obj_init, void *obj_init_arg,
		   int socket_id, unsigned flags);

/**
 * Create an empty mempool
 *
 * The mempool is allocated and initialized, but it is not populated: no
 * memory is allocated for the mempool elements. The user has to call
 * rte_mempool_populate_*() to add memory chunks to the pool. Once
 * populated, the user may also want to initialize each object with
 * rte_mempool_obj_iter().
 *
 * @param name
 *   The name of the mempool.
 * @param n
 *   The maximum number of elements that can be added in the mempool.
 *   The optimum size (in terms of memory usage) for a mempool is when n
 *   is a power of two minus one: n = (2^q - 1).
 * @param elt_size
 *   The size of each element.
 * @param cache_size
 *   Size of the cache. See rte_mempool_create() for details.
 * @param private_data_size
 *   The size of the private data appended after the mempool
 *   structure. This is useful for storing some private data after the
 *   mempool structure, as is done for rte_mbuf_pool for example.
 * @param socket_id
 *   The *socket_id* argument is the socket identifier in the case of
 *   NUMA. The value can be *SOCKET_ID_ANY* if there is no NUMA
 *   constraint for the reserved zone.
 * @param flags
 *   Flags controlling the behavior of the mempool. See
 *   rte_mempool_create() for details.
 * @return
 *   The pointer to the new allocated mempool, on success. NULL on error
 *   with rte_errno set appropriately. See rte_mempool_create() for details.
 */
struct rte_mempool *
rte_mempool_create_empty(const char *name, unsigned n, unsigned elt_size,
	unsigned cache_size, unsigned private_data_size,
	int socket_id, unsigned flags);
/**
 * Free a mempool
 *
 * Unlink the mempool from global list, free the memory chunks, and all
 * memory referenced by the mempool. The objects must not be used by
 * other cores as they will be freed.
 *
 * @param mp
 *   A pointer to the mempool structure.
 */
void
rte_mempool_free(struct rte_mempool *mp);

/**
 * Add physically contiguous memory for objects in the pool at init
 *
 * Add a virtually and physically contiguous memory chunk in the pool
 * where objects can be instantiated.
 *
 * If the given IO address is unknown (iova = RTE_BAD_IOVA),
 * the chunk doesn't need to be physically contiguous (only virtually),
 * and allocated objects may span two pages.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param vaddr
 *   The virtual address of memory that should be used to store objects.
 * @param iova
 *   The IO address
 * @param len
 *   The length of memory in bytes.
 * @param free_cb
 *   The callback used to free this chunk when destroying the mempool.
 * @param opaque
 *   An opaque argument passed to free_cb.
 * @return
 *   The number of objects added on success (strictly positive).
 *   On error, the chunk is not added in the memory list of the
 *   mempool the following code is returned:
 *     (0): not enough room in chunk for one object.
 *     (-ENOSPC): mempool is already populated.
 *     (-ENOMEM): allocation failure.
 */
int rte_mempool_populate_iova(struct rte_mempool *mp, char *vaddr,
	rte_iova_t iova, size_t len, rte_mempool_memchunk_free_cb_t *free_cb,
	void *opaque);

/**
 * Add virtually contiguous memory for objects in the pool at init
 *
 * Add a virtually contiguous memory chunk in the pool where objects can
 * be instantiated.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param addr
 *   The virtual address of memory that should be used to store objects.
 * @param len
 *   The length of memory in bytes.
 * @param pg_sz
 *   The size of memory pages in this virtual area.
 * @param free_cb
 *   The callback used to free this chunk when destroying the mempool.
 * @param opaque
 *   An opaque argument passed to free_cb.
 * @return
 *   The number of objects added on success (strictly positive).
 *   On error, the chunk is not added in the memory list of the
 *   mempool the following code is returned:
 *     (0): not enough room in chunk for one object.
 *     (-ENOSPC): mempool is already populated.
 *     (-ENOMEM): allocation failure.
 */
int
rte_mempool_populate_virt(struct rte_mempool *mp, char *addr,
	size_t len, size_t pg_sz, rte_mempool_memchunk_free_cb_t *free_cb,
	void *opaque);

/**
 * Add memory for objects in the pool at init
 *
 * This is the default function used by rte_mempool_create() to populate
 * the mempool. It adds memory allocated using rte_memzone_reserve().
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   The number of objects added on success.
 *   On error, the chunk is not added in the memory list of the
 *   mempool and a negative errno is returned.
 */
int rte_mempool_populate_default(struct rte_mempool *mp);

/**
 * Add memory from anonymous mapping for objects in the pool at init
 *
 * This function mmap an anonymous memory zone that is locked in
 * memory to store the objects of the mempool.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   The number of objects added on success.
 *   On error, 0 is returned, rte_errno is set, and the chunk is not added in
 *   the memory list of the mempool.
 */
int rte_mempool_populate_anon(struct rte_mempool *mp);

/**
 * Call a function for each mempool element
 *
 * Iterate across all objects attached to a rte_mempool and call the
 * callback function on it.
 *
 * @param mp
 *   A pointer to an initialized mempool.
 * @param obj_cb
 *   A function pointer that is called for each object.
 * @param obj_cb_arg
 *   An opaque pointer passed to the callback function.
 * @return
 *   Number of objects iterated.
 */
uint32_t rte_mempool_obj_iter(struct rte_mempool *mp,
	rte_mempool_obj_cb_t *obj_cb, void *obj_cb_arg);

/**
 * Call a function for each mempool memory chunk
 *
 * Iterate across all memory chunks attached to a rte_mempool and call
 * the callback function on it.
 *
 * @param mp
 *   A pointer to an initialized mempool.
 * @param mem_cb
 *   A function pointer that is called for each memory chunk.
 * @param mem_cb_arg
 *   An opaque pointer passed to the callback function.
 * @return
 *   Number of memory chunks iterated.
 */
uint32_t rte_mempool_mem_iter(struct rte_mempool *mp,
	rte_mempool_mem_cb_t *mem_cb, void *mem_cb_arg);

/**
 * Dump the status of the mempool to a file.
 *
 * @param f
 *   A pointer to a file for output
 * @param mp
 *   A pointer to the mempool structure.
 */
void rte_mempool_dump(FILE *f, struct rte_mempool *mp);

/**
 * Create a user-owned mempool cache.
 *
 * This can be used by unregistered non-EAL threads to enable caching when they
 * interact with a mempool.
 *
 * @param size
 *   The size of the mempool cache. See rte_mempool_create()'s cache_size
 *   parameter description for more information. The same limits and
 *   considerations apply here too.
 * @param socket_id
 *   The socket identifier in the case of NUMA. The value can be
 *   SOCKET_ID_ANY if there is no NUMA constraint for the reserved zone.
 */
struct rte_mempool_cache *
rte_mempool_cache_create(uint32_t size, int socket_id);

/**
 * Free a user-owned mempool cache.
 *
 * @param cache
 *   A pointer to the mempool cache.
 */
void
rte_mempool_cache_free(struct rte_mempool_cache *cache);

/**
 * Get a pointer to the per-lcore default mempool cache.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param lcore_id
 *   The logical core id.
 * @return
 *   A pointer to the mempool cache or NULL if disabled or unregistered non-EAL
 *   thread.
 */
static __rte_always_inline struct rte_mempool_cache *
rte_mempool_default_cache(struct rte_mempool *mp, unsigned lcore_id)
{
	if (mp->cache_size == 0)
		return NULL;

	if (lcore_id >= RTE_MAX_LCORE)
		return NULL;

	rte_mempool_trace_default_cache(mp, lcore_id,
		&mp->local_cache[lcore_id]);
	return &mp->local_cache[lcore_id];
}

/**
 * Flush a user-owned mempool cache to the specified mempool.
 *
 * @param cache
 *   A pointer to the mempool cache.
 * @param mp
 *   A pointer to the mempool.
 */
static __rte_always_inline void
rte_mempool_cache_flush(struct rte_mempool_cache *cache,
			struct rte_mempool *mp)
{
	if (cache == NULL)
		cache = rte_mempool_default_cache(mp, rte_lcore_id());
	if (cache == NULL || cache->len == 0)
		return;
	rte_mempool_trace_cache_flush(cache, mp);
	rte_mempool_ops_enqueue_bulk(mp, cache->objs, cache->len);
	cache->len = 0;
}

/**
 * @internal Put several objects back in the mempool; used internally.
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to store back in the mempool, must be strictly
 *   positive.
 * @param cache
 *   A pointer to a mempool cache structure. May be NULL if not needed.
 */
static __rte_always_inline void
__mempool_generic_put(struct rte_mempool *mp, void * const *obj_table,
		      unsigned int n, struct rte_mempool_cache *cache)
{
	void **cache_objs;

	/* increment stat now, adding in mempool always success */
	__MEMPOOL_STAT_ADD(mp, put, n);

	/* No cache provided or if put would overflow mem allocated for cache */
	if (unlikely(cache == NULL || n > RTE_MEMPOOL_CACHE_MAX_SIZE))
		goto ring_enqueue;

	cache_objs = &cache->objs[cache->len];

	/*
	 * The cache follows the following algorithm
	 *   1. Add the objects to the cache
	 *   2. Anything greater than the cache min value (if it crosses the
	 *   cache flush threshold) is flushed to the ring.
	 */

	/* Add elements back into the cache */
	rte_memcpy(&cache_objs[0], obj_table, sizeof(void *) * n);

	cache->len += n;

	if (cache->len >= cache->flushthresh) {
		rte_mempool_ops_enqueue_bulk(mp, &cache->objs[cache->size],
				cache->len - cache->size);
		cache->len = cache->size;
	}

	return;

ring_enqueue:

	/* push remaining objects in ring */
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	if (rte_mempool_ops_enqueue_bulk(mp, obj_table, n) < 0)
		rte_panic("cannot put objects in mempool\n");
#else
	rte_mempool_ops_enqueue_bulk(mp, obj_table, n);
#endif
}


/**
 * Put several objects back in the mempool.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the mempool from the obj_table.
 * @param cache
 *   A pointer to a mempool cache structure. May be NULL if not needed.
 */
static __rte_always_inline void
rte_mempool_generic_put(struct rte_mempool *mp, void * const *obj_table,
			unsigned int n, struct rte_mempool_cache *cache)
{
	rte_mempool_trace_generic_put(mp, obj_table, n, cache);
	__mempool_check_cookies(mp, obj_table, n, 0);
	__mempool_generic_put(mp, obj_table, n, cache);
}

/**
 * Put several objects back in the mempool.
 *
 * This function calls the multi-producer or the single-producer
 * version depending on the default behavior that was specified at
 * mempool creation time (see flags).
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the mempool from obj_table.
 */
static __rte_always_inline void
rte_mempool_put_bulk(struct rte_mempool *mp, void * const *obj_table,
		     unsigned int n)
{
	struct rte_mempool_cache *cache;
	cache = rte_mempool_default_cache(mp, rte_lcore_id());
	rte_mempool_trace_put_bulk(mp, obj_table, n, cache);
	rte_mempool_generic_put(mp, obj_table, n, cache);
}

/**
 * Put one object back in the mempool.
 *
 * This function calls the multi-producer or the single-producer
 * version depending on the default behavior that was specified at
 * mempool creation time (see flags).
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj
 *   A pointer to the object to be added.
 */
static __rte_always_inline void
rte_mempool_put(struct rte_mempool *mp, void *obj)
{
	rte_mempool_put_bulk(mp, &obj, 1);
}

/**
 * @internal Get several objects from the mempool; used internally.
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to get, must be strictly positive.
 * @param cache
 *   A pointer to a mempool cache structure. May be NULL if not needed.
 * @return
 *   - >=0: Success; number of objects supplied.
 *   - <0: Error; code of ring dequeue function.
 */
static __rte_always_inline int
__mempool_generic_get(struct rte_mempool *mp, void **obj_table,
		      unsigned int n, struct rte_mempool_cache *cache)
{
	int ret;
	uint32_t index, len;
	void **cache_objs;

	/* No cache provided or cannot be satisfied from cache */
	if (unlikely(cache == NULL || n >= cache->size))
		goto ring_dequeue;

	cache_objs = cache->objs;

	/* Can this be satisfied from the cache? */
	if (cache->len < n) {
		/* No. Backfill the cache first, and then fill from it */
		uint32_t req = n + (cache->size - cache->len);

		/* How many do we require i.e. number to fill the cache + the request */
		ret = rte_mempool_ops_dequeue_bulk(mp,
			&cache->objs[cache->len], req);
		if (unlikely(ret < 0)) {
			/*
			 * In the off chance that we are buffer constrained,
			 * where we are not able to allocate cache + n, go to
			 * the ring directly. If that fails, we are truly out of
			 * buffers.
			 */
			goto ring_dequeue;
		}

		cache->len += req;
	}

	/* Now fill in the response ... */
	for (index = 0, len = cache->len - 1; index < n; ++index, len--, obj_table++)
		*obj_table = cache_objs[len];

	cache->len -= n;

	__MEMPOOL_STAT_ADD(mp, get_success, n);

	return 0;

ring_dequeue:

	/* get remaining objects from ring */
	ret = rte_mempool_ops_dequeue_bulk(mp, obj_table, n);

	if (ret < 0)
		__MEMPOOL_STAT_ADD(mp, get_fail, n);
	else
		__MEMPOOL_STAT_ADD(mp, get_success, n);

	return ret;
}

/**
 * Get several objects from the mempool.
 *
 * If cache is enabled, objects will be retrieved first from cache,
 * subsequently from the common pool. Note that it can return -ENOENT when
 * the local cache and common pool are empty, even if cache from other
 * lcores are full.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to get from mempool to obj_table.
 * @param cache
 *   A pointer to a mempool cache structure. May be NULL if not needed.
 * @return
 *   - 0: Success; objects taken.
 *   - -ENOENT: Not enough entries in the mempool; no object is retrieved.
 */
static __rte_always_inline int
rte_mempool_generic_get(struct rte_mempool *mp, void **obj_table,
			unsigned int n, struct rte_mempool_cache *cache)
{
	int ret;
	ret = __mempool_generic_get(mp, obj_table, n, cache);
	if (ret == 0)
		__mempool_check_cookies(mp, obj_table, n, 1);
	rte_mempool_trace_generic_get(mp, obj_table, n, cache);
	return ret;
}

/**
 * Get several objects from the mempool.
 *
 * This function calls the multi-consumers or the single-consumer
 * version, depending on the default behaviour that was specified at
 * mempool creation time (see flags).
 *
 * If cache is enabled, objects will be retrieved first from cache,
 * subsequently from the common pool. Note that it can return -ENOENT when
 * the local cache and common pool are empty, even if cache from other
 * lcores are full.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to get from the mempool to obj_table.
 * @return
 *   - 0: Success; objects taken
 *   - -ENOENT: Not enough entries in the mempool; no object is retrieved.
 */
static __rte_always_inline int
rte_mempool_get_bulk(struct rte_mempool *mp, void **obj_table, unsigned int n)
{
	struct rte_mempool_cache *cache;
	cache = rte_mempool_default_cache(mp, rte_lcore_id());
	rte_mempool_trace_get_bulk(mp, obj_table, n, cache);
	return rte_mempool_generic_get(mp, obj_table, n, cache);
}

/**
 * Get one object from the mempool.
 *
 * This function calls the multi-consumers or the single-consumer
 * version, depending on the default behavior that was specified at
 * mempool creation (see flags).
 *
 * If cache is enabled, objects will be retrieved first from cache,
 * subsequently from the common pool. Note that it can return -ENOENT when
 * the local cache and common pool are empty, even if cache from other
 * lcores are full.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param obj_p
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success; objects taken.
 *   - -ENOENT: Not enough entries in the mempool; no object is retrieved.
 */
static __rte_always_inline int
rte_mempool_get(struct rte_mempool *mp, void **obj_p)
{
	return rte_mempool_get_bulk(mp, obj_p, 1);
}

/**
 * Get a contiguous blocks of objects from the mempool.
 *
 * If cache is enabled, consider to flush it first, to reuse objects
 * as soon as possible.
 *
 * The application should check that the driver supports the operation
 * by calling rte_mempool_ops_get_info() and checking that `contig_block_size`
 * is not zero.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @param first_obj_table
 *   A pointer to a pointer to the first object in each block.
 * @param n
 *   The number of blocks to get from mempool.
 * @return
 *   - 0: Success; blocks taken.
 *   - -ENOBUFS: Not enough entries in the mempool; no object is retrieved.
 *   - -EOPNOTSUPP: The mempool driver does not support block dequeue
 */
static __rte_always_inline int
rte_mempool_get_contig_blocks(struct rte_mempool *mp,
			      void **first_obj_table, unsigned int n)
{
	int ret;

	ret = rte_mempool_ops_dequeue_contig_blocks(mp, first_obj_table, n);
	if (ret == 0) {
		__MEMPOOL_CONTIG_BLOCKS_STAT_ADD(mp, get_success, n);
		__mempool_contig_blocks_check_cookies(mp, first_obj_table, n,
						      1);
	} else {
		__MEMPOOL_CONTIG_BLOCKS_STAT_ADD(mp, get_fail, n);
	}

	rte_mempool_trace_get_contig_blocks(mp, first_obj_table, n);
	return ret;
}

/**
 * Return the number of entries in the mempool.
 *
 * When cache is enabled, this function has to browse the length of
 * all lcores, so it should not be used in a data path, but only for
 * debug purposes. User-owned mempool caches are not accounted for.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   The number of entries in the mempool.
 */
unsigned int rte_mempool_avail_count(const struct rte_mempool *mp);

/**
 * Return the number of elements which have been allocated from the mempool
 *
 * When cache is enabled, this function has to browse the length of
 * all lcores, so it should not be used in a data path, but only for
 * debug purposes.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   The number of free entries in the mempool.
 */
unsigned int
rte_mempool_in_use_count(const struct rte_mempool *mp);

/**
 * Test if the mempool is full.
 *
 * When cache is enabled, this function has to browse the length of all
 * lcores, so it should not be used in a data path, but only for debug
 * purposes. User-owned mempool caches are not accounted for.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   - 1: The mempool is full.
 *   - 0: The mempool is not full.
 */
static inline int
rte_mempool_full(const struct rte_mempool *mp)
{
	return rte_mempool_avail_count(mp) == mp->size;
}

/**
 * Test if the mempool is empty.
 *
 * When cache is enabled, this function has to browse the length of all
 * lcores, so it should not be used in a data path, but only for debug
 * purposes. User-owned mempool caches are not accounted for.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   - 1: The mempool is empty.
 *   - 0: The mempool is not empty.
 */
static inline int
rte_mempool_empty(const struct rte_mempool *mp)
{
	return rte_mempool_avail_count(mp) == 0;
}

/**
 * Return the IO address of elt, which is an element of the pool mp.
 *
 * @param elt
 *   A pointer (virtual address) to the element of the pool.
 * @return
 *   The IO address of the elt element.
 *   If the mempool was created with MEMPOOL_F_NO_IOVA_CONTIG, the
 *   returned value is RTE_BAD_IOVA.
 */
static inline rte_iova_t
rte_mempool_virt2iova(const void *elt)
{
	const struct rte_mempool_objhdr *hdr;
	hdr = (const struct rte_mempool_objhdr *)RTE_PTR_SUB(elt,
		sizeof(*hdr));
	return hdr->iova;
}

/**
 * Check the consistency of mempool objects.
 *
 * Verify the coherency of fields in the mempool structure. Also check
 * that the cookies of mempool objects (even the ones that are not
 * present in pool) have a correct value. If not, a panic will occur.
 *
 * @param mp
 *   A pointer to the mempool structure.
 */
void rte_mempool_audit(struct rte_mempool *mp);

/**
 * Return a pointer to the private data in an mempool structure.
 *
 * @param mp
 *   A pointer to the mempool structure.
 * @return
 *   A pointer to the private data.
 */
static inline void *rte_mempool_get_priv(struct rte_mempool *mp)
{
	return (char *)mp +
		MEMPOOL_HEADER_SIZE(mp, mp->cache_size);
}

/**
 * Dump the status of all mempools on the console
 *
 * @param f
 *   A pointer to a file for output
 */
void rte_mempool_list_dump(FILE *f);

/**
 * Search a mempool from its name
 *
 * @param name
 *   The name of the mempool.
 * @return
 *   The pointer to the mempool matching the name, or NULL if not found.
 *   NULL on error
 *   with rte_errno set appropriately. Possible rte_errno values include:
 *    - ENOENT - required entry not available to return.
 *
 */
struct rte_mempool *rte_mempool_lookup(const char *name);

/**
 * Get the header, trailer and total size of a mempool element.
 *
 * Given a desired size of the mempool element and mempool flags,
 * calculates header, trailer, body and total sizes of the mempool object.
 *
 * @param elt_size
 *   The size of each element, without header and trailer.
 * @param flags
 *   The flags used for the mempool creation.
 *   Consult rte_mempool_create() for more information about possible values.
 *   The size of each element.
 * @param sz
 *   The calculated detailed size the mempool object. May be NULL.
 * @return
 *   Total size of the mempool object.
 */
uint32_t rte_mempool_calc_obj_size(uint32_t elt_size, uint32_t flags,
	struct rte_mempool_objsz *sz);

/**
 * Walk list of all memory pools
 *
 * @param func
 *   Iterator function
 * @param arg
 *   Argument passed to iterator
 */
void rte_mempool_walk(void (*func)(struct rte_mempool *, void *arg),
		      void *arg);

/**
 * @internal Get page size used for mempool object allocation.
 * This function is internal to mempool library and mempool drivers.
 */
int
rte_mempool_get_page_size(struct rte_mempool *mp, size_t *pg_sz);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_MEMPOOL_H_ */

/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_CORE_H_
#define _RTE_RING_CORE_H_

/**
 * @file
 * This file contains definion of RTE ring structure itself,
 * init flags and some related macros.
 * For majority of DPDK entities, it is not recommended to include
 * this file directly, use include <rte_ring.h> or <rte_ring_elem.h>
 * instead.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/queue.h>
#include <errno.h>
#include <rte_common.h>
#include <rte_config.h>
#include <rte_memory.h>
#include <rte_lcore.h>
#include <rte_atomic.h>
#include <rte_branch_prediction.h>
#include <rte_memzone.h>
#include <rte_pause.h>
#include <rte_debug.h>

#define RTE_TAILQ_RING_NAME "RTE_RING"

/** enqueue/dequeue behavior types */
enum rte_ring_queue_behavior {
	/** Enq/Deq a fixed number of items from a ring */
	RTE_RING_QUEUE_FIXED = 0,
	/** Enq/Deq as many items as possible from ring */
	RTE_RING_QUEUE_VARIABLE
};

#define RTE_RING_MZ_PREFIX "RG_"
/** The maximum length of a ring name. */
#define RTE_RING_NAMESIZE (RTE_MEMZONE_NAMESIZE - \
			   sizeof(RTE_RING_MZ_PREFIX) + 1)

/** prod/cons sync types */
enum rte_ring_sync_type {
	RTE_RING_SYNC_MT,     /**< multi-thread safe (default mode) */
	RTE_RING_SYNC_ST,     /**< single thread only */
#ifdef ALLOW_EXPERIMENTAL_API
	RTE_RING_SYNC_MT_RTS, /**< multi-thread relaxed tail sync */
	RTE_RING_SYNC_MT_HTS, /**< multi-thread head/tail sync */
#endif
};

/**
 * structures to hold a pair of head/tail values and other metadata.
 * Depending on sync_type format of that structure might be different,
 * but offset for *sync_type* and *tail* values should remain the same.
 */
struct rte_ring_headtail {
	volatile uint32_t head;      /**< prod/consumer head. */
	volatile uint32_t tail;      /**< prod/consumer tail. */
	RTE_STD_C11
	union {
		/** sync type of prod/cons */
		enum rte_ring_sync_type sync_type;
		/** deprecated -  True if single prod/cons */
		uint32_t single;
	};
};

union __rte_ring_rts_poscnt {
	/** raw 8B value to read/write *cnt* and *pos* as one atomic op */
	uint64_t raw __rte_aligned(8);
	struct {
		uint32_t cnt; /**< head/tail reference counter */
		uint32_t pos; /**< head/tail position */
	} val;
};

struct rte_ring_rts_headtail {
	volatile union __rte_ring_rts_poscnt tail;
	enum rte_ring_sync_type sync_type;  /**< sync type of prod/cons */
	uint32_t htd_max;   /**< max allowed distance between head/tail */
	volatile union __rte_ring_rts_poscnt head;
};

union __rte_ring_hts_pos {
	/** raw 8B value to read/write *head* and *tail* as one atomic op */
	uint64_t raw __rte_aligned(8);
	struct {
		uint32_t head; /**< head position */
		uint32_t tail; /**< tail position */
	} pos;
};

struct rte_ring_hts_headtail {
	volatile union __rte_ring_hts_pos ht;
	enum rte_ring_sync_type sync_type;  /**< sync type of prod/cons */
};

/**
 * An RTE ring structure.
 *
 * The producer and the consumer have a head and a tail index. The particularity
 * of these index is that they are not between 0 and size(ring). These indexes
 * are between 0 and 2^32, and we mask their value when we access the ring[]
 * field. Thanks to this assumption, we can do subtractions between 2 index
 * values in a modulo-32bit base: that's why the overflow of the indexes is not
 * a problem.
 */
struct rte_ring {
	/*
	 * Note: this field kept the RTE_MEMZONE_NAMESIZE size due to ABI
	 * compatibility requirements, it could be changed to RTE_RING_NAMESIZE
	 * next time the ABI changes
	 */
	char name[RTE_MEMZONE_NAMESIZE] __rte_cache_aligned;
	/**< Name of the ring. */
	int flags;               /**< Flags supplied at creation. */
	const struct rte_memzone *memzone;
			/**< Memzone, if any, containing the rte_ring */
	uint32_t size;           /**< Size of ring. */
	uint32_t mask;           /**< Mask (size-1) of ring. */
	uint32_t capacity;       /**< Usable size of ring */

	char pad0 __rte_cache_aligned; /**< empty cache line */

	/** Ring producer status. */
	RTE_STD_C11
	union {
		struct rte_ring_headtail prod;
		struct rte_ring_hts_headtail hts_prod;
		struct rte_ring_rts_headtail rts_prod;
	}  __rte_cache_aligned;

	char pad1 __rte_cache_aligned; /**< empty cache line */

	/** Ring consumer status. */
	RTE_STD_C11
	union {
		struct rte_ring_headtail cons;
		struct rte_ring_hts_headtail hts_cons;
		struct rte_ring_rts_headtail rts_cons;
	}  __rte_cache_aligned;

	char pad2 __rte_cache_aligned; /**< empty cache line */
};

#define RING_F_SP_ENQ 0x0001 /**< The default enqueue is "single-producer". */
#define RING_F_SC_DEQ 0x0002 /**< The default dequeue is "single-consumer". */
/**
 * Ring is to hold exactly requested number of entries.
 * Without this flag set, the ring size requested must be a power of 2, and the
 * usable space will be that size - 1. With the flag, the requested size will
 * be rounded up to the next power of two, but the usable space will be exactly
 * that requested. Worst case, if a power-of-2 size is requested, half the
 * ring space will be wasted.
 */
#define RING_F_EXACT_SZ 0x0004
#define RTE_RING_SZ_MASK  (0x7fffffffU) /**< Ring size mask */

#define RING_F_MP_RTS_ENQ 0x0008 /**< The default enqueue is "MP RTS". */
#define RING_F_MC_RTS_DEQ 0x0010 /**< The default dequeue is "MC RTS". */

#define RING_F_MP_HTS_ENQ 0x0020 /**< The default enqueue is "MP HTS". */
#define RING_F_MC_HTS_DEQ 0x0040 /**< The default dequeue is "MC HTS". */

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_CORE_H_ */

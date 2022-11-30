#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright(c) 2013 6WIND S.A.
 */

#include <inttypes.h>
#include <string.h>

#include <rte_log.h>
#include <rte_string_fns.h>

#include "eal_internal_cfg.h"
#include "eal_memalloc.h"
#include "eal_memcfg.h"
#include "eal_private.h"

/** @file Functions common to EALs that support dynamic memory allocation. */

int
eal_dynmem_memseg_lists_init(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct memtype {
		uint64_t page_sz;
		int socket_id;
	} *memtypes = NULL;
	int i, hpi_idx, msl_idx, ret = -1; /* fail unless told to succeed */
	struct rte_memseg_list *msl;
	uint64_t max_mem, max_mem_per_type;
	unsigned int max_seglists_per_type;
	unsigned int n_memtypes, cur_type;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* no-huge does not need this at all */
	if (internal_conf->no_hugetlbfs)
		return 0;

	/*
	 * figuring out amount of memory we're going to have is a long and very
	 * involved process. the basic element we're operating with is a memory
	 * type, defined as a combination of NUMA node ID and page size (so that
	 * e.g. 2 sockets with 2 page sizes yield 4 memory types in total).
	 *
	 * deciding amount of memory going towards each memory type is a
	 * balancing act between maximum segments per type, maximum memory per
	 * type, and number of detected NUMA nodes. the goal is to make sure
	 * each memory type gets at least one memseg list.
	 *
	 * the total amount of memory is limited by RTE_MAX_MEM_MB value.
	 *
	 * the total amount of memory per type is limited by either
	 * RTE_MAX_MEM_MB_PER_TYPE, or by RTE_MAX_MEM_MB divided by the number
	 * of detected NUMA nodes. additionally, maximum number of segments per
	 * type is also limited by RTE_MAX_MEMSEG_PER_TYPE. this is because for
	 * smaller page sizes, it can take hundreds of thousands of segments to
	 * reach the above specified per-type memory limits.
	 *
	 * additionally, each type may have multiple memseg lists associated
	 * with it, each limited by either RTE_MAX_MEM_MB_PER_LIST for bigger
	 * page sizes, or RTE_MAX_MEMSEG_PER_LIST segments for smaller ones.
	 *
	 * the number of memseg lists per type is decided based on the above
	 * limits, and also taking number of detected NUMA nodes, to make sure
	 * that we don't run out of memseg lists before we populate all NUMA
	 * nodes with memory.
	 *
	 * we do this in three stages. first, we collect the number of types.
	 * then, we figure out memory constraints and populate the list of
	 * would-be memseg lists. then, we go ahead and allocate the memseg
	 * lists.
	 */

	/* create space for mem types */
	n_memtypes = internal_conf->num_hugepage_sizes * rte_socket_count();
	memtypes = calloc(n_memtypes, sizeof(*memtypes));
	if (memtypes == NULL) {
		RTE_LOG(ERR, EAL, "Cannot allocate space for memory types\n");
		return -1;
	}

	/* populate mem types */
	cur_type = 0;
	for (hpi_idx = 0; hpi_idx < (int) internal_conf->num_hugepage_sizes;
			hpi_idx++) {
		struct hugepage_info *hpi;
		uint64_t hugepage_sz;

		hpi = &internal_conf->hugepage_info[hpi_idx];
		hugepage_sz = hpi->hugepage_sz;

		for (i = 0; i < (int) rte_socket_count(); i++, cur_type++) {
			int socket_id = rte_socket_id_by_idx(i);

#ifndef RTE_EAL_NUMA_AWARE_HUGEPAGES
			/* we can still sort pages by socket in legacy mode */
			if (!internal_conf->legacy_mem && socket_id > 0)
				break;
#endif
			memtypes[cur_type].page_sz = hugepage_sz;
			memtypes[cur_type].socket_id = socket_id;

			RTE_LOG(DEBUG, EAL, "Detected memory type: "
				"socket_id:%u hugepage_sz:%" PRIu64 "\n",
				socket_id, hugepage_sz);
		}
	}
	/* number of memtypes could have been lower due to no NUMA support */
	n_memtypes = cur_type;

	/* set up limits for types */
	max_mem = (uint64_t)RTE_MAX_MEM_MB << 20;
	max_mem_per_type = RTE_MIN((uint64_t)RTE_MAX_MEM_MB_PER_TYPE << 20,
			max_mem / n_memtypes);
	/*
	 * limit maximum number of segment lists per type to ensure there's
	 * space for memseg lists for all NUMA nodes with all page sizes
	 */
	max_seglists_per_type = RTE_MAX_MEMSEG_LISTS / n_memtypes;

	if (max_seglists_per_type == 0) {
		RTE_LOG(ERR, EAL, "Cannot accommodate all memory types, please increase %s\n",
			RTE_STR(RTE_MAX_MEMSEG_LISTS));
		goto out;
	}

	/* go through all mem types and create segment lists */
	msl_idx = 0;
	for (cur_type = 0; cur_type < n_memtypes; cur_type++) {
		unsigned int cur_seglist, n_seglists, n_segs;
		unsigned int max_segs_per_type, max_segs_per_list;
		struct memtype *type = &memtypes[cur_type];
		uint64_t max_mem_per_list, pagesz;
		int socket_id;

		pagesz = type->page_sz;
		socket_id = type->socket_id;

		/*
		 * we need to create segment lists for this type. we must take
		 * into account the following things:
		 *
		 * 1. total amount of memory we can use for this memory type
		 * 2. total amount of memory per memseg list allowed
		 * 3. number of segments needed to fit the amount of memory
		 * 4. number of segments allowed per type
		 * 5. number of segments allowed per memseg list
		 * 6. number of memseg lists we are allowed to take up
		 */

		/* calculate how much segments we will need in total */
		max_segs_per_type = max_mem_per_type / pagesz;
		/* limit number of segments to maximum allowed per type */
		max_segs_per_type = RTE_MIN(max_segs_per_type,
				(unsigned int)RTE_MAX_MEMSEG_PER_TYPE);
		/* limit number of segments to maximum allowed per list */
		max_segs_per_list = RTE_MIN(max_segs_per_type,
				(unsigned int)RTE_MAX_MEMSEG_PER_LIST);

		/* calculate how much memory we can have per segment list */
		max_mem_per_list = RTE_MIN(max_segs_per_list * pagesz,
				(uint64_t)RTE_MAX_MEM_MB_PER_LIST << 20);

		/* calculate how many segments each segment list will have */
		n_segs = RTE_MIN(max_segs_per_list, max_mem_per_list / pagesz);

		/* calculate how many segment lists we can have */
		n_seglists = RTE_MIN(max_segs_per_type / n_segs,
				max_mem_per_type / max_mem_per_list);

		/* limit number of segment lists according to our maximum */
		n_seglists = RTE_MIN(n_seglists, max_seglists_per_type);

		RTE_LOG(DEBUG, EAL, "Creating %i segment lists: "
				"n_segs:%i socket_id:%i hugepage_sz:%" PRIu64 "\n",
			n_seglists, n_segs, socket_id, pagesz);

		/* create all segment lists */
		for (cur_seglist = 0; cur_seglist < n_seglists; cur_seglist++) {
			if (msl_idx >= RTE_MAX_MEMSEG_LISTS) {
				RTE_LOG(ERR, EAL,
					"No more space in memseg lists, please increase %s\n",
					RTE_STR(RTE_MAX_MEMSEG_LISTS));
				goto out;
			}
			msl = &mcfg->memsegs[msl_idx++];

			if (eal_memseg_list_init(msl, pagesz, n_segs,
					socket_id, cur_seglist, true))
				goto out;

			if (eal_memseg_list_alloc(msl, 0)) {
				RTE_LOG(ERR, EAL, "Cannot allocate VA space for memseg list\n");
				goto out;
			}
		}
	}
	/* we're successful */
	ret = 0;
out:
	free(memtypes);
	return ret;
}

static int __rte_unused
hugepage_count_walk(const struct rte_memseg_list *msl, void *arg)
{
	struct hugepage_info *hpi = arg;

	if (msl->page_sz != hpi->hugepage_sz)
		return 0;

	hpi->num_pages[msl->socket_id] += msl->memseg_arr.len;
	return 0;
}

static int
limits_callback(int socket_id, size_t cur_limit, size_t new_len)
{
	RTE_SET_USED(socket_id);
	RTE_SET_USED(cur_limit);
	RTE_SET_USED(new_len);
	return -1;
}

int
eal_dynmem_hugepage_init(void)
{
	struct hugepage_info used_hp[MAX_HUGEPAGE_SIZES];
	uint64_t memory[RTE_MAX_NUMA_NODES];
	int hp_sz_idx, socket_id;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	memset(used_hp, 0, sizeof(used_hp));

	for (hp_sz_idx = 0;
			hp_sz_idx < (int) internal_conf->num_hugepage_sizes;
			hp_sz_idx++) {
#ifndef RTE_ARCH_64
		struct hugepage_info dummy;
		unsigned int i;
#endif
		/* also initialize used_hp hugepage sizes in used_hp */
		struct hugepage_info *hpi;
		hpi = &internal_conf->hugepage_info[hp_sz_idx];
		used_hp[hp_sz_idx].hugepage_sz = hpi->hugepage_sz;

#ifndef RTE_ARCH_64
		/* for 32-bit, limit number of pages on socket to whatever we've
		 * preallocated, as we cannot allocate more.
		 */
		memset(&dummy, 0, sizeof(dummy));
		dummy.hugepage_sz = hpi->hugepage_sz;
		if (rte_memseg_list_walk(hugepage_count_walk, &dummy) < 0)
			return -1;

		for (i = 0; i < RTE_DIM(dummy.num_pages); i++) {
			hpi->num_pages[i] = RTE_MIN(hpi->num_pages[i],
					dummy.num_pages[i]);
		}
#endif
	}

	/* make a copy of socket_mem, needed for balanced allocation. */
	for (hp_sz_idx = 0; hp_sz_idx < RTE_MAX_NUMA_NODES; hp_sz_idx++)
		memory[hp_sz_idx] = internal_conf->socket_mem[hp_sz_idx];

	/* calculate final number of pages */
	if (eal_dynmem_calc_num_pages_per_socket(memory,
			internal_conf->hugepage_info, used_hp,
			internal_conf->num_hugepage_sizes) < 0)
		return -1;

	for (hp_sz_idx = 0;
			hp_sz_idx < (int)internal_conf->num_hugepage_sizes;
			hp_sz_idx++) {
		for (socket_id = 0; socket_id < RTE_MAX_NUMA_NODES;
				socket_id++) {
			struct rte_memseg **pages;
			struct hugepage_info *hpi = &used_hp[hp_sz_idx];
			unsigned int num_pages = hpi->num_pages[socket_id];
			unsigned int num_pages_alloc;

			if (num_pages == 0)
				continue;

			RTE_LOG(DEBUG, EAL,
				"Allocating %u pages of size %" PRIu64 "M "
				"on socket %i\n",
				num_pages, hpi->hugepage_sz >> 20, socket_id);

			/* we may not be able to allocate all pages in one go,
			 * because we break up our memory map into multiple
			 * memseg lists. therefore, try allocating multiple
			 * times and see if we can get the desired number of
			 * pages from multiple allocations.
			 */

			num_pages_alloc = 0;
			do {
				int i, cur_pages, needed;

				needed = num_pages - num_pages_alloc;

				pages = malloc(sizeof(*pages) * needed);

				/* do not request exact number of pages */
				cur_pages = eal_memalloc_alloc_seg_bulk(pages,
						needed, hpi->hugepage_sz,
						socket_id, false);
				if (cur_pages <= 0) {
					free(pages);
					return -1;
				}

				/* mark preallocated pages as unfreeable */
				for (i = 0; i < cur_pages; i++) {
					struct rte_memseg *ms = pages[i];
					ms->flags |=
						RTE_MEMSEG_FLAG_DO_NOT_FREE;
				}
				free(pages);

				num_pages_alloc += cur_pages;
			} while (num_pages_alloc != num_pages);
		}
	}

	/* if socket limits were specified, set them */
	if (internal_conf->force_socket_limits) {
		unsigned int i;
		for (i = 0; i < RTE_MAX_NUMA_NODES; i++) {
			uint64_t limit = internal_conf->socket_limit[i];
			if (limit == 0)
				continue;
			if (rte_mem_alloc_validator_register("socket-limit",
					limits_callback, i, limit))
				RTE_LOG(ERR, EAL, "Failed to register socket limits validator callback\n");
		}
	}
	return 0;
}

__rte_unused /* function is unused on 32-bit builds */
static inline uint64_t
get_socket_mem_size(int socket)
{
	uint64_t size = 0;
	unsigned int i;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	for (i = 0; i < internal_conf->num_hugepage_sizes; i++) {
		struct hugepage_info *hpi = &internal_conf->hugepage_info[i];
		size += hpi->hugepage_sz * hpi->num_pages[socket];
	}

	return size;
}

int
eal_dynmem_calc_num_pages_per_socket(
	uint64_t *memory, struct hugepage_info *hp_info,
	struct hugepage_info *hp_used, unsigned int num_hp_info)
{
	unsigned int socket, j, i = 0;
	unsigned int requested, available;
	int total_num_pages = 0;
	uint64_t remaining_mem, cur_mem;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();
	uint64_t total_mem = internal_conf->memory;

	if (num_hp_info == 0)
		return -1;

	/* if specific memory amounts per socket weren't requested */
	if (internal_conf->force_sockets == 0) {
		size_t total_size;
#ifdef RTE_ARCH_64
		int cpu_per_socket[RTE_MAX_NUMA_NODES];
		size_t default_size;
		unsigned int lcore_id;

		/* Compute number of cores per socket */
		memset(cpu_per_socket, 0, sizeof(cpu_per_socket));
		RTE_LCORE_FOREACH(lcore_id) {
			cpu_per_socket[rte_lcore_to_socket_id(lcore_id)]++;
		}

		/*
		 * Automatically spread requested memory amongst detected
		 * sockets according to number of cores from CPU mask present
		 * on each socket.
		 */
		total_size = internal_conf->memory;
		for (socket = 0; socket < RTE_MAX_NUMA_NODES && total_size != 0;
				socket++) {

			/* Set memory amount per socket */
			default_size = internal_conf->memory *
				cpu_per_socket[socket] / rte_lcore_count();

			/* Limit to maximum available memory on socket */
			default_size = RTE_MIN(
				default_size, get_socket_mem_size(socket));

			/* Update sizes */
			memory[socket] = default_size;
			total_size -= default_size;
		}

		/*
		 * If some memory is remaining, try to allocate it by getting
		 * all available memory from sockets, one after the other.
		 */
		for (socket = 0; socket < RTE_MAX_NUMA_NODES && total_size != 0;
				socket++) {
			/* take whatever is available */
			default_size = RTE_MIN(
				get_socket_mem_size(socket) - memory[socket],
				total_size);

			/* Update sizes */
			memory[socket] += default_size;
			total_size -= default_size;
		}
#else
		/* in 32-bit mode, allocate all of the memory only on main
		 * lcore socket
		 */
		total_size = internal_conf->memory;
		for (socket = 0; socket < RTE_MAX_NUMA_NODES && total_size != 0;
				socket++) {
			struct rte_config *cfg = rte_eal_get_configuration();
			unsigned int main_lcore_socket;

			main_lcore_socket =
				rte_lcore_to_socket_id(cfg->main_lcore);

			if (main_lcore_socket != socket)
				continue;

			/* Update sizes */
			memory[socket] = total_size;
			break;
		}
#endif
	}

	for (socket = 0; socket < RTE_MAX_NUMA_NODES && total_mem != 0;
			socket++) {
		/* skips if the memory on specific socket wasn't requested */
		for (i = 0; i < num_hp_info && memory[socket] != 0; i++) {
			rte_strscpy(hp_used[i].hugedir, hp_info[i].hugedir,
				sizeof(hp_used[i].hugedir));
			hp_used[i].num_pages[socket] = RTE_MIN(
					memory[socket] / hp_info[i].hugepage_sz,
					hp_info[i].num_pages[socket]);

			cur_mem = hp_used[i].num_pages[socket] *
					hp_used[i].hugepage_sz;

			memory[socket] -= cur_mem;
			total_mem -= cur_mem;

			total_num_pages += hp_used[i].num_pages[socket];

			/* check if we have met all memory requests */
			if (memory[socket] == 0)
				break;

			/* Check if we have any more pages left at this size,
			 * if so, move on to next size.
			 */
			if (hp_used[i].num_pages[socket] ==
					hp_info[i].num_pages[socket])
				continue;
			/* At this point we know that there are more pages
			 * available that are bigger than the memory we want,
			 * so lets see if we can get enough from other page
			 * sizes.
			 */
			remaining_mem = 0;
			for (j = i+1; j < num_hp_info; j++)
				remaining_mem += hp_info[j].hugepage_sz *
				hp_info[j].num_pages[socket];

			/* Is there enough other memory?
			 * If not, allocate another page and quit.
			 */
			if (remaining_mem < memory[socket]) {
				cur_mem = RTE_MIN(
					memory[socket], hp_info[i].hugepage_sz);
				memory[socket] -= cur_mem;
				total_mem -= cur_mem;
				hp_used[i].num_pages[socket]++;
				total_num_pages++;
				break; /* we are done with this socket*/
			}
		}

		/* if we didn't satisfy all memory requirements per socket */
		if (memory[socket] > 0 &&
				internal_conf->socket_mem[socket] != 0) {
			/* to prevent icc errors */
			requested = (unsigned int)(
				internal_conf->socket_mem[socket] / 0x100000);
			available = requested -
				((unsigned int)(memory[socket] / 0x100000));
			RTE_LOG(ERR, EAL, "Not enough memory available on "
				"socket %u! Requested: %uMB, available: %uMB\n",
				socket, requested, available);
			return -1;
		}
	}

	/* if we didn't satisfy total memory requirements */
	if (total_mem > 0) {
		requested = (unsigned int)(internal_conf->memory / 0x100000);
		available = requested - (unsigned int)(total_mem / 0x100000);
		RTE_LOG(ERR, EAL, "Not enough memory available! "
			"Requested: %uMB, available: %uMB\n",
			requested, available);
		return -1;
	}
	return total_num_pages;
}

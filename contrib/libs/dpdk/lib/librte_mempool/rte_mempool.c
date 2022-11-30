#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation.
 * Copyright(c) 2016 6WIND S.A.
 */

#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/queue.h>

#include <rte_common.h>
#include <rte_log.h>
#include <rte_debug.h>
#include <rte_memory.h>
#include <rte_memzone.h>
#include <rte_malloc.h>
#include <rte_atomic.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_eal_memconfig.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_errno.h>
#include <rte_string_fns.h>
#include <rte_spinlock.h>
#include <rte_tailq.h>
#include <rte_eal_paging.h>

#include "rte_mempool.h"
#include "rte_mempool_trace.h"

TAILQ_HEAD(rte_mempool_list, rte_tailq_entry);

static struct rte_tailq_elem rte_mempool_tailq = {
	.name = "RTE_MEMPOOL",
};
EAL_REGISTER_TAILQ(rte_mempool_tailq)

#define CACHE_FLUSHTHRESH_MULTIPLIER 1.5
#define CALC_CACHE_FLUSHTHRESH(c)	\
	((typeof(c))((c) * CACHE_FLUSHTHRESH_MULTIPLIER))

#if defined(RTE_ARCH_X86)
/*
 * return the greatest common divisor between a and b (fast algorithm)
 *
 */
static unsigned get_gcd(unsigned a, unsigned b)
{
	unsigned c;

	if (0 == a)
		return b;
	if (0 == b)
		return a;

	if (a < b) {
		c = a;
		a = b;
		b = c;
	}

	while (b != 0) {
		c = a % b;
		a = b;
		b = c;
	}

	return a;
}

/*
 * Depending on memory configuration on x86 arch, objects addresses are spread
 * between channels and ranks in RAM: the pool allocator will add
 * padding between objects. This function return the new size of the
 * object.
 */
static unsigned int
arch_mem_object_align(unsigned int obj_size)
{
	unsigned nrank, nchan;
	unsigned new_obj_size;

	/* get number of channels */
	nchan = rte_memory_get_nchannel();
	if (nchan == 0)
		nchan = 4;

	nrank = rte_memory_get_nrank();
	if (nrank == 0)
		nrank = 1;

	/* process new object size */
	new_obj_size = (obj_size + RTE_MEMPOOL_ALIGN_MASK) / RTE_MEMPOOL_ALIGN;
	while (get_gcd(new_obj_size, nrank * nchan) != 1)
		new_obj_size++;
	return new_obj_size * RTE_MEMPOOL_ALIGN;
}
#else
static unsigned int
arch_mem_object_align(unsigned int obj_size)
{
	return obj_size;
}
#endif

struct pagesz_walk_arg {
	int socket_id;
	size_t min;
};

static int
find_min_pagesz(const struct rte_memseg_list *msl, void *arg)
{
	struct pagesz_walk_arg *wa = arg;
	bool valid;

	/*
	 * we need to only look at page sizes available for a particular socket
	 * ID.  so, we either need an exact match on socket ID (can match both
	 * native and external memory), or, if SOCKET_ID_ANY was specified as a
	 * socket ID argument, we must only look at native memory and ignore any
	 * page sizes associated with external memory.
	 */
	valid = msl->socket_id == wa->socket_id;
	valid |= wa->socket_id == SOCKET_ID_ANY && msl->external == 0;

	if (valid && msl->page_sz < wa->min)
		wa->min = msl->page_sz;

	return 0;
}

static size_t
get_min_page_size(int socket_id)
{
	struct pagesz_walk_arg wa;

	wa.min = SIZE_MAX;
	wa.socket_id = socket_id;

	rte_memseg_list_walk(find_min_pagesz, &wa);

	return wa.min == SIZE_MAX ? (size_t) rte_mem_page_size() : wa.min;
}


static void
mempool_add_elem(struct rte_mempool *mp, __rte_unused void *opaque,
		 void *obj, rte_iova_t iova)
{
	struct rte_mempool_objhdr *hdr;
	struct rte_mempool_objtlr *tlr __rte_unused;

	/* set mempool ptr in header */
	hdr = RTE_PTR_SUB(obj, sizeof(*hdr));
	hdr->mp = mp;
	hdr->iova = iova;
	STAILQ_INSERT_TAIL(&mp->elt_list, hdr, next);
	mp->populated_size++;

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	hdr->cookie = RTE_MEMPOOL_HEADER_COOKIE2;
	tlr = __mempool_get_trailer(obj);
	tlr->cookie = RTE_MEMPOOL_TRAILER_COOKIE;
#endif
}

/* call obj_cb() for each mempool element */
uint32_t
rte_mempool_obj_iter(struct rte_mempool *mp,
	rte_mempool_obj_cb_t *obj_cb, void *obj_cb_arg)
{
	struct rte_mempool_objhdr *hdr;
	void *obj;
	unsigned n = 0;

	STAILQ_FOREACH(hdr, &mp->elt_list, next) {
		obj = (char *)hdr + sizeof(*hdr);
		obj_cb(mp, obj_cb_arg, obj, n);
		n++;
	}

	return n;
}

/* call mem_cb() for each mempool memory chunk */
uint32_t
rte_mempool_mem_iter(struct rte_mempool *mp,
	rte_mempool_mem_cb_t *mem_cb, void *mem_cb_arg)
{
	struct rte_mempool_memhdr *hdr;
	unsigned n = 0;

	STAILQ_FOREACH(hdr, &mp->mem_list, next) {
		mem_cb(mp, mem_cb_arg, hdr, n);
		n++;
	}

	return n;
}

/* get the header, trailer and total size of a mempool element. */
uint32_t
rte_mempool_calc_obj_size(uint32_t elt_size, uint32_t flags,
	struct rte_mempool_objsz *sz)
{
	struct rte_mempool_objsz lsz;

	sz = (sz != NULL) ? sz : &lsz;

	sz->header_size = sizeof(struct rte_mempool_objhdr);
	if ((flags & MEMPOOL_F_NO_CACHE_ALIGN) == 0)
		sz->header_size = RTE_ALIGN_CEIL(sz->header_size,
			RTE_MEMPOOL_ALIGN);

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	sz->trailer_size = sizeof(struct rte_mempool_objtlr);
#else
	sz->trailer_size = 0;
#endif

	/* element size is 8 bytes-aligned at least */
	sz->elt_size = RTE_ALIGN_CEIL(elt_size, sizeof(uint64_t));

	/* expand trailer to next cache line */
	if ((flags & MEMPOOL_F_NO_CACHE_ALIGN) == 0) {
		sz->total_size = sz->header_size + sz->elt_size +
			sz->trailer_size;
		sz->trailer_size += ((RTE_MEMPOOL_ALIGN -
				  (sz->total_size & RTE_MEMPOOL_ALIGN_MASK)) &
				 RTE_MEMPOOL_ALIGN_MASK);
	}

	/*
	 * increase trailer to add padding between objects in order to
	 * spread them across memory channels/ranks
	 */
	if ((flags & MEMPOOL_F_NO_SPREAD) == 0) {
		unsigned new_size;
		new_size = arch_mem_object_align
			    (sz->header_size + sz->elt_size + sz->trailer_size);
		sz->trailer_size = new_size - sz->header_size - sz->elt_size;
	}

	/* this is the size of an object, including header and trailer */
	sz->total_size = sz->header_size + sz->elt_size + sz->trailer_size;

	return sz->total_size;
}

/* free a memchunk allocated with rte_memzone_reserve() */
static void
rte_mempool_memchunk_mz_free(__rte_unused struct rte_mempool_memhdr *memhdr,
	void *opaque)
{
	const struct rte_memzone *mz = opaque;
	rte_memzone_free(mz);
}

/* Free memory chunks used by a mempool. Objects must be in pool */
static void
rte_mempool_free_memchunks(struct rte_mempool *mp)
{
	struct rte_mempool_memhdr *memhdr;
	void *elt;

	while (!STAILQ_EMPTY(&mp->elt_list)) {
		rte_mempool_ops_dequeue_bulk(mp, &elt, 1);
		(void)elt;
		STAILQ_REMOVE_HEAD(&mp->elt_list, next);
		mp->populated_size--;
	}

	while (!STAILQ_EMPTY(&mp->mem_list)) {
		memhdr = STAILQ_FIRST(&mp->mem_list);
		STAILQ_REMOVE_HEAD(&mp->mem_list, next);
		if (memhdr->free_cb != NULL)
			memhdr->free_cb(memhdr, memhdr->opaque);
		rte_free(memhdr);
		mp->nb_mem_chunks--;
	}
}

static int
mempool_ops_alloc_once(struct rte_mempool *mp)
{
	int ret;

	/* create the internal ring if not already done */
	if ((mp->flags & MEMPOOL_F_POOL_CREATED) == 0) {
		ret = rte_mempool_ops_alloc(mp);
		if (ret != 0)
			return ret;
		mp->flags |= MEMPOOL_F_POOL_CREATED;
	}
	return 0;
}

/* Add objects in the pool, using a physically contiguous memory
 * zone. Return the number of objects added, or a negative value
 * on error.
 */
int
rte_mempool_populate_iova(struct rte_mempool *mp, char *vaddr,
	rte_iova_t iova, size_t len, rte_mempool_memchunk_free_cb_t *free_cb,
	void *opaque)
{
	unsigned i = 0;
	size_t off;
	struct rte_mempool_memhdr *memhdr;
	int ret;

	ret = mempool_ops_alloc_once(mp);
	if (ret != 0)
		return ret;

	/* mempool is already populated */
	if (mp->populated_size >= mp->size)
		return -ENOSPC;

	memhdr = rte_zmalloc("MEMPOOL_MEMHDR", sizeof(*memhdr), 0);
	if (memhdr == NULL)
		return -ENOMEM;

	memhdr->mp = mp;
	memhdr->addr = vaddr;
	memhdr->iova = iova;
	memhdr->len = len;
	memhdr->free_cb = free_cb;
	memhdr->opaque = opaque;

	if (mp->flags & MEMPOOL_F_NO_CACHE_ALIGN)
		off = RTE_PTR_ALIGN_CEIL(vaddr, 8) - vaddr;
	else
		off = RTE_PTR_ALIGN_CEIL(vaddr, RTE_MEMPOOL_ALIGN) - vaddr;

	if (off > len) {
		ret = 0;
		goto fail;
	}

	i = rte_mempool_ops_populate(mp, mp->size - mp->populated_size,
		(char *)vaddr + off,
		(iova == RTE_BAD_IOVA) ? RTE_BAD_IOVA : (iova + off),
		len - off, mempool_add_elem, NULL);

	/* not enough room to store one object */
	if (i == 0) {
		ret = 0;
		goto fail;
	}

	STAILQ_INSERT_TAIL(&mp->mem_list, memhdr, next);
	mp->nb_mem_chunks++;

	rte_mempool_trace_populate_iova(mp, vaddr, iova, len, free_cb, opaque);
	return i;

fail:
	rte_free(memhdr);
	return ret;
}

static rte_iova_t
get_iova(void *addr)
{
	struct rte_memseg *ms;

	/* try registered memory first */
	ms = rte_mem_virt2memseg(addr, NULL);
	if (ms == NULL || ms->iova == RTE_BAD_IOVA)
		/* fall back to actual physical address */
		return rte_mem_virt2iova(addr);
	return ms->iova + RTE_PTR_DIFF(addr, ms->addr);
}

/* Populate the mempool with a virtual area. Return the number of
 * objects added, or a negative value on error.
 */
int
rte_mempool_populate_virt(struct rte_mempool *mp, char *addr,
	size_t len, size_t pg_sz, rte_mempool_memchunk_free_cb_t *free_cb,
	void *opaque)
{
	rte_iova_t iova;
	size_t off, phys_len;
	int ret, cnt = 0;

	if (mp->flags & MEMPOOL_F_NO_IOVA_CONTIG)
		return rte_mempool_populate_iova(mp, addr, RTE_BAD_IOVA,
			len, free_cb, opaque);

	for (off = 0; off < len &&
		     mp->populated_size < mp->size; off += phys_len) {

		iova = get_iova(addr + off);

		/* populate with the largest group of contiguous pages */
		for (phys_len = RTE_MIN(
			(size_t)(RTE_PTR_ALIGN_CEIL(addr + off + 1, pg_sz) -
				(addr + off)),
			len - off);
		     off + phys_len < len;
		     phys_len = RTE_MIN(phys_len + pg_sz, len - off)) {
			rte_iova_t iova_tmp;

			iova_tmp = get_iova(addr + off + phys_len);

			if (iova_tmp == RTE_BAD_IOVA ||
					iova_tmp != iova + phys_len)
				break;
		}

		ret = rte_mempool_populate_iova(mp, addr + off, iova,
			phys_len, free_cb, opaque);
		if (ret == 0)
			continue;
		if (ret < 0)
			goto fail;
		/* no need to call the free callback for next chunks */
		free_cb = NULL;
		cnt += ret;
	}

	rte_mempool_trace_populate_virt(mp, addr, len, pg_sz, free_cb, opaque);
	return cnt;

 fail:
	rte_mempool_free_memchunks(mp);
	return ret;
}

/* Get the minimal page size used in a mempool before populating it. */
int
rte_mempool_get_page_size(struct rte_mempool *mp, size_t *pg_sz)
{
	bool need_iova_contig_obj;
	bool alloc_in_ext_mem;
	int ret;

	/* check if we can retrieve a valid socket ID */
	ret = rte_malloc_heap_socket_is_external(mp->socket_id);
	if (ret < 0)
		return -EINVAL;
	alloc_in_ext_mem = (ret == 1);
	need_iova_contig_obj = !(mp->flags & MEMPOOL_F_NO_IOVA_CONTIG);

	if (!need_iova_contig_obj)
		*pg_sz = 0;
	else if (rte_eal_has_hugepages() || alloc_in_ext_mem)
		*pg_sz = get_min_page_size(mp->socket_id);
	else
		*pg_sz = rte_mem_page_size();

	rte_mempool_trace_get_page_size(mp, *pg_sz);
	return 0;
}

/* Default function to populate the mempool: allocate memory in memzones,
 * and populate them. Return the number of objects added, or a negative
 * value on error.
 */
int
rte_mempool_populate_default(struct rte_mempool *mp)
{
	unsigned int mz_flags = RTE_MEMZONE_1GB|RTE_MEMZONE_SIZE_HINT_ONLY;
	char mz_name[RTE_MEMZONE_NAMESIZE];
	const struct rte_memzone *mz;
	ssize_t mem_size;
	size_t align, pg_sz, pg_shift = 0;
	rte_iova_t iova;
	unsigned mz_id, n;
	int ret;
	bool need_iova_contig_obj;
	size_t max_alloc_size = SIZE_MAX;

	ret = mempool_ops_alloc_once(mp);
	if (ret != 0)
		return ret;

	/* mempool must not be populated */
	if (mp->nb_mem_chunks != 0)
		return -EEXIST;

	/*
	 * the following section calculates page shift and page size values.
	 *
	 * these values impact the result of calc_mem_size operation, which
	 * returns the amount of memory that should be allocated to store the
	 * desired number of objects. when not zero, it allocates more memory
	 * for the padding between objects, to ensure that an object does not
	 * cross a page boundary. in other words, page size/shift are to be set
	 * to zero if mempool elements won't care about page boundaries.
	 * there are several considerations for page size and page shift here.
	 *
	 * if we don't need our mempools to have physically contiguous objects,
	 * then just set page shift and page size to 0, because the user has
	 * indicated that there's no need to care about anything.
	 *
	 * if we do need contiguous objects (if a mempool driver has its
	 * own calc_size() method returning min_chunk_size = mem_size),
	 * there is also an option to reserve the entire mempool memory
	 * as one contiguous block of memory.
	 *
	 * if we require contiguous objects, but not necessarily the entire
	 * mempool reserved space to be contiguous, pg_sz will be != 0,
	 * and the default ops->populate() will take care of not placing
	 * objects across pages.
	 *
	 * if our IO addresses are physical, we may get memory from bigger
	 * pages, or we might get memory from smaller pages, and how much of it
	 * we require depends on whether we want bigger or smaller pages.
	 * However, requesting each and every memory size is too much work, so
	 * what we'll do instead is walk through the page sizes available, pick
	 * the smallest one and set up page shift to match that one. We will be
	 * wasting some space this way, but it's much nicer than looping around
	 * trying to reserve each and every page size.
	 *
	 * If we fail to get enough contiguous memory, then we'll go and
	 * reserve space in smaller chunks.
	 */

	need_iova_contig_obj = !(mp->flags & MEMPOOL_F_NO_IOVA_CONTIG);
	ret = rte_mempool_get_page_size(mp, &pg_sz);
	if (ret < 0)
		return ret;

	if (pg_sz != 0)
		pg_shift = rte_bsf32(pg_sz);

	for (mz_id = 0, n = mp->size; n > 0; mz_id++, n -= ret) {
		size_t min_chunk_size;

		mem_size = rte_mempool_ops_calc_mem_size(
			mp, n, pg_shift, &min_chunk_size, &align);

		if (mem_size < 0) {
			ret = mem_size;
			goto fail;
		}

		ret = snprintf(mz_name, sizeof(mz_name),
			RTE_MEMPOOL_MZ_FORMAT "_%d", mp->name, mz_id);
		if (ret < 0 || ret >= (int)sizeof(mz_name)) {
			ret = -ENAMETOOLONG;
			goto fail;
		}

		/* if we're trying to reserve contiguous memory, add appropriate
		 * memzone flag.
		 */
		if (min_chunk_size == (size_t)mem_size)
			mz_flags |= RTE_MEMZONE_IOVA_CONTIG;

		/* Allocate a memzone, retrying with a smaller area on ENOMEM */
		do {
			mz = rte_memzone_reserve_aligned(mz_name,
				RTE_MIN((size_t)mem_size, max_alloc_size),
				mp->socket_id, mz_flags, align);

			if (mz != NULL || rte_errno != ENOMEM)
				break;

			max_alloc_size = RTE_MIN(max_alloc_size,
						(size_t)mem_size) / 2;
		} while (mz == NULL && max_alloc_size >= min_chunk_size);

		if (mz == NULL) {
			ret = -rte_errno;
			goto fail;
		}

		if (need_iova_contig_obj)
			iova = mz->iova;
		else
			iova = RTE_BAD_IOVA;

		if (pg_sz == 0 || (mz_flags & RTE_MEMZONE_IOVA_CONTIG))
			ret = rte_mempool_populate_iova(mp, mz->addr,
				iova, mz->len,
				rte_mempool_memchunk_mz_free,
				(void *)(uintptr_t)mz);
		else
			ret = rte_mempool_populate_virt(mp, mz->addr,
				mz->len, pg_sz,
				rte_mempool_memchunk_mz_free,
				(void *)(uintptr_t)mz);
		if (ret == 0) /* should not happen */
			ret = -ENOBUFS;
		if (ret < 0) {
			rte_memzone_free(mz);
			goto fail;
		}
	}

	rte_mempool_trace_populate_default(mp);
	return mp->size;

 fail:
	rte_mempool_free_memchunks(mp);
	return ret;
}

/* return the memory size required for mempool objects in anonymous mem */
static ssize_t
get_anon_size(const struct rte_mempool *mp)
{
	ssize_t size;
	size_t pg_sz, pg_shift;
	size_t min_chunk_size;
	size_t align;

	pg_sz = rte_mem_page_size();
	pg_shift = rte_bsf32(pg_sz);
	size = rte_mempool_ops_calc_mem_size(mp, mp->size, pg_shift,
					     &min_chunk_size, &align);

	return size;
}

/* unmap a memory zone mapped by rte_mempool_populate_anon() */
static void
rte_mempool_memchunk_anon_free(struct rte_mempool_memhdr *memhdr,
	void *opaque)
{
	ssize_t size;

	/*
	 * Calculate size since memhdr->len has contiguous chunk length
	 * which may be smaller if anon map is split into many contiguous
	 * chunks. Result must be the same as we calculated on populate.
	 */
	size = get_anon_size(memhdr->mp);
	if (size < 0)
		return;

	rte_mem_unmap(opaque, size);
}

/* populate the mempool with an anonymous mapping */
int
rte_mempool_populate_anon(struct rte_mempool *mp)
{
	ssize_t size;
	int ret;
	char *addr;

	/* mempool is already populated, error */
	if ((!STAILQ_EMPTY(&mp->mem_list)) || mp->nb_mem_chunks != 0) {
		rte_errno = EINVAL;
		return 0;
	}

	ret = mempool_ops_alloc_once(mp);
	if (ret < 0) {
		rte_errno = -ret;
		return 0;
	}

	size = get_anon_size(mp);
	if (size < 0) {
		rte_errno = -size;
		return 0;
	}

	/* get chunk of virtually continuous memory */
	addr = rte_mem_map(NULL, size, RTE_PROT_READ | RTE_PROT_WRITE,
		RTE_MAP_SHARED | RTE_MAP_ANONYMOUS, -1, 0);
	if (addr == NULL)
		return 0;
	/* can't use MMAP_LOCKED, it does not exist on BSD */
	if (rte_mem_lock(addr, size) < 0) {
		rte_mem_unmap(addr, size);
		return 0;
	}

	ret = rte_mempool_populate_virt(mp, addr, size, rte_mem_page_size(),
		rte_mempool_memchunk_anon_free, addr);
	if (ret == 0) /* should not happen */
		ret = -ENOBUFS;
	if (ret < 0) {
		rte_errno = -ret;
		goto fail;
	}

	rte_mempool_trace_populate_anon(mp);
	return mp->populated_size;

 fail:
	rte_mempool_free_memchunks(mp);
	return 0;
}

/* free a mempool */
void
rte_mempool_free(struct rte_mempool *mp)
{
	struct rte_mempool_list *mempool_list = NULL;
	struct rte_tailq_entry *te;

	if (mp == NULL)
		return;

	mempool_list = RTE_TAILQ_CAST(rte_mempool_tailq.head, rte_mempool_list);
	rte_mcfg_tailq_write_lock();
	/* find out tailq entry */
	TAILQ_FOREACH(te, mempool_list, next) {
		if (te->data == (void *)mp)
			break;
	}

	if (te != NULL) {
		TAILQ_REMOVE(mempool_list, te, next);
		rte_free(te);
	}
	rte_mcfg_tailq_write_unlock();

	rte_mempool_trace_free(mp);
	rte_mempool_free_memchunks(mp);
	rte_mempool_ops_free(mp);
	rte_memzone_free(mp->mz);
}

static void
mempool_cache_init(struct rte_mempool_cache *cache, uint32_t size)
{
	cache->size = size;
	cache->flushthresh = CALC_CACHE_FLUSHTHRESH(size);
	cache->len = 0;
}

/*
 * Create and initialize a cache for objects that are retrieved from and
 * returned to an underlying mempool. This structure is identical to the
 * local_cache[lcore_id] pointed to by the mempool structure.
 */
struct rte_mempool_cache *
rte_mempool_cache_create(uint32_t size, int socket_id)
{
	struct rte_mempool_cache *cache;

	if (size == 0 || size > RTE_MEMPOOL_CACHE_MAX_SIZE) {
		rte_errno = EINVAL;
		return NULL;
	}

	cache = rte_zmalloc_socket("MEMPOOL_CACHE", sizeof(*cache),
				  RTE_CACHE_LINE_SIZE, socket_id);
	if (cache == NULL) {
		RTE_LOG(ERR, MEMPOOL, "Cannot allocate mempool cache.\n");
		rte_errno = ENOMEM;
		return NULL;
	}

	mempool_cache_init(cache, size);

	rte_mempool_trace_cache_create(size, socket_id, cache);
	return cache;
}

/*
 * Free a cache. It's the responsibility of the user to make sure that any
 * remaining objects in the cache are flushed to the corresponding
 * mempool.
 */
void
rte_mempool_cache_free(struct rte_mempool_cache *cache)
{
	rte_mempool_trace_cache_free(cache);
	rte_free(cache);
}

/* create an empty mempool */
struct rte_mempool *
rte_mempool_create_empty(const char *name, unsigned n, unsigned elt_size,
	unsigned cache_size, unsigned private_data_size,
	int socket_id, unsigned flags)
{
	char mz_name[RTE_MEMZONE_NAMESIZE];
	struct rte_mempool_list *mempool_list;
	struct rte_mempool *mp = NULL;
	struct rte_tailq_entry *te = NULL;
	const struct rte_memzone *mz = NULL;
	size_t mempool_size;
	unsigned int mz_flags = RTE_MEMZONE_1GB|RTE_MEMZONE_SIZE_HINT_ONLY;
	struct rte_mempool_objsz objsz;
	unsigned lcore_id;
	int ret;

	/* compilation-time checks */
	RTE_BUILD_BUG_ON((sizeof(struct rte_mempool) &
			  RTE_CACHE_LINE_MASK) != 0);
	RTE_BUILD_BUG_ON((sizeof(struct rte_mempool_cache) &
			  RTE_CACHE_LINE_MASK) != 0);
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	RTE_BUILD_BUG_ON((sizeof(struct rte_mempool_debug_stats) &
			  RTE_CACHE_LINE_MASK) != 0);
	RTE_BUILD_BUG_ON((offsetof(struct rte_mempool, stats) &
			  RTE_CACHE_LINE_MASK) != 0);
#endif

	mempool_list = RTE_TAILQ_CAST(rte_mempool_tailq.head, rte_mempool_list);

	/* asked for zero items */
	if (n == 0) {
		rte_errno = EINVAL;
		return NULL;
	}

	/* asked cache too big */
	if (cache_size > RTE_MEMPOOL_CACHE_MAX_SIZE ||
	    CALC_CACHE_FLUSHTHRESH(cache_size) > n) {
		rte_errno = EINVAL;
		return NULL;
	}

	/* "no cache align" imply "no spread" */
	if (flags & MEMPOOL_F_NO_CACHE_ALIGN)
		flags |= MEMPOOL_F_NO_SPREAD;

	/* calculate mempool object sizes. */
	if (!rte_mempool_calc_obj_size(elt_size, flags, &objsz)) {
		rte_errno = EINVAL;
		return NULL;
	}

	rte_mcfg_mempool_write_lock();

	/*
	 * reserve a memory zone for this mempool: private data is
	 * cache-aligned
	 */
	private_data_size = (private_data_size +
			     RTE_MEMPOOL_ALIGN_MASK) & (~RTE_MEMPOOL_ALIGN_MASK);


	/* try to allocate tailq entry */
	te = rte_zmalloc("MEMPOOL_TAILQ_ENTRY", sizeof(*te), 0);
	if (te == NULL) {
		RTE_LOG(ERR, MEMPOOL, "Cannot allocate tailq entry!\n");
		goto exit_unlock;
	}

	mempool_size = MEMPOOL_HEADER_SIZE(mp, cache_size);
	mempool_size += private_data_size;
	mempool_size = RTE_ALIGN_CEIL(mempool_size, RTE_MEMPOOL_ALIGN);

	ret = snprintf(mz_name, sizeof(mz_name), RTE_MEMPOOL_MZ_FORMAT, name);
	if (ret < 0 || ret >= (int)sizeof(mz_name)) {
		rte_errno = ENAMETOOLONG;
		goto exit_unlock;
	}

	mz = rte_memzone_reserve(mz_name, mempool_size, socket_id, mz_flags);
	if (mz == NULL)
		goto exit_unlock;

	/* init the mempool structure */
	mp = mz->addr;
	memset(mp, 0, MEMPOOL_HEADER_SIZE(mp, cache_size));
	ret = strlcpy(mp->name, name, sizeof(mp->name));
	if (ret < 0 || ret >= (int)sizeof(mp->name)) {
		rte_errno = ENAMETOOLONG;
		goto exit_unlock;
	}
	mp->mz = mz;
	mp->size = n;
	mp->flags = flags;
	mp->socket_id = socket_id;
	mp->elt_size = objsz.elt_size;
	mp->header_size = objsz.header_size;
	mp->trailer_size = objsz.trailer_size;
	/* Size of default caches, zero means disabled. */
	mp->cache_size = cache_size;
	mp->private_data_size = private_data_size;
	STAILQ_INIT(&mp->elt_list);
	STAILQ_INIT(&mp->mem_list);

	/*
	 * local_cache pointer is set even if cache_size is zero.
	 * The local_cache points to just past the elt_pa[] array.
	 */
	mp->local_cache = (struct rte_mempool_cache *)
		RTE_PTR_ADD(mp, MEMPOOL_HEADER_SIZE(mp, 0));

	/* Init all default caches. */
	if (cache_size != 0) {
		for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++)
			mempool_cache_init(&mp->local_cache[lcore_id],
					   cache_size);
	}

	te->data = mp;

	rte_mcfg_tailq_write_lock();
	TAILQ_INSERT_TAIL(mempool_list, te, next);
	rte_mcfg_tailq_write_unlock();
	rte_mcfg_mempool_write_unlock();

	rte_mempool_trace_create_empty(name, n, elt_size, cache_size,
		private_data_size, flags, mp);
	return mp;

exit_unlock:
	rte_mcfg_mempool_write_unlock();
	rte_free(te);
	rte_mempool_free(mp);
	return NULL;
}

/* create the mempool */
struct rte_mempool *
rte_mempool_create(const char *name, unsigned n, unsigned elt_size,
	unsigned cache_size, unsigned private_data_size,
	rte_mempool_ctor_t *mp_init, void *mp_init_arg,
	rte_mempool_obj_cb_t *obj_init, void *obj_init_arg,
	int socket_id, unsigned flags)
{
	int ret;
	struct rte_mempool *mp;

	mp = rte_mempool_create_empty(name, n, elt_size, cache_size,
		private_data_size, socket_id, flags);
	if (mp == NULL)
		return NULL;

	/*
	 * Since we have 4 combinations of the SP/SC/MP/MC examine the flags to
	 * set the correct index into the table of ops structs.
	 */
	if ((flags & MEMPOOL_F_SP_PUT) && (flags & MEMPOOL_F_SC_GET))
		ret = rte_mempool_set_ops_byname(mp, "ring_sp_sc", NULL);
	else if (flags & MEMPOOL_F_SP_PUT)
		ret = rte_mempool_set_ops_byname(mp, "ring_sp_mc", NULL);
	else if (flags & MEMPOOL_F_SC_GET)
		ret = rte_mempool_set_ops_byname(mp, "ring_mp_sc", NULL);
	else
		ret = rte_mempool_set_ops_byname(mp, "ring_mp_mc", NULL);

	if (ret)
		goto fail;

	/* call the mempool priv initializer */
	if (mp_init)
		mp_init(mp, mp_init_arg);

	if (rte_mempool_populate_default(mp) < 0)
		goto fail;

	/* call the object initializers */
	if (obj_init)
		rte_mempool_obj_iter(mp, obj_init, obj_init_arg);

	rte_mempool_trace_create(name, n, elt_size, cache_size,
		private_data_size, mp_init, mp_init_arg, obj_init,
		obj_init_arg, flags, mp);
	return mp;

 fail:
	rte_mempool_free(mp);
	return NULL;
}

/* Return the number of entries in the mempool */
unsigned int
rte_mempool_avail_count(const struct rte_mempool *mp)
{
	unsigned count;
	unsigned lcore_id;

	count = rte_mempool_ops_get_count(mp);

	if (mp->cache_size == 0)
		return count;

	for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++)
		count += mp->local_cache[lcore_id].len;

	/*
	 * due to race condition (access to len is not locked), the
	 * total can be greater than size... so fix the result
	 */
	if (count > mp->size)
		return mp->size;
	return count;
}

/* return the number of entries allocated from the mempool */
unsigned int
rte_mempool_in_use_count(const struct rte_mempool *mp)
{
	return mp->size - rte_mempool_avail_count(mp);
}

/* dump the cache status */
static unsigned
rte_mempool_dump_cache(FILE *f, const struct rte_mempool *mp)
{
	unsigned lcore_id;
	unsigned count = 0;
	unsigned cache_count;

	fprintf(f, "  internal cache infos:\n");
	fprintf(f, "    cache_size=%"PRIu32"\n", mp->cache_size);

	if (mp->cache_size == 0)
		return count;

	for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++) {
		cache_count = mp->local_cache[lcore_id].len;
		fprintf(f, "    cache_count[%u]=%"PRIu32"\n",
			lcore_id, cache_count);
		count += cache_count;
	}
	fprintf(f, "    total_cache_count=%u\n", count);
	return count;
}

#ifndef __INTEL_COMPILER
#pragma GCC diagnostic ignored "-Wcast-qual"
#endif

/* check and update cookies or panic (internal) */
void rte_mempool_check_cookies(const struct rte_mempool *mp,
	void * const *obj_table_const, unsigned n, int free)
{
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	struct rte_mempool_objhdr *hdr;
	struct rte_mempool_objtlr *tlr;
	uint64_t cookie;
	void *tmp;
	void *obj;
	void **obj_table;

	/* Force to drop the "const" attribute. This is done only when
	 * DEBUG is enabled */
	tmp = (void *) obj_table_const;
	obj_table = tmp;

	while (n--) {
		obj = obj_table[n];

		if (rte_mempool_from_obj(obj) != mp)
			rte_panic("MEMPOOL: object is owned by another "
				  "mempool\n");

		hdr = __mempool_get_header(obj);
		cookie = hdr->cookie;

		if (free == 0) {
			if (cookie != RTE_MEMPOOL_HEADER_COOKIE1) {
				RTE_LOG(CRIT, MEMPOOL,
					"obj=%p, mempool=%p, cookie=%" PRIx64 "\n",
					obj, (const void *) mp, cookie);
				rte_panic("MEMPOOL: bad header cookie (put)\n");
			}
			hdr->cookie = RTE_MEMPOOL_HEADER_COOKIE2;
		} else if (free == 1) {
			if (cookie != RTE_MEMPOOL_HEADER_COOKIE2) {
				RTE_LOG(CRIT, MEMPOOL,
					"obj=%p, mempool=%p, cookie=%" PRIx64 "\n",
					obj, (const void *) mp, cookie);
				rte_panic("MEMPOOL: bad header cookie (get)\n");
			}
			hdr->cookie = RTE_MEMPOOL_HEADER_COOKIE1;
		} else if (free == 2) {
			if (cookie != RTE_MEMPOOL_HEADER_COOKIE1 &&
			    cookie != RTE_MEMPOOL_HEADER_COOKIE2) {
				RTE_LOG(CRIT, MEMPOOL,
					"obj=%p, mempool=%p, cookie=%" PRIx64 "\n",
					obj, (const void *) mp, cookie);
				rte_panic("MEMPOOL: bad header cookie (audit)\n");
			}
		}
		tlr = __mempool_get_trailer(obj);
		cookie = tlr->cookie;
		if (cookie != RTE_MEMPOOL_TRAILER_COOKIE) {
			RTE_LOG(CRIT, MEMPOOL,
				"obj=%p, mempool=%p, cookie=%" PRIx64 "\n",
				obj, (const void *) mp, cookie);
			rte_panic("MEMPOOL: bad trailer cookie\n");
		}
	}
#else
	RTE_SET_USED(mp);
	RTE_SET_USED(obj_table_const);
	RTE_SET_USED(n);
	RTE_SET_USED(free);
#endif
}

void
rte_mempool_contig_blocks_check_cookies(const struct rte_mempool *mp,
	void * const *first_obj_table_const, unsigned int n, int free)
{
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	struct rte_mempool_info info;
	const size_t total_elt_sz =
		mp->header_size + mp->elt_size + mp->trailer_size;
	unsigned int i, j;

	rte_mempool_ops_get_info(mp, &info);

	for (i = 0; i < n; ++i) {
		void *first_obj = first_obj_table_const[i];

		for (j = 0; j < info.contig_block_size; ++j) {
			void *obj;

			obj = (void *)((uintptr_t)first_obj + j * total_elt_sz);
			rte_mempool_check_cookies(mp, &obj, 1, free);
		}
	}
#else
	RTE_SET_USED(mp);
	RTE_SET_USED(first_obj_table_const);
	RTE_SET_USED(n);
	RTE_SET_USED(free);
#endif
}

#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
static void
mempool_obj_audit(struct rte_mempool *mp, __rte_unused void *opaque,
	void *obj, __rte_unused unsigned idx)
{
	__mempool_check_cookies(mp, &obj, 1, 2);
}

static void
mempool_audit_cookies(struct rte_mempool *mp)
{
	unsigned num;

	num = rte_mempool_obj_iter(mp, mempool_obj_audit, NULL);
	if (num != mp->size) {
		rte_panic("rte_mempool_obj_iter(mempool=%p, size=%u) "
			"iterated only over %u elements\n",
			mp, mp->size, num);
	}
}
#else
#define mempool_audit_cookies(mp) do {} while(0)
#endif

#ifndef __INTEL_COMPILER
#pragma GCC diagnostic error "-Wcast-qual"
#endif

/* check cookies before and after objects */
static void
mempool_audit_cache(const struct rte_mempool *mp)
{
	/* check cache size consistency */
	unsigned lcore_id;

	if (mp->cache_size == 0)
		return;

	for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++) {
		const struct rte_mempool_cache *cache;
		cache = &mp->local_cache[lcore_id];
		if (cache->len > cache->flushthresh) {
			RTE_LOG(CRIT, MEMPOOL, "badness on cache[%u]\n",
				lcore_id);
			rte_panic("MEMPOOL: invalid cache len\n");
		}
	}
}

/* check the consistency of mempool (size, cookies, ...) */
void
rte_mempool_audit(struct rte_mempool *mp)
{
	mempool_audit_cache(mp);
	mempool_audit_cookies(mp);

	/* For case where mempool DEBUG is not set, and cache size is 0 */
	RTE_SET_USED(mp);
}

/* dump the status of the mempool on the console */
void
rte_mempool_dump(FILE *f, struct rte_mempool *mp)
{
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	struct rte_mempool_info info;
	struct rte_mempool_debug_stats sum;
	unsigned lcore_id;
#endif
	struct rte_mempool_memhdr *memhdr;
	struct rte_mempool_ops *ops;
	unsigned common_count;
	unsigned cache_count;
	size_t mem_len = 0;

	RTE_ASSERT(f != NULL);
	RTE_ASSERT(mp != NULL);

	fprintf(f, "mempool <%s>@%p\n", mp->name, mp);
	fprintf(f, "  flags=%x\n", mp->flags);
	fprintf(f, "  socket_id=%d\n", mp->socket_id);
	fprintf(f, "  pool=%p\n", mp->pool_data);
	fprintf(f, "  iova=0x%" PRIx64 "\n", mp->mz->iova);
	fprintf(f, "  nb_mem_chunks=%u\n", mp->nb_mem_chunks);
	fprintf(f, "  size=%"PRIu32"\n", mp->size);
	fprintf(f, "  populated_size=%"PRIu32"\n", mp->populated_size);
	fprintf(f, "  header_size=%"PRIu32"\n", mp->header_size);
	fprintf(f, "  elt_size=%"PRIu32"\n", mp->elt_size);
	fprintf(f, "  trailer_size=%"PRIu32"\n", mp->trailer_size);
	fprintf(f, "  total_obj_size=%"PRIu32"\n",
	       mp->header_size + mp->elt_size + mp->trailer_size);

	fprintf(f, "  private_data_size=%"PRIu32"\n", mp->private_data_size);

	fprintf(f, "  ops_index=%d\n", mp->ops_index);
	ops = rte_mempool_get_ops(mp->ops_index);
	fprintf(f, "  ops_name: <%s>\n", (ops != NULL) ? ops->name : "NA");

	STAILQ_FOREACH(memhdr, &mp->mem_list, next)
		mem_len += memhdr->len;
	if (mem_len != 0) {
		fprintf(f, "  avg bytes/object=%#Lf\n",
			(long double)mem_len / mp->size);
	}

	cache_count = rte_mempool_dump_cache(f, mp);
	common_count = rte_mempool_ops_get_count(mp);
	if ((cache_count + common_count) > mp->size)
		common_count = mp->size - cache_count;
	fprintf(f, "  common_pool_count=%u\n", common_count);

	/* sum and dump statistics */
#ifdef RTE_LIBRTE_MEMPOOL_DEBUG
	rte_mempool_ops_get_info(mp, &info);
	memset(&sum, 0, sizeof(sum));
	for (lcore_id = 0; lcore_id < RTE_MAX_LCORE; lcore_id++) {
		sum.put_bulk += mp->stats[lcore_id].put_bulk;
		sum.put_objs += mp->stats[lcore_id].put_objs;
		sum.get_success_bulk += mp->stats[lcore_id].get_success_bulk;
		sum.get_success_objs += mp->stats[lcore_id].get_success_objs;
		sum.get_fail_bulk += mp->stats[lcore_id].get_fail_bulk;
		sum.get_fail_objs += mp->stats[lcore_id].get_fail_objs;
		sum.get_success_blks += mp->stats[lcore_id].get_success_blks;
		sum.get_fail_blks += mp->stats[lcore_id].get_fail_blks;
	}
	fprintf(f, "  stats:\n");
	fprintf(f, "    put_bulk=%"PRIu64"\n", sum.put_bulk);
	fprintf(f, "    put_objs=%"PRIu64"\n", sum.put_objs);
	fprintf(f, "    get_success_bulk=%"PRIu64"\n", sum.get_success_bulk);
	fprintf(f, "    get_success_objs=%"PRIu64"\n", sum.get_success_objs);
	fprintf(f, "    get_fail_bulk=%"PRIu64"\n", sum.get_fail_bulk);
	fprintf(f, "    get_fail_objs=%"PRIu64"\n", sum.get_fail_objs);
	if (info.contig_block_size > 0) {
		fprintf(f, "    get_success_blks=%"PRIu64"\n",
			sum.get_success_blks);
		fprintf(f, "    get_fail_blks=%"PRIu64"\n", sum.get_fail_blks);
	}
#else
	fprintf(f, "  no statistics available\n");
#endif

	rte_mempool_audit(mp);
}

/* dump the status of all mempools on the console */
void
rte_mempool_list_dump(FILE *f)
{
	struct rte_mempool *mp = NULL;
	struct rte_tailq_entry *te;
	struct rte_mempool_list *mempool_list;

	mempool_list = RTE_TAILQ_CAST(rte_mempool_tailq.head, rte_mempool_list);

	rte_mcfg_mempool_read_lock();

	TAILQ_FOREACH(te, mempool_list, next) {
		mp = (struct rte_mempool *) te->data;
		rte_mempool_dump(f, mp);
	}

	rte_mcfg_mempool_read_unlock();
}

/* search a mempool from its name */
struct rte_mempool *
rte_mempool_lookup(const char *name)
{
	struct rte_mempool *mp = NULL;
	struct rte_tailq_entry *te;
	struct rte_mempool_list *mempool_list;

	mempool_list = RTE_TAILQ_CAST(rte_mempool_tailq.head, rte_mempool_list);

	rte_mcfg_mempool_read_lock();

	TAILQ_FOREACH(te, mempool_list, next) {
		mp = (struct rte_mempool *) te->data;
		if (strncmp(name, mp->name, RTE_MEMPOOL_NAMESIZE) == 0)
			break;
	}

	rte_mcfg_mempool_read_unlock();

	if (te == NULL) {
		rte_errno = ENOENT;
		return NULL;
	}

	return mp;
}

void rte_mempool_walk(void (*func)(struct rte_mempool *, void *),
		      void *arg)
{
	struct rte_tailq_entry *te = NULL;
	struct rte_mempool_list *mempool_list;
	void *tmp_te;

	mempool_list = RTE_TAILQ_CAST(rte_mempool_tailq.head, rte_mempool_list);

	rte_mcfg_mempool_read_lock();

	TAILQ_FOREACH_SAFE(te, mempool_list, next, tmp_te) {
		(*func)((struct rte_mempool *) te->data, arg);
	}

	rte_mcfg_mempool_read_unlock();
}

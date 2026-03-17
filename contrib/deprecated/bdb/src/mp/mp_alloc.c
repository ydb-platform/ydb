/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

/*
 * This configuration parameter limits the number of hash buckets which
 * __memp_alloc() searches through while excluding buffers with a 'high'
 * priority.
 */
#if !defined(MPOOL_ALLOC_SEARCH_LIMIT)
#define	MPOOL_ALLOC_SEARCH_LIMIT	500
#endif

/*
 * __memp_alloc --
 *	Allocate some space from a cache region.
 *
 * PUBLIC: int __memp_alloc __P((DB_MPOOL *,
 * PUBLIC:     REGINFO *, MPOOLFILE *, size_t, roff_t *, void *));
 */
int
__memp_alloc(dbmp, infop, mfp, len, offsetp, retp)
	DB_MPOOL *dbmp;
	REGINFO *infop;
	MPOOLFILE *mfp;
	size_t len;
	roff_t *offsetp;
	void *retp;
{
	BH *bhp, *current_bhp, *mvcc_bhp, *oldest_bhp;
	BH_FROZEN_PAGE *frozen_bhp;
	DB_LSN oldest_reader, vlsn;
	DB_MPOOL_HASH *dbht, *hp, *hp_end, *hp_saved, *hp_tmp;
	ENV *env;
	MPOOL *c_mp;
	MPOOLFILE *bh_mfp;
	size_t freed_space;
	u_int32_t buckets, bucket_priority, buffers, cache_reduction;
	u_int32_t dirty_eviction, high_priority, priority, versions;
	u_int32_t priority_saved, put_counter, lru_generation, total_buckets;
	int aggressive, alloc_freeze, b_lock, giveup;
	int h_locked, need_free, obsolete, ret, write_error;
	u_int8_t *endp;
	void *p;

	env = dbmp->env;
	c_mp = infop->primary;
	dbht = R_ADDR(infop, c_mp->htab);
	hp_end = &dbht[c_mp->htab_buckets];
	hp_saved = NULL;
	priority_saved = 0;
	write_error = 0;

	buckets = buffers = put_counter = total_buckets = versions = 0;
	aggressive = alloc_freeze = giveup = h_locked = 0;

	/*
	 * If we're allocating a buffer, and the one we're discarding is the
	 * same size, we don't want to waste the time to re-integrate it into
	 * the shared memory free list.  If the DB_MPOOLFILE argument isn't
	 * NULL, we'll compare the underlying page sizes of the two buffers
	 * before free-ing and re-allocating buffers.
	 */
	if (mfp != NULL) {
		len = SSZA(BH, buf) + mfp->pagesize;
		/* Add space for alignment padding for MVCC diagnostics. */
		MVCC_BHSIZE(mfp, len);
	}

	STAT_INC(env, mpool, nallocs, c_mp->stat.st_alloc, len);

	MPOOL_REGION_LOCK(env, infop);

	/*
	 * First we try to allocate from free memory.  If that fails, scan the
	 * buffer pool to find buffers with low priorities.  We consider small
	 * sets of hash buckets each time to limit the amount of work needing
	 * to be done.  This approximates LRU, but not very well.  We either
	 * find a buffer of the same size to use, or we will free 3 times what
	 * we need in the hopes it will coalesce into a contiguous chunk of the
	 * right size.  In the latter case we branch back here and try again.
	 */
alloc:	if ((ret = __env_alloc(infop, len, &p)) == 0) {
		if (mfp != NULL) {
			/*
			 * For MVCC diagnostics, align the pointer so that the
			 * buffer starts on a page boundary.
			 */
			MVCC_BHALIGN(p);
			bhp = (BH *)p;

			if ((ret = __mutex_alloc(env, MTX_MPOOL_BH,
			    DB_MUTEX_SHARED, &bhp->mtx_buf)) != 0) {
				MVCC_BHUNALIGN(bhp);
				__env_alloc_free(infop, bhp);
				goto search;
			}
			c_mp->pages++;
		}
		MPOOL_REGION_UNLOCK(env, infop);
found:		if (offsetp != NULL)
			*offsetp = R_OFFSET(infop, p);
		*(void **)retp = p;

		/*
		 * Update the search statistics.
		 *
		 * We're not holding the region locked here, these statistics
		 * can't be trusted.
		 */
#ifdef HAVE_STATISTICS
		total_buckets += buckets;
		if (total_buckets != 0) {
			if (total_buckets > c_mp->stat.st_alloc_max_buckets)
				STAT_SET(env, mpool, alloc_max_buckets,
				    c_mp->stat.st_alloc_max_buckets,
				    total_buckets, infop->id);
			STAT_ADJUST(env, mpool, alloc_buckets,
			    c_mp->stat.st_alloc_buckets,
			    total_buckets, infop->id);
		}
		if (buffers != 0) {
			if (buffers > c_mp->stat.st_alloc_max_pages)
				STAT_SET(env, mpool, alloc_max_pages,
				    c_mp->stat.st_alloc_max_pages,
				    buffers, infop->id);
			STAT_ADJUST(env, mpool, alloc_pages,
			    c_mp->stat.st_alloc_pages, buffers, infop->id);
		}
#endif
		return (0);
	} else if (giveup || c_mp->pages == 0) {
		MPOOL_REGION_UNLOCK(env, infop);

		__db_errx(env, DB_STR("3017",
		    "unable to allocate space from the buffer cache"));
		return ((ret == ENOMEM && write_error != 0) ? EIO : ret);
	}

search:
	/*
	 * Anything newer than 1/10th of the buffer pool is ignored during the
	 * first MPOOL_SEARCH_ALLOC_LIMIT buckets worth of allocation.
	 */
	cache_reduction = c_mp->pages / 10;
	high_priority = aggressive ? MPOOL_LRU_MAX :
	    c_mp->lru_priority - cache_reduction;
	lru_generation = c_mp->lru_generation;

	ret = 0;
	MAX_LSN(oldest_reader);

	/*
	 * We re-attempt the allocation every time we've freed 3 times what
	 * we need.  Reset our free-space counter.
	 */
	freed_space = 0;
	total_buckets += buckets;
	buckets = 0;

	/*
	 * Walk the hash buckets and find the next two with potentially useful
	 * buffers.  Free the buffer with the lowest priority from the buckets'
	 * chains.
	 */
	for (;;) {
		/* All pages have been freed, make one last try */
		if (c_mp->pages == 0)
			goto alloc;

		/* Check for wrap around. */
		hp = &dbht[c_mp->last_checked++];
		if (hp >= hp_end) {
			c_mp->last_checked = 0;
			hp = &dbht[c_mp->last_checked++];
		}

		/*
		 * The failure mode is when there are too many buffers we can't
		 * write or there's not enough memory in the system to support
		 * the number of pinned buffers.
		 *
		 * Get aggressive if we've reviewed the entire cache without
		 * freeing the needed space.  (The code resets "aggressive"
		 * when we free any space.)  Aggressive means:
		 *
		 * a: set a flag to attempt to flush high priority buffers as
		 *    well as other buffers.
		 * b: look at a buffer in every hash bucket rather than choose
		 *    the more preferable of two.
		 * c: start to think about giving up.
		 *
		 * If we get here three or more times, sync the mpool to force
		 * out queue extent pages.  While we might not have enough
		 * space for what we want and flushing is expensive, why not?
		 * Then sleep for a second, hopefully someone else will run and
		 * free up some memory.
		 *
		 * Always try to allocate memory too, in case some other thread
		 * returns its memory to the region.
		 *
		 * We don't have any way to know an allocation has no way to
		 * succeed.  Fail if no pages are returned to the cache after
		 * we've been trying for a relatively long time.
		 *
		 * !!!
		 * This test ignores pathological cases like no buffers in the
		 * system -- we check for that early on, so it isn't possible.
		 */
		if (buckets++ == c_mp->htab_buckets) {
			if (freed_space > 0)
				goto alloc;
			MPOOL_REGION_UNLOCK(env, infop);

			aggressive++;
			/*
			 * Once aggressive, we consider all buffers. By setting
			 * this to MPOOL_LRU_MAX, we'll still select a victim
			 * even if all buffers have the highest normal priority.
			 */
			high_priority = MPOOL_LRU_MAX;
			PERFMON4(env, mpool, alloc_wrap,
			    len, infop->id, aggressive, c_mp->put_counter);
			switch (aggressive) {
			case 1:
				break;
			case 2:
				put_counter = c_mp->put_counter;
				break;
			case 3:
			case 4:
			case 5:
			case 6:
				(void)__memp_sync_int(
				    env, NULL, 0, DB_SYNC_ALLOC, NULL, NULL);

				__os_yield(env, 1, 0);
				break;
			default:
				aggressive = 1;
				if (put_counter == c_mp->put_counter)
					giveup = 1;
				break;
			}

			MPOOL_REGION_LOCK(env, infop);
			goto alloc;
		}

		/*
		 * Skip empty buckets.
		 *
		 * We can check for empty buckets before locking the hash
		 * bucket as we only care if the pointer is zero or non-zero.
		 */
		if (SH_TAILQ_FIRST(&hp->hash_bucket, __bh) == NULL)
			continue;

		/* Set aggressive if we have already searched for too long. */
		if (aggressive == 0 && buckets >= MPOOL_ALLOC_SEARCH_LIMIT) {
			aggressive = 1;
			/* Once aggressive, we consider all buffers. */
			high_priority = MPOOL_LRU_MAX;
		}

		/* Unlock the region and lock the hash bucket. */
		MPOOL_REGION_UNLOCK(env, infop);
		MUTEX_READLOCK(env, hp->mtx_hash);
		h_locked = 1;
		b_lock = 0;

		/*
		 * Find a buffer we can use.
		 *
		 * We use the lowest-LRU singleton buffer if we find one and
		 * it's better than the result of another hash bucket we've
		 * reviewed.  We do not use a buffer which has a priority
		 * greater than high_priority unless we are being aggressive.
		 *
		 * With MVCC buffers, the situation is more complicated: we
		 * don't want to free a buffer out of the middle of an MVCC
		 * chain, since that requires I/O.  So, walk the buffers,
		 * looking for an obsolete buffer at the end of an MVCC chain.
		 * Once a buffer becomes obsolete, its LRU priority is
		 * irrelevant because that version can never be accessed again.
		 *
		 * If we don't find any obsolete MVCC buffers, we will get
		 * aggressive, and in that case consider the lowest priority
		 * buffer within a chain.
		 *
		 * Ignore referenced buffers, we can't get rid of them.
		 */
retry_search:	bhp = NULL;
		bucket_priority = high_priority;
		obsolete = 0;
		SH_TAILQ_FOREACH(current_bhp, &hp->hash_bucket, hq, __bh) {
			/*
			 * First, do the standard LRU check for singletons.
			 * We can use the buffer if it is unreferenced, has a
			 * priority that isn't too high (unless we are
			 * aggressive), and is better than the best candidate
			 * we have found so far in this bucket.
			 */
#ifdef MPOOL_ALLOC_SEARCH_DYN
			if (aggressive == 0 &&
			     ++high_priority >= c_mp->lru_priority)
				aggressive = 1;
#endif

			if (SH_CHAIN_SINGLETON(current_bhp, vc)) {
				if (BH_REFCOUNT(current_bhp) != 0)
					continue;
				buffers++;
				if (bucket_priority > current_bhp->priority) {
					bucket_priority = current_bhp->priority;
					if (bhp != NULL)
						atomic_dec(env, &bhp->ref);
					bhp = current_bhp;
					atomic_inc(env, &bhp->ref);
				}
				continue;
			}

			/*
			 * For MVCC buffers, walk through the chain.  If we are
			 * aggressive, choose the best candidate from within
			 * the chain for freezing.
			 */
			for (mvcc_bhp = oldest_bhp = current_bhp;
			    mvcc_bhp != NULL;
			    oldest_bhp = mvcc_bhp,
			    mvcc_bhp = SH_CHAIN_PREV(mvcc_bhp, vc, __bh)) {
#ifdef MPOOL_ALLOC_SEARCH_DYN
				if (aggressive == 0 &&
				     ++high_priority >= c_mp->lru_priority)
					aggressive = 1;
#endif
				DB_ASSERT(env, mvcc_bhp !=
				    SH_CHAIN_PREV(mvcc_bhp, vc, __bh));
				if ((aggressive < 2 &&
				    ++versions < (buffers >> 2)) ||
				    BH_REFCOUNT(mvcc_bhp) != 0)
					continue;
				buffers++;
				if (!F_ISSET(mvcc_bhp, BH_FROZEN) &&
				    (bhp == NULL ||
				    bhp->priority > mvcc_bhp->priority)) {
					if (bhp != NULL)
						atomic_dec(env, &bhp->ref);
					bhp = mvcc_bhp;
					atomic_inc(env, &bhp->ref);
				}
			}

			/*
			 * oldest_bhp is the last buffer on the MVCC chain, and
			 * an obsolete buffer at the end of the MVCC chain gets
			 * used without further search. Before checking for
			 * obsolescence, update the cached oldest reader LSN in
			 * the bucket if it is older than call's oldest_reader.
			 */
			if (BH_REFCOUNT(oldest_bhp) != 0)
				continue;

			if (LOG_COMPARE(&oldest_reader, &hp->old_reader) > 0) {
				if (IS_MAX_LSN(oldest_reader) &&
				   (ret = __txn_oldest_reader(
				    env, &oldest_reader)) != 0) {
					MUTEX_UNLOCK(env, hp->mtx_hash);
					if (bhp != NULL)
						atomic_dec(env, &bhp->ref);
					return (ret);
				}
				if (LOG_COMPARE(&oldest_reader,
				    &hp->old_reader) > 0)
					hp->old_reader = oldest_reader;
			}

			if (BH_OBSOLETE(oldest_bhp, hp->old_reader, vlsn)) {
				if (aggressive < 2)
					buffers++;
				obsolete = 1;
				if (bhp != NULL)
					atomic_dec(env, &bhp->ref);
				bhp = oldest_bhp;
				atomic_inc(env, &bhp->ref);
				goto this_buffer;
			}
		}

		/*
		 * bhp is either NULL or the best candidate buffer.
		 * We'll use the chosen buffer only if we have compared its
		 * priority against one chosen from another hash bucket.
		 */
		if (bhp == NULL)
			goto next_hb;

		priority = bhp->priority;

		/*
		 * Compare two hash buckets and select the one with the lower
		 * priority. Performance testing showed looking at two improves
		 * the LRU-ness and looking at more only does a little better.
		 */
		if (hp_saved == NULL) {
			hp_saved = hp;
			priority_saved = priority;
			goto next_hb;
		}

		/*
		 * If the buffer we just found is a better choice than our
		 * previous choice, use it.
		 *
		 * If the previous choice was better, pretend we're moving
		 * from this hash bucket to the previous one and re-do the
		 * search.
		 *
		 * We don't worry about simply swapping between two buckets
		 * because that could only happen if a buffer was removed
		 * from the chain, or its priority updated.   If a buffer
		 * is removed from the chain, some other thread has managed
		 * to discard a buffer, so we're moving forward.  Updating
		 * a buffer's priority will make it a high-priority buffer,
		 * so we'll ignore it when we search again, and so we will
		 * eventually zero in on a buffer to use, or we'll decide
		 * there are no buffers we can use.
		 *
		 * If there's only a single hash bucket with buffers, we'll
		 * search the bucket once, choose a buffer, walk the entire
		 * list of buckets and search it again.   In the case of a
		 * system that's busy, it's possible to imagine a case where
		 * we'd loop for a long while.  For that reason, and because
		 * the test is easy, we special case and test for it.
		 */
		if (priority > priority_saved && hp != hp_saved) {
			MUTEX_UNLOCK(env, hp->mtx_hash);
			hp_tmp = hp_saved;
			hp_saved = hp;
			hp = hp_tmp;
			priority_saved = priority;
			MUTEX_READLOCK(env, hp->mtx_hash);
			h_locked = 1;
			DB_ASSERT(env, BH_REFCOUNT(bhp) > 0);
			atomic_dec(env, &bhp->ref);
			goto retry_search;
		}

		/*
		 * If another thread has called __memp_reset_lru() while we were
		 * looking for this buffer, it is possible that we've picked a
		 * poor choice for a victim. If so toss it and start over.
		 */
		if (lru_generation != c_mp->lru_generation) {
			DB_ASSERT(env, BH_REFCOUNT(bhp) > 0);
			atomic_dec(env, &bhp->ref);
			MUTEX_UNLOCK(env, hp->mtx_hash);
			MPOOL_REGION_LOCK(env, infop);
			hp_saved = NULL;
			goto search;
		}

this_buffer:	/*
		 * Discard any previously remembered hash bucket, we've got
		 * a winner.
		 */
		hp_saved = NULL;

		/* Drop the hash mutex and lock the buffer exclusively. */
		MUTEX_UNLOCK(env, hp->mtx_hash);
		h_locked = 0;

		/* Don't bother trying to latch a busy buffer. */
		if (BH_REFCOUNT(bhp) > 1)
			goto next_hb;

		/* We cannot block as the caller is probably holding locks. */
		if ((ret = MUTEX_TRYLOCK(env, bhp->mtx_buf)) != 0) {
			if (ret != DB_LOCK_NOTGRANTED)
				return (ret);
			goto next_hb;
		}
		F_SET(bhp, BH_EXCLUSIVE);
		b_lock = 1;

		/* Someone may have grabbed it while we got the lock. */
		if (BH_REFCOUNT(bhp) != 1)
			goto next_hb;

		/* Find the associated MPOOLFILE. */
		bh_mfp = R_ADDR(dbmp->reginfo, bhp->mf_offset);

		/* If the page is dirty, write it. */
		ret = 0;
		dirty_eviction = 0;
		if (F_ISSET(bhp, BH_DIRTY)) {
			DB_ASSERT(env, atomic_read(&hp->hash_page_dirty) > 0);
			ret = __memp_bhwrite(dbmp, hp, bh_mfp, bhp, 0);
			DB_ASSERT(env, atomic_read(&bhp->ref) > 0);

			/*
			 * If a write fails for any reason, we can't proceed.
			 *
			 * If there's a write error and we're having problems
			 * finding something to allocate, avoid selecting this
			 * buffer again by maximizing its priority.
			 */
			if (ret != 0) {
				if (ret != EPERM && ret != EAGAIN) {
					write_error++;
					__db_errx(env, DB_STR_A("3018",
		"%s: unwritable page %d remaining in the cache after error %d",
					    "%s %d %d"),
					    __memp_fns(dbmp, bh_mfp),
					    bhp->pgno, ret);
				}
				bhp->priority = MPOOL_LRU_REDZONE;

				goto next_hb;
			}

			dirty_eviction = 1;
		}

		/*
		 * Freeze this buffer, if necessary.  That is, if the buffer is
		 * part of an MVCC chain and could be required by a reader.
		 */
		if (SH_CHAIN_HASPREV(bhp, vc) ||
		    (SH_CHAIN_HASNEXT(bhp, vc) && !obsolete)) {
			if (!aggressive ||
			    F_ISSET(bhp, BH_DIRTY | BH_FROZEN))
				goto next_hb;
			ret = __memp_bh_freeze(
			    dbmp, infop, hp, bhp, &alloc_freeze);
			if (ret == EIO)
				write_error++;
			if (ret == EBUSY || ret == EIO ||
			    ret == ENOMEM || ret == ENOSPC) {
				ret = 0;
				goto next_hb;
			} else if (ret != 0) {
				DB_ASSERT(env, BH_REFCOUNT(bhp) > 0);
				atomic_dec(env, &bhp->ref);
				DB_ASSERT(env, b_lock);
				F_CLR(bhp, BH_EXCLUSIVE);
				MUTEX_UNLOCK(env, bhp->mtx_buf);
				DB_ASSERT(env, !h_locked);
				return (ret);
			}
		}

		MUTEX_LOCK(env, hp->mtx_hash);
		h_locked = 1;

		/*
		 * We released the hash bucket lock while doing I/O, so another
		 * thread may have acquired this buffer and incremented the ref
		 * count or dirtied the buffer or installed a new version after
		 * we wrote it, in which case we can't have it.
		 */
		if (BH_REFCOUNT(bhp) != 1 || F_ISSET(bhp, BH_DIRTY) ||
		    (SH_CHAIN_HASNEXT(bhp, vc) &&
		    SH_CHAIN_NEXTP(bhp, vc, __bh)->td_off != bhp->td_off &&
		    !BH_OBSOLETE(bhp, hp->old_reader, vlsn)))
			goto next_hb;

		/*
		 * If the buffer is frozen, thaw it and look for another one
		 * we can use. (Calling __memp_bh_freeze above will not
		 * mark bhp BH_FROZEN.)
		 */
		if (F_ISSET(bhp, BH_FROZEN)) {
			DB_ASSERT(env, obsolete || SH_CHAIN_SINGLETON(bhp, vc));
			DB_ASSERT(env, BH_REFCOUNT(bhp) > 0);
			if (!F_ISSET(bhp, BH_THAWED)) {
				/*
				 * This call releases the hash bucket mutex.
				 * We're going to retry the search, so we need
				 * to re-lock it.
				 */
				if ((ret = __memp_bh_thaw(dbmp,
				    infop, hp, bhp, NULL)) != 0)
					return (ret);
				MUTEX_READLOCK(env, hp->mtx_hash);
			} else {
				need_free = (atomic_dec(env, &bhp->ref) == 0);
				F_CLR(bhp, BH_EXCLUSIVE);
				MUTEX_UNLOCK(env, bhp->mtx_buf);
				if (need_free) {
					MPOOL_REGION_LOCK(env, infop);
					SH_TAILQ_INSERT_TAIL(&c_mp->free_frozen,
					    bhp, hq);
					MPOOL_REGION_UNLOCK(env, infop);
				}
			}
			bhp = NULL;
			b_lock = alloc_freeze = 0;
			goto retry_search;
		}

		/* We are certainly freeing this buf; now update statistic. */
		if (dirty_eviction)
			STAT_INC(env, mpool,
			    dirty_eviction, c_mp->stat.st_rw_evict, infop->id);
		else
			STAT_INC(env, mpool,
			    clean_eviction, c_mp->stat.st_ro_evict, infop->id);
		/*
		 * If we need some empty buffer headers for freezing, turn the
		 * buffer we've found into frozen headers and put them on the
		 * free list.  Only reset alloc_freeze if we've actually
		 * allocated some frozen buffer headers.
		 */
		if (alloc_freeze) {
			if ((ret = __memp_bhfree(dbmp,
			     infop, bh_mfp, hp, bhp, 0)) != 0)
				return (ret);
			b_lock = 0;
			h_locked = 0;

			MVCC_MPROTECT(bhp->buf, bh_mfp->pagesize,
			    PROT_READ | PROT_WRITE | PROT_EXEC);

			MPOOL_REGION_LOCK(env, infop);
			SH_TAILQ_INSERT_TAIL(&c_mp->alloc_frozen,
			    (BH_FROZEN_ALLOC *)bhp, links);
			frozen_bhp = (BH_FROZEN_PAGE *)
			    ((BH_FROZEN_ALLOC *)bhp + 1);
			endp = (u_int8_t *)bhp->buf + bh_mfp->pagesize;
			while ((u_int8_t *)(frozen_bhp + 1) < endp) {
				frozen_bhp->header.mtx_buf = MUTEX_INVALID;
				SH_TAILQ_INSERT_TAIL(&c_mp->free_frozen,
				    (BH *)frozen_bhp, hq);
				frozen_bhp++;
			}
			MPOOL_REGION_UNLOCK(env, infop);

			alloc_freeze = 0;
			MUTEX_READLOCK(env, hp->mtx_hash);
			h_locked = 1;
			goto retry_search;
		}

		/*
		 * Check to see if the buffer is the size we're looking for.
		 * If so, we can simply reuse it.  Otherwise, free the buffer
		 * and its space and keep looking.
		 */
		if (mfp != NULL && mfp->pagesize == bh_mfp->pagesize) {
			if ((ret = __memp_bhfree(dbmp,
			     infop, bh_mfp, hp, bhp, 0)) != 0)
				return (ret);
			p = bhp;
			goto found;
		}

		freed_space += sizeof(*bhp) + bh_mfp->pagesize;
		if ((ret =
		    __memp_bhfree(dbmp, infop,
			 bh_mfp, hp, bhp, BH_FREE_FREEMEM)) != 0)
			return (ret);

		/* Reset "aggressive" and "write_error" if we free any space. */
		if (aggressive > 1)
			aggressive = 1;
		write_error = 0;

		/*
		 * Unlock this buffer and re-acquire the region lock. If
		 * we're reaching here as a result of calling memp_bhfree, the
		 * buffer lock has already been discarded.
		 */
		if (0) {
next_hb:		if (bhp != NULL) {
				DB_ASSERT(env, BH_REFCOUNT(bhp) > 0);
				atomic_dec(env, &bhp->ref);
				if (b_lock) {
					F_CLR(bhp, BH_EXCLUSIVE);
					MUTEX_UNLOCK(env, bhp->mtx_buf);
				}
			}
			if (h_locked)
				MUTEX_UNLOCK(env, hp->mtx_hash);
			h_locked = 0;
		}
		MPOOL_REGION_LOCK(env, infop);

		/*
		 * Retry the allocation as soon as we've freed up sufficient
		 * space.  We're likely to have to coalesce of memory to
		 * satisfy the request, don't try until it's likely (possible?)
		 * we'll succeed.
		 */
		if (freed_space >= 3 * len)
			goto alloc;
	}
	/* NOTREACHED */
}

/*
 * __memp_free --
 *	Free some space from a cache region.
 *
 * PUBLIC: void __memp_free __P((REGINFO *, void *));
 */
void
__memp_free(infop, buf)
	REGINFO *infop;
	void *buf;
{
	__env_alloc_free(infop, buf);
}

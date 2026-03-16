/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1999, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"

static int __bam_compact_dups __P((DBC *,
     PAGE **, u_int32_t, int, DB_COMPACT *, int *));
static int __bam_compact_isdone __P((DBC *, DBT *, PAGE *, int *));
static int __bam_csearch __P((DBC *, DBT *, u_int32_t, int));
static int __bam_lock_tree __P((DBC *, EPG *, EPG *csp, u_int32_t, u_int32_t));
static int __bam_lock_subtree __P((DBC *, PAGE *, u_int32_t, u_int32_t));
static int __bam_merge __P((DBC *,
     DBC *,  u_int32_t, DBT *, DB_COMPACT *,int *));
static int __bam_merge_internal __P((DBC *, DBC *, int, DB_COMPACT *, int *));
static int __bam_merge_pages __P((DBC *, DBC *, DB_COMPACT *));
static int __bam_merge_records __P((DBC *, DBC*,  u_int32_t, DB_COMPACT *));
static int __bam_truncate_internal_overflow __P((DBC *, PAGE *, DB_COMPACT *));
static int __bam_truncate_root_page __P((DBC *,
     PAGE *, u_int32_t, DB_COMPACT *));

#ifdef HAVE_FTRUNCATE
static int __bam_savekey __P((DBC *, int, DBT *));
#endif

/*
 * __bam_csearch -- isolate search code for bam_compact.
 * This routine hides the differences between searching
 * a BTREE and a RECNO from the rest of the code.
 */
#define	CS_READ	0	/* We are just reading. */
#define	CS_PARENT	1	/* We want the parent too, write lock. */
#define	CS_NEXT		2	/* Get the next page. */
#define	CS_NEXT_WRITE	3	/* Get the next page and write lock. */
#define	CS_DEL		4	/* Get a stack to delete a page. */
#define	CS_START	5	/* Starting level for stack, write lock. */
#define	CS_NEXT_BOTH	6	/* Get this page and the next, write lock. */
#define	CS_GETRECNO     0x80	/* Extract record number from start. */

static int
__bam_csearch(dbc, start, sflag, level)
	DBC *dbc;
	DBT *start;
	u_int32_t sflag;
	int level;
{
	BTREE_CURSOR *cp;
	int not_used, ret;

	cp = (BTREE_CURSOR *)dbc->internal;

	if (dbc->dbtype == DB_RECNO) {
		/* If GETRECNO is not set the cp->recno is what we want. */
		if (FLD_ISSET(sflag, CS_GETRECNO)) {
			if (start == NULL || start->size == 0)
				cp->recno = 1;
			else if ((ret =
			     __ram_getno(dbc, start, &cp->recno, 0)) != 0)
				return (ret);
			FLD_CLR(sflag, CS_GETRECNO);
		}
		switch (sflag) {
		case CS_READ:
			sflag = SR_READ;
			break;
		case CS_NEXT:
			sflag = SR_PARENT | SR_READ;
			break;
		case CS_START:
			level = LEAFLEVEL;
			/* FALLTHROUGH */
		case CS_DEL:
		case CS_NEXT_WRITE:
			sflag = SR_STACK;
			break;
		case CS_NEXT_BOTH:
			sflag = SR_BOTH | SR_NEXT | SR_WRITE;
			break;
		case CS_PARENT:
			sflag = SR_PARENT | SR_WRITE;
			break;
		default:
			return (__env_panic(dbc->env, EINVAL));
		}
		if ((ret = __bam_rsearch(dbc,
		     &cp->recno, sflag, level, &not_used)) != 0)
			return (ret);
		/* Reset the cursor's recno to the beginning of the page. */
		cp->recno -= cp->csp->indx;
	} else {
		FLD_CLR(sflag, CS_GETRECNO);
		switch (sflag) {
		case CS_READ:
			sflag = SR_READ | SR_DUPFIRST;
			break;
		case CS_DEL:
			sflag = SR_DEL;
			break;
		case CS_NEXT:
			sflag = SR_NEXT;
			break;
		case CS_NEXT_WRITE:
			sflag = SR_NEXT | SR_WRITE;
			break;
		case CS_NEXT_BOTH:
			sflag = SR_BOTH | SR_NEXT | SR_WRITE;
			break;
		case CS_START:
			sflag = SR_START | SR_WRITE;
			break;
		case CS_PARENT:
			sflag = SR_PARENT | SR_WRITE;
			break;
		default:
			return (__env_panic(dbc->env, EINVAL));
		}
		if (start == NULL || start->size == 0)
			FLD_SET(sflag, SR_MIN);

		if ((ret = __bam_search(dbc,
		     PGNO_INVALID, start, sflag, level, NULL, &not_used)) != 0)
			return (ret);
	}

	return (0);
}

/*
 * __bam_compact_int -- internal compaction routine.
 *	Called either with a cursor on the main database
 * or a cursor initialized to the root of an off page duplicate
 * tree.
 * PUBLIC: int __bam_compact_int __P((DBC *,
 * PUBLIC:      DBT *, DBT *, u_int32_t, int *, DB_COMPACT *, int *));
 */
int
__bam_compact_int(dbc, start, stop, factor, spanp, c_data, donep)
	DBC *dbc;
	DBT *start, *stop;
	u_int32_t factor;
	int *spanp;
	DB_COMPACT *c_data;
	int *donep;
{
	BTREE_CURSOR *cp, *ncp;
	DB *dbp;
	DBC *ndbc;
	DB_LOCK metalock, next_lock, nnext_lock, prev_lock, saved_lock;
	DB_MPOOLFILE *dbmp;
	ENV *env;
	EPG *epg;
	PAGE *pg, *ppg, *npg;
	db_pgno_t metapgno, npgno, nnext_pgno;
	db_pgno_t pgno, prev_pgno, ppgno, saved_pgno;
	db_recno_t next_recno;
	u_int32_t nentry, sflag, pgs_free;
	int check_dups, check_trunc, clear_root, do_commit, isdone;
	int merged, next_p, pgs_done, ret, t_ret, tdone;

#ifdef	DEBUG
#define	CTRACE(dbc, location, t, start, f) do {				\
		DBT __trace;						\
		DB_SET_DBT(__trace, t, strlen(t));			\
		DEBUG_LWRITE(						\
		    dbc, (dbc)->txn, location, &__trace, start, f)	\
	} while (0)
#define	PTRACE(dbc, location, p, start, f) do {				\
		char __buf[32];						\
		(void)snprintf(__buf,					\
		    sizeof(__buf), "pgno: %lu", (u_long)p);		\
		CTRACE(dbc, location, __buf, start, f);			\
	} while (0)
#else
#define	CTRACE(dbc, location, t, start, f)
#define	PTRACE(dbc, location, p, start, f)
#endif

	ndbc = NULL;
	pg = NULL;
	npg = NULL;

	isdone = 0;
	tdone = 0;
	pgs_done = 0;
	do_commit = 0;
	next_recno = 0;
	next_p = 0;
	clear_root = 0;
	metapgno = PGNO_BASE_MD;
	ppgno = PGNO_INVALID;
	LOCK_INIT(next_lock);
	LOCK_INIT(nnext_lock);
	LOCK_INIT(saved_lock);
	LOCK_INIT(metalock);
	LOCK_INIT(prev_lock);
	check_trunc = c_data->compact_truncate != PGNO_INVALID;
	check_dups = (!F_ISSET(dbc, DBC_OPD) &&
	     F_ISSET(dbc->dbp, DB_AM_DUP)) || check_trunc;

	dbp = dbc->dbp;
	env = dbp->env;
	dbmp = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	pgs_free = c_data->compact_pages_free;

	/* Search down the tree for the starting point. */
	if ((ret = __bam_csearch(dbc,
	    start, CS_READ | CS_GETRECNO, LEAFLEVEL)) != 0) {
		/* Its not an error to compact an empty db. */
		if (ret == DB_NOTFOUND)
			ret = 0;
		isdone = 1;
		goto err;
	}

	/*
	 * Get the first leaf page. The loop below will change pg so
	 * we clear the stack reference so we don't put a a page twice.
	 */
	pg = cp->csp->page;
	cp->csp->page = NULL;
	next_recno = cp->recno;
next:	/*
	 * This is the start of the main compaction loop.  There are 3
	 * parts to the process:
	 * 1) Walk the leaf pages of the tree looking for a page to
	 *	process.  We do this with read locks.  Save the
	 *	key from the page and release it.
	 * 2) Set up a cursor stack which will write lock the page
	 *	and enough of its ancestors to get the job done.
	 *	This could go to the root if we might delete a subtree
	 *	or we have record numbers to update.
	 * 3) Loop fetching pages after the above page and move enough
	 *	data to fill it.
	 * We exit the loop if we are at the end of the leaf pages, are
	 * about to lock a new subtree (we span) or on error.
	 */

	/* Walk the pages looking for something to fill up. */
	while ((npgno = NEXT_PGNO(pg)) != PGNO_INVALID) {
		c_data->compact_pages_examine++;
		PTRACE(dbc, "Next", PGNO(pg), start, 0);

		/* If we have fetched the next page, get the new key. */
		if (next_p == 1 &&
		    dbc->dbtype != DB_RECNO && NUM_ENT(pg) != 0) {
			if ((ret = __db_ret(dbc, pg, 0, start,
			    &start->data, &start->ulen)) != 0)
				goto err;
		}
		next_recno += NUM_ENT(pg);
		if (P_FREESPACE(dbp, pg) > factor ||
		     (check_trunc && PGNO(pg) > c_data->compact_truncate))
			break;
		if (stop != NULL && stop->size > 0) {
			if ((ret = __bam_compact_isdone(dbc,
			    stop, pg, &isdone)) != 0)
				goto err;
			if (isdone)
				goto done;
		}

		/*
		 * The page does not need more data or to be swapped,
		 * check to see if we want to look at possible duplicate
		 * trees or overflow records and the move on to the next page.
		 */
		cp->recno += NUM_ENT(pg);
		next_p = 1;
		tdone = pgs_done;
		PTRACE(dbc, "Dups", PGNO(pg), start, 0);
		if (check_dups && (ret = __bam_compact_dups(
		     dbc, &pg, factor, 0, c_data, &pgs_done)) != 0)
			goto err;
		npgno = NEXT_PGNO(pg);
		if ((ret = __memp_fput(dbmp,
		     dbc->thread_info, pg, dbc->priority)) != 0)
			goto err;
		pg = NULL;
		/*
		 * If we don't do anything we don't need to hold
		 * the lock on the previous page, so couple always.
		 */
		if ((ret = __db_lget(dbc,
		    tdone == pgs_done ? LCK_COUPLE_ALWAYS : LCK_COUPLE,
		    npgno, DB_LOCK_READ, 0, &cp->csp->lock)) != 0)
			goto err;
		if ((ret = __memp_fget(dbmp, &npgno,
		     dbc->thread_info, dbc->txn, 0, &pg)) != 0)
			goto err;
	}

	/*
	 * When we get here we have 3 cases:
	 * 1) We've reached the end of the leaf linked list and are done.
	 * 2) A page whose freespace exceeds our target and therefore needs
	 *	to have data added to it.
	 * 3) A page that doesn't have too much free space but needs to be
	 *	checked for truncation.
	 * In both cases 2 and 3, we need that page's first key or record
	 * number.  We may already have it, if not get it here.
	 */
	if ((nentry = NUM_ENT(pg)) != 0) {
		/* Get a copy of the first recno on the page. */
		if (dbc->dbtype == DB_RECNO) {
			if ((ret = __db_retcopy(dbp->env, start,
			     &cp->recno, sizeof(cp->recno),
			     &start->data, &start->ulen)) != 0)
				goto err;
		} else if (((next_p == 1 && npgno == PGNO_INVALID) ||
		    start->size == 0) && (ret = __db_ret(dbc,
		    pg, 0, start, &start->data, &start->ulen)) != 0)
			goto err;

		next_p = 0;
		/*
		 * If there is no next page we can stop unless there is
		 * a possibility of moving this data to a lower numbered
		 * page.
		 */
		if (npgno == PGNO_INVALID &&
		    (!check_trunc || PGNO(pg) <= c_data->compact_truncate ||
		    PGNO(pg) == BAM_ROOT_PGNO(dbc))) {
			/* End of the tree, check its duplicates and exit. */
			PTRACE(dbc, "GoDone", PGNO(pg), start, 0);
			if (check_dups && (ret = __bam_compact_dups(dbc,
			   &pg, factor, 0, c_data, &pgs_done)) != 0)
				goto err;
			c_data->compact_pages_examine++;
			isdone = 1;
			goto done;
		}
	}

	/* Release the page so we don't deadlock getting its parent. */
	if ((ret = __memp_fput(dbmp, dbc->thread_info, pg, dbc->priority)) != 0)
		goto err;
	if ((ret = __LPUT(dbc, cp->csp->lock)) != 0)
		goto err;
	BT_STK_CLR(cp);
	pg = NULL;
	saved_pgno = PGNO_INVALID;
	prev_pgno = PGNO_INVALID;
	nnext_pgno = PGNO_INVALID;

	/*
	 * We must lock the metadata page first because we cannot block
	 * while holding interior nodes of the tree pinned.
	 */

	if (!LOCK_ISSET(metalock) && pgs_free == c_data->compact_pages_free &&
	    (ret = __db_lget(dbc,
	    LCK_ALWAYS, metapgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;

	/*
	 * Setup the cursor stack. There are 3 cases:
	 * 1) the page is empty and will be deleted: nentry == 0.
	 * 2) the next page has the same parent: *spanp == 0.
	 * 3) the next page has a different parent: *spanp == 1.
	 *
	 * We now need to search the tree again, getting a write lock
	 * on the page we are going to merge or delete.  We do this by
	 * searching down the tree and locking as much of the subtree
	 * above the page as needed.  In the case of a delete we will
	 * find the maximal subtree that can be deleted. In the case
	 * of merge if the current page and the next page are siblings
	 * with the same parent then we only need to lock the parent.
	 * Otherwise *span will be set and we need to search to find the
	 * lowest common ancestor.  Dbc will be set to contain the subtree
	 * containing the page to be merged or deleted. Ndbc will contain
	 * the minimal subtree containing that page and its next sibling.
	 * In all cases for DB_RECNO we simplify things and get the whole
	 * tree if we need more than a single parent.
	 * The tree can collapse while we don't have it locked, so the
	 * page we are looking for may be gone.  If so we are at
	 * the right most end of the leaf pages and are done.
	 */

retry:	pg = NULL;
	if (npg != NULL && (ret = __memp_fput(dbmp,
	     dbc->thread_info, npg, dbc->priority)) != 0)
		goto err;
	npg = NULL;
	if (ndbc != NULL) {
		ncp = (BTREE_CURSOR *)ndbc->internal;
		if (clear_root == 1) {
			ncp->sp->page = NULL;
			LOCK_INIT(ncp->sp->lock);
		}
		if ((ret = __bam_stkrel(ndbc, 0)) != 0)
			goto err;
	}
	clear_root = 0;
	/* Case 1 -- page is empty. */
	if (nentry == 0) {
		CTRACE(dbc, "Empty", "", start, 0);
		if (next_p == 1)
			sflag = CS_NEXT_WRITE;
		else
			sflag = CS_DEL;
		if ((ret = __bam_csearch(dbc, start, sflag, LEAFLEVEL)) != 0) {
			isdone = 1;
			if (ret == DB_NOTFOUND)
				ret = 0;
			goto err;
		}

		pg = cp->csp->page;
		/* Check to see if the page is still empty. */
		if (NUM_ENT(pg) != 0)
			npgno = PGNO(pg);
		else {
			npgno = NEXT_PGNO(pg);
			/* If this is now the root, we are very done. */
			if (PGNO(pg) == BAM_ROOT_PGNO(dbc))
				isdone = 1;
			else {
				if (npgno != PGNO_INVALID) {
					TRY_LOCK(dbc, npgno, saved_pgno,
					    next_lock, DB_LOCK_WRITE, retry);
					if (ret != 0)
						goto err;
				}
				if (PREV_PGNO(pg) != PGNO_INVALID) {
					TRY_LOCK(dbc, PREV_PGNO(pg), prev_pgno,
					    prev_lock, DB_LOCK_WRITE, retry);
					if (ret != 0)
						goto err;
				}
				if ((ret =
				    __bam_dpages(dbc, 0, BTD_RELINK)) != 0)
					goto err;
				c_data->compact_pages_free++;
				if ((ret = __TLPUT(dbc, prev_lock)) != 0)
					goto err;
				LOCK_INIT(prev_lock);
				if ((ret = __TLPUT(dbc, next_lock)) != 0)
					goto err;
				LOCK_INIT(next_lock);
				saved_pgno = PGNO_INVALID;
				goto next_no_release;
			}
		}
		goto next_page;
	}

	/* case 3 -- different parents. */
	if (*spanp) {
		CTRACE(dbc, "Span", "", start, 0);
		/*
		 * Search the tree looking for the page containing and
		 * the next page after the current key.
		 * The stack will be rooted at the page that spans
		 * the current and next pages. The two subtrees
		 * are returned below that.  For BTREE the current
		 * page subtree will be first while for RECNO the
		 * next page subtree will be first
		 */
		if (ndbc == NULL && (ret = __dbc_dup(dbc, &ndbc, 0)) != 0)
			goto err;
		DB_ASSERT(env, ndbc != NULL);
		ncp = (BTREE_CURSOR *)ndbc->internal;

		ncp->recno = cp->recno;
		cp->recno = next_recno;

		if ((ret = __bam_csearch(dbc, start, CS_NEXT_BOTH, 0)) != 0) {
			if (ret == DB_NOTFOUND) {
				isdone = 1;
				ret = 0;
			}
			goto err;
		}

		/*
		 * Find the top of the stack for the second subtree.
		 */
		for (epg = cp->csp - 1; epg > cp->sp; epg--)
			if (LEVEL(epg->page) == LEAFLEVEL)
				break;
		DB_ASSERT(env, epg != cp->sp);

		/*
		 * Copy the root. We will have two instances of the
		 * same page, be careful not to free both.
		 */
		BT_STK_PUSH(env, ncp, cp->sp->page, cp->sp->indx,
		     cp->sp->lock, cp->sp->lock_mode, ret);
		if (ret != 0)
			goto err;
		clear_root = 1;

		/* Copy the stack containing the next page. */
		for (epg++; epg <= cp->csp; epg++) {
			BT_STK_PUSH(env, ncp, epg->page, epg->indx,
			     epg->lock, epg->lock_mode, ret);
			if (ret != 0)
				goto err;
		}
		/* adjust the stack pointer to remove these items. */
		ncp->csp--;
		cp->csp -= ncp->csp - ncp->sp;

		/*
		 * If this is RECNO then we want to swap the stacks.
		 */
		if (dbc->dbtype == DB_RECNO) {
			ndbc->internal = (DBC_INTERNAL *)cp;
			dbc->internal = (DBC_INTERNAL *)ncp;
			cp = ncp;
			ncp = (BTREE_CURSOR *)ndbc->internal;
			cp->sp->indx--;
		} else
			ncp->sp->indx++;

		DB_ASSERT(env,
		    NEXT_PGNO(cp->csp->page) == PGNO(ncp->csp->page));
		pg = cp->csp->page;

		/*
		 * The page may have emptied while we waited for the
		 * lock or the record we are looking for may have
		 * moved.
		 * Reset npgno so we re-get this page when we go back
		 * to the top.
		 */
		if (NUM_ENT(pg) == 0 ||
		     (dbc->dbtype == DB_RECNO &&
		     NEXT_PGNO(cp->csp->page) != PGNO(ncp->csp->page))) {
			npgno = PGNO(pg);
			*spanp = 0;
			goto next_page;
		}

		if (check_trunc && PGNO(pg) > c_data->compact_truncate) {
			if (PREV_PGNO(pg) != PGNO_INVALID) {
				TRY_LOCK2(dbc, ndbc, PREV_PGNO(pg), prev_pgno,
				    prev_lock, DB_LOCK_WRITE, retry);
				if (ret != 0)
					goto err1;
			}
			pgs_done++;
			/* Get a fresh low numbered page. */
			if ((ret = __db_exchange_page(dbc,
			    &cp->csp->page, ncp->csp->page,
			    PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
				goto err1;
			if ((ret = __TLPUT(dbc, prev_lock)) != 0)
				goto err1;
			LOCK_INIT(prev_lock);
			pg = cp->csp->page;
		}
		*spanp = 0;
		PTRACE(dbc, "SDups", PGNO(ncp->csp->page), start, 0);
		if (check_dups && (ret = __bam_compact_dups(ndbc,
		     &ncp->csp->page, factor, 1, c_data, &pgs_done)) != 0)
			goto err1;

		DB_ASSERT(env, ndbc != NULL);
		/* Check to see if the tree collapsed. */
		/*lint -e{794} */
		if (PGNO(ncp->csp->page) == BAM_ROOT_PGNO(ndbc))
			goto done;

		pg = cp->csp->page;
		npgno = NEXT_PGNO(pg);
		PTRACE(dbc, "SDups", PGNO(pg), start, 0);
		if (check_dups && (ret =
		     __bam_compact_dups(dbc, &cp->csp->page,
		     factor, 1, c_data, &pgs_done)) != 0)
			goto err1;

		/*
		 * We may have dropped our locks, check again
		 * to see if we still need to fill this page and
		 * we are in a spanning situation.
		 */

		if (P_FREESPACE(dbp, pg) <= factor ||
		     cp->csp[-1].indx != NUM_ENT(cp->csp[-1].page) - 1)
			goto next_page;

		/*
		 * Try to move things into a single parent.
		 */
		merged = 0;
		for (epg = cp->sp; epg != cp->csp; epg++) {
			PTRACE(dbc, "PMerge", PGNO(epg->page), start, 0);
			if ((ret = __bam_merge_internal(dbc,
			    ndbc, LEVEL(epg->page), c_data, &merged)) != 0)
				break;
			if (merged)
				break;
		}

		if (ret != 0 && ret != DB_LOCK_NOTGRANTED)
			goto err1;
		/*
		 * If we merged the parent, then we nolonger span.
		 * Otherwise if we tried to merge the parent but would
		 * block on one of the other leaf pages try again.
		 * If we did not merge any records of the parent,
		 * exit to commit any local transactions and try again.
		 */
		if (merged || (pgs_done > 0 && ret == DB_LOCK_NOTGRANTED)) {
			if (merged)
				pgs_done++;
			else
				goto done;
			if (cp->csp->page == NULL)
				goto deleted;
			npgno = PGNO(pg);
			next_recno = cp->recno;
			goto next_page;
		}
		PTRACE(dbc, "SMerge", PGNO(cp->csp->page), start, 0);

		/* if we remove the next page, then we need its next locked */
		npgno = NEXT_PGNO(ncp->csp->page);
		if (npgno != PGNO_INVALID) {
			TRY_LOCK2(dbc, ndbc, npgno,
			    nnext_pgno, nnext_lock, DB_LOCK_WRITE, retry);
			if (ret != 0)
				goto err1;
		}
		/*lint -e{794} */
		if ((ret = __bam_merge(dbc,
		     ndbc, factor, stop, c_data, &isdone)) != 0)
			goto err1;
		pgs_done++;
		/*
		 * __bam_merge could have freed our stack if it
		 * deleted a page possibly collapsing the tree.
		 */
		if (cp->csp->page == NULL)
			goto deleted;
		cp->recno += NUM_ENT(pg);

		if ((ret = __TLPUT(dbc, nnext_lock)) != 0)
			goto err1;
		LOCK_INIT(nnext_lock);
		nnext_pgno = PGNO_INVALID;

		/* If we did not bump to the next page something did not fit. */
		if (npgno != NEXT_PGNO(pg)) {
			npgno = NEXT_PGNO(pg);
			goto next_page;
		}
	} else {
		/* Case 2 -- same parents. */
		CTRACE(dbc, "Sib", "", start, 0);
		if ((ret =
		    __bam_csearch(dbc, start, CS_PARENT, LEAFLEVEL)) != 0) {
			if (ret == DB_NOTFOUND) {
				isdone = 1;
				ret = 0;
			}
			goto err;
		}

		pg = cp->csp->page;
		DB_ASSERT(env, IS_DIRTY(pg));
		DB_ASSERT(env,
		    PGNO(pg) == BAM_ROOT_PGNO(dbc) ||
		    IS_DIRTY(cp->csp[-1].page));

		/* Check to see if we moved to a new parent. */
		if (PGNO(pg) != BAM_ROOT_PGNO(dbc) &&
		    ppgno != PGNO(cp->csp[-1].page) && pgs_done != 0) {
			do_commit = 1;
			goto next_page;
		}

		/* We now have a write lock, recheck the page. */
		if ((nentry = NUM_ENT(pg)) == 0) {
			npgno = PGNO(pg);
			goto next_page;
		}

		/* Check duplicate trees, we have a write lock on the page. */
		PTRACE(dbc, "SibDup", PGNO(pg), start, 0);
		if (check_dups && (ret =
		     __bam_compact_dups(dbc, &cp->csp->page,
		     factor, 1, c_data, &pgs_done)) != 0)
			goto err1;
		pg = cp->csp->page;
		npgno = NEXT_PGNO(pg);

		/* Check to see if the tree collapsed. */
		if (PGNO(pg) == BAM_ROOT_PGNO(dbc))
			goto err1;
		DB_ASSERT(env, cp->csp - cp->sp == 1);

		/* After re-locking check to see if we still need to fill. */
		if (P_FREESPACE(dbp, pg) <= factor) {
			if (check_trunc &&
			    PGNO(pg) > c_data->compact_truncate) {
				if (PREV_PGNO(pg) != PGNO_INVALID) {
					TRY_LOCK(dbc, PREV_PGNO(pg), prev_pgno,
					    prev_lock, DB_LOCK_WRITE, retry);
					if (ret != 0)
						goto err1;
				}
				if (npgno != PGNO_INVALID) {
					TRY_LOCK(dbc, npgno, saved_pgno,
					    next_lock, DB_LOCK_WRITE, retry);
					if (ret != 0)
						goto err1;
				}
				/* Get a fresh low numbered page. */
				pgno = PGNO(pg);
				if ((ret = __db_exchange_page(dbc,
				    &cp->csp->page, NULL,
				    PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
					goto err1;
				if ((ret = __TLPUT(dbc, prev_lock)) != 0)
					goto err1;
				LOCK_INIT(prev_lock);
				prev_pgno = PGNO_INVALID;
				if ((ret = __TLPUT(dbc, next_lock)) != 0)
					goto err1;
				LOCK_INIT(next_lock);
				saved_pgno = PGNO_INVALID;
				pg = cp->csp->page;
				if (pgno != PGNO(pg)) {
					pgs_done++;
					pgno = PGNO(pg);
				}
			}
			/*
			 * If we are going to leave this parent commit
			 * the current transaction before continuing.
			 */
			epg = &cp->csp[-1];
			if ((ppgno != PGNO(epg->page) &&
			    ppgno != PGNO_INVALID) ||
			    epg->indx == NUM_ENT(epg->page) - 1)
				do_commit = 1;
			ppgno = PGNO(epg->page);
			goto next_page;
		}

		/* If they have the same parent, just dup the cursor */
		if (ndbc != NULL && (ret = __dbc_close(ndbc)) != 0)
			goto err1;
		if ((ret = __dbc_dup(dbc, &ndbc, DB_POSITION)) != 0)
			goto err1;
		ncp = (BTREE_CURSOR *)ndbc->internal;

		/*
		 * ncp->recno needs to have the recno of the next page.
		 * Bump it by the number of records on the current page.
		 */
		ncp->recno += NUM_ENT(pg);
	}

	pgno = PGNO(cp->csp->page);
	ppgno = PGNO(cp->csp[-1].page);
	/* Fetch pages until we fill this one. */
	while (!isdone && npgno != PGNO_INVALID &&
	     P_FREESPACE(dbp, pg) > factor && c_data->compact_pages != 0) {
		/*
		 * merging may have to free the parent page, if it does,
		 * refetch it but do it descending the tree.
		 */
		epg = &cp->csp[-1];
		if ((ppg = epg->page) == NULL) {
			if ((ret = __memp_fput(dbmp, dbc->thread_info,
			     cp->csp->page, dbc->priority)) != 0)
				goto err1;
			pg = cp->csp->page = NULL;
			if (F_ISSET(dbc->dbp, DB_AM_READ_UNCOMMITTED) &&
				(ret = __db_lget(dbc, 0, ppgno,
				DB_LOCK_WRITE, 0, &epg->lock)) != 0)
					goto err1;
			if ((ret = __memp_fget(dbmp, &ppgno, dbc->thread_info,
			    dbc->txn, DB_MPOOL_DIRTY, &ppg)) != 0)
				goto err1;
			if (F_ISSET(dbc->dbp, DB_AM_READ_UNCOMMITTED) &&
				(ret = __db_lget(dbc, 0, pgno,
				DB_LOCK_WRITE, 0, &cp->csp->lock)) != 0)
					goto err1;
			if ((ret = __memp_fget(dbmp, &pgno, dbc->thread_info,
			    dbc->txn, DB_MPOOL_DIRTY, &pg)) != 0)
				goto err1;
			epg->page = ppg;
			cp->csp->page = pg;
		}

		/*
		 * If our current position is the last one on a parent
		 * page, then we are about to merge across different
		 * internal nodes.  Thus, we need to lock higher up
		 * in the tree.  We will exit the routine and commit
		 * what we have done so far.  Set spanp so we know
		 * we are in this case when we come back.
		 */
		if (epg->indx == NUM_ENT(ppg) - 1) {
			*spanp = 1;
			do_commit = 1;
			npgno = PGNO(pg);
			next_recno = cp->recno;
			epg->page = ppg;
			goto next_page;
		}

		/* Lock and get the next page. */
		TRY_LOCK(dbc, npgno,
		    saved_pgno, saved_lock, DB_LOCK_WRITE, retry);
		if (ret != 0)
			goto err1;
		if ((ret = __LPUT(dbc, ncp->lock)) != 0)
			goto err1;
		ncp->lock = saved_lock;
		LOCK_INIT(saved_lock);
		saved_pgno = PGNO_INVALID;

		if ((ret = __memp_fget(dbmp, &npgno,
		    dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &npg)) != 0)
			goto err1;

		if (check_trunc &&
		    PGNO(pg) > c_data->compact_truncate) {
			if (PREV_PGNO(pg) != PGNO_INVALID) {
				TRY_LOCK(dbc, PREV_PGNO(pg),
				    prev_pgno, prev_lock, DB_LOCK_WRITE, retry);
				if (ret != 0)
					goto err1;
			}
			pgno = PGNO(pg);
			/* Get a fresh low numbered page. */
			if ((ret = __db_exchange_page(dbc, &cp->csp->page,
			    npg, PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
				goto err1;
			if ((ret = __TLPUT(dbc, prev_lock)) != 0)
				goto err1;
			LOCK_INIT(prev_lock);
			prev_pgno = PGNO_INVALID;
			pg = cp->csp->page;
			if (pgno != PGNO(pg)) {
				pgs_done++;
				pgno = PGNO(pg);
			}
		}
		c_data->compact_pages_examine++;

		PTRACE(dbc, "MDups", PGNO(npg), start, 0);
		if (check_dups && (ret = __bam_compact_dups(ndbc,
		     &npg, factor, 1, c_data, &pgs_done)) != 0)
			goto err1;

		npgno = NEXT_PGNO(npg);
		if (npgno != PGNO_INVALID) {
			TRY_LOCK(dbc, npgno,
			    nnext_pgno, nnext_lock, DB_LOCK_WRITE, retry);
			if (ret != 0)
				goto err1;
		}

		/* copy the common parent to the stack. */
		BT_STK_PUSH(env, ncp, ppg,
		     epg->indx + 1, epg->lock, epg->lock_mode, ret);
		if (ret != 0)
			goto err1;

		/* Put the page on the stack. */
		BT_STK_ENTER(env, ncp, npg, 0, ncp->lock, DB_LOCK_WRITE, ret);

		LOCK_INIT(ncp->lock);
		npg = NULL;

		/*
		 * Merge the pages.  This will either free the next
		 * page or just update its parent pointer.
		 */
		PTRACE(dbc, "Merge", PGNO(cp->csp->page), start, 0);
		if ((ret = __bam_merge(dbc,
		     ndbc, factor, stop, c_data, &isdone)) != 0)
			goto err1;

		pgs_done++;

		if ((ret = __TLPUT(dbc, nnext_lock)) != 0)
			goto err1;
		LOCK_INIT(nnext_lock);
		nnext_pgno = PGNO_INVALID;

		/*
		 * __bam_merge could have freed our stack if it
		 * deleted a page possibly collapsing the tree.
		 */
		if (cp->csp->page == NULL)
			goto deleted;
		/* If we did not bump to the next page something did not fit. */
		if (npgno != NEXT_PGNO(pg))
			break;
	}

	/* Bottom of the main loop.  Move to the next page. */
	npgno = NEXT_PGNO(pg);
	cp->recno += NUM_ENT(pg);
	next_recno = cp->recno;

next_page:
	if (ndbc != NULL) {
		ncp = (BTREE_CURSOR *)ndbc->internal;
		if (ncp->sp->page == cp->sp->page) {
			ncp->sp->page = NULL;
			LOCK_INIT(ncp->sp->lock);
		}
		if ((ret = __bam_stkrel(ndbc,
		     pgs_done == 0 ? STK_NOLOCK : 0)) != 0)
			goto err;
	}
	/*
	 * Unlatch the tree before trying to lock the next page.  We must
	 * unlatch to avoid a latch deadlock but we want to hold the
	 * lock on the parent node so this leaf cannot be unlinked.
	 */
	pg = NULL;
	if ((ret = __bam_stkrel(dbc, STK_PGONLY)) != 0)
		goto err;
	if (npgno != PGNO_INVALID &&
	    (ret = __db_lget(dbc, 0, npgno, DB_LOCK_READ, 0, &next_lock)) != 0)
		goto err;
	if ((ret = __bam_stkrel(dbc, pgs_done == 0 ? STK_NOLOCK : 0)) != 0)
		goto err;
	if ((ret = __TLPUT(dbc, saved_lock)) != 0)
		goto err;
	if ((ret = __TLPUT(dbc, prev_lock)) != 0)
		goto err;

next_no_release:
	pg = NULL;

	if (npgno == PGNO_INVALID || c_data->compact_pages  == 0)
		isdone = 1;
	if (!isdone) {
		/*
		 * If we are at the end of this parent commit the
		 * transaction so we don't tie things up.
		 */
		if (do_commit && !F_ISSET(dbc, DBC_OPD) &&
		   (atomic_read(&dbp->mpf->mfp->multiversion) != 0 ||
		   pgs_done != 0)) {
deleted:		if (ndbc != NULL &&
			     ((ret = __bam_stkrel(ndbc, 0)) != 0 ||
			     (ret = __dbc_close(ndbc)) != 0))
				goto err;
			goto out;
		}

		/* Reget the next page to look at. */
		cp->recno = next_recno;
		if ((ret = __memp_fget(dbmp, &npgno,
		    dbc->thread_info, dbc->txn, 0, &pg)) != 0)
			goto err;
		cp->csp->lock = next_lock;
		LOCK_INIT(next_lock);
		next_p = 1;
		do_commit = 0;
		/* If we did not do anything we can drop the metalock. */
		if (pgs_done == 0 && (ret = __LPUT(dbc, metalock)) != 0)
			goto err;
		goto next;
	}

done:
	if (0) {
		/*
		 * We come here if pg came from cp->csp->page and could
		 * have already been fput.
		 */
err1:		pg = NULL;
	}
err:	/*
	 * Don't release locks (STK_PGONLY)if we had an error, we could reveal
	 * a bad tree to a dirty reader.  Wait till the abort to free the locks.
	 */
	sflag = STK_CLRDBC;
	if (dbc->txn != NULL && ret != 0)
		sflag |= STK_PGONLY;
	if (ndbc != NULL) {
		ncp = (BTREE_CURSOR *)ndbc->internal;
		if (npg == ncp->csp->page)
			npg = NULL;
		if (ncp->sp->page == cp->sp->page) {
			ncp->sp->page = NULL;
			LOCK_INIT(ncp->sp->lock);
		}
		if ((t_ret = __bam_stkrel(ndbc, sflag)) != 0 && ret == 0)
			ret = t_ret;
		else if ((t_ret = __dbc_close(ndbc)) != 0 && ret == 0)
			ret = t_ret;
	}
	if (pg == cp->csp->page)
		pg = NULL;
	if ((t_ret = __bam_stkrel(dbc, sflag)) != 0 && ret == 0)
		ret = t_ret;

	if ((t_ret = __TLPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;

	if (pg != NULL && (t_ret =
	     __memp_fput(dbmp,
		  dbc->thread_info, pg, dbc->priority) != 0) && ret == 0)
		ret = t_ret;
	if (npg != NULL && (t_ret =
	     __memp_fput(dbmp,
		  dbc->thread_info, npg, dbc->priority) != 0) && ret == 0)
		ret = t_ret;

out:	*donep = isdone;

	/* For OPD trees return if we did anything in the span variable. */
	if (F_ISSET(dbc, DBC_OPD))
		*spanp = pgs_done;

	return (ret);
}

/*
 * __bam_merge -- do actual merging of leaf pages.
 */
static int
__bam_merge(dbc, ndbc, factor, stop, c_data, donep)
	DBC *dbc, *ndbc;
	u_int32_t factor;
	DBT *stop;
	DB_COMPACT *c_data;
	int *donep;
{
	BTREE_CURSOR *cp, *ncp;
	DB *dbp;
	PAGE *pg, *npg;
	db_indx_t nent;
	int ret;

	DB_ASSERT(NULL, dbc != NULL);
	DB_ASSERT(NULL, ndbc != NULL);
	dbp = dbc->dbp;
	cp = (BTREE_CURSOR *)dbc->internal;
	ncp = (BTREE_CURSOR *)ndbc->internal;
	pg = cp->csp->page;
	npg = ncp->csp->page;

	nent = NUM_ENT(npg);

	/* If the page is empty just throw it away. */
	if (nent == 0)
		goto free_page;

	/* Find if the stopping point is on this page. */
	if (stop != NULL && stop->size != 0) {
		if ((ret = __bam_compact_isdone(dbc, stop, npg, donep)) != 0)
			return (ret);
		if (*donep)
			return (0);
	}

	/*
	 * If there is too much data then just move records one at a time.
	 * Otherwise copy the data space over and fix up the index table.
	 * If we are on the left most child we will effect our parent's
	 * index entry so we call merge_records to figure out key sizes.
	 */
	if ((dbc->dbtype == DB_BTREE &&
	    ncp->csp[-1].indx == 0 && ncp->csp[-1].entries != 1) ||
	    (int)(P_FREESPACE(dbp, pg) -
	    ((dbp->pgsize - P_OVERHEAD(dbp)) -
	    P_FREESPACE(dbp, npg))) < (int)factor)
		ret = __bam_merge_records(dbc, ndbc, factor, c_data);
	else
		/*lint -e{794} */
free_page:	ret = __bam_merge_pages(dbc, ndbc, c_data);

	return (ret);
}

static int
__bam_merge_records(dbc, ndbc, factor, c_data)
	DBC *dbc, *ndbc;
	u_int32_t factor;
	DB_COMPACT *c_data;
{
	BINTERNAL *bi;
	BKEYDATA *bk, *tmp_bk;
	BTREE *t;
	BTREE_CURSOR *cp, *ncp;
	DB *dbp;
	DBT a, b, data, hdr;
	ENV *env;
	EPG *epg;
	PAGE *pg, *npg;
	db_indx_t adj, indx, nent, *ninp, pind;
	int32_t adjust;
	u_int32_t freespace, len, nksize, pfree, size;
	int first_dup, is_dup, next_dup, n_ok, ret;
	size_t (*func) __P((DB *, const DBT *, const DBT *));

	dbp = dbc->dbp;
	env = dbp->env;
	t = dbp->bt_internal;
	cp = (BTREE_CURSOR *)dbc->internal;
	ncp = (BTREE_CURSOR *)ndbc->internal;
	pg = cp->csp->page;
	memset(&hdr, 0, sizeof(hdr));
	pind = NUM_ENT(pg);
	n_ok = 0;
	adjust = 0;
	ret = 0;

	/* See if we want to swap out this page. */
	if (c_data->compact_truncate != PGNO_INVALID &&
	     PGNO(ncp->csp->page) > c_data->compact_truncate) {
		/* Get a fresh low numbered page. */
		if ((ret = __db_exchange_page(ndbc,
		   &ncp->csp->page, pg, PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
			goto err;
	}

	npg = ncp->csp->page;
	nent = NUM_ENT(npg);

	DB_ASSERT(env, nent != 0);

	ninp = P_INP(dbp, npg);

	/*
	 * pg is the page that is being filled, it is in the stack in cp.
	 * npg is the next page, it is in the stack in ncp.
	 */
	freespace = P_FREESPACE(dbp, pg);

	adj = TYPE(npg) == P_LBTREE ? P_INDX : O_INDX;
	/*
	 * Loop through the records and find the stopping point.
	 */
	for (indx = 0; indx < nent; indx += adj)  {
		bk = GET_BKEYDATA(dbp, npg, indx);

		/* Size of the key. */
		size = BITEM_PSIZE(bk);

		/* Size of the data. */
		if (TYPE(pg) == P_LBTREE)
			size += BITEM_PSIZE(GET_BKEYDATA(dbp, npg, indx + 1));
		/*
		 * If we are at a duplicate set, skip ahead to see and
		 * get the total size for the group.
		 */
		n_ok = adj;
		if (TYPE(pg) == P_LBTREE &&
		     indx < nent - adj &&
		     ninp[indx] == ninp[indx + adj]) {
			do {
				/* Size of index for key reference. */
				size += sizeof(db_indx_t);
				n_ok++;
				/* Size of data item. */
				size += BITEM_PSIZE(
				    GET_BKEYDATA(dbp, npg, indx + n_ok));
				n_ok++;
			} while (indx + n_ok < nent &&
			    ninp[indx] == ninp[indx + n_ok]);
		}
		/* if the next set will not fit on the page we are done. */
		if (freespace < size)
			break;

		/*
		 * Otherwise figure out if we are past the goal and if
		 * adding this set will put us closer to the goal than
		 * we are now.
		 */
		if ((freespace - size) < factor) {
			if (freespace - factor > factor - (freespace - size))
				indx += n_ok;
			break;
		}
		freespace -= size;
		indx += n_ok - adj;
	}

	/* If we have hit the first record then there is nothing we can move. */
	if (indx == 0)
		goto done;
	if (TYPE(pg) != P_LBTREE && TYPE(pg) != P_LDUP) {
		if (indx == nent)
			return (__bam_merge_pages(dbc, ndbc, c_data));
		goto no_check;
	}
	/*
	 * We need to update npg's parent key.  Avoid creating a new key
	 * that will be too big. Get what space will be available on the
	 * parents. Then if there will not be room for this key, see if
	 * prefix compression will make it work, if not backup till we
	 * find something that will.  (Needless to say, this is a very
	 * unlikely event.)  If we are deleting this page then we will
	 * need to propagate the next key to our grand parents, so we
	 * see if that will fit.
	 */
	pfree = dbp->pgsize;
	for (epg = &ncp->csp[-1]; epg >= ncp->sp; epg--)
		if ((freespace = P_FREESPACE(dbp, epg->page)) < pfree) {
			bi = GET_BINTERNAL(dbp, epg->page, epg->indx);
			/* Add back in the key we will be deleting. */
			freespace += BINTERNAL_PSIZE(bi->len);
			if (freespace < pfree)
				pfree = freespace;
			if (epg->indx != 0)
				break;
		}

	/*
	 * If we are at the end, we will delete this page.  We need to
	 * check the next parent key only if we are the leftmost page and
	 * will therefore have to propagate the key up the tree.
	 */
	if (indx == nent) {
		if (ncp->csp[-1].indx != 0 || ncp->csp[-1].entries == 1 ||
		     BINTERNAL_PSIZE(GET_BINTERNAL(dbp,
		     ncp->csp[-1].page, 1)->len) <= pfree)
			return (__bam_merge_pages(dbc, ndbc, c_data));
		indx -= adj;
	}
	bk = GET_BKEYDATA(dbp, npg, indx);
	len = (B_TYPE(bk->type) != B_KEYDATA) ? BOVERFLOW_SIZE : bk->len;
	if (indx != 0 && BINTERNAL_SIZE(len) >= pfree) {
		if (F_ISSET(dbc, DBC_OPD)) {
			if (dbp->dup_compare == __bam_defcmp)
				func = __bam_defpfx;
			else
				func = NULL;
		} else
			func = t->bt_prefix;
	} else
		func = NULL;

	/* Skip to the beginning of a duplicate set. */
	while (indx != 0 && ninp[indx] == ninp[indx - adj])
		indx -= adj;

	while (indx != 0 && BINTERNAL_SIZE(len) >= pfree) {
		if (B_TYPE(bk->type) != B_KEYDATA)
			goto noprefix;
		/*
		 * Figure out if we can truncate this key.
		 * Code borrowed from bt_split.c
		 */
		if (func == NULL)
			goto noprefix;
		tmp_bk = GET_BKEYDATA(dbp, npg, indx - adj);
		if (B_TYPE(tmp_bk->type) != B_KEYDATA)
			goto noprefix;
		memset(&a, 0, sizeof(a));
		a.size = tmp_bk->len;
		a.data = tmp_bk->data;
		memset(&b, 0, sizeof(b));
		b.size = bk->len;
		b.data = bk->data;
		nksize = (u_int32_t)func(dbp, &a, &b);
		if (BINTERNAL_PSIZE(nksize) < pfree)
			break;
noprefix:
		/* Skip to the beginning of a duplicate set. */
		do {
			indx -= adj;
		} while (indx != 0 &&  ninp[indx] == ninp[indx - adj]);

		bk = GET_BKEYDATA(dbp, npg, indx);
		len =
		    (B_TYPE(bk->type) != B_KEYDATA) ? BOVERFLOW_SIZE : bk->len;
	}

	/*
	 * indx references the first record that will not move to the previous
	 * page.  If it is 0 then we could not find a key that would fit in
	 * the parent that would permit us to move any records.
	 */
	if (indx == 0)
		goto done;
	DB_ASSERT(env, indx <= nent);

	/* Loop through the records and move them from npg to pg. */
no_check: is_dup = first_dup = next_dup = 0;
	pg = cp->csp->page;
	npg = ncp->csp->page;
	DB_ASSERT(env, IS_DIRTY(pg));
	DB_ASSERT(env, IS_DIRTY(npg));
	ninp = P_INP(dbp, npg);
	do {
		bk = GET_BKEYDATA(dbp, npg, 0);
		/* Figure out if we are in a duplicate group or not. */
		if ((NUM_ENT(npg) % 2) == 0) {
			if (NUM_ENT(npg) > 2 && ninp[0] == ninp[2]) {
				if (!is_dup) {
					first_dup = 1;
					is_dup = 1;
				} else
					first_dup = 0;

				next_dup = 1;
			} else if (next_dup) {
				is_dup = 1;
				first_dup = 0;
				next_dup = 0;
			} else
				is_dup = 0;
		}

		if (is_dup && !first_dup && (pind % 2) == 0) {
			/* Duplicate key. */
			if ((ret = __bam_adjindx(dbc,
			     pg, pind, pind - P_INDX, 1)) != 0)
				goto err;
			if (!next_dup)
				is_dup = 0;
		} else switch (B_TYPE(bk->type)) {
		case B_KEYDATA:
			hdr.data = bk;
			hdr.size = SSZA(BKEYDATA, data);
			data.size = bk->len;
			data.data = bk->data;
			if ((ret = __db_pitem(dbc, pg, pind,
			     BKEYDATA_SIZE(bk->len), &hdr, &data)) != 0)
				goto err;
			break;
		case B_OVERFLOW:
		case B_DUPLICATE:
			data.size = BOVERFLOW_SIZE;
			data.data = bk;
			if ((ret = __db_pitem(dbc, pg, pind,
			     BOVERFLOW_SIZE, &data, NULL)) != 0)
				goto err;
			break;
		default:
			__db_errx(env, DB_STR_A("1022",
			    "Unknown record format, page %lu, indx 0",
			    "%lu"), (u_long)PGNO(pg));
			ret = EINVAL;
			goto err;
		}
		pind++;
		if (next_dup && (NUM_ENT(npg) % 2) == 0) {
			if ((ret = __bam_adjindx(ndbc,
			     npg, 0, O_INDX, 0)) != 0)
				goto err;
		} else {
			if ((ret = __db_ditem(ndbc,
			     npg, 0, BITEM_SIZE(bk))) != 0)
				goto err;
		}
		adjust++;
	} while (--indx != 0);

	DB_ASSERT(env, NUM_ENT(npg) != 0);

	if (adjust != 0 &&
	     (F_ISSET(cp, C_RECNUM) || F_ISSET(dbc, DBC_OPD))) {
		if (TYPE(pg) == P_LBTREE)
			adjust /= P_INDX;
		if ((ret = __bam_adjust(ndbc, -adjust)) != 0)
			goto err;

		if ((ret = __bam_adjust(dbc, adjust)) != 0)
			goto err;
	}

	/* Update parent with new key. */
	if (ndbc->dbtype == DB_BTREE &&
	    (ret = __bam_pupdate(ndbc, pg)) != 0)
		goto err;

done:	if (cp->sp->page == ncp->sp->page) {
		cp->sp->page = NULL;
		LOCK_INIT(cp->sp->lock);
	}
	ret = __bam_stkrel(ndbc, STK_CLRDBC);

err:	return (ret);
}

static int
__bam_merge_pages(dbc, ndbc, c_data)
	DBC *dbc, *ndbc;
	DB_COMPACT *c_data;
{
	BTREE_CURSOR *cp, *ncp;
	DB *dbp;
	DBT data, hdr;
	DB_LOCK root_lock;
	DB_MPOOLFILE *dbmp;
	PAGE *pg, *npg;
	db_indx_t nent, *ninp, *pinp;
	db_pgno_t pgno, ppgno;
	u_int8_t *bp;
	u_int32_t len;
	int i, level, ret;

	LOCK_INIT(root_lock);
	COMPQUIET(ppgno, PGNO_INVALID);
	dbp = dbc->dbp;
	dbmp = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	ncp = (BTREE_CURSOR *)ndbc->internal;
	pg = cp->csp->page;
	npg = ncp->csp->page;
	memset(&hdr, 0, sizeof(hdr));
	nent = NUM_ENT(npg);

	/* If the page is empty just throw it away. */
	if (nent == 0)
		goto free_page;

	pg = cp->csp->page;
	npg = ncp->csp->page;
	DB_ASSERT(dbp->env, IS_DIRTY(pg));
	DB_ASSERT(dbp->env, IS_DIRTY(npg));
	DB_ASSERT(dbp->env, nent == NUM_ENT(npg));

	/* Bulk copy the data to the new page. */
	len = dbp->pgsize - HOFFSET(npg);
	if (DBC_LOGGING(dbc)) {
		memset(&hdr, 0, sizeof(hdr));
		hdr.data = npg;
		hdr.size = LOFFSET(dbp, npg);
		memset(&data, 0, sizeof(data));
		data.data = (u_int8_t *)npg + HOFFSET(npg);
		data.size = len;
		if ((ret = __db_merge_log(dbp,
		     dbc->txn, &LSN(pg), 0, PGNO(pg),
		     &LSN(pg), PGNO(npg), &LSN(npg), &hdr, &data, 0)) != 0)
			goto err;
	} else
		LSN_NOT_LOGGED(LSN(pg));
	LSN(npg) = LSN(pg);
	bp = (u_int8_t *)pg + HOFFSET(pg) - len;
	memcpy(bp, (u_int8_t *)npg + HOFFSET(npg), len);

	/* Copy index table offset by what was there already. */
	pinp = P_INP(dbp, pg) + NUM_ENT(pg);
	ninp = P_INP(dbp, npg);
	for (i = 0; i < NUM_ENT(npg); i++)
		*pinp++ = *ninp++ - (dbp->pgsize - HOFFSET(pg));
	HOFFSET(pg) -= len;
	NUM_ENT(pg) += i;

	NUM_ENT(npg) = 0;
	HOFFSET(npg) += len;

	if (F_ISSET(cp, C_RECNUM) || F_ISSET(dbc, DBC_OPD)) {
		/*
		 * There are two cases here regarding the stack.
		 * Either we have two two level stacks but only ndbc
		 * references the parent page or we have a multilevel
		 * stack and only ndbc has an entry for the spanning
		 * page.
		 */
		if (TYPE(pg) == P_LBTREE)
			i /= P_INDX;
		if ((ret = __bam_adjust(ndbc, -i)) != 0)
			goto err;

		if ((ret = __bam_adjust(dbc, i)) != 0)
			goto err;
	}

free_page:
	/*
	 * __bam_dpages may decide to collapse the tree.
	 * This can happen if we have the root and there
	 * are exactly 2 pointers left in it.
	 * If it can collapse the tree we must free the other
	 * stack since it will nolonger be valid.  This
	 * must be done before hand because we cannot
	 * hold a page pinned if it might be truncated.
	 */
	if ((ret = __db_relink(dbc,
	    ncp->csp->page, cp->csp->page, PGNO_INVALID)) != 0)
		goto err;
	/* Drop the duplicate reference to the sub tree root. */
	cp->sp->page = NULL;
	LOCK_INIT(cp->sp->lock);
	if (PGNO(ncp->sp->page) == BAM_ROOT_PGNO(ndbc) &&
	    NUM_ENT(ncp->sp->page) == 2) {
		if ((ret = __bam_stkrel(dbc, STK_CLRDBC | STK_PGONLY)) != 0)
			goto err;
		level = LEVEL(ncp->sp->page);
		ppgno = PGNO(ncp->csp[-1].page);
	} else
		level = 0;
	COMPACT_TRUNCATE(c_data);
	if ((ret = __bam_dpages(ndbc,
	    0, ndbc->dbtype == DB_RECNO ? 0 : BTD_UPDATE)) != 0)
		goto err;
	npg = NULL;
	c_data->compact_pages_free++;
	c_data->compact_pages--;
	if (level != 0) {
		pgno = PGNO_INVALID;
		BAM_GET_ROOT(ndbc, pgno, npg, 0, DB_LOCK_READ, root_lock, ret);
		if (ret != 0)
			goto err;
		DB_ASSERT(dbp->env, npg != NULL);
		if (level == LEVEL(npg))
			level = 0;
		if ((ret = __memp_fput(dbmp,
		     dbc->thread_info, npg, dbc->priority)) != 0)
			goto err;
		if ((ret = __LPUT(ndbc, root_lock)) != 0)
			goto err;
		npg = NULL;
		if (level != 0) {
			c_data->compact_levels++;
			c_data->compact_pages_free++;
			COMPACT_TRUNCATE(c_data);
			if (c_data->compact_pages != 0)
				c_data->compact_pages--;
		}
	}

err:	return (ret);
}

/*
 * __bam_merge_internal --
 *	Merge internal nodes of the tree.
 */
static int
__bam_merge_internal(dbc, ndbc, level, c_data, merged)
	DBC *dbc, *ndbc;
	int level;
	DB_COMPACT *c_data;
	int *merged;
{
	BINTERNAL bi, *bip, *fip;
	BTREE_CURSOR *cp, *ncp;
	DB *dbp;
	DBT data, hdr;
	DB_LOCK root_lock;
	DB_MPOOLFILE *dbmp;
	EPG *epg, *save_csp, *nsave_csp;
	PAGE *pg, *npg;
	RINTERNAL *rk;
	db_indx_t first, indx, pind;
	db_pgno_t pgno, ppgno;
	int32_t nrecs, trecs;
	u_int16_t size;
	u_int32_t freespace, pfree;
	int ret;

	COMPQUIET(bip, NULL);
	COMPQUIET(ppgno, PGNO_INVALID);
	DB_ASSERT(NULL, dbc != NULL);
	DB_ASSERT(NULL, ndbc != NULL);
	LOCK_INIT(root_lock);

	/*
	 * ndbc will contain the the dominating parent of the subtree.
	 * dbc will have the tree containing the left child.
	 *
	 * The stacks descend to the leaf level.
	 * If this is a recno tree then both stacks will start at the root.
	 */
	dbp = dbc->dbp;
	dbmp = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	ncp = (BTREE_CURSOR *)ndbc->internal;
	*merged = 0;
	ret = 0;

	/*
	 * Set the stacks to the level requested.
	 * Save the old value to restore when we exit.
	 */
	save_csp = cp->csp;
	cp->csp = &cp->csp[-level + 1];
	pg = cp->csp->page;
	pind = NUM_ENT(pg);

	nsave_csp = ncp->csp;
	ncp->csp = &ncp->csp[-level + 1];
	npg = ncp->csp->page;
	indx = NUM_ENT(npg);

	/*
	 * The caller may have two stacks that include common ancestors, we
	 * check here for convenience.
	 */
	if (npg == pg)
		goto done;

	if (TYPE(pg) == P_IBTREE) {
		/*
		 * Check for overflow keys on both pages while we have
		 * them locked.
		 */
		 if ((ret =
		      __bam_truncate_internal_overflow(dbc, pg, c_data)) != 0)
			goto err;
		 if ((ret =
		      __bam_truncate_internal_overflow(dbc, npg, c_data)) != 0)
			goto err;
	}

	/*
	 * If we are about to move data off the left most page of an
	 * internal node we will need to update its parents, make sure there
	 * will be room for the new key on all the parents in the stack.
	 * If not, move less data.
	 */
	fip = NULL;
	if (TYPE(pg) == P_IBTREE) {
		/* See where we run out of space. */
		freespace = P_FREESPACE(dbp, pg);
		/*
		 * The leftmost key of an internal page is not accurate.
		 * Go up the tree to find a non-leftmost parent.
		 */
		epg = ncp->csp;
		while (--epg >= ncp->sp && epg->indx == 0)
			continue;
		fip = bip = GET_BINTERNAL(dbp, epg->page, epg->indx);
		epg = ncp->csp;

		for (indx = 0;;) {
			size = BINTERNAL_PSIZE(bip->len);
			if (size > freespace)
				break;
			freespace -= size;
			if (++indx >= NUM_ENT(npg))
				break;
			bip = GET_BINTERNAL(dbp, npg, indx);
		}

		/* See if we are deleting the page and we are not left most. */
		if (indx == NUM_ENT(npg) && epg[-1].indx != 0)
			goto fits;

		pfree = dbp->pgsize;
		for (epg--; epg >= ncp->sp; epg--)
			if ((freespace = P_FREESPACE(dbp, epg->page)) < pfree) {
				bip = GET_BINTERNAL(dbp, epg->page, epg->indx);
				/* Add back in the key we will be deleting. */
				freespace += BINTERNAL_PSIZE(bip->len);
				if (freespace < pfree)
					pfree = freespace;
				if (epg->indx != 0)
					break;
			}
		epg = ncp->csp;

		/* If we are at the end of the page we will delete it. */
		if (indx == NUM_ENT(npg)) {
			if (NUM_ENT(epg[-1].page) == 1)
				goto fits;
			bip =
			     GET_BINTERNAL(dbp, epg[-1].page, epg[-1].indx + 1);
		} else
			bip = GET_BINTERNAL(dbp, npg, indx);

		/* Back up until we have a key that fits. */
		while (indx != 0 && BINTERNAL_PSIZE(bip->len) > pfree) {
			indx--;
			bip = GET_BINTERNAL(dbp, npg, indx);
		}
		if (indx == 0)
			goto done;
	}

fits:	memset(&bi, 0, sizeof(bi));
	memset(&hdr, 0, sizeof(hdr));
	memset(&data, 0, sizeof(data));
	trecs = 0;

	/*
	 * Copy data between internal nodes till one is full
	 * or the other is empty.
	 */
	first = 0;
	nrecs = 0;
	do {
		if (dbc->dbtype == DB_BTREE) {
			bip = GET_BINTERNAL(dbp, npg, 0);
			size = fip == NULL ?
			     BINTERNAL_SIZE(bip->len) :
			     BINTERNAL_SIZE(fip->len);
			if (P_FREESPACE(dbp, pg) < size + sizeof(db_indx_t))
				break;

			if (fip == NULL) {
				data.size = bip->len;
				data.data = bip->data;
			} else {
				data.size = fip->len;
				data.data = fip->data;
			}
			bi.len = data.size;
			B_TSET(bi.type, bip->type);
			bi.pgno = bip->pgno;
			bi.nrecs = bip->nrecs;
			hdr.data = &bi;
			hdr.size = SSZA(BINTERNAL, data);
			if (F_ISSET(cp, C_RECNUM) || F_ISSET(dbc, DBC_OPD))
				nrecs = (int32_t)bip->nrecs;
		} else {
			rk = GET_RINTERNAL(dbp, npg, 0);
			size = RINTERNAL_SIZE;
			if (P_FREESPACE(dbp, pg) < size + sizeof(db_indx_t))
				break;

			hdr.data = rk;
			hdr.size = size;
			nrecs = (int32_t)rk->nrecs;
		}
		/*
		 * Try to lock the subtree leaf records without waiting.
		 * We must lock the subtree below the record we are merging
		 * and the one after it since that is were a search will wind
		 * up if it has already looked at our parent.  After the first
		 * move we have the current subtree already locked.
		 * If we merged any records then we will revisit this
		 * node when we merge its leaves.  If not we will return
		 * NOTGRANTED and our caller will do a retry.  We only
		 * need to do this if we are in a transaction. If not then
		 * we cannot abort and things will be hosed up on error
		 * anyway.
		 */
		if (dbc->txn != NULL && (ret = __bam_lock_tree(ndbc,
		    ncp->csp, nsave_csp, first,
		    NUM_ENT(ncp->csp->page) == 1 ? 1 : 2)) != 0) {
			if (ret != DB_LOCK_NOTGRANTED)
				goto err;
			break;
		}
		first = 1;
		if ((ret = __db_pitem(dbc, pg, pind, size, &hdr, &data)) != 0)
			goto err;
		pind++;
		if (fip != NULL) {
			/* reset size to be for the record being deleted. */
			size = BINTERNAL_SIZE(bip->len);
			fip = NULL;
		}
		if ((ret = __db_ditem(ndbc, npg, 0, size)) != 0)
			goto err;
		*merged = 1;
		trecs += nrecs;
	} while (--indx != 0);

	if (!*merged)
		goto done;

	if (trecs != 0) {
		cp->csp--;
		ret = __bam_adjust(dbc, trecs);
		if (ret != 0)
			goto err;
		cp->csp++;
		ncp->csp--;
		if ((ret = __bam_adjust(ndbc, -trecs)) != 0)
			goto err;
		ncp->csp++;
	}

	/*
	 * Either we emptied the page or we need to update its
	 * parent to reflect the first page we now point to.
	 * First get rid of the bottom of the stack,
	 * bam_dpages will clear the stack.  Maintain transactional
	 * locks on the leaf pages to protect changes at this level.
	 */
	do {
		if ((ret = __memp_fput(dbmp, dbc->thread_info,
		    nsave_csp->page, dbc->priority)) != 0)
			goto err;
		nsave_csp->page = NULL;
		if ((ret = __TLPUT(dbc, nsave_csp->lock)) != 0)
			goto err;
		LOCK_INIT(nsave_csp->lock);
		nsave_csp--;
	} while (nsave_csp != ncp->csp);

	if (NUM_ENT(npg) == 0)  {
		/*
		 * __bam_dpages may decide to collapse the tree
		 * so we need to free our other stack.  The tree
		 * will change in height and our stack will nolonger
		 * be valid.
		 */
		cp->csp = save_csp;
		cp->sp->page = NULL;
		LOCK_INIT(cp->sp->lock);
		if (PGNO(ncp->sp->page) == BAM_ROOT_PGNO(ndbc) &&
		    NUM_ENT(ncp->sp->page) == 2) {
			if ((ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0)
				goto err;
			level = LEVEL(ncp->sp->page);
			ppgno = PGNO(ncp->csp[-1].page);
		} else
			level = 0;

		COMPACT_TRUNCATE(c_data);
		ret = __bam_dpages(ndbc,
		     0, ndbc->dbtype == DB_RECNO ?
		     BTD_RELINK : BTD_UPDATE | BTD_RELINK);
		c_data->compact_pages_free++;
		if (ret == 0 && level != 0) {
			pgno = PGNO_INVALID;
			BAM_GET_ROOT(ndbc,
			    pgno, npg, 0, DB_LOCK_READ, root_lock, ret);
			if (ret != 0)
				goto err;
			if (level == LEVEL(npg))
				level = 0;
			if ((ret = __LPUT(ndbc, root_lock)) != 0)
				goto err;
			if ((ret = __memp_fput(dbmp,
			    dbc->thread_info, npg, dbc->priority)) != 0)
				goto err;
			npg = NULL;
			if (level != 0) {
				c_data->compact_levels++;
				c_data->compact_pages_free++;
				COMPACT_TRUNCATE(c_data);
				if (c_data->compact_pages != 0)
					c_data->compact_pages--;
			}
		}
	} else {
		ret = __bam_pupdate(ndbc, npg);

		if (NUM_ENT(npg) != 0 &&
		    c_data->compact_truncate != PGNO_INVALID &&
		    PGNO(npg) > c_data->compact_truncate &&
		    ncp->csp != ncp->sp) {
			if ((ret = __db_exchange_page(ndbc, &ncp->csp->page,
			    pg, PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
				goto err;
		}
		if (c_data->compact_truncate != PGNO_INVALID &&
		     PGNO(pg) > c_data->compact_truncate && cp->csp != cp->sp) {
			if ((ret = __db_exchange_page(dbc, &cp->csp->page,
			    ncp->csp->page,
			    PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
				goto err;
		}
	}
	cp->csp = save_csp;

	return (ret);

done:
err:	cp->csp = save_csp;
	ncp->csp = nsave_csp;

	return (ret);
}

/*
 * __bam_compact_dups -- try to compress off page dup trees.
 * We may or may not have a write lock on this page.
 */
static int
__bam_compact_dups(dbc, ppg, factor, have_lock, c_data, donep)
	DBC *dbc;
	PAGE **ppg;
	u_int32_t factor;
	int have_lock;
	DB_COMPACT *c_data;
	int *donep;
{
	BOVERFLOW *bo;
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_MPOOLFILE *dbmp;
	db_indx_t i;
	db_pgno_t pgno;
	int ret;

	ret = 0;

	DB_ASSERT(NULL, dbc != NULL);
	dbp = dbc->dbp;
	dbmp = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	for (i = 0; i <  NUM_ENT(*ppg); i++) {
		bo = GET_BOVERFLOW(dbp, *ppg, i);
		if (B_TYPE(bo->type) == B_KEYDATA)
			continue;
		c_data->compact_pages_examine++;
		if (bo->pgno > c_data->compact_truncate) {
			(*donep)++;
			if (!have_lock) {
				/*
				 * The caller should have the page at
				 * least read locked.  Drop the buffer
				 * and get the write lock.
				 */
				pgno = PGNO(*ppg);
				if ((ret = __memp_fput(dbmp, dbc->thread_info,
				     *ppg, dbc->priority)) != 0)
					goto err;
				*ppg = NULL;
				if ((ret = __db_lget(dbc, 0, pgno,
				     DB_LOCK_WRITE, 0, &cp->csp->lock)) != 0)
					goto err;
				have_lock = 1;
				if ((ret = __memp_fget(dbmp, &pgno,
				    dbc->thread_info,
				    dbc->txn, DB_MPOOL_DIRTY, ppg)) != 0)
					goto err;
			}
			if ((ret = __bam_truncate_root_page(dbc,
			     *ppg, i, c_data)) != 0)
				goto err;
			/* Just in case it should move.  Could it? */
			bo = GET_BOVERFLOW(dbp, *ppg, i);
		}

		if (B_TYPE(bo->type) == B_OVERFLOW) {
			if ((ret = __db_truncate_overflow(dbc,
			    bo->pgno, have_lock ? NULL : ppg, c_data)) != 0)
				goto err;
			(*donep)++;
			continue;
		}
		if ((ret = __bam_compact_opd(dbc, bo->pgno,
		    have_lock ? NULL : ppg, factor, c_data, donep)) != 0)
			goto err;
	}

err:
	return (ret);
}

/*
 * __bam_compact_opd -- compact an off page duplicate tree.
 *
 * PUBLIC: int __bam_compact_opd __P((DBC *,
 * PUBLIC:      db_pgno_t, PAGE **, u_int32_t, DB_COMPACT *, int *));
 */
int
__bam_compact_opd(dbc, root_pgno, ppg, factor, c_data, donep)
	DBC *dbc;
	db_pgno_t root_pgno;
	PAGE **ppg;
	u_int32_t factor;
	DB_COMPACT *c_data;
	int *donep;
{
	BTREE_CURSOR *cp;
	DBC *opd;
	DBT start;
	DB_MPOOLFILE *dbmp;
	ENV *env;
	PAGE *dpg;
	int isdone, level, ret, span, t_ret;
	db_pgno_t pgno;

	LOCK_CHECK_OFF(dbc->thread_info);

	opd = NULL;
	env = dbc->dbp->env;
	dbmp = dbc->dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * Take a peek at the root.  If it's a leaf then
	 * there is no tree here, avoid all the trouble.
	 */
	if ((ret = __memp_fget(dbmp, &root_pgno,
	     dbc->thread_info, dbc->txn, 0, &dpg)) != 0)
		goto err;

	level = dpg->level;
	if ((ret = __memp_fput(dbmp,
	     dbc->thread_info, dpg, dbc->priority)) != 0)
		goto err;
	if (level == LEAFLEVEL)
		goto done;
	if ((ret = __dbc_newopd(dbc, root_pgno, NULL, &opd)) != 0)
		goto err;
	if (ppg != NULL) {
		/*
		 * The caller should have the page at
		 * least read locked.  Drop the buffer
		 * and get the write lock.
		 */
		pgno = PGNO(*ppg);
		if ((ret = __memp_fput(dbmp, dbc->thread_info,
		     *ppg, dbc->priority)) != 0)
			goto err;
		*ppg = NULL;
		if ((ret = __db_lget(dbc, 0, pgno,
		     DB_LOCK_WRITE, 0, &cp->csp->lock)) != 0)
			goto err;
		if ((ret = __memp_fget(dbmp, &pgno,
		    dbc->thread_info,
		    dbc->txn, DB_MPOOL_DIRTY, ppg)) != 0)
			goto err;
	}
	memset(&start, 0, sizeof(start));
	do {
		span = 0;
		if ((ret = __bam_compact_int(opd, &start,
		     NULL, factor, &span, c_data, &isdone)) != 0)
			break;
		/* For OPD the number of pages dirtied is returned in span. */
		*donep += span;
	} while (!isdone);

	if (start.data != NULL)
		__os_free(env, start.data);

err:	if (opd != NULL && (t_ret = __dbc_close(opd)) != 0 && ret == 0)
		ret = t_ret;
done:
	LOCK_CHECK_ON(dbc->thread_info);

	return (ret);
}

/*
 * __bam_truncate_root_page -- swap a page which is
 *    the root of an off page dup tree or the head of an overflow.
 * The page is reference by the pg/indx passed in.
 */
static int
__bam_truncate_root_page(dbc, pg, indx, c_data)
	DBC *dbc;
	PAGE *pg;
	u_int32_t indx;
	DB_COMPACT *c_data;
{
	BINTERNAL *bi;
	BOVERFLOW *bo;
	DB *dbp;
	db_pgno_t *pgnop;
	u_int32_t tlen;

	COMPQUIET(c_data, NULL);
	COMPQUIET(bo, NULL);
	dbp = dbc->dbp;
	if (TYPE(pg) == P_IBTREE) {
		bi = GET_BINTERNAL(dbp, pg, indx);
		if (B_TYPE(bi->type) == B_OVERFLOW) {
			bo = (BOVERFLOW *)(bi->data);
			pgnop = &bo->pgno;
			tlen = bo->tlen;
		} else {
			/* Tlen is not used if this is not an overflow. */
			tlen = 0;
			pgnop = &bi->pgno;
		}
	} else {
		bo = GET_BOVERFLOW(dbp, pg, indx);
		pgnop = &bo->pgno;
		tlen = bo->tlen;
	}

	DB_ASSERT(dbp->env, IS_DIRTY(pg));

	return (__db_truncate_root(dbc, pg, indx, pgnop, tlen));
}

/*
 * -- bam_truncate_internal_overflow -- find overflow keys
 *	on internal pages and if they have high page
 * numbers swap them with lower pages and truncate them.
 * Note that if there are overflow keys in the internal
 * nodes they will get copied adding pages to the database.
 */
static int
__bam_truncate_internal_overflow(dbc, page, c_data)
	DBC *dbc;
	PAGE *page;
	DB_COMPACT *c_data;
{
	BINTERNAL *bi;
	BOVERFLOW *bo;
	db_indx_t indx;
	int ret;

	COMPQUIET(bo, NULL);
	ret = 0;
	for (indx = 0; indx < NUM_ENT(page); indx++) {
		bi = GET_BINTERNAL(dbc->dbp, page, indx);
		if (B_TYPE(bi->type) != B_OVERFLOW)
			continue;
		bo = (BOVERFLOW *)(bi->data);
		if (bo->pgno > c_data->compact_truncate && (ret =
		     __bam_truncate_root_page(dbc, page, indx, c_data)) != 0)
			break;
		if ((ret = __db_truncate_overflow(
		     dbc, bo->pgno, NULL, c_data)) != 0)
			break;
	}
	return (ret);
}

/*
 * __bam_compact_isdone ---
 *
 * Check to see if the stop key specified by the caller is on the
 * current page, in which case we are done compacting.
 */
static int
__bam_compact_isdone(dbc, stop, pg, isdone)
	DBC *dbc;
	DBT *stop;
	PAGE *pg;
	int *isdone;
{
	db_recno_t recno;
	BTREE *t;
	BTREE_CURSOR *cp;
	int cmp, ret;

	*isdone = 0;
	cp = (BTREE_CURSOR *)dbc->internal;
	t = dbc->dbp->bt_internal;

	if (dbc->dbtype == DB_RECNO) {
		if ((ret = __ram_getno(dbc, stop, &recno, 0)) != 0)
			return (ret);
		*isdone = cp->recno > recno;
	} else {
		DB_ASSERT(dbc->dbp->env, TYPE(pg) == P_LBTREE);
		if ((ret = __bam_cmp(dbc, stop, pg, 0,
		    t->bt_compare, &cmp)) != 0)
			return (ret);

		*isdone = cmp <= 0;
	}
	return (0);
}

/*
 * Lock the subtrees from the top of the stack.
 *	The 0'th child may be in the stack and locked otherwise iterate
 * through the records by calling __bam_lock_subtree.
 */
static int
__bam_lock_tree(dbc, sp, csp, start, stop)
	DBC *dbc;
	EPG *sp, *csp;
	u_int32_t start, stop;
{
	PAGE *cpage;
	db_pgno_t pgno;
	int ret;

	if (dbc->dbtype == DB_RECNO)
		pgno = GET_RINTERNAL(dbc->dbp, sp->page, 0)->pgno;
	else
		pgno = GET_BINTERNAL(dbc->dbp, sp->page, 0)->pgno;
	cpage = (sp + 1)->page;
	/*
	 * First recurse down the left most sub tree if it is in the cursor
	 * stack.  We already have these pages latched and locked if its a
	 * leaf.
	 */
	if (start == 0 && sp + 1 != csp && pgno == PGNO(cpage) &&
	    (ret = __bam_lock_tree(dbc, sp + 1, csp, 0, NUM_ENT(cpage))) != 0)
		return (ret);

	/*
	 * Then recurse on the other records on the page if needed.
	 * If the page is in the stack then its already locked or
	 * was processed above.
	 */
	if (start == 0 && pgno == PGNO(cpage))
		start = 1;

	if (start == stop)
		return (0);
	return (__bam_lock_subtree(dbc, sp->page, start, stop));

}

/*
 * Lock the subtree from the current node.
 */
static int
__bam_lock_subtree(dbc, page, indx, stop)
	DBC *dbc;
	PAGE *page;
	u_int32_t indx, stop;
{
	DB *dbp;
	DB_LOCK lock;
	PAGE *cpage;
	db_pgno_t pgno;
	int ret, t_ret;

	dbp = dbc->dbp;

	for (; indx < stop; indx++) {
		if (dbc->dbtype == DB_RECNO)
			pgno = GET_RINTERNAL(dbc->dbp, page, indx)->pgno;
		else
			pgno = GET_BINTERNAL(dbc->dbp, page, indx)->pgno;
		if (LEVEL(page) - 1 == LEAFLEVEL) {
			if ((ret = __db_lget(dbc, 0, pgno,
			     DB_LOCK_WRITE, DB_LOCK_NOWAIT, &lock)) != 0) {
				if (ret == DB_LOCK_DEADLOCK)
					return (DB_LOCK_NOTGRANTED);
				return (ret);
			}
		} else {
			if ((ret = __memp_fget(dbp->mpf, &pgno,
			     dbc->thread_info, dbc->txn, 0, &cpage)) != 0)
				return (ret);
			ret = __bam_lock_subtree(dbc, cpage, 0, NUM_ENT(cpage));
			if ((t_ret = __memp_fput(dbp->mpf, dbc->thread_info,
			    cpage, dbc->priority)) != 0 && ret == 0)
				ret = t_ret;
			if (ret != 0)
				return (ret);
		}
	}
	return (0);
}

#ifdef HAVE_FTRUNCATE
/*
 * __bam_savekey -- save the key from an internal page.
 *  We need to save information so that we can
 * fetch then next internal node of the tree.  This means
 * we need the btree key on this current page, or the
 * next record number.
 */
static int
__bam_savekey(dbc, next, start)
	DBC *dbc;
	int next;
	DBT *start;
{
	BINTERNAL *bi;
	BKEYDATA *bk;
	BOVERFLOW *bo;
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_LOCK lock;
	ENV *env;
	PAGE *pg;
	RINTERNAL *ri;
	db_indx_t indx, top;
	db_pgno_t pgno, saved_pgno;
	int ret, t_ret;
	u_int32_t len;
	u_int8_t *data;
	int level;

	dbp = dbc->dbp;
	env = dbp->env;
	cp = (BTREE_CURSOR *)dbc->internal;
	pg = cp->csp->page;
	ret = 0;

	if (dbc->dbtype == DB_RECNO) {
		if (next)
			for (indx = 0, top = NUM_ENT(pg); indx != top; indx++) {
				ri = GET_RINTERNAL(dbp, pg, indx);
				cp->recno += ri->nrecs;
			}
		return (__db_retcopy(env, start, &cp->recno,
		     sizeof(cp->recno), &start->data, &start->ulen));

	}

	bi = GET_BINTERNAL(dbp, pg, NUM_ENT(pg) - 1);
	data = bi->data;
	len = bi->len;
	LOCK_INIT(lock);
	saved_pgno = PGNO_INVALID;
	/* If there is single record on the page it may have an empty key. */
	while (len == 0) {
		/*
		 * We should not have an empty data page, since we just
		 * compacted things, check anyway and punt.
		 */
		if (NUM_ENT(pg) == 0)
			goto no_key;
		pgno = bi->pgno;
		level = LEVEL(pg);
		if (pg != cp->csp->page &&
		    (ret = __memp_fput(dbp->mpf,
			 dbc->thread_info, pg, dbc->priority)) != 0) {
			pg = NULL;
			goto err;
		}
		pg = NULL;
		if (level - 1 == LEAFLEVEL) {
			TRY_LOCK(dbc, pgno, saved_pgno,
			    lock, DB_LOCK_READ, retry);
			if (ret != 0)
				goto err;
		}
		if ((ret = __memp_fget(dbp->mpf, &pgno,
		     dbc->thread_info, dbc->txn, 0, &pg)) != 0)
			goto err;

		/*
		 * At the data level use the last key to try and avoid the
		 * possibility that the user has a zero length key, if they
		 * do, we punt.
		 */
		if (pg->level == LEAFLEVEL) {
			bk = GET_BKEYDATA(dbp, pg, NUM_ENT(pg) - 2);
			data = bk->data;
			len = bk->len;
			if (len == 0) {
no_key:				__db_errx(env, DB_STR("1023",
				    "Compact cannot handle zero length key"));
				ret = DB_NOTFOUND;
				goto err;
			}
		} else {
			bi = GET_BINTERNAL(dbp, pg, NUM_ENT(pg) - 1);
			data = bi->data;
			len = bi->len;
		}
	}
	if (B_TYPE(bi->type) == B_OVERFLOW) {
		bo = (BOVERFLOW *)(data);
		ret = __db_goff(dbc, start, bo->tlen, bo->pgno,
		    &start->data, &start->ulen);
	}
	else
		ret = __db_retcopy(env,
		     start, data, len,  &start->data, &start->ulen);

err:	if (pg != NULL && pg != cp->csp->page &&
	    (t_ret = __memp_fput(dbp->mpf, dbc->thread_info,
		 pg, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);

retry:	return (DB_LOCK_NOTGRANTED);
}

/*
 * bam_truncate_ipages --
 *	Find high numbered pages in the internal nodes of a tree and
 *	swap them for lower numbered pages.
 * PUBLIC:  int __bam_truncate_ipages __P((DB *,
 * PUBLIC:    DB_THREAD_INFO *, DB_TXN *, DB_COMPACT *));
 */
int
__bam_truncate_ipages(dbp, ip, txn, c_data)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	DB_COMPACT *c_data;
{
	BTMETA *meta;
	BTREE *bt;
	BTREE_CURSOR *cp;
	DBC *dbc;
	DBMETA *dbmeta;
	DBT start;
	DB_LOCK meta_lock, root_lock;
	DB_TXN *txn_orig;
	PAGE *pg, *root;
	db_pgno_t pgno;
	u_int32_t sflag;
	int level, local_txn, ret, rlevel, t_ret;

	COMPQUIET(pg, NULL);
	dbc = NULL;
	memset(&start, 0, sizeof(start));
	LOCK_INIT(root_lock);
	txn_orig = txn;

	if (IS_DB_AUTO_COMMIT(dbp, txn)) {
		local_txn = 1;
		txn = NULL;
	} else
		local_txn = 0;

	level = LEAFLEVEL + 1;
	sflag = CS_READ | CS_GETRECNO;
	LOCK_INIT(meta_lock);
	bt = dbp->bt_internal;
	meta = NULL;
	root = NULL;

new_txn:
	if (local_txn &&
	    (ret = __txn_begin(dbp->env, ip, txn_orig, &txn, 0)) != 0)
		goto err;

	if ((ret = __db_cursor(dbp, ip, txn, &dbc, 0)) != 0)
		goto err;
	cp = (BTREE_CURSOR *)dbc->internal;

	/*
	 * If the the root is a leaf we have nothing to do.
	 * Searching an empty RECNO tree will return NOTFOUND below and loop.
	 */
	pgno = PGNO_INVALID;
	BAM_GET_ROOT(dbc, pgno, root, 0, DB_LOCK_READ, root_lock, ret);
	if (ret != 0)
		goto err;

	rlevel = LEVEL(root);
	if ((ret = __memp_fput(dbp->mpf, ip, root, dbp->priority)) != 0)
		goto err;
	root = NULL;

	if (rlevel == LEAFLEVEL)
		goto again;

	pgno = PGNO_INVALID;
	do {
		if ((ret = __bam_csearch(dbc, &start, sflag, level)) != 0) {
			/* No more at this level, go up one. */
			if (ret == DB_NOTFOUND) {
				level++;
				if (start.data != NULL)
					__os_free(dbp->env, start.data);
				memset(&start, 0, sizeof(start));
				sflag = CS_READ | CS_GETRECNO;
				continue;
			}
			goto err;
		}
		c_data->compact_pages_examine++;

		pg = cp->csp->page;
		pgno = PGNO(pg);

		sflag = CS_NEXT | CS_GETRECNO;
		/* Grab info about the page and drop the stack. */
		if (pgno != BAM_ROOT_PGNO(dbc) && (ret = __bam_savekey(dbc,
		    pgno <= c_data->compact_truncate, &start)) != 0) {
			if (ret == DB_LOCK_NOTGRANTED)
				continue;
			goto err;
		}

		/* We only got read locks so we can drop them. */
		if ((ret = __bam_stkrel(dbc, STK_NOLOCK)) != 0)
			goto err;
		if (pgno == BAM_ROOT_PGNO(dbc))
			break;

		if (pgno <= c_data->compact_truncate)
			continue;

		/* Get the meta page lock before latching interior nodes. */
		if (!LOCK_ISSET(meta_lock) && (ret = __db_lget(dbc,
		     0, PGNO_BASE_MD, DB_LOCK_WRITE, 0, &meta_lock)) != 0)
			goto err;

		/* Reget the page with a write latch, and its parent too. */
		if ((ret = __bam_csearch(dbc,
		    &start, CS_PARENT | CS_GETRECNO, level)) != 0) {
			if (ret == DB_NOTFOUND) {
				ret = 0;
			}
			goto err;
		}
		pgno = PGNO(cp->csp->page);

		if (pgno > c_data->compact_truncate) {
			if ((ret = __db_exchange_page(dbc, &cp->csp->page,
			    NULL, PGNO_INVALID, DB_EXCH_DEFAULT)) != 0)
				goto err;
		}

		/*
		 * For RECNO we need to bump the saved key to the next
		 * page since CS_NEXT will not do that.
		 */
		if (dbc->dbtype == DB_RECNO &&
		    (ret = __bam_savekey(dbc, 1, &start)) != 0)
			goto err;

		pg = cp->csp->page;
		if ((ret = __bam_stkrel(dbc,
		     pgno != PGNO(pg) ? 0 : STK_NOLOCK)) != 0)
			goto err;

		/* We are locking subtrees, so drop the write locks asap. */
		if (local_txn && pgno != PGNO(pg))
			break;
		/* We really break from the loop above on this condition. */
	} while (pgno != BAM_ROOT_PGNO(dbc));

	if ((ret = __LPUT(dbc, root_lock)) != 0)
		goto err;
	if ((ret = __dbc_close(dbc)) != 0)
		goto err;
	dbc = NULL;
	if (local_txn) {
		if ((ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0)
			goto err;
		txn = NULL;
		LOCK_INIT(meta_lock);
	}
	if (pgno != bt->bt_root)
		goto new_txn;

	/*
	 * Attempt to move the subdatabase metadata and/or root pages.
	 * Grab the metadata page and verify the revision, if its out
	 * of date reopen and try again.
	 */
again:	if (F_ISSET(dbp, DB_AM_SUBDB) &&
	    (bt->bt_root > c_data->compact_truncate ||
	    bt->bt_meta > c_data->compact_truncate)) {
		if (local_txn && txn == NULL &&
		    (ret = __txn_begin(dbp->env, ip, txn_orig, &txn, 0)) != 0)
			goto err;
		if (dbc == NULL &&
		    (ret = __db_cursor(dbp, ip, txn, &dbc, 0)) != 0)
			goto err;
		if ((ret = __db_lget(dbc,
		     0, bt->bt_meta, DB_LOCK_WRITE, 0, &meta_lock)) != 0)
			goto err;
		if ((ret = __memp_fget(dbp->mpf, &bt->bt_meta,
		     dbc->thread_info, dbc->txn, DB_MPOOL_DIRTY, &meta)) != 0)
			goto err;
		if (bt->revision != dbp->mpf->mfp->revision) {
			if ((ret = __memp_fput(dbp->mpf,
			    ip, meta, dbp->priority)) != 0)
				goto err;
			meta = NULL;
			if (local_txn) {
				if ((ret = __dbc_close(dbc)) != 0)
					goto err;
				dbc = NULL;
				ret = __txn_abort(txn);
				txn = NULL;
				if (ret != 0)
					goto err;
			} else {
				if ((ret = __LPUT(dbc, meta_lock)) != 0)
					goto err;
			}
			if ((ret = __db_reopen(dbc)) != 0)
				goto err;
			goto again;
		}
		if (PGNO(meta) > c_data->compact_truncate) {
			dbmeta = (DBMETA *)meta;
			ret = __db_move_metadata(dbc, &dbmeta, c_data);
			meta = (BTMETA *)dbmeta;
			if (ret != 0)
				goto err;
		}
		if (bt->bt_root > c_data->compact_truncate) {
			if ((ret = __db_lget(dbc, 0,
			   bt->bt_root, DB_LOCK_WRITE, 0, &root_lock)) != 0)
				goto err;
			if ((ret = __memp_fget(dbp->mpf,
			     &bt->bt_root, dbc->thread_info,
			     dbc->txn, DB_MPOOL_DIRTY, &root)) != 0)
				goto err;
			c_data->compact_pages_examine++;
			/*
			 * Bump the revision first since any reader will be
			 * blocked on the latch on the old page.  That latch
			 * will get dropped when we free the page and the
			 * reader will do a __db_reopen and wait till the meta
			 * page latch is released.
			 */
			++dbp->mpf->mfp->revision;
			if ((ret = __db_exchange_page(dbc,
			    &root, NULL, PGNO_INVALID, DB_EXCH_FREE)) != 0)
				goto err;
			if (PGNO(root) == bt->bt_root)
				goto err;
			if (DBC_LOGGING(dbc)) {
				if ((ret =
				    __bam_root_log(dbp, txn, &LSN(meta), 0,
				    PGNO(meta), PGNO(root), &LSN(meta))) != 0)
					goto err;
			} else
				LSN_NOT_LOGGED(LSN(meta));
			bt->bt_root = meta->root = PGNO(root);
			bt->revision = dbp->mpf->mfp->revision;
			if ((ret = __memp_fput(dbp->mpf,
			    ip, root, dbp->priority)) != 0)
				goto err;
			root = NULL;
			if (txn == NULL && (ret = __LPUT(dbc, root_lock)) != 0)
				goto err;

		}
		if ((ret = __memp_fput(dbp->mpf, ip, meta, dbp->priority)) != 0)
			goto err;
		meta = NULL;
		if ((ret = __dbc_close(dbc)) != 0)
			goto err;
		dbc = NULL;
		if (local_txn) {
			ret = __txn_commit(txn, DB_TXN_NOSYNC);
			txn = NULL;
			LOCK_INIT(meta_lock);
			LOCK_INIT(root_lock);
		}
	}

err:	if (txn != NULL && ret != 0)
		sflag = STK_PGONLY;
	else
		sflag = 0;
	if (txn == NULL) {
		if (dbc != NULL &&
		    (t_ret = __LPUT(dbc, meta_lock)) != 0 && ret == 0)
			ret = t_ret;
		if (dbc != NULL &&
		    (t_ret = __LPUT(dbc, root_lock)) != 0 && ret == 0)
			ret = t_ret;
	}
	if (meta != NULL && (t_ret = __memp_fput(dbp->mpf,
	    ip, meta, dbp->priority)) != 0 && ret == 0)
		ret = t_ret;
	if (root != NULL && (t_ret = __memp_fput(dbp->mpf,
	    ip, root, dbp->priority)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __bam_stkrel(dbc, sflag)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __dbc_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (local_txn &&
	    txn != NULL && (t_ret = __txn_abort(txn)) != 0 && ret == 0)
		ret = t_ret;
	if (start.data != NULL)
		__os_free(dbp->env, start.data);
	return (ret);
}

#endif

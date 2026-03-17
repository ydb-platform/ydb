/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_verify.h"
#include "dbinc/heap.h"
#include "dbinc/lock.h"
#include "dbinc/mp.h"

static	int __heap_safe_gsplit __P((DB *, VRFY_DBINFO *, PAGE *, db_indx_t,
   DBT *));
static	int __heap_verify_offset_cmp __P((const void *, const void *));

/*
 * __heap_vrfy_meta --
 *	Verify the heap-specific part of a metadata page.
 *
 * PUBLIC: int __heap_vrfy_meta __P((DB *, VRFY_DBINFO *, HEAPMETA *,
 * PUBLIC:     db_pgno_t, u_int32_t));
 */
int
__heap_vrfy_meta(dbp, vdp, meta, pgno, flags)
	DB *dbp;
	VRFY_DBINFO *vdp;
	HEAPMETA *meta;
	db_pgno_t pgno;
	u_int32_t flags;
{
	HEAP *h;
	VRFY_PAGEINFO *pip;
	db_pgno_t last_pgno, max_pgno, npgs;
	int isbad, ret;

	if ((ret = __db_vrfy_getpageinfo(vdp, pgno, &pip)) != 0)
		return (ret);

	isbad = 0;
	/*
	 * Heap can't be used in subdatabases, so if this isn't set
	 * something very odd is going on.
	 */
	if (!F_ISSET(pip, VRFY_INCOMPLETE))
		EPRINT((dbp->env, DB_STR_A("1156",
		    "Page %lu: Heap databases must be one-per-file",
		    "%lu"), (u_long)pgno));

	/*
	 * We have already checked the common fields in __db_vrfy_pagezero.
	 * However, we used the on-disk metadata page, it may have been stale.
	 * We now have the page from mpool, so check that.
	 */
	if ((ret = __db_vrfy_meta(dbp, vdp, &meta->dbmeta, pgno, flags)) != 0) {
		if (ret == DB_VERIFY_BAD)
			isbad = 1;
		else
			goto err;
	}

	/*
	 * Check that nregions is correct.  The last page in the database must
	 * belong to the nregion-th page.
	 */
	h = (HEAP *)dbp->heap_internal;
	h->region_size = meta->region_size;
	last_pgno = meta->dbmeta.last_pgno;
	if (meta->nregions != HEAP_REGION_NUM(dbp, last_pgno)) {
		EPRINT((dbp->env, DB_STR_A("1157",
		    "Page %lu: Number of heap regions incorrect",
		    "%lu"), (u_long)pgno));
		isbad = 1;
	}

	/*
	 * Check that last_pgno doesn't surpass the end of a fixed size
	 * database.
	 */
	if (meta->gbytes != 0 || meta->bytes != 0) {
		/*
		 * We don't have to worry about rounding with gbytes, as pgsize
		 * is always a multiple of 2, but we round down if bytes isn't
		 * a multiple of the page size.
		 */
		npgs = (db_pgno_t)(meta->gbytes * (GIGABYTE / dbp->pgsize));
		npgs += (db_pgno_t)(meta->bytes / dbp->pgsize);
		max_pgno = npgs - 1;
		if (last_pgno > max_pgno) {
			EPRINT((dbp->env, DB_STR_A("1158",
			    "Page %lu: last_pgno beyond end of fixed size heap",
			    "%lu"), (u_long)pgno));
			isbad = 1;
		}
	}

err:	if (LF_ISSET(DB_SALVAGE))
		ret = __db_salvage_markdone(vdp, pgno);

	return (ret == 0 && isbad == 1 ? DB_VERIFY_BAD : ret);
}

/*
 * __heap_vrfy --
 *	Verify a heap data or internal page.
 *
 * PUBLIC: int __heap_vrfy __P((DB *,
 * PUBLIC:     VRFY_DBINFO *, PAGE *, db_pgno_t, u_int32_t));
 */
int
__heap_vrfy(dbp, vdp, h, pgno, flags)
	DB *dbp;
	VRFY_DBINFO *vdp;
	PAGE *h;
	db_pgno_t pgno;
	u_int32_t flags;
{
	HEAPHDR *hdr;
	int cnt, i, j, ret;
	db_indx_t *offsets, *offtbl, end;

	if ((ret = __db_vrfy_datapage(dbp, vdp, h, pgno, flags)) != 0)
		goto err;

	if (TYPE(h) == P_IHEAP)
		/* Nothing to verify on a region page. */
		return (0);

	offtbl = HEAP_OFFSETTBL(dbp, h);

	if ((ret = __os_malloc(dbp->env,
	    NUM_ENT(h) * sizeof(db_indx_t), &offsets)) != 0)
		goto err;

	/*
	 * Build a sorted list of all the offsets in the table.  Entries in the
	 * offset table are not always sorted.  While we're here, check that
	 * flags are sane.
	 */
	cnt = 0;
	for (i = 0; i <= HEAP_HIGHINDX(h); i++) {
		if (offtbl[i] == 0)
			/* Unused index. */
			continue;
		if (cnt >= NUM_ENT(h)) {
			/* Unexpected entry in the offset table. */
			EPRINT((dbp->env, DB_STR_A("1159",
		 "Page %lu: incorrect number of entries in page's offset table",
			    "%lu"), (u_long)pgno));
			ret = DB_VERIFY_BAD;
			goto err;
		}
		hdr = (HEAPHDR *)P_ENTRY(dbp, h, i);
		if (!F_ISSET(hdr, HEAP_RECSPLIT) &&
		    F_ISSET(hdr, HEAP_RECFIRST | HEAP_RECLAST)) {
			EPRINT((dbp->env, DB_STR_A("1165",
			    "Page %lu: record %lu has invalid flags",
			    "%lu %lu"), (u_long)pgno, (u_long)i));
			ret = DB_VERIFY_BAD;
			goto err;
		}

		offsets[cnt] = offtbl[i];
		cnt++;
	}
	if (cnt == 0) {
		/* Empty page. */
		ret = 0;
		goto err;
	}
	qsort(offsets, cnt, sizeof(db_indx_t), __heap_verify_offset_cmp);

	/*
	 * Now check that the record at each offset does not overlap the next
	 * record.  We can't use the P_ENTRY macro because we've kept track of
	 * the offsets, not the indexes.
	 */
	for (i = 0; i < cnt - 1; i++) {
		hdr = (HEAPHDR *)((u_int8_t *)h + offsets[i]);
		end = offsets[i] + HEAP_HDRSIZE(hdr) + hdr->size;
		if (end > offsets[i+1]) {
			/*
			 * Find the record number for this offset, for the error
			 * msg.
			 */
			for (j = 0; j < HEAP_HIGHINDX(h); j++)
				if (offtbl[j] == offsets[i])
					break;
			EPRINT((dbp->env, DB_STR_A("1160",
		       "Page %lu: record %lu (length %lu) overlaps next record",
			    "%lu %lu %lu"),
			    (u_long)pgno, (u_long)j, (u_long)hdr->size));
			ret = DB_VERIFY_BAD;
		}
	}

	/* Finally, check that the last record doesn't overflow the page */
	hdr = (HEAPHDR *)((u_int8_t *)h + offsets[i]);
	end = offsets[i] + HEAP_HDRSIZE(hdr) + hdr->size;
	if (end > dbp->pgsize) {
		/* Find the record number for this offset, for the error msg. */
		for (j = 0; j < HEAP_HIGHINDX(h); j++)
			if (offtbl[j] == offsets[i])
				break;
		EPRINT((dbp->env, DB_STR_A("1161",
		    "Page %lu: record %lu (length %lu) beyond end of page",
		    "%lu %lu %lu"),
		    (u_long)pgno, (u_long)j, (u_long)hdr->size));
		ret = DB_VERIFY_BAD;
	}

 err:	__os_free(dbp->env, offsets);
	return (ret);
}

static int
__heap_verify_offset_cmp(off1, off2)
     const void *off1;
     const void *off2;
{
	return (*(db_indx_t *)off1 - *(db_indx_t *)off2);
}

/*
 * __heap_vrfy_structure --
 *	Verify the structure of a heap database.
 *
 * PUBLIC: int __heap_vrfy_structure __P((DB *, VRFY_DBINFO *, u_int32_t));
 */
int
__heap_vrfy_structure(dbp, vdp, flags)
	DB *dbp;
	VRFY_DBINFO *vdp;
	u_int32_t flags;
{
	VRFY_PAGEINFO *pip;
	db_pgno_t i, next_region, high_pgno;
	int ret, isbad;

	isbad = 0;

	if ((ret = __db_vrfy_getpageinfo(vdp, PGNO_BASE_MD, &pip)) != 0)
		return (ret);

	if (pip->type != P_HEAPMETA) {
		EPRINT((dbp->env, DB_STR_A("1162",
		    "Page %lu: heap database has no meta page", "%lu"),
		    (u_long)PGNO_BASE_MD));
		isbad = 1;
		goto err;
	}

	if ((ret = __db_vrfy_pgset_inc(
	    vdp->pgset, vdp->thread_info, vdp->txn, 0)) != 0)
		goto err;

	/*
	 * Not much structure to verify.  Just make sure region pages are where
	 * they're supposed to be.  If we don't have FTRUNCATE, there could be
	 * a zero'd out page where the region page is supposed to be.
	 */
	next_region = FIRST_HEAP_RPAGE;
	high_pgno = 0;
	for (i = 1; i <= vdp->last_pgno; i++) {
		/* Send feedback to the application about our progress. */
		if (!LF_ISSET(DB_SALVAGE))
			__db_vrfy_struct_feedback(dbp, vdp);

		if ((ret = __db_vrfy_putpageinfo(dbp->env, vdp, pip)) != 0 ||
		    (ret = __db_vrfy_getpageinfo(vdp, i, &pip)) != 0)
			return (ret);
		if (i != next_region &&
		    pip->type != P_HEAP && pip->type != P_INVALID) {
			EPRINT((dbp->env, DB_STR_A("1163",
			   "Page %lu: heap database page of incorrect type %lu",
			    "%lu %lu"), (u_long)i, (u_long)pip->type));
			isbad = 1;
		} else if (i == next_region && pip->type != P_IHEAP
#ifndef HAVE_FTRUNCATE
		    && pip->type != P_INVALID
#endif
		) {
			EPRINT((dbp->env, DB_STR_A("1164",
		  "Page %lu: heap database missing region page (page type %lu)",
			    "%lu %lu"), (u_long)i, (u_long)pip->type));
			isbad = 1;
		} else if ((ret = __db_vrfy_pgset_inc(vdp->pgset,
		    vdp->thread_info, vdp->txn, i)) != 0)
			goto err;

		if (i == next_region) {
			high_pgno = pip->prev_pgno;
			next_region += HEAP_REGION_SIZE(dbp) + 1;
		} else if (pip->type != P_INVALID && i > high_pgno) {
			EPRINT((dbp->env, DB_STR_A("1166",
		"Page %lu heap database page beyond high page in region",
			"%lu"), (u_long) i));
			isbad = 1;
		}
	}

err:	if ((ret = __db_vrfy_putpageinfo(dbp->env, vdp, pip)) != 0)
		return (ret);
	return (isbad == 1 ? DB_VERIFY_BAD : 0);
}

/*
 * __heap_salvage --
 *	Safely dump out anything that looks like a record on an alleged heap
 *	data page.
 *
 * PUBLIC: int __heap_salvage __P((DB *, VRFY_DBINFO *, db_pgno_t,
 * PUBLIC:     PAGE *, void *, int (*)(void *, const void *), u_int32_t));
 */
int
__heap_salvage(dbp, vdp, pgno, h, handle, callback, flags)
	DB *dbp;
	VRFY_DBINFO *vdp;
	db_pgno_t pgno;
	PAGE *h;
	void *handle;
	int (*callback) __P((void *, const void *));
	u_int32_t flags;
{
	DBT dbt;
	HEAPHDR *hdr;
	db_indx_t i, *offtbl;
	int err_ret, ret, t_ret;

	COMPQUIET(flags, 0);
	memset(&dbt, 0, sizeof(DBT));

	offtbl = (db_indx_t *)HEAP_OFFSETTBL(dbp, h);
	err_ret = ret = t_ret = 0;

	/*
	 * Walk the page, dumping non-split records and retrieving split records
	 * when the first piece is encountered,
	 */
	for (i = 0; i <= HEAP_HIGHINDX(h); i++) {
		if (offtbl[i] == 0)
			continue;
		hdr = (HEAPHDR *)P_ENTRY(dbp, h, i);
		if (F_ISSET(hdr, HEAP_RECSPLIT)) {
			if (!F_ISSET(hdr, HEAP_RECFIRST))
				continue;
			/*
			 * We don't completely trust hdr->tsize if it's huge,
			 * gsplit() is able to realloc as needed.
			 */
			dbt.size = ((HEAPSPLITHDR *)hdr)->tsize;
			if (dbt.size > dbp->pgsize * 4)
				dbt.size = dbp->pgsize * 4;
			if ((ret =
			    __os_malloc(dbp->env, dbt.size, &dbt.data)) != 0)
				goto err;
			__heap_safe_gsplit(dbp, vdp, h, i, &dbt);
		} else {
			dbt.data = (u_int8_t *)hdr + HEAP_HDRSIZE(hdr);
			dbt.size = hdr->size;
		}

		if ((ret = __db_vrfy_prdbt(&dbt,
		    0, " ", handle, callback, 0, 0, vdp)) != 0)
			err_ret = ret;
		if (F_ISSET(hdr, HEAP_RECSPLIT))
			__os_free(dbp->env, dbt.data);
	}

err:	if ((t_ret = __db_salvage_markdone(vdp, pgno)) != 0)
		return (t_ret);
	return ((ret == 0 && err_ret != 0) ? err_ret : ret);
}

/*
 * __heap_safe_gsplit --
 *	Given a page and offset, retrieve a split record.
 */
static int
__heap_safe_gsplit(dbp, vdp, h, i, dbt)
     DB *dbp;
     VRFY_DBINFO *vdp;
     PAGE *h;
     db_indx_t i;
     DBT *dbt;
{
	DB_MPOOLFILE *mpf;
	HEAPSPLITHDR *hdr;
	int gotpg, ret, t_ret;
	u_int32_t bufsz, reclen;
	u_int8_t *buf;

	mpf = dbp->mpf;
	buf = dbt->data;
	bufsz = dbt->size;
	dbt->size = 0;
	ret = 0;

	gotpg = 0;
	for (;;) {
		hdr = (HEAPSPLITHDR *)P_ENTRY(dbp, h, i);
		reclen = hdr->std_hdr.size;
		/* First copy the data from this page */
		if (dbt->size + reclen > bufsz) {
			bufsz = dbt->size + reclen;
			if ((ret = __os_realloc(
			    dbp->env, bufsz, &dbt->data)) != 0)
				goto err;
			buf = (u_int8_t *)dbt->data + dbt->size;
		}
		memcpy(buf, (u_int8_t *)hdr + sizeof(HEAPSPLITHDR), reclen);
		buf += reclen;
		dbt->size += reclen;

		/* If we're not at the end of the record, grab the next page. */
		if (F_ISSET(&hdr->std_hdr, HEAP_RECLAST))
			break;
		if (gotpg && (ret = __memp_fput(mpf,
		    vdp->thread_info, h, DB_PRIORITY_UNCHANGED)) != 0)
			return (ret);
		gotpg = 0;
		if ((ret = __memp_fget(mpf,
		    &hdr->nextpg, vdp->thread_info, NULL, 0, &h)) != 0)
			goto err;
		gotpg = 1;
		i = hdr->nextindx;
	}

err:	if (gotpg && (t_ret = __memp_fput(
	    mpf, vdp->thread_info, h, DB_PRIORITY_UNCHANGED)) != 0 && ret == 0)
		t_ret = ret;
	return (ret);
}

/*
 * __heap_meta2pgset --
 *	Given a known-good meta page, populate pgsetp with the db_pgno_t's
 *	corresponding to the pages in the heap.  This is just all pages in the
 *	database.
 *
 * PUBLIC: int __heap_meta2pgset __P((DB *, VRFY_DBINFO *, HEAPMETA *, DB *));
 */
int
__heap_meta2pgset(dbp, vdp, heapmeta, pgset)
	DB *dbp;
	VRFY_DBINFO *vdp;
	HEAPMETA *heapmeta;
	DB *pgset;
{
	db_pgno_t pgno, last;
	int ret;

	COMPQUIET(dbp, NULL);

	last = heapmeta->dbmeta.last_pgno;
	ret = 0;

	for (pgno = 1; pgno <= last; pgno++)
		if ((ret = __db_vrfy_pgset_inc(
		    pgset, vdp->thread_info, vdp->txn, pgno)) != 0)
			break;
	return (ret);
}

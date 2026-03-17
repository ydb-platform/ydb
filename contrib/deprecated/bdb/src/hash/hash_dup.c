/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1996, 2012 Oracle and/or its affiliates.  All rights reserved.
 */
/*
 * Copyright (c) 1990, 1993, 1994
 *	The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Margo Seltzer.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $Id$
 */

/*
 * PACKAGE:  hashing
 *
 * DESCRIPTION:
 *	Manipulation of duplicates for the hash package.
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/hash.h"
#include "dbinc/btree.h"
#include "dbinc/mp.h"

static int __hamc_chgpg __P((DBC *,
    db_pgno_t, u_int32_t, db_pgno_t, u_int32_t));
static int __ham_check_move __P((DBC *, u_int32_t));
static int __ham_dcursor __P((DBC *, db_pgno_t, u_int32_t));
static int __ham_move_offpage __P((DBC *, PAGE *, u_int32_t, db_pgno_t));
static int __hamc_chgpg_func
    __P((DBC *, DBC *, u_int32_t *, db_pgno_t, u_int32_t, void *));

/*
 * Called from hash_access to add a duplicate key. nval is the new
 * value that we want to add.  The flags correspond to the flag values
 * to cursor_put indicating where to add the new element.
 * There are 4 cases.
 * Case 1: The existing duplicate set already resides on a separate page.
 *	   We return and let the common code handle this.
 * Case 2: The element is small enough to just be added to the existing set.
 * Case 3: The element is large enough to be a big item, so we're going to
 *	   have to push the set onto a new page.
 * Case 4: The element is large enough to push the duplicate set onto a
 *	   separate page.
 *
 * PUBLIC: int __ham_add_dup __P((DBC *, DBT *, u_int32_t, db_pgno_t *));
 */
int
__ham_add_dup(dbc, nval, flags, pgnop)
	DBC *dbc;
	DBT *nval;
	u_int32_t flags;
	db_pgno_t *pgnop;
{
	DB *dbp;
	DBT pval, tmp_val;
	DB_MPOOLFILE *mpf;
	ENV *env;
	HASH_CURSOR *hcp;
	u_int32_t add_bytes, new_size;
	int cmp, ret;
	u_int8_t *hk;

	dbp = dbc->dbp;
	env = dbp->env;
	mpf = dbp->mpf;
	hcp = (HASH_CURSOR *)dbc->internal;

	DB_ASSERT(env, flags != DB_CURRENT);

	add_bytes = nval->size +
	    (F_ISSET(nval, DB_DBT_PARTIAL) ? nval->doff : 0);
	add_bytes = DUP_SIZE(add_bytes);

	if ((ret = __ham_check_move(dbc, add_bytes)) != 0)
		return (ret);

	/*
	 * Check if resulting duplicate set is going to need to go
	 * onto a separate duplicate page.  If so, convert the
	 * duplicate set and add the new one.  After conversion,
	 * hcp->dndx is the first free ndx or the index of the
	 * current pointer into the duplicate set.
	 */
	hk = H_PAIRDATA(dbp, hcp->page, hcp->indx);
	/* Add the len bytes to the current singleton. */
	if (HPAGE_PTYPE(hk) != H_DUPLICATE)
		add_bytes += DUP_SIZE(0);
	new_size =
	    LEN_HKEYDATA(dbp, hcp->page, dbp->pgsize, H_DATAINDEX(hcp->indx)) +
	    add_bytes;

	/*
	 * We convert to off-page duplicates if the item is a big item,
	 * the addition of the new item will make the set large, or
	 * if there isn't enough room on this page to add the next item.
	 */
	if (HPAGE_PTYPE(hk) != H_OFFDUP &&
	    (HPAGE_PTYPE(hk) == H_OFFPAGE || ISBIG(hcp, new_size) ||
	    add_bytes > P_FREESPACE(dbp, hcp->page))) {

		if ((ret = __ham_dup_convert(dbc)) != 0)
			return (ret);
		return (hcp->opd->am_put(hcp->opd,
		    NULL, nval, flags, NULL));
	}

	/* There are two separate cases here: on page and off page. */
	if (HPAGE_PTYPE(hk) != H_OFFDUP) {
		if (HPAGE_PTYPE(hk) != H_DUPLICATE) {
			pval.flags = 0;
			pval.data = HKEYDATA_DATA(hk);
			pval.size = LEN_HDATA(dbp, hcp->page, dbp->pgsize,
			    hcp->indx);
			if ((ret = __ham_make_dup(env,
			    &pval, &tmp_val, &dbc->my_rdata.data,
			    &dbc->my_rdata.ulen)) != 0 || (ret =
			    __ham_replpair(dbc, &tmp_val, H_DUPLICATE)) != 0)
				return (ret);
			hk = H_PAIRDATA(dbp, hcp->page, hcp->indx);
			HPAGE_PTYPE(hk) = H_DUPLICATE;

			/*
			 * Update the cursor position since we now are in
			 * duplicates.
			 */
			F_SET(hcp, H_ISDUP);
			hcp->dup_off = 0;
			hcp->dup_len = pval.size;
			hcp->dup_tlen = DUP_SIZE(hcp->dup_len);
		}

		/* Now make the new entry a duplicate. */
		if ((ret = __ham_make_dup(env, nval,
		    &tmp_val, &dbc->my_rdata.data, &dbc->my_rdata.ulen)) != 0)
			return (ret);

		tmp_val.dlen = 0;
		switch (flags) {			/* On page. */
		case DB_KEYFIRST:
		case DB_KEYLAST:
		case DB_NODUPDATA:
		case DB_OVERWRITE_DUP:
			if (dbp->dup_compare != NULL) {
				__ham_dsearch(dbc,
				    nval, &tmp_val.doff, &cmp, flags);

				/*
				 * Duplicate duplicates are not supported w/
				 * sorted dups.  We can either overwrite or
				 * return DB_KEYEXIST.
				 */
				if (cmp == 0) {
					if (flags == DB_OVERWRITE_DUP)
						return (__ham_overwrite(dbc,
						    nval, flags));
					return (__db_duperr(dbp, flags));
				}
			} else {
				hcp->dup_tlen = LEN_HDATA(dbp, hcp->page,
				    dbp->pgsize, hcp->indx);
				hcp->dup_len = nval->size;
				F_SET(hcp, H_ISDUP);
				if (flags == DB_KEYFIRST)
					hcp->dup_off = tmp_val.doff = 0;
				else
					hcp->dup_off =
					    tmp_val.doff = hcp->dup_tlen;
			}
			break;
		case DB_BEFORE:
			tmp_val.doff = hcp->dup_off;
			break;
		case DB_AFTER:
			tmp_val.doff = hcp->dup_off + DUP_SIZE(hcp->dup_len);
			break;
		default:
			return (__db_unknown_path(env, "__ham_add_dup"));
		}

		/* Add the duplicate. */
		if ((ret = __memp_dirty(mpf, &hcp->page,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0 ||
		    (ret = __ham_replpair(dbc, &tmp_val, H_DUPLICATE)) != 0)
			return (ret);

		/* Now, update the cursor if necessary. */
		switch (flags) {
		case DB_AFTER:
			hcp->dup_off += DUP_SIZE(hcp->dup_len);
			hcp->dup_len = nval->size;
			hcp->dup_tlen += (db_indx_t)DUP_SIZE(nval->size);
			break;
		case DB_BEFORE:
		case DB_KEYFIRST:
		case DB_KEYLAST:
		case DB_NODUPDATA:
		case DB_OVERWRITE_DUP:
			hcp->dup_tlen += (db_indx_t)DUP_SIZE(nval->size);
			hcp->dup_len = nval->size;
			break;
		default:
			return (__db_unknown_path(env, "__ham_add_dup"));
		}
		ret = __hamc_update(dbc, tmp_val.size, DB_HAM_CURADJ_ADD, 1);
		return (ret);
	}

	/*
	 * If we get here, then we're on duplicate pages; set pgnop and
	 * return so the common code can handle it.
	 */
	memcpy(pgnop, HOFFDUP_PGNO(H_PAIRDATA(dbp, hcp->page, hcp->indx)),
	    sizeof(db_pgno_t));

	return (ret);
}

/*
 * Convert an on-page set of duplicates to an offpage set of duplicates.
 *
 * PUBLIC: int __ham_dup_convert __P((DBC *));
 */
int
__ham_dup_convert(dbc)
	DBC *dbc;
{
	BOVERFLOW bo;
	DB *dbp;
	DBC **hcs;
	DBT dbt;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	ENV *env;
	HASH_CURSOR *hcp;
	HOFFPAGE ho;
	PAGE *dp;
	db_indx_t i, len, off;
	int c, ret, t_ret;
	u_int8_t *p, *pend;

	dbp = dbc->dbp;
	env = dbp->env;
	mpf = dbp->mpf;
	hcp = (HASH_CURSOR *)dbc->internal;

	/*
	 * Create a new page for the duplicates.
	 */
	if ((ret = __db_new(dbc,
	    dbp->dup_compare == NULL ? P_LRECNO : P_LDUP, NULL, &dp)) != 0)
		return (ret);
	P_INIT(dp, dbp->pgsize,
	    dp->pgno, PGNO_INVALID, PGNO_INVALID, LEAFLEVEL, TYPE(dp));

	/*
	 * Get the list of cursors that may need to be updated.
	 */
	if ((ret = __ham_get_clist(dbp,
	    PGNO(hcp->page), (u_int32_t)hcp->indx, &hcs)) != 0)
		goto err;

	/*
	 * Now put the duplicates onto the new page.
	 */
	dbt.flags = 0;
	switch (HPAGE_PTYPE(H_PAIRDATA(dbp, hcp->page, hcp->indx))) {
	case H_KEYDATA:
		/* Simple case, one key on page; move it to dup page. */
		dbt.size = LEN_HDATA(dbp, hcp->page, dbp->pgsize, hcp->indx);
		dbt.data = HKEYDATA_DATA(H_PAIRDATA(dbp, hcp->page, hcp->indx));
		ret = __db_pitem(dbc,
		    dp, 0, BKEYDATA_SIZE(dbt.size), NULL, &dbt);
		goto finish;
	case H_OFFPAGE:
		/* Simple case, one key on page; move it to dup page. */
		memcpy(&ho, P_ENTRY(dbp, hcp->page, H_DATAINDEX(hcp->indx)),
		    HOFFPAGE_SIZE);
		UMRW_SET(bo.unused1);
		B_TSET(bo.type, ho.type);
		UMRW_SET(bo.unused2);
		bo.pgno = ho.pgno;
		bo.tlen = ho.tlen;
		dbt.size = BOVERFLOW_SIZE;
		dbt.data = &bo;

		ret = __db_pitem(dbc, dp, 0, dbt.size, &dbt, NULL);
finish:		if (ret == 0) {
			/* Update any other cursors. */
			if (hcs != NULL && DBC_LOGGING(dbc) &&
			    IS_SUBTRANSACTION(dbc->txn)) {
				if ((ret = __ham_chgpg_log(dbp, dbc->txn,
				    &lsn, 0, DB_HAM_DUP, PGNO(hcp->page),
				    PGNO(dp), hcp->indx, 0)) != 0)
					break;
			}
			for (c = 0; hcs != NULL && hcs[c] != NULL; c++)
				if ((ret = __ham_dcursor(hcs[c],
				    PGNO(dp), 0)) != 0)
					break;
		}
		break;
	case H_DUPLICATE:
		p = HKEYDATA_DATA(H_PAIRDATA(dbp, hcp->page, hcp->indx));
		pend = p +
		    LEN_HDATA(dbp, hcp->page, dbp->pgsize, hcp->indx);

		/*
		 * We need to maintain the duplicate cursor position.
		 * Keep track of where we are in the duplicate set via
		 * the offset, and when it matches the one in the cursor,
		 * set the off-page duplicate cursor index to the current
		 * index.
		 */
		for (off = 0, i = 0; p < pend; i++) {
			memcpy(&len, p, sizeof(db_indx_t));
			dbt.size = len;
			p += sizeof(db_indx_t);
			dbt.data = p;
			p += len + sizeof(db_indx_t);
			if ((ret = __db_pitem(dbc, dp,
			    i, BKEYDATA_SIZE(dbt.size), NULL, &dbt)) != 0)
				break;

			/* Update any other cursors */
			if (hcs != NULL && DBC_LOGGING(dbc) &&
			    IS_SUBTRANSACTION(dbc->txn)) {
				if ((ret = __ham_chgpg_log(dbp, dbc->txn,
				    &lsn, 0, DB_HAM_DUP, PGNO(hcp->page),
				    PGNO(dp), hcp->indx, i)) != 0)
					break;
			}
			for (c = 0; hcs != NULL && hcs[c] != NULL; c++)
				if (((HASH_CURSOR *)(hcs[c]->internal))->dup_off
				    == off && (ret = __ham_dcursor(hcs[c],
				    PGNO(dp), i)) != 0)
					goto err;
			off += len + 2 * sizeof(db_indx_t);
		}
		break;
	default:
		ret = __db_pgfmt(env, hcp->pgno);
		break;
	}

	/*
	 * Now attach this to the source page in place of the old duplicate
	 * item.
	 */
	if (ret == 0)
		ret = __memp_dirty(mpf,
		    &hcp->page, dbc->thread_info, dbc->txn, dbc->priority, 0);

	if (ret == 0)
		ret = __ham_move_offpage(dbc, hcp->page,
		    (u_int32_t)H_DATAINDEX(hcp->indx), PGNO(dp));

err:	if ((t_ret = __memp_fput(mpf,
	    dbc->thread_info, dp, dbc->priority)) != 0 && ret == 0)
		ret = t_ret;

	if (ret == 0)
		hcp->dup_tlen = hcp->dup_off = hcp->dup_len = 0;

	if (hcs != NULL)
		__os_free(env, hcs);

	return (ret);
}

/*
 * __ham_make_dup
 *
 * Take a regular dbt and make it into a duplicate item with all the partial
 * information set appropriately. If the incoming dbt is a partial, assume
 * we are creating a new entry and make sure that we do any initial padding.
 *
 * PUBLIC: int __ham_make_dup __P((ENV *,
 * PUBLIC:     const DBT *, DBT *d, void **, u_int32_t *));
 */
int
__ham_make_dup(env, notdup, duplicate, bufp, sizep)
	ENV *env;
	const DBT *notdup;
	DBT *duplicate;
	void **bufp;
	u_int32_t *sizep;
{
	db_indx_t tsize, item_size;
	int ret;
	u_int8_t *p;

	item_size = (db_indx_t)notdup->size;
	if (F_ISSET(notdup, DB_DBT_PARTIAL))
		item_size += notdup->doff;

	tsize = DUP_SIZE(item_size);
	if ((ret = __ham_init_dbt(env, duplicate, tsize, bufp, sizep)) != 0)
		return (ret);

	duplicate->dlen = 0;
	duplicate->flags = notdup->flags;
	F_SET(duplicate, DB_DBT_PARTIAL);

	p = duplicate->data;
	memcpy(p, &item_size, sizeof(db_indx_t));
	p += sizeof(db_indx_t);
	if (F_ISSET(notdup, DB_DBT_PARTIAL)) {
		memset(p, 0, notdup->doff);
		p += notdup->doff;
	}
	memcpy(p, notdup->data, notdup->size);
	p += notdup->size;
	memcpy(p, &item_size, sizeof(db_indx_t));

	duplicate->doff = 0;
	duplicate->dlen = notdup->size;

	return (0);
}

/*
 * __ham_check_move --
 *
 * Check if we can do whatever we need to on this page.  If not,
 * then we'll have to move the current element to a new page.
 */
static int
__ham_check_move(dbc, add_len)
	DBC *dbc;
	u_int32_t add_len;
{
	DB *dbp;
	DBT k, d;
	DB_LSN new_lsn;
	DB_MPOOLFILE *mpf;
	HASH_CURSOR *hcp;
	PAGE *new_pagep, *next_pagep;
	db_pgno_t next_pgno;
	u_int32_t data_type, key_type, new_datalen, old_len;
	db_indx_t new_indx;
	u_int8_t *hk;
	int found, match, ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	hcp = (HASH_CURSOR *)dbc->internal;

	hk = H_PAIRDATA(dbp, hcp->page, hcp->indx);
	found = 0;

	/*
	 * If the item is already off page duplicates or an offpage item,
	 * then we know we can do whatever we need to do in-place
	 */
	if (HPAGE_PTYPE(hk) == H_OFFDUP || HPAGE_PTYPE(hk) == H_OFFPAGE)
		return (0);

	old_len =
	    LEN_HITEM(dbp, hcp->page, dbp->pgsize, H_DATAINDEX(hcp->indx));
	new_datalen = (old_len - HKEYDATA_SIZE(0)) + add_len;
	if (HPAGE_PTYPE(hk) != H_DUPLICATE)
		new_datalen += DUP_SIZE(0);

	/*
	 * We need to add a new page under two conditions:
	 * 1. The addition makes the total data length cross the BIG
	 *    threshold and the OFFDUP structure won't fit on this page.
	 * 2. The addition does not make the total data cross the
	 *    threshold, but the new data won't fit on the page.
	 * If neither of these is true, then we can return.
	 */
	if (ISBIG(hcp, new_datalen) && (old_len > HOFFDUP_SIZE ||
	    HOFFDUP_SIZE - old_len <= P_FREESPACE(dbp, hcp->page)))
		return (0);

	if (!ISBIG(hcp, new_datalen) &&
	    (new_datalen - old_len) <= P_FREESPACE(dbp, hcp->page))
		return (0);

	/*
	 * If we get here, then we need to move the item to a new page.
	 * Check if there are more pages in the chain.  We now need to
	 * update new_datalen to include the size of both the key and
	 * the data that we need to move.
	 */

	new_datalen = ISBIG(hcp, new_datalen) ?
	    HOFFDUP_SIZE : HKEYDATA_SIZE(new_datalen);
	new_datalen +=
	    LEN_HITEM(dbp, hcp->page, dbp->pgsize, H_KEYINDEX(hcp->indx));

	new_pagep = NULL;
	next_pagep = hcp->page;
	for (next_pgno = NEXT_PGNO(hcp->page); next_pgno != PGNO_INVALID;
	    next_pgno = NEXT_PGNO(next_pagep)) {
		if (next_pagep != hcp->page && (ret = __memp_fput(mpf,
		    dbc->thread_info, next_pagep, dbc->priority)) != 0)
			return (ret);

		if ((ret = __memp_fget(mpf,
		    &next_pgno, dbc->thread_info, dbc->txn,
		    DB_MPOOL_CREATE, &next_pagep)) != 0)
			return (ret);

		if (P_FREESPACE(dbp, next_pagep) >= new_datalen) {
			found = 1;
			break;
		}
	}

	if (found != 0) {
		/* Found a page with space, dirty it and the original. */
		new_pagep = next_pagep;
		if ((ret = __memp_dirty(mpf, &hcp->page,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
			goto err;
		if ((ret = __memp_dirty(mpf, &new_pagep,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
			goto err;
	} else {
		if ((ret = __memp_dirty(mpf, &next_pagep,
		    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
			goto err;

		/* Add new page at the end of the chain. */
		new_pagep = next_pagep;
		if ((ret = __ham_add_ovflpage(dbc, &new_pagep)) != 0)
			goto err;

		if (next_pagep != hcp->page) {
			if ((ret = __memp_fput(mpf,
			    dbc->thread_info, next_pagep, dbc->priority)) != 0)
				goto err;
			next_pagep = NULL;
			/* Dirty the original page to update it. */
			if ((ret = __memp_dirty(mpf, &hcp->page,
			    dbc->thread_info, dbc->txn, dbc->priority, 0)) != 0)
				goto err;
		}
	}

	/* Copy the item to the new page. */
	if (DBC_LOGGING(dbc)) {
		memset(&k, 0, sizeof(DBT));
		d.flags = 0;
		if (HPAGE_PTYPE(
		    H_PAIRKEY(dbp, hcp->page, hcp->indx)) == H_OFFPAGE) {
			k.data = H_PAIRKEY(dbp, hcp->page, hcp->indx);
			k.size = HOFFPAGE_SIZE;
			key_type = H_OFFPAGE;
		} else {
			k.data =
			    HKEYDATA_DATA(H_PAIRKEY(dbp, hcp->page, hcp->indx));
			k.size =
			    LEN_HKEY(dbp, hcp->page, dbp->pgsize, hcp->indx);
			key_type = H_KEYDATA;
		}

		/* Resolve the insert index so it can be written to the log. */
		if ((ret = __ham_getindex(dbc, new_pagep, &k,
		    key_type, &match, &new_indx)) != 0)
			return (ret);

		if ((data_type = HPAGE_PTYPE(hk)) == H_OFFPAGE) {
			d.data = hk;
			d.size = HOFFPAGE_SIZE;
		} else if (data_type == H_OFFDUP) {
			d.data = hk;
			d.size = HOFFDUP_SIZE;
		} else {
			d.data = HKEYDATA_DATA(hk);
			d.size = LEN_HDATA(dbp,
			     hcp->page, dbp->pgsize, hcp->indx);
		}

		if ((ret = __ham_insdel_log(dbp, dbc->txn, &new_lsn,
		    0, PUTPAIR, PGNO(new_pagep), (u_int32_t)new_indx,
		    &LSN(new_pagep), OP_SET(key_type, new_pagep), &k,
		    OP_SET(data_type, new_pagep), &d)) != 0) {
			(void)__memp_fput(mpf,
			    dbc->thread_info, new_pagep, dbc->priority);
			return (ret);
		}
	} else {
		LSN_NOT_LOGGED(new_lsn);
		/*
		 * Ensure that an invalid index is passed to __ham_copypair, so
		 * it knows to resolve the index. Resolving the insert index
		 * here would require creating a temporary DBT with the key,
		 * and calling __ham_getindex. Let __ham_copypair do the
		 * resolution using the final key DBT.
		 */
		new_indx = NDX_INVALID;
	}

	/* Move lsn onto page. */
	LSN(new_pagep) = new_lsn;	/* Structure assignment. */

	if ((ret = __ham_copypair(dbc, hcp->page,
	    H_KEYINDEX(hcp->indx), new_pagep, &new_indx, 0)) != 0)
		goto err;

	/* Update all cursors that used to point to this item. */
	if ((ret = __hamc_chgpg(dbc, PGNO(hcp->page), H_KEYINDEX(hcp->indx),
	    PGNO(new_pagep), new_indx)) != 0)
		goto err;

	/* Now delete the pair from the current page. */
	if ((ret = __ham_del_pair(dbc, HAM_DEL_NO_RECLAIM, NULL)) != 0)
		goto err;

	/*
	 * __ham_del_pair decremented nelem.  This is incorrect;  we
	 * manually copied the element elsewhere, so the total number
	 * of elements hasn't changed.  Increment it again.
	 *
	 * !!!
	 * Note that we still have the metadata page pinned, and
	 * __ham_del_pair dirtied it, so we don't need to set the dirty
	 * flag again.
	 */
	if (!STD_LOCKING(dbc))
		hcp->hdr->nelem++;

	ret = __memp_fput(mpf, dbc->thread_info, hcp->page, dbc->priority);
	hcp->page = new_pagep;
	hcp->pgno = PGNO(hcp->page);
	hcp->indx = new_indx;
	F_SET(hcp, H_EXPAND);
	F_CLR(hcp, H_DELETED);

	return (ret);

err:	if (new_pagep != NULL)
		(void)__memp_fput(mpf,
			dbc->thread_info, new_pagep, dbc->priority);
	if (next_pagep != NULL &&
	    next_pagep != hcp->page && next_pagep != new_pagep)
		(void)__memp_fput(mpf,
			dbc->thread_info, next_pagep, dbc->priority);
	return (ret);

}

/*
 * __ham_move_offpage --
 *	Replace an onpage set of duplicates with the OFFDUP structure
 *	that references the duplicate page.
 *
 * XXX
 * This is really just a special case of __onpage_replace; we should
 * probably combine them.
 *
 */
static int
__ham_move_offpage(dbc, pagep, ndx, pgno)
	DBC *dbc;
	PAGE *pagep;
	u_int32_t ndx;
	db_pgno_t pgno;
{
	DB *dbp;
	DBT new_dbt;
	DBT old_dbt;
	HOFFDUP od;
	db_indx_t i, *inp;
	int32_t difflen;
	u_int8_t *src;
	int ret;

	dbp = dbc->dbp;
	od.type = H_OFFDUP;
	UMRW_SET(od.unused[0]);
	UMRW_SET(od.unused[1]);
	UMRW_SET(od.unused[2]);
	od.pgno = pgno;
	ret = 0;

	if (DBC_LOGGING(dbc)) {
		HKEYDATA *hk;
		new_dbt.data = &od;
		new_dbt.size = HOFFDUP_SIZE;
		hk = (HKEYDATA *)P_ENTRY(dbp, pagep, ndx);
		if (hk->type == H_KEYDATA || hk->type == H_DUPLICATE) {
			old_dbt.data = hk->data;
			old_dbt.size = LEN_HITEM(dbp, pagep, dbp->pgsize, ndx) -
			     SSZA(HKEYDATA, data);
		} else {
			old_dbt.data = hk;
			old_dbt.size = LEN_HITEM(dbp, pagep, dbp->pgsize, ndx);
		}
		if ((ret = __ham_replace_log(dbp, dbc->txn, &LSN(pagep), 0,
		    PGNO(pagep), (u_int32_t)ndx, &LSN(pagep), -1,
		    OP_SET(hk->type, pagep), &old_dbt,
		    OP_SET(H_OFFDUP, pagep), &new_dbt)) != 0)
			return (ret);
	} else
		LSN_NOT_LOGGED(LSN(pagep));

	/*
	 * difflen is the difference in the lengths, and so may be negative.
	 * We know that the difference between two unsigned lengths from a
	 * database page will fit into an int32_t.
	 */
	difflen =
	    (int32_t)LEN_HITEM(dbp, pagep, dbp->pgsize, ndx) -
	    (int32_t)HOFFDUP_SIZE;
	if (difflen != 0) {
		/* Copy data. */
		inp = P_INP(dbp, pagep);
		src = (u_int8_t *)(pagep) + HOFFSET(pagep);
		memmove(src + difflen, src, inp[ndx] - HOFFSET(pagep));
		HOFFSET(pagep) += difflen;

		/* Update index table. */
		for (i = ndx; i < NUM_ENT(pagep); i++)
			inp[i] += difflen;
	}

	/* Now copy the offdup entry onto the page. */
	memcpy(P_ENTRY(dbp, pagep, ndx), &od, HOFFDUP_SIZE);
	return (ret);
}

/*
 * __ham_dsearch:
 *	Locate a particular duplicate in a duplicate set.  Make sure that
 *	we exit with the cursor set appropriately.
 *
 * PUBLIC: void __ham_dsearch
 * PUBLIC:     __P((DBC *, DBT *, u_int32_t *, int *, u_int32_t));
 */
void
__ham_dsearch(dbc, dbt, offp, cmpp, flags)
	DBC *dbc;
	DBT *dbt;
	u_int32_t *offp, flags;
	int *cmpp;
{
	DB *dbp;
	DBT cur;
	HASH_CURSOR *hcp;
	db_indx_t i, len;
	int (*func) __P((DB *, const DBT *, const DBT *));
	u_int8_t *data;

	dbp = dbc->dbp;
	hcp = (HASH_CURSOR *)dbc->internal;
	func = dbp->dup_compare == NULL ? __bam_defcmp : dbp->dup_compare;

	i = F_ISSET(hcp, H_CONTINUE) ? hcp->dup_off: 0;
	data = HKEYDATA_DATA(H_PAIRDATA(dbp, hcp->page, hcp->indx)) + i;
	hcp->dup_tlen = LEN_HDATA(dbp, hcp->page, dbp->pgsize, hcp->indx);
	len = hcp->dup_len;
	while (i < hcp->dup_tlen) {
		memcpy(&len, data, sizeof(db_indx_t));
		data += sizeof(db_indx_t);
		DB_SET_DBT(cur, data, len);

		/*
		 * If we find an exact match, we're done.  If in a sorted
		 * duplicate set and the item is larger than our test item,
		 * we're done.  In the latter case, if permitting partial
		 * matches, it's not a failure.
		 */
		*cmpp = func(dbp, dbt, &cur);
		if (*cmpp == 0)
			break;
		if (*cmpp < 0 && dbp->dup_compare != NULL) {
			if (flags == DB_GET_BOTH_RANGE)
				*cmpp = 0;
			break;
		}

		i += len + 2 * sizeof(db_indx_t);
		data += len + sizeof(db_indx_t);
	}

	*offp = i;
	hcp->dup_off = i;
	hcp->dup_len = len;
	F_SET(hcp, H_ISDUP);
}

/*
 * __ham_dcursor --
 *
 *	Create an off page duplicate cursor for this cursor.
 */
static int
__ham_dcursor(dbc, pgno, indx)
	DBC *dbc;
	db_pgno_t pgno;
	u_int32_t indx;
{
	BTREE_CURSOR *dcp;
	DB *dbp;
	HASH_CURSOR *hcp;
	int ret;

	dbp = dbc->dbp;
	hcp = (HASH_CURSOR *)dbc->internal;

	if ((ret = __dbc_newopd(dbc, pgno, hcp->opd, &hcp->opd)) != 0)
		return (ret);

	dcp = (BTREE_CURSOR *)hcp->opd->internal;
	dcp->pgno = pgno;
	dcp->indx = indx;

	if (dbp->dup_compare == NULL) {
		/*
		 * Converting to off-page Recno trees is tricky.  The
		 * record number for the cursor is the index + 1 (to
		 * convert to 1-based record numbers).
		 */
		dcp->recno = indx + 1;
	}

	/*
	 * Transfer the deleted flag from the top-level cursor to the
	 * created one.
	 */
	if (F_ISSET(hcp, H_DELETED)) {
		F_SET(dcp, C_DELETED);
		F_CLR(hcp, H_DELETED);
	}

	return (0);
}

struct __hamc_chgpg_args {
	db_pgno_t new_pgno;
	db_indx_t new_index;
	DB_TXN *my_txn;
};

static int
__hamc_chgpg_func(cp, my_dbc, foundp, old_pgno, old_index, vargs)
	DBC *cp, *my_dbc;
	u_int32_t *foundp;
	db_pgno_t old_pgno;
	u_int32_t old_index;
	void *vargs;
{
	HASH_CURSOR *hcp;
	struct __hamc_chgpg_args *args;

	if (cp == my_dbc || cp->dbtype != DB_HASH)
		return (0);

	hcp = (HASH_CURSOR *)cp->internal;

	/*
	 * If a cursor is deleted, it doesn't refer to this
	 * item--it just happens to have the same indx, but
	 * it points to a former neighbor.  Don't move it.
	 */
	if (F_ISSET(hcp, H_DELETED))
		return (0);

	args = vargs;

	if (hcp->pgno == old_pgno &&
	    hcp->indx == old_index &&
	    !MVCC_SKIP_CURADJ(cp, old_pgno)) {
		hcp->pgno = args->new_pgno;
		hcp->indx = args->new_index;
		if (args->my_txn != NULL && cp->txn != args->my_txn)
			*foundp = 1;
	}
	return (0);
}

/*
 * __hamc_chgpg --
 *	Adjust the cursors after moving an item to a new page.  We only
 *	move cursors that are pointing at this one item and are not
 *	deleted;  since we only touch non-deleted cursors, and since
 *	(by definition) no item existed at the pgno/indx we're moving the
 *	item to, we're guaranteed that all the cursors we affect here or
 *	on abort really do refer to this one item.
 */
static int
__hamc_chgpg(dbc, old_pgno, old_index, new_pgno, new_index)
	DBC *dbc;
	db_pgno_t old_pgno, new_pgno;
	u_int32_t old_index, new_index;
{
	DB *dbp;
	DB_LSN lsn;
	int ret;
	u_int32_t found;
	struct __hamc_chgpg_args args;

	dbp = dbc->dbp;

	args.my_txn = IS_SUBTRANSACTION(dbc->txn) ? dbc->txn : NULL;
	args.new_pgno = new_pgno;
	args.new_index = new_index;

	if ((ret = __db_walk_cursors(dbp, dbc,
	    __hamc_chgpg_func, &found, old_pgno, old_index, &args)) != 0)
		return (ret);
	if (found != 0 && DBC_LOGGING(dbc)) {
		if ((ret = __ham_chgpg_log(dbp,
		    args.my_txn, &lsn, 0, DB_HAM_CHGPG,
		    old_pgno, new_pgno, old_index, new_index)) != 0)
			return (ret);
	}
	return (0);
}

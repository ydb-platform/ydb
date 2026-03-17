/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id:
 */

#ifndef HAVE_HEAP
#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/heap.h"

/*
 * If the library wasn't compiled with the Heap access method, various
 * routines aren't available.  Stub them here, returning an appropriate
 * error.
 */

/*
 * __db_no_heap_am --
 *	Error when a Berkeley DB build doesn't include the access method.
 *
 * PUBLIC: int __db_no_heap_am __P((ENV *));
 */
int
__db_no_heap_am(env)
	ENV *env;
{
	__db_errx(env,
	    "library build did not include support for the Heap access method");
	return (DB_OPNOTSUP);
}

int
__heap_db_create(dbp)
	DB *dbp;
{
	COMPQUIET(dbp, NULL);
	return (0);
}

int
__heap_db_close(dbp)
	DB *dbp;
{
	COMPQUIET(dbp, NULL);
	return (0);
}

int
__heap_get_heapsize(dbp, gbytes, bytes)
	DB *dbp;
	u_int32_t *gbytes, *bytes;
{
	COMPQUIET(gbytes, NULL);
	COMPQUIET(bytes, NULL);
	return (__db_no_heap_am(dbp->env));
}

int
__heapc_dup(orig_dbc, new_dbc)
	DBC *orig_dbc, *new_dbc;
{
	COMPQUIET(new_dbc, NULL);
	return (__db_no_heap_am(orig_dbc->env));
}

int
__heapc_gsplit(dbc, dbt, bpp, bpsz)
	DBC *dbc;
	DBT *dbt;
	void **bpp;
	u_int32_t *bpsz;
{
	COMPQUIET(dbt, NULL);
	COMPQUIET(bpp, NULL);
	COMPQUIET(bpsz, NULL);
	return (__db_no_heap_am(dbc->env));
}

int
__heap_append(dbc, key, data)
	DBC *dbc;
	DBT *key, *data;
{
	COMPQUIET(key, NULL);
	COMPQUIET(data, NULL);
	return (__db_no_heap_am(dbc->env));
}

int
__heap_backup(dbenv, dbp, ip, fp, handle, flags)
	DB_ENV *dbenv;
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_FH *fp;
	void *handle;
	u_int32_t flags;
{
	COMPQUIET(dbp, NULL);
	COMPQUIET(ip, NULL);
	COMPQUIET(fp, NULL);
	COMPQUIET(handle, NULL);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbenv->env));
}

int
__heapc_init(dbc)
	DBC *dbc;
{
	return (__db_no_heap_am(dbc->env));
}

int
__heap_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	COMPQUIET(env, NULL);
	COMPQUIET(dtabp, NULL);
	return (0);
}

int
__heap_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	COMPQUIET(env, NULL);
	COMPQUIET(dtabp, NULL);
	return (0);
}

int
__heap_meta2pgset(dbp, vdp, heapmeta, pgset)
	DB *dbp;
	VRFY_DBINFO *vdp;
	HEAPMETA *heapmeta;
	DB *pgset;
{
	COMPQUIET(vdp, NULL);
	COMPQUIET(heapmeta, NULL);
	COMPQUIET(pgset, NULL);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_metachk(dbp, name, hm)
	DB *dbp;
	const char *name;
	HEAPMETA *hm;
{
	COMPQUIET(name, NULL);
	COMPQUIET(hm, NULL);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_new_file(dbp, ip, txn, fhp, name)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	DB_FH *fhp;
	const char *name;
{
	COMPQUIET(ip, NULL);
	COMPQUIET(txn, NULL);
	COMPQUIET(fhp, NULL);
	COMPQUIET(name, NULL);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_open(dbp, ip, txn, name, base_pgno, flags)
	DB *dbp;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	const char *name;
	db_pgno_t base_pgno;
	u_int32_t flags;
{
	COMPQUIET(ip, NULL);
	COMPQUIET(txn, NULL);
	COMPQUIET(name, NULL);
	COMPQUIET(base_pgno, 0);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_pgin(dbp, pg, pp, cookie)
	DB *dbp;
	db_pgno_t pg;
	void *pp;
	DBT *cookie;
{
	COMPQUIET(pg, 0);
	COMPQUIET(pp, NULL);
	COMPQUIET(cookie, NULL);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_pgout(dbp, pg, pp, cookie)
	 DB *dbp;
	 db_pgno_t pg;
	 void *pp;
	 DBT *cookie;
{
	COMPQUIET(pg, 0);
	COMPQUIET(pp, NULL);
	COMPQUIET(cookie, NULL);
	return (__db_no_heap_am(dbp->env));
}

void
__heap_print_cursor(dbc)
	DBC *dbc;
{
	(void)__db_no_heap_am(dbc->env);
}

int
__heapc_refresh(dbc)
	DBC *dbc;
{
	return (__db_no_heap_am(dbc->env));
}

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
	COMPQUIET(vdp, NULL);
	COMPQUIET(pgno, 0);
	COMPQUIET(handle, NULL);
	COMPQUIET(h, NULL);
	COMPQUIET(callback, NULL);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_stat(dbc, spp, flags)
	DBC *dbc;
	void *spp;
	u_int32_t flags;
{
	COMPQUIET(spp, NULL);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbc->env));
}

int
__heap_stat_print(dbc, flags)
	DBC *dbc;
	u_int32_t flags;
{
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbc->env));
}

int
__heap_truncate(dbc, countp)
	DBC *dbc;
	u_int32_t *countp;
{
	COMPQUIET(countp, NULL);
	return (__db_no_heap_am(dbc->env));
}

int
__heap_vrfy(dbp, vdbp, h, pgno, flags)
	DB *dbp;
	VRFY_DBINFO *vdbp;
	PAGE *h;
	db_pgno_t pgno;
	u_int32_t flags;
{
	COMPQUIET(h, NULL);
	COMPQUIET(vdbp, NULL);
	COMPQUIET(pgno, 0);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_vrfy_meta(dbp, vdp, meta, pgno, flags)
	DB *dbp;
	VRFY_DBINFO *vdp;
	HEAPMETA *meta;
	db_pgno_t pgno;
	u_int32_t flags;
{
	COMPQUIET(vdp, NULL);
	COMPQUIET(meta, NULL);
	COMPQUIET(pgno, 0);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_vrfy_structure(dbp, vdp, flags)
	DB *dbp;
	VRFY_DBINFO *vdp;
	u_int32_t flags;
{
	COMPQUIET(vdp, NULL);
	COMPQUIET(flags, 0);
	return (__db_no_heap_am(dbp->env));
}

int
__heap_exist()
{
	return (0);
}
#endif /* !HAVE_HEAP */

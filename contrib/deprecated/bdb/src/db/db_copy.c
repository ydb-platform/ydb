/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"

/*
 * db_copy --
 *	Copy a database file coordinated with mpool.
 * This is for backward compatibility to the quick fix in 5.2.
 *
 * EXTERN: int db_copy __P((DB_ENV *,
 * EXTERN:     const char *, const char *, const char *));
 */
int
db_copy(dbenv, dbfile, target, passwd)
	DB_ENV *dbenv;
	const char *dbfile;
	const char *target;
	const char *passwd;
{
	COMPQUIET(passwd, NULL);
	return (__db_dbbackup_pp(dbenv, dbfile, target, 0));
}

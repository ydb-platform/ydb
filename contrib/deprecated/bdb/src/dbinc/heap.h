/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 */

#ifndef _DB_HEAP_H_
#define _DB_HEAP_H_

#if defined(__cplusplus)
extern "C" {
#endif

/* Forward structure declarations. */
struct __heap;		typedef struct __heap HEAP;
struct __heap_cursor;	typedef struct __heap_cursor HEAP_CURSOR;

/*
 * The in-memory, per-heap data structure.
 */
struct __heap {		/* Heap access method. */
	
	u_int32_t gbytes;	/* Initial heap size. */
	u_int32_t bytes;	/* Initial heap size. */
	u_int32_t region_size;	/* Size of each region. */

	db_pgno_t curregion;	/* The region of the next insert. */
	db_pgno_t maxpgno;	/* Maximum page number of a fixed size heap. */
	int curpgindx;	/* The last used offset in the region's space bitmap. */
};

struct __heap_cursor {
	/* struct __dbc_internal */
	__DBC_INTERNAL

	/* Heap private part */

	u_int32_t	flags;
};

#define HEAP_PG_FULL	3	/* No space on page. */
#define HEAP_PG_GT66	2	/* Page greater than 66% full */
#define HEAP_PG_GT33	1	/* Page greater than 33% full */
#define HEAP_PG_LT33	0	/* Page less than 33% full */

#define HEAP_PG_FULL_PCT	5	/* Less than 5% of page is free. */
#define HEAP_PG_GT66_PCT	33	/* Less than 33% of page is free. */
#define HEAP_PG_GT33_PCT	66	/* Less than 66% of page is free. */

#if defined(__cplusplus)
}
#endif

#include <contrib/deprecated/bdb/src/dbinc_auto/heap_auto.h>
#include <contrib/deprecated/bdb/src/dbinc_auto/heap_ext.h>
#include <contrib/deprecated/bdb/src/dbinc/db_am.h>
#endif

	

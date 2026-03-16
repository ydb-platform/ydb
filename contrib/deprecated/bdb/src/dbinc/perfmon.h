/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#ifndef _DB_PERFMON_H_
#define	_DB_PERFMON_H_

/*******************************************************
 * Oracle Berkeley DB Performance Event Monitoring
 *
 * Some events inside of Oracle Berkeley DB can be 'published'
 * to the operating environment's performance tracing system
 * as they occur. Current support includes
 *	--enable-dtrace
 *		Solaris
 *		Linux (via SystemTap's dtrace wrappers)
 *		Darwin (Mac OS X)
 *		QNX(?)  
 *
 ******************************************************/

/*
 * The performance monitoring system can display many of the statistics which
 * are obtainable through the {DB,DB_ENV}->xxx_stat() functions. By default
 * they are excluded. They can be enabled with --enable-perfmon-statistics.
 */
#ifdef HAVE_PERFMON_STATISTICS
#define STAT_PERFMON1(env, cat, id, a1)		PERFMON1(env, cat, id, (a1))
#define STAT_PERFMON2(env, cat, id, a1, a2) 	\
    PERFMON2(env, cat, id, (a1), (a2))
#define STAT_PERFMON3(env, cat, id, a1, a2, a3)	\
    PERFMON3(env, cat, id, (a1), (a2), (a3))
#else
#define STAT_PERFMON1(env, cat, id, a1)		NOP_STATEMENT
#define STAT_PERFMON2(env, cat, id, a1, a2)	NOP_STATEMENT
#define STAT_PERFMON3(env, cat, id, a1, a2, a3)	NOP_STATEMENT
#endif


#if defined(HAVE_PERFMON) && defined(HAVE_STATISTICS)
/*
 * The DTrace macros which are generated at configure time in db_provider.h can
 * have full function signatures. These declarations are needed for compilation
 * when DTrace support is enabled. It is "too early" in the include sequence
 * to include the header files which define these structs.
 */
struct _db_page;
struct __bh;
struct __db_dbt;
struct __sh_dbt;
struct __db_mutex_t;

#if defined(HAVE_DTRACE)
/*
 * Solaris 10, Darwin/Mac OS X starting in 10.6 (Snow Leopard), Linux with
 * the DTrace-compatible version of SystemTap, possibly QNX.
 */
#include "db_provider.h"

#define PERFMON0(env, cat, id)		bdb_##cat##_##id()
#define PERFMON1(env, cat, id, a1)	bdb_##cat##_##id(a1)
#define PERFMON2(env, cat, id, a1, a2)					\
    bdb_##cat##_##id((a1), (a2))
#define PERFMON3(env, cat, id, a1, a2, a3)				\
    do {								\
    	if (PERFMON_ENABLED(env, cat, id))				\
	    bdb_##cat##_##id((a1), (a2), (a3));			\
    } while (0)
#define PERFMON4(env, cat, id, a1, a2, a3, a4)				\
    do {								\
    	if (PERFMON_ENABLED(env, cat, id))				\
	    bdb_##cat##_##id((a1), (a2), (a3), (a4));			\
    } while (0)
#define PERFMON5(env, cat, id, a1, a2, a3, a4, a5)			\
    do {								\
    	if (PERFMON_ENABLED(env, cat, id))				\
	    bdb_##cat##_##id((a1), (a2), (a3), (a4), (a5));		\
    } while (0)
#define PERFMON6(env, cat, id, a1, a2, a3, a4, a5, a6)			\
    do {								\
    	if (PERFMON_ENABLED(env, cat, id))				\
	    bdb_##cat##_##id((a1), (a2), (a3), (a4), (a5), (a6));	\
    } while (0)
#define PERFMON_ENABLED(env, cat, id)	 bdb_##cat##_##id##_enabled()
#endif

#else
/* Without HAVE_PERFMON or HAVE_STATISTICS these macros map to null bodies. */
#define PERFMON0(env, cat, id)				NOP_STATEMENT
#define PERFMON1(env, cat, id, a1)			NOP_STATEMENT
#define PERFMON2(env, cat, id, a1, a2)			NOP_STATEMENT
#define PERFMON3(env, cat, id, a1, a2, a3)		NOP_STATEMENT
#define PERFMON4(env, cat, id, a1, a2, a3, a4)		NOP_STATEMENT
#define PERFMON5(env, cat, id, a1, a2, a3, a4, a5)	NOP_STATEMENT
#define PERFMON6(env, cat, id, a1, a2, a3, a4, a5, a6)	NOP_STATEMENT
#define PERFMON_ENABLED(env, cat, id)	 		FALSE
#endif

#endif

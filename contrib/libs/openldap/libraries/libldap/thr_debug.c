/* thr_debug.c - wrapper around the chosen thread wrapper, for debugging. */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2005-2022 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/*
 * This package provides several types of thread operation debugging:
 *
 * - Check the results of operations on threads, mutexes, condition
 *   variables and read/write locks.  Also check some thread pool
 *   operations, but not those for which failure can happen in normal
 *   slapd operation.
 *
 * - Wrap those types except threads and pools in structs with state
 *   information, and check that on all operations:
 *
 *   + Check that the resources are initialized and are only used at
 *     their original address (i.e. not realloced or copied).
 *
 *   + Check the owner (thread ID) on mutex operations.
 *
 *   + Optionally allocate a reference to a byte of dummy memory.
 *     This lets malloc debuggers see some incorrect use as memory
 *     leaks, access to freed memory, etc.
 *
 * - Print an error message and by default abort() upon errors.
 *
 * - Print a count of leaked thread resources after cleanup.
 *
 * Compile-time (./configure) setup:  Macros defined in CPPFLAGS.
 *
 *   LDAP_THREAD_DEBUG or LDAP_THREAD_DEBUG=2
 *      Enables debugging, but value & 2 turns off type wrapping.
 *
 *   LDAP_UINTPTR_T=integer type to hold pointers, preferably unsigned.
 *      Used by dummy memory option "scramble". Default = unsigned long.
 *
 *   LDAP_DEBUG_THREAD_NONE = initializer for a "no thread" thread ID.
 *
 *   In addition, you may need to set up an implementation-specific way
 *      to enable whatever error checking your thread library provides.
 *      Currently only implemented for Posix threads (pthreads), where
 *      you may need to define LDAP_INT_THREAD_MUTEXATTR.  The default
 *      is PTHREAD_MUTEX_ERRORCHECK, or PTHREAD_MUTEX_ERRORCHECK_NP for
 *      Linux threads.  See pthread_mutexattr_settype(3).
 *
 * Run-time configuration:
 *
 *  Memory debugging tools:
 *   Tools that report uninitialized memory accesses should disable
 *   such warnings about the function debug_already_initialized().
 *   Alternatively, include "noreinit" (below) in $LDAP_THREAD_DEBUG.
 *
 *  Environment variable $LDAP_THREAD_DEBUG:
 *   The variable may contain a comma- or space-separated option list.
 *   Options:
 *      off      - Disable this package.  (It still slows things down).
 *      tracethreads - Report create/join/exit/kill of threads.
 *      noabort  - Do not abort() on errors.
 *      noerror  - Do not report errors.  Implies noabort.
 *      nocount  - Do not report counts of unreleased resources.
 *      nosync   - Disable tests that use synchronization and thus
 *                 clearly affect thread scheduling:
 *                 Implies nocount, and cancels threadID if that is set.
 *                 Note that if you turn on tracethreads or malloc
 *                 debugging, these also use library calls which may
 *                 affect thread scheduling (fprintf and malloc).
 *   The following options do not apply if type wrapping is disabled:
 *      nomem    - Do not check memory operations.
 *                 Implies noreinit,noalloc.
 *      noreinit - Do not catch reinitialization of existing resources.
 *                 (That test accesses uninitialized memory).
 *      threadID - Trace thread IDs.  Currently mostly useless.
 *     Malloc debugging -- allocate dummy memory for initialized
 *     resources, so malloc debuggers will report them as memory leaks:
 *      noalloc  - Default.  Do not allocate dummy memory.
 *      alloc    - Store a pointer to dummy memory.   However, leak
 *                 detectors might not catch unreleased resources in
 *                 global variables.
 *      scramble - Store bitwise complement of dummy memory pointer.
 *                 That never escapes memory leak detectors -
 *                 but detection while the program is running will
 *                 report active resources as leaks.  Do not
 *                 use this if a garbage collector is in use:-)
 *      adjptr   - Point to end of dummy memory.
 *                 Purify reports these as "potential leaks" (PLK).
 *                 I have not checked other malloc debuggers.
 */

#include "portable.h"

#if defined( LDAP_THREAD_DEBUG )

#include <stdio.h>
#include <ac/errno.h>
#include <ac/stdlib.h>
#include <ac/string.h>

#include "ldap_pvt_thread.h" /* Get the thread interface */
#define LDAP_THREAD_IMPLEMENTATION
#define LDAP_THREAD_DEBUG_IMPLEMENTATION
#define LDAP_THREAD_RDWR_IMPLEMENTATION
#define LDAP_THREAD_POOL_IMPLEMENTATION
#include "ldap_thr_debug.h"  /* Get the underlying implementation */

#ifndef LDAP_THREAD_DEBUG_WRAP
#undef	LDAP_THREAD_DEBUG_THREAD_ID
#elif !defined LDAP_THREAD_DEBUG_THREAD_ID
#define	LDAP_THREAD_DEBUG_THREAD_ID 1
#endif

/* Use native malloc - the OpenLDAP wrappers may defeat malloc debuggers */
#undef malloc
#undef calloc
#undef realloc
#undef free


/* Options from environment variable $LDAP_THREAD_DEBUG */
enum { Count_no = 0, Count_yes, Count_reported, Count_reported_more };
static int count = Count_yes;
#ifdef LDAP_THREAD_DEBUG_WRAP
enum { Wrap_noalloc, Wrap_alloc, Wrap_scramble, Wrap_adjptr };
static int wraptype = Wrap_noalloc, wrap_offset, unwrap_offset;
static int nomem, noreinit;
#endif
#if LDAP_THREAD_DEBUG_THREAD_ID +0
static int threadID;
#else
enum { threadID = 0 };
#endif
static int nodebug, noabort, noerror, nosync, tracethreads;
static int wrap_threads;
static int options_done;


/* ldap_pvt_thread_initialize() called, ldap_pvt_thread_destroy() not called */
static int threading_enabled;


/* Resource counts */
enum {
	Idx_unexited_thread, Idx_unjoined_thread, Idx_locked_mutex,
	Idx_mutex, Idx_cond, Idx_rdwr, Idx_tpool, Idx_max
};
static int resource_counts[Idx_max];
static const char *const resource_names[] = {
	"unexited threads", "unjoined threads", "locked mutexes",
	"mutexes", "conds", "rdwrs", "thread pools"
};
static ldap_int_thread_mutex_t resource_mutexes[Idx_max];


/* Hide pointers from malloc debuggers. */
#define SCRAMBLE(ptr) (~(LDAP_UINTPTR_T) (ptr))
#define UNSCRAMBLE_usagep(num) ((ldap_debug_usage_info_t *) ~(num))
#define UNSCRAMBLE_dummyp(num) ((unsigned char *) ~(num))


#define WARN(var, msg)   (warn (__FILE__, __LINE__, (msg), #var, (var)))
#define WARN_IF(rc, msg) {if (rc) warn (__FILE__, __LINE__, (msg), #rc, (rc));}

#define ERROR(var, msg) { \
	if (!noerror) { \
		errmsg(__FILE__, __LINE__, (msg), #var, (var)); \
		if( !noabort ) abort(); \
	} \
}

#define ERROR_IF(rc, msg) { \
	if (!noerror) { \
		int rc_ = (rc); \
		if (rc_) { \
			errmsg(__FILE__, __LINE__, (msg), #rc, rc_); \
			if( !noabort ) abort(); \
		} \
	} \
}

#ifdef LDAP_THREAD_DEBUG_WRAP
#define MEMERROR_IF(rc, msg, mem_act) { \
	if (!noerror) { \
		int rc_ = (rc); \
		if (rc_) { \
			errmsg(__FILE__, __LINE__, (msg), #rc, rc_); \
			if( wraptype != Wrap_noalloc ) { mem_act; } \
			if( !noabort ) abort(); \
		} \
	} \
}
#endif /* LDAP_THREAD_DEBUG_WRAP */

#if 0
static void
warn( const char *file, int line, const char *msg, const char *var, int val )
{
	fprintf( stderr,
		(strpbrk( var, "!=" )
		 ? "%s:%d: %s warning: %s\n"
		 : "%s:%d: %s warning: %s is %d\n"),
		file, line, msg, var, val );
}
#endif

static void
errmsg( const char *file, int line, const char *msg, const char *var, int val )
{
	fprintf( stderr,
		(strpbrk( var, "!=" )
		 ? "%s:%d: %s error: %s\n"
		 : "%s:%d: %s error: %s is %d\n"),
		file, line, msg, var, val );
}

static void
count_resource_leaks( void )
{
	int i, j;
	char errbuf[200];
	if( count == Count_yes ) {
		count = Count_reported;
#if 0 /* Could break if there are still threads after atexit */
		for( i = j = 0; i < Idx_max; i++ )
			j |= ldap_int_thread_mutex_destroy( &resource_mutexes[i] );
		WARN_IF( j, "ldap_debug_thread_destroy:mutexes" );
#endif
		for( i = j = 0; i < Idx_max; i++ )
			if( resource_counts[i] )
				j += sprintf( errbuf + j, ", %d %s",
					resource_counts[i], resource_names[i] );
		if( j )
			fprintf( stderr, "== thr_debug: Leaked%s. ==\n", errbuf + 1 );
	}
}

static void
get_options( void )
{
	static const struct option_info_s {
		const char	*name;
		int       	*var, val;
	} option_info[] = {
		{ "off",        &nodebug,  1 },
		{ "noabort",    &noabort,  1 },
		{ "noerror",    &noerror,  1 },
		{ "nocount",    &count,    Count_no },
		{ "nosync",     &nosync,   1 },
#if LDAP_THREAD_DEBUG_THREAD_ID +0
		{ "threadID",   &threadID, 1 },
#endif
#ifdef LDAP_THREAD_DEBUG_WRAP
		{ "nomem",      &nomem,    1 },
		{ "noreinit",   &noreinit, 1 },
		{ "noalloc",    &wraptype, Wrap_noalloc },
		{ "alloc",      &wraptype, Wrap_alloc },
		{ "adjptr",     &wraptype, Wrap_adjptr },
		{ "scramble",	&wraptype, Wrap_scramble },
#endif
		{ "tracethreads", &tracethreads, 1 },
		{ NULL, NULL, 0 }
	};
	const char *s = getenv( "LDAP_THREAD_DEBUG" );
	if( s != NULL ) {
		while( *(s += strspn( s, ", \t\r\n" )) != '\0' ) {
			size_t optlen = strcspn( s, ", \t\r\n" );
			const struct option_info_s *oi = option_info;
			while( oi->name &&
				   (strncasecmp( oi->name, s, optlen ) || oi->name[optlen]) )
				oi++;
			if( oi->name )
				*oi->var = oi->val;
			else
				fprintf( stderr,
					"== thr_debug: Unknown $%s option '%.*s' ==\n",
					"LDAP_THREAD_DEBUG", (int) optlen, s );
			s += optlen;
		}
	}
	if( nodebug ) {
		tracethreads = 0;
		nosync = noerror = 1;
	}
	if( nosync )
		count = Count_no;
	if( noerror )
		noabort = 1;
#if LDAP_THREAD_DEBUG_THREAD_ID +0
	if( nosync )
		threadID = 0;
#endif
#ifdef LDAP_THREAD_DEBUG_WRAP
	if( noerror )
		nomem = 1;
	if( !nomem ) {
		static const ldap_debug_usage_info_t usage;
		if( sizeof(LDAP_UINTPTR_T) < sizeof(unsigned char *)
			|| sizeof(LDAP_UINTPTR_T) < sizeof(ldap_debug_usage_info_t *)
			|| UNSCRAMBLE_usagep( SCRAMBLE( &usage ) ) != &usage
			|| UNSCRAMBLE_dummyp( SCRAMBLE( (unsigned char *) 0 ) ) )
		{
			fputs( "== thr_debug: Memory checks unsupported, "
				"adding nomem to $LDAP_THREAD_DEBUG ==\n", stderr );
			nomem = 1;
		}
	}
	if( nomem ) {
		noreinit = 1;
		wraptype = Wrap_noalloc;
	}
	unwrap_offset = -(wrap_offset = (wraptype == Wrap_adjptr));
#endif
	wrap_threads = (tracethreads || threadID || count);
	options_done = 1;
}


#ifndef LDAP_THREAD_DEBUG_WRAP

#define	WRAPPED(ptr)			(ptr)
#define	GET_OWNER(ptr)			0
#define	SET_OWNER(ptr, thread)	((void) 0)
#define	RESET_OWNER(ptr)		((void) 0)
#define	ASSERT_OWNER(ptr, msg)	((void) 0)
#define	ASSERT_NO_OWNER(ptr, msg) ((void) 0)

#define init_usage(ptr, msg)	((void) 0)
#define check_usage(ptr, msg)	((void) 0)
#define destroy_usage(ptr)		((void) 0)

#else /* LDAP_THREAD_DEBUG_WRAP */

/* Specialize this if the initializer is not appropriate. */
/* The ASSERT_NO_OWNER() definition may also need an override. */
#ifndef LDAP_DEBUG_THREAD_NONE
#define	LDAP_DEBUG_THREAD_NONE { -1 } /* "no thread" ldap_int_thread_t value */
#endif

static const ldap_int_thread_t ldap_debug_thread_none = LDAP_DEBUG_THREAD_NONE;

#define THREAD_MUTEX_OWNER(mutex) \
	ldap_int_thread_equal( (mutex)->owner, ldap_int_thread_self() )

void
ldap_debug_thread_assert_mutex_owner(
	const char *file,
	int line,
	const char *msg,
	ldap_pvt_thread_mutex_t *mutex )
{
	if( !(noerror || THREAD_MUTEX_OWNER( mutex )) ) {
		errmsg( file, line, msg, "ASSERT_MUTEX_OWNER", 0 );
		if( !noabort ) abort();
	}
}

#define	WRAPPED(ptr)			(&(ptr)->wrapped)
#define	GET_OWNER(ptr)			((ptr)->owner)
#define	SET_OWNER(ptr, thread)	((ptr)->owner = (thread))
#define	RESET_OWNER(ptr)		((ptr)->owner = ldap_debug_thread_none)
#define	ASSERT_OWNER(ptr, msg)	ERROR_IF( !THREAD_MUTEX_OWNER( ptr ), msg )
#ifndef	ASSERT_NO_OWNER
#define	ASSERT_NO_OWNER(ptr, msg) ERROR_IF( \
	!ldap_int_thread_equal( (ptr)->owner, ldap_debug_thread_none ), msg )
#endif

/* Try to provoke memory access error (for malloc debuggers) */
#define PEEK(mem) {if (-*(volatile const unsigned char *)(mem)) debug_noop();}

static void debug_noop( void );
static int debug_already_initialized( const ldap_debug_usage_info_t *usage );

/* Name used for clearer error message */
#define IS_COPY_OR_MOVED(usage) ((usage)->self != SCRAMBLE( usage ))

#define DUMMY_ADDR(usage) \
	(wraptype == Wrap_scramble \
	 ? UNSCRAMBLE_dummyp( (usage)->mem.num ) \
	 : (usage)->mem.ptr + unwrap_offset)

/* Mark resource as initialized */
static void
init_usage( ldap_debug_usage_info_t *usage, const char *msg )
{
	if( !options_done )
		get_options();
	if( !nomem ) {
		if( !noreinit ) {
			MEMERROR_IF( debug_already_initialized( usage ), msg, {
				/* Provoke malloc debuggers */
				unsigned char *dummy = DUMMY_ADDR( usage );
				PEEK( dummy );
				free( dummy );
				free( dummy );
			} );
		}
		if( wraptype != Wrap_noalloc ) {
			unsigned char *dummy = malloc( 1 );
			assert( dummy != NULL );
			if( wraptype == Wrap_scramble ) {
				usage->mem.num = SCRAMBLE( dummy );
				/* Verify that ptr<->integer casts work on this host */
				assert( UNSCRAMBLE_dummyp( usage->mem.num ) == dummy );
			} else {
				usage->mem.ptr = dummy + wrap_offset;
			}
		}
	} else {
		/* Unused, but set for readability in debugger */
		usage->mem.ptr = NULL;
	}
	usage->self = SCRAMBLE( usage );	/* If nomem, only for debugger */
	usage->magic = ldap_debug_magic;
	usage->state = ldap_debug_state_inited;
}

/* Check that resource is initialized and not copied/realloced */
static void
check_usage( const ldap_debug_usage_info_t *usage, const char *msg )
{
	enum { Is_destroyed = 1 };	/* Name used for clearer error message */

	if( usage->magic != ldap_debug_magic ) {
		ERROR( usage->magic, msg );
		return;
	}
	switch( usage->state ) {
	case ldap_debug_state_destroyed:
		MEMERROR_IF( Is_destroyed, msg, {
			PEEK( DUMMY_ADDR( usage ) );
		} );
		break;
	default:
		ERROR( usage->state, msg );
		break;
	case ldap_debug_state_inited:
		if( !nomem ) {
			MEMERROR_IF( IS_COPY_OR_MOVED( usage ), msg, {
				PEEK( DUMMY_ADDR( usage ) );
				PEEK( UNSCRAMBLE_usagep( usage->self ) );
			} );
		}
		break;
	}
}

/* Mark resource as destroyed. */
/* Does not check for errors, call check_usage()/init_usage() first. */
static void
destroy_usage( ldap_debug_usage_info_t *usage )
{
	if( usage->state == ldap_debug_state_inited ) {
		if( wraptype != Wrap_noalloc ) {
			free( DUMMY_ADDR( usage ) );
			/* Do not reset the DUMMY_ADDR, leave it for malloc debuggers
			 * in case the resource is used after it is freed. */
		}
		usage->state = ldap_debug_state_destroyed;
	}
}

/* Define these after they are used, so they are hopefully not inlined */

static void
debug_noop( void )
{
}

/*
 * Valid programs access uninitialized memory here unless "noreinit".
 *
 * Returns true if the resource is initialized and not copied/realloced.
 */
LDAP_GCCATTR((noinline))
static int
debug_already_initialized( const ldap_debug_usage_info_t *usage )
{
	/*
	 * 'ret' keeps the Valgrind warning "Conditional jump or move
	 * depends on uninitialised value(s)" _inside_ this function.
	 */
	volatile int ret = 0;
	if( usage->state == ldap_debug_state_inited )
		if( !IS_COPY_OR_MOVED( usage ) )
	        if( usage->magic == ldap_debug_magic )
				ret = 1;
	return ret;
}

#endif /* LDAP_THREAD_DEBUG_WRAP */


#if !(LDAP_THREAD_DEBUG_THREAD_ID +0)

typedef void ldap_debug_thread_t;
#define init_thread_info()	{}
#define with_thread_info_lock(statements) { statements; }
#define thread_info_detached(t)	0
#define add_thread_info(msg, thr, det)	((void) 0)
#define remove_thread_info(tinfo, msg)	((void) 0)
#define get_thread_info(thread, msg)	NULL

#else /* LDAP_THREAD_DEBUG_THREAD_ID */

/*
 * Thread ID tracking.  Currently achieves little.
 * Should be either expanded or deleted.
 */

/*
 * Array of threads.  Used instead of making ldap_pvt_thread_t a wrapper
 * around ldap_int_thread_t, which would slow down ldap_pvt_thread_self().
 */
typedef struct {
	ldap_pvt_thread_t           wrapped;
	ldap_debug_usage_info_t     usage;
	int                         detached;
	int                         idx;
} ldap_debug_thread_t;

static ldap_debug_thread_t      **thread_info;
static unsigned int             thread_info_size, thread_info_used;
static ldap_int_thread_mutex_t  thread_info_mutex;

#define init_thread_info() { \
	if( threadID ) { \
		int mutex_init_rc = ldap_int_thread_mutex_init( &thread_info_mutex ); \
		assert( mutex_init_rc == 0 ); \
	} \
}

#define with_thread_info_lock(statements) { \
	int rc_wtl_ = ldap_int_thread_mutex_lock( &thread_info_mutex ); \
	assert( rc_wtl_ == 0 ); \
	{ statements; } \
	rc_wtl_ = ldap_int_thread_mutex_unlock( &thread_info_mutex ); \
	assert( rc_wtl_ == 0 ); \
}

#define thread_info_detached(t) ((t)->detached)

static void
add_thread_info(
	const char *msg,
	const ldap_pvt_thread_t *thread,
	int detached )
{
	ldap_debug_thread_t *t;

	if( thread_info_used >= thread_info_size ) {
		unsigned int more = thread_info_size + 8;
		unsigned int new_size = thread_info_size + more;

		t = calloc( more, sizeof(ldap_debug_thread_t) );
		assert( t != NULL );
		thread_info = realloc( thread_info, new_size * sizeof(*thread_info) );
		assert( thread_info != NULL );
		do {
			t->idx = thread_info_size;
			thread_info[thread_info_size++] = t++;
		} while( thread_info_size < new_size );
	}

	t = thread_info[thread_info_used];
	init_usage( &t->usage, msg );
	t->wrapped = *thread;
	t->detached = detached;
	thread_info_used++;
}

static void
remove_thread_info( ldap_debug_thread_t *t, const char *msg )
{
		ldap_debug_thread_t *last;
		int idx;
		check_usage( &t->usage, msg );
		destroy_usage( &t->usage );
		idx = t->idx;
		assert( thread_info[idx] == t );
		last = thread_info[--thread_info_used];
		assert( last->idx == thread_info_used );
		(thread_info[idx]              = last)->idx = idx;
		(thread_info[thread_info_used] = t   )->idx = thread_info_used;
}

static ldap_debug_thread_t *
get_thread_info( ldap_pvt_thread_t thread, const char *msg )
{
	unsigned int i;
	ldap_debug_thread_t *t;
	for( i = 0; i < thread_info_used; i++ ) {
		if( ldap_pvt_thread_equal( thread, thread_info[i]->wrapped ) )
			break;
	}
	ERROR_IF( i == thread_info_used, msg );
	t = thread_info[i];
	check_usage( &t->usage, msg );
	return t;
}

#endif /* LDAP_THREAD_DEBUG_THREAD_ID */


static char *
thread_name( char *buf, int bufsize, ldap_pvt_thread_t thread )
{
	int i;
	--bufsize;
	if( bufsize > 2*sizeof(thread) )
		bufsize = 2*sizeof(thread);
	for( i = 0; i < bufsize; i += 2 )
		snprintf( buf+i, 3, "%02x", ((unsigned char *)&thread)[i/2] );
	return buf;
}


/* Add <adjust> (+/-1) to resource count <which> unless "nocount". */
static void
adjust_count( int which, int adjust )
{
	int rc;
	switch( count ) {
	case Count_no:
		break;
	case Count_yes:
		rc = ldap_int_thread_mutex_lock( &resource_mutexes[which] );
		assert( rc == 0 );
		resource_counts[which] += adjust;
		rc = ldap_int_thread_mutex_unlock( &resource_mutexes[which] );
		assert( rc == 0 );
		break;
	case Count_reported:
		fputs( "== thr_debug: More thread activity after exit ==\n", stderr );
		count = Count_reported_more;
		/* FALL THROUGH */
	case Count_reported_more:
		/* Not used, but result might be inspected with debugger */
		/* (Hopefully threading is disabled by now...) */
		resource_counts[which] += adjust;
		break;
	}
}


/* Wrappers for LDAP_THREAD_IMPLEMENTATION: */

/* Used instead of ldap_int_thread_initialize by ldap_pvt_thread_initialize */
int
ldap_debug_thread_initialize( void )
{
	int i, rc, rc2;
	if( !options_done )
		get_options();
	ERROR_IF( threading_enabled, "ldap_debug_thread_initialize" );
	threading_enabled = 1;
	rc = ldap_int_thread_initialize();
	if( rc ) {
		ERROR( rc, "ldap_debug_thread_initialize:threads" );
		threading_enabled = 0;
	} else {
		init_thread_info();
		if( count != Count_no ) {
			for( i = rc2 = 0; i < Idx_max; i++ )
				rc2 |= ldap_int_thread_mutex_init( &resource_mutexes[i] );
			assert( rc2 == 0 );
			/* FIXME: Only for static libldap as in init.c? If so, why? */
			atexit( count_resource_leaks );
		}
	}
	return rc;
}

/* Used instead of ldap_int_thread_destroy by ldap_pvt_thread_destroy */
int
ldap_debug_thread_destroy( void )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_debug_thread_destroy" );
	/* sleep(1) -- need to wait for thread pool to finish? */
	rc = ldap_int_thread_destroy();
	if( rc ) {
		ERROR( rc, "ldap_debug_thread_destroy:threads" );
	} else {
		threading_enabled = 0;
	}
	return rc;
}

int
ldap_pvt_thread_set_concurrency( int n )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_set_concurrency" );
	rc = ldap_int_thread_set_concurrency( n );
	ERROR_IF( rc, "ldap_pvt_thread_set_concurrency" );
	return rc;
}

int
ldap_pvt_thread_get_concurrency( void )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_get_concurrency" );
	rc = ldap_int_thread_get_concurrency();
	ERROR_IF( rc, "ldap_pvt_thread_get_concurrency" );
	return rc;
}

unsigned int
ldap_pvt_thread_sleep( unsigned int interval )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_sleep" );
	rc = ldap_int_thread_sleep( interval );
	ERROR_IF( rc, "ldap_pvt_thread_sleep" );
	return 0;
}

static void
thread_exiting( const char *how, const char *msg )
{
	ldap_pvt_thread_t thread;
#if 0 /* Detached threads may exit after ldap_debug_thread_destroy(). */
	ERROR_IF( !threading_enabled, msg );
#endif
	thread = ldap_pvt_thread_self();
	if( tracethreads ) {
		char buf[40];
		fprintf( stderr, "== thr_debug: %s thread %s ==\n",
			how, thread_name( buf, sizeof(buf), thread ) );
	}
	if( threadID ) {
		with_thread_info_lock({
			ldap_debug_thread_t *t = get_thread_info( thread, msg );
			if( thread_info_detached( t ) )
				remove_thread_info( t, msg );
		});
	}
	adjust_count( Idx_unexited_thread, -1 );
}

void
ldap_pvt_thread_exit( void *retval )
{
	thread_exiting( "Exiting", "ldap_pvt_thread_exit" );
	ldap_int_thread_exit( retval );
}

typedef struct {
	void *(*start_routine)( void * );
	void *arg;
} ldap_debug_thread_call_t;

static void *
ldap_debug_thread_wrapper( void *arg )
{
	void *ret;
	ldap_debug_thread_call_t call = *(ldap_debug_thread_call_t *)arg;
	free( arg );
	ret = call.start_routine( call.arg );
	thread_exiting( "Returning from", "ldap_debug_thread_wrapper" );
	return ret;
}

int
ldap_pvt_thread_create(
	ldap_pvt_thread_t *thread,
	int detach,
	void *(*start_routine)( void * ),
	void *arg )
{
	int rc;
	if( !options_done )
		get_options();
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_create" );

	if( wrap_threads ) {
		ldap_debug_thread_call_t *call = malloc(
			sizeof( ldap_debug_thread_call_t ) );
		assert( call != NULL );
		call->start_routine = start_routine;
		call->arg = arg;
		start_routine = ldap_debug_thread_wrapper;
		arg = call;
	}
	if( threadID ) {
		with_thread_info_lock({
			rc = ldap_int_thread_create( thread, detach, start_routine, arg );
			if( rc == 0 )
				add_thread_info( "ldap_pvt_thread_create", thread, detach );
		});
	} else {
		rc = ldap_int_thread_create( thread, detach, start_routine, arg );
	}
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_create" );
		if( wrap_threads )
			free( arg );
	} else {
		if( tracethreads ) {
			char buf[40], buf2[40];
			fprintf( stderr,
				"== thr_debug: Created thread %s%s from thread %s ==\n",
				thread_name( buf, sizeof(buf), *thread ),
				detach ? " (detached)" : "",
				thread_name( buf2, sizeof(buf2), ldap_pvt_thread_self() ) );
		}
		adjust_count( Idx_unexited_thread, +1 );
		if( !detach )
			adjust_count( Idx_unjoined_thread, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_join( ldap_pvt_thread_t thread, void **thread_return )
{
	int rc;
	ldap_debug_thread_t *t = NULL;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_join" );
	if( tracethreads ) {
		char buf[40], buf2[40];
		fprintf( stderr, "== thr_debug: Joining thread %s in thread %s ==\n",
			thread_name( buf, sizeof(buf), thread ),
			thread_name( buf2, sizeof(buf2), ldap_pvt_thread_self() ) );
	}
	if( threadID )
		with_thread_info_lock( {
			t = get_thread_info( thread, "ldap_pvt_thread_join" );
			ERROR_IF( thread_info_detached( t ), "ldap_pvt_thread_join" );
		} );
	rc = ldap_int_thread_join( thread, thread_return );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_join" );
	} else {
		if( threadID )
			with_thread_info_lock(
				remove_thread_info( t, "ldap_pvt_thread_join" ) );
		adjust_count( Idx_unjoined_thread, -1 );
	}

	return rc;
}

int
ldap_pvt_thread_kill( ldap_pvt_thread_t thread, int signo )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_kill" );
	if( tracethreads ) {
		char buf[40], buf2[40];
		fprintf( stderr,
			"== thr_debug: Killing thread %s (sig %i) from thread %s ==\n",
			thread_name( buf, sizeof(buf), thread ), signo,
			thread_name( buf2, sizeof(buf2), ldap_pvt_thread_self() ) );
	}
	rc = ldap_int_thread_kill( thread, signo );
	ERROR_IF( rc, "ldap_pvt_thread_kill" );
	return rc;
}

int
ldap_pvt_thread_yield( void )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_yield" );
	rc = ldap_int_thread_yield();
	ERROR_IF( rc, "ldap_pvt_thread_yield" );
	return rc;
}

ldap_pvt_thread_t
ldap_pvt_thread_self( void )
{
#if 0 /* Function is used by ch_free() via slap_sl_contxt() in slapd */
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_self" );
#endif
	return ldap_int_thread_self();
}

int
ldap_pvt_thread_cond_init( ldap_pvt_thread_cond_t *cond )
{
	int rc;
	init_usage( &cond->usage, "ldap_pvt_thread_cond_init" );
	rc = ldap_int_thread_cond_init( WRAPPED( cond ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_cond_init" );
		destroy_usage( &cond->usage );
	} else {
		adjust_count( Idx_cond, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_cond_destroy( ldap_pvt_thread_cond_t *cond )
{
	int rc;
	check_usage( &cond->usage, "ldap_pvt_thread_cond_destroy" );
	rc = ldap_int_thread_cond_destroy( WRAPPED( cond ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_cond_destroy" );
	} else {
		destroy_usage( &cond->usage );
		adjust_count( Idx_cond, -1 );
	}
	return rc;
}

int
ldap_pvt_thread_cond_signal( ldap_pvt_thread_cond_t *cond )
{
	int rc;
	check_usage( &cond->usage, "ldap_pvt_thread_cond_signal" );
	rc = ldap_int_thread_cond_signal( WRAPPED( cond ) );
	ERROR_IF( rc, "ldap_pvt_thread_cond_signal" );
	return rc;
}

int
ldap_pvt_thread_cond_broadcast( ldap_pvt_thread_cond_t *cond )
{
	int rc;
	check_usage( &cond->usage, "ldap_pvt_thread_cond_broadcast" );
	rc = ldap_int_thread_cond_broadcast( WRAPPED( cond ) );
	ERROR_IF( rc, "ldap_pvt_thread_cond_broadcast" );
	return rc;
}

int
ldap_pvt_thread_cond_wait(
	ldap_pvt_thread_cond_t *cond,
	ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	ldap_int_thread_t owner;
	check_usage( &cond->usage, "ldap_pvt_thread_cond_wait:cond" );
	check_usage( &mutex->usage, "ldap_pvt_thread_cond_wait:mutex" );
	adjust_count( Idx_locked_mutex, -1 );
	owner = GET_OWNER( mutex );
	ASSERT_OWNER( mutex, "ldap_pvt_thread_cond_wait" );
	RESET_OWNER( mutex );
	rc = ldap_int_thread_cond_wait( WRAPPED( cond ), WRAPPED( mutex ) );
	ASSERT_NO_OWNER( mutex, "ldap_pvt_thread_cond_wait" );
	SET_OWNER( mutex, rc ? owner : ldap_int_thread_self() );
	adjust_count( Idx_locked_mutex, +1 );
	ERROR_IF( rc, "ldap_pvt_thread_cond_wait" );
	return rc;
}

int
ldap_pvt_thread_mutex_recursive_init( ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	init_usage( &mutex->usage, "ldap_pvt_thread_mutex_recursive_init" );
	rc = ldap_int_thread_mutex_recursive_init( WRAPPED( mutex ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_mutex_recursive_init" );
		destroy_usage( &mutex->usage );
	} else {
		RESET_OWNER( mutex );
		adjust_count( Idx_mutex, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_mutex_init( ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	init_usage( &mutex->usage, "ldap_pvt_thread_mutex_init" );
	rc = ldap_int_thread_mutex_init( WRAPPED( mutex ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_mutex_init" );
		destroy_usage( &mutex->usage );
	} else {
		RESET_OWNER( mutex );
		adjust_count( Idx_mutex, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_mutex_destroy( ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	check_usage( &mutex->usage, "ldap_pvt_thread_mutex_destroy" );
	ASSERT_NO_OWNER( mutex, "ldap_pvt_thread_mutex_destroy" );
	rc = ldap_int_thread_mutex_destroy( WRAPPED( mutex ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_mutex_destroy" );
	} else {
		destroy_usage( &mutex->usage );
		RESET_OWNER( mutex );
		adjust_count( Idx_mutex, -1 );
	}
	return rc;
}

int
ldap_pvt_thread_mutex_lock( ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	check_usage( &mutex->usage, "ldap_pvt_thread_mutex_lock" );
	rc = ldap_int_thread_mutex_lock( WRAPPED( mutex ) );
	if( rc ) {
		ERROR_IF( rc, "ldap_pvt_thread_mutex_lock" );
	} else {
		ASSERT_NO_OWNER( mutex, "ldap_pvt_thread_mutex_lock" );
		SET_OWNER( mutex, ldap_int_thread_self() );
		adjust_count( Idx_locked_mutex, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_mutex_trylock( ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	check_usage( &mutex->usage, "ldap_pvt_thread_mutex_trylock" );
	rc = ldap_int_thread_mutex_trylock( WRAPPED( mutex ) );
	if( rc == 0 ) {
		ASSERT_NO_OWNER( mutex, "ldap_pvt_thread_mutex_trylock" );
		SET_OWNER( mutex, ldap_int_thread_self() );
		adjust_count( Idx_locked_mutex, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_mutex_unlock( ldap_pvt_thread_mutex_t *mutex )
{
	int rc;
	check_usage( &mutex->usage, "ldap_pvt_thread_mutex_unlock" );
	ASSERT_OWNER( mutex, "ldap_pvt_thread_mutex_unlock" );
	RESET_OWNER( mutex ); /* Breaks if this thread did not own the mutex */
	rc = ldap_int_thread_mutex_unlock( WRAPPED( mutex ) );
	if( rc ) {
		ERROR_IF( rc, "ldap_pvt_thread_mutex_unlock" );
	} else {
		adjust_count( Idx_locked_mutex, -1 );
	}
	return rc;
}


/* Wrappers for LDAP_THREAD_RDWR_IMPLEMENTATION: */

int
ldap_pvt_thread_rdwr_init( ldap_pvt_thread_rdwr_t *rwlock )
{
	int rc;
	init_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_init" );
	rc = ldap_int_thread_rdwr_init( WRAPPED( rwlock ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_rdwr_init" );
		destroy_usage( &rwlock->usage );
	} else {
		adjust_count( Idx_rdwr, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_rdwr_destroy( ldap_pvt_thread_rdwr_t *rwlock )
{
	int rc;
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_destroy" );
	rc = ldap_int_thread_rdwr_destroy( WRAPPED( rwlock ) );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_rdwr_destroy" );
	} else {
		destroy_usage( &rwlock->usage );
		adjust_count( Idx_rdwr, -1 );
	}
	return rc;
}

int
ldap_pvt_thread_rdwr_rlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	int rc;
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_rlock" );
	rc = ldap_int_thread_rdwr_rlock( WRAPPED( rwlock ) );
	ERROR_IF( rc, "ldap_pvt_thread_rdwr_rlock" );
	return rc;
}

int
ldap_pvt_thread_rdwr_rtrylock( ldap_pvt_thread_rdwr_t *rwlock )
{
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_rtrylock" );
	return ldap_int_thread_rdwr_rtrylock( WRAPPED( rwlock ) );
}

int
ldap_pvt_thread_rdwr_runlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	int rc;
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_runlock" );
	rc = ldap_int_thread_rdwr_runlock( WRAPPED( rwlock ) );
	ERROR_IF( rc, "ldap_pvt_thread_rdwr_runlock" );
	return rc;
}

int
ldap_pvt_thread_rdwr_wlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	int rc;
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_wlock" );
	rc = ldap_int_thread_rdwr_wlock( WRAPPED( rwlock ) );
	ERROR_IF( rc, "ldap_pvt_thread_rdwr_wlock" );
	return rc;
}

int
ldap_pvt_thread_rdwr_wtrylock( ldap_pvt_thread_rdwr_t *rwlock )
{
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_wtrylock" );
	return ldap_int_thread_rdwr_wtrylock( WRAPPED( rwlock ) );
}

int
ldap_pvt_thread_rdwr_wunlock( ldap_pvt_thread_rdwr_t *rwlock )
{
	int rc;
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_wunlock" );
	rc = ldap_int_thread_rdwr_wunlock( WRAPPED( rwlock ) );
	ERROR_IF( rc, "ldap_pvt_thread_rdwr_wunlock" );
	return rc;
}

#if defined(LDAP_RDWR_DEBUG) && !defined(LDAP_THREAD_HAVE_RDWR)

int
ldap_pvt_thread_rdwr_readers( ldap_pvt_thread_rdwr_t *rwlock )
{
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_readers" );
	return ldap_int_thread_rdwr_readers( WRAPPED( rwlock ) );
}

int
ldap_pvt_thread_rdwr_writers( ldap_pvt_thread_rdwr_t *rwlock )
{
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_writers" );
	return ldap_int_thread_rdwr_writers( WRAPPED( rwlock ) );
}

int
ldap_pvt_thread_rdwr_active( ldap_pvt_thread_rdwr_t *rwlock )
{
	check_usage( &rwlock->usage, "ldap_pvt_thread_rdwr_active" );
	return ldap_int_thread_rdwr_active( WRAPPED( rwlock ) );
}

#endif /* LDAP_RDWR_DEBUG && !LDAP_THREAD_HAVE_RDWR */


/* Some wrappers for LDAP_THREAD_POOL_IMPLEMENTATION: */
#ifdef LDAP_THREAD_POOL_IMPLEMENTATION

int
ldap_pvt_thread_pool_init(
	ldap_pvt_thread_pool_t *tpool,
	int max_threads,
	int max_pending )
{
	int rc;
	if( !options_done )
		get_options();
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_init" );
	rc = ldap_int_thread_pool_init( tpool, max_threads, max_pending );
	if( rc ) {
		ERROR( rc, "ldap_pvt_thread_pool_init" );
	} else {
		adjust_count( Idx_tpool, +1 );
	}
	return rc;
}

int
ldap_pvt_thread_pool_submit(
	ldap_pvt_thread_pool_t *tpool,
	ldap_pvt_thread_start_t *start_routine, void *arg )
{
	int rc, has_pool;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_submit" );
	has_pool = (tpool && *tpool);
	rc = ldap_int_thread_pool_submit( tpool, start_routine, arg );
	if( has_pool )
		ERROR_IF( rc, "ldap_pvt_thread_pool_submit" );
	return rc;
}

int
ldap_pvt_thread_pool_maxthreads(
	ldap_pvt_thread_pool_t *tpool,
	int max_threads )
{
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_maxthreads" );
	return ldap_int_thread_pool_maxthreads(	tpool, max_threads );
}

int
ldap_pvt_thread_pool_backload( ldap_pvt_thread_pool_t *tpool )
{
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_backload" );
	return ldap_int_thread_pool_backload( tpool );
}

int
ldap_pvt_thread_pool_destroy( ldap_pvt_thread_pool_t *tpool, int run_pending )
{
	int rc, has_pool;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_destroy" );
	has_pool = (tpool && *tpool);
	rc = ldap_int_thread_pool_destroy( tpool, run_pending );
	if( has_pool ) {
		if( rc ) {
			ERROR( rc, "ldap_pvt_thread_pool_destroy" );
		} else {
			adjust_count( Idx_tpool, -1 );
		}
	}
	return rc;
}

int
ldap_pvt_thread_pool_close( ldap_pvt_thread_pool_t *tpool, int run_pending )
{
	int rc, has_pool;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_close" );
	has_pool = (tpool && *tpool);
	rc = ldap_int_thread_pool_close( tpool, run_pending );
	if( has_pool && rc ) {
		ERROR( rc, "ldap_pvt_thread_pool_close" );
	}
	return rc;
}

int
ldap_pvt_thread_pool_free( ldap_pvt_thread_pool_t *tpool )
{
	int rc, has_pool;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_free" );
	has_pool = (tpool && *tpool);
	rc = ldap_int_thread_pool_free( tpool );
	if( has_pool ) {
		if( rc ) {
			ERROR( rc, "ldap_pvt_thread_pool_free" );
		} else {
			adjust_count( Idx_tpool, -1 );
		}
	}
	return rc;
}

int
ldap_pvt_thread_pool_pause( ldap_pvt_thread_pool_t *tpool )
{
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_pause" );
	return ldap_int_thread_pool_pause( tpool );
}

int
ldap_pvt_thread_pool_resume( ldap_pvt_thread_pool_t *tpool )
{
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_resume" );
	return ldap_int_thread_pool_resume( tpool );
}

int
ldap_pvt_thread_pool_getkey(
	void *xctx,
	void *key,
	void **data,
	ldap_pvt_thread_pool_keyfree_t **kfree )
{
#if 0 /* Function is used by ch_free() via slap_sl_contxt() in slapd */
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_getkey" );
#endif
	return ldap_int_thread_pool_getkey( xctx, key, data, kfree );
}

int
ldap_pvt_thread_pool_setkey(
	void *xctx,
	void *key,
	void *data,
	ldap_pvt_thread_pool_keyfree_t *kfree,
	void **olddatap,
	ldap_pvt_thread_pool_keyfree_t **oldkfreep )
{
	int rc;
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_setkey" );
	rc = ldap_int_thread_pool_setkey(
		xctx, key, data, kfree, olddatap, oldkfreep );
	ERROR_IF( rc, "ldap_pvt_thread_pool_setkey" );
	return rc;
}

void
ldap_pvt_thread_pool_purgekey( void *key )
{
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_purgekey" );
	ldap_int_thread_pool_purgekey( key );
}

void *
ldap_pvt_thread_pool_context( void )
{
#if 0 /* Function is used by ch_free() via slap_sl_contxt() in slapd */
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_context" );
#endif
	return ldap_int_thread_pool_context();
}

void
ldap_pvt_thread_pool_context_reset( void *vctx )
{
	ERROR_IF( !threading_enabled, "ldap_pvt_thread_pool_context_reset" );
	ldap_int_thread_pool_context_reset( vctx );
}

#endif /* LDAP_THREAD_POOL_IMPLEMENTATION */

#endif /* LDAP_THREAD_DEBUG */

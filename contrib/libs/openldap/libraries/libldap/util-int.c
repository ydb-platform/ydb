/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
 * Portions Copyright 1998 A. Hartgers.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* ACKNOWLEDGEMENTS:
 * This work was initially developed by Bart Hartgers for inclusion in
 * OpenLDAP Software.
 */

/*
 * util-int.c	Various functions to replace missing threadsafe ones.
 *				Without the real *_r funcs, things will
 *				work, but might not be threadsafe. 
 */

#include "portable.h"

#include <ac/stdlib.h>

#include <ac/errno.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/unistd.h>

#include "ldap-int.h"

#ifndef h_errno
/* newer systems declare this in <netdb.h> for you, older ones don't.
 * harmless to declare it again (unless defined by a macro).
 */
extern int h_errno;
#endif

#ifdef HAVE_HSTRERROR
# define HSTRERROR(e)	hstrerror(e)
#else
# define HSTRERROR(e)	hp_strerror(e)
#endif

#ifndef LDAP_R_COMPILE
# undef HAVE_REENTRANT_FUNCTIONS
# undef HAVE_CTIME_R
# undef HAVE_GETHOSTBYNAME_R
# undef HAVE_GETHOSTBYADDR_R

#else
# include <ldap_pvt_thread.h>
  ldap_pvt_thread_mutex_t ldap_int_resolv_mutex;
  ldap_pvt_thread_mutex_t ldap_int_hostname_mutex;
  static ldap_pvt_thread_mutex_t ldap_int_gettime_mutex;

# if (defined( HAVE_CTIME_R ) || defined( HAVE_REENTRANT_FUNCTIONS)) \
	 && defined( CTIME_R_NARGS )
#   define USE_CTIME_R
# else
	static ldap_pvt_thread_mutex_t ldap_int_ctime_mutex;
# endif

/* USE_GMTIME_R and USE_LOCALTIME_R defined in ldap_pvt.h */

#if !defined( USE_GMTIME_R ) || !defined( USE_LOCALTIME_R )
	/* we use the same mutex for gmtime(3) and localtime(3)
	 * because implementations may use the same buffer
	 * for both functions */
	static ldap_pvt_thread_mutex_t ldap_int_gmtime_mutex;
#endif

# if defined(HAVE_GETHOSTBYNAME_R) && \
	(GETHOSTBYNAME_R_NARGS < 5) || (6 < GETHOSTBYNAME_R_NARGS)
	/* Don't know how to handle this version, pretend it's not there */
#	undef HAVE_GETHOSTBYNAME_R
# endif
# if defined(HAVE_GETHOSTBYADDR_R) && \
	(GETHOSTBYADDR_R_NARGS < 7) || (8 < GETHOSTBYADDR_R_NARGS)
	/* Don't know how to handle this version, pretend it's not there */
#	undef HAVE_GETHOSTBYADDR_R
# endif
#endif /* LDAP_R_COMPILE */

char *ldap_pvt_ctime( const time_t *tp, char *buf )
{
#ifdef USE_CTIME_R
# if (CTIME_R_NARGS > 3) || (CTIME_R_NARGS < 2)
#	error "CTIME_R_NARGS should be 2 or 3"
# elif CTIME_R_NARGS > 2 && defined(CTIME_R_RETURNS_INT)
	return( ctime_r(tp,buf,26) < 0 ? 0 : buf );
# elif CTIME_R_NARGS > 2
	return ctime_r(tp,buf,26);
# else
	return ctime_r(tp,buf);
# endif	  

#else

	LDAP_MUTEX_LOCK( &ldap_int_ctime_mutex );
	AC_MEMCPY( buf, ctime(tp), 26 );
	LDAP_MUTEX_UNLOCK( &ldap_int_ctime_mutex );

	return buf;
#endif	
}

#if !defined( USE_GMTIME_R ) || !defined( USE_LOCALTIME_R )
int
ldap_pvt_gmtime_lock( void )
{
# ifndef LDAP_R_COMPILE
	return 0;
# else /* LDAP_R_COMPILE */
	return ldap_pvt_thread_mutex_lock( &ldap_int_gmtime_mutex );
# endif /* LDAP_R_COMPILE */
}

int
ldap_pvt_gmtime_unlock( void )
{
# ifndef LDAP_R_COMPILE
	return 0;
# else /* LDAP_R_COMPILE */
	return ldap_pvt_thread_mutex_unlock( &ldap_int_gmtime_mutex );
# endif /* LDAP_R_COMPILE */
}
#endif /* !USE_GMTIME_R || !USE_LOCALTIME_R */

#ifndef USE_GMTIME_R
struct tm *
ldap_pvt_gmtime( const time_t *timep, struct tm *result )
{
	struct tm *tm_ptr;

	LDAP_MUTEX_LOCK( &ldap_int_gmtime_mutex );
	tm_ptr = gmtime( timep );
	if ( tm_ptr == NULL ) {
		result = NULL;

	} else {
		*result = *tm_ptr;
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_gmtime_mutex );

	return result;
}
#endif /* !USE_GMTIME_R */

#ifndef USE_LOCALTIME_R
struct tm *
ldap_pvt_localtime( const time_t *timep, struct tm *result )
{
	struct tm *tm_ptr;

	LDAP_MUTEX_LOCK( &ldap_int_gmtime_mutex );
	tm_ptr = localtime( timep );
	if ( tm_ptr == NULL ) {
		result = NULL;

	} else {
		*result = *tm_ptr;
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_gmtime_mutex );

	return result;
}
#endif /* !USE_LOCALTIME_R */

static int _ldap_pvt_gt_subs;

#ifdef _WIN32
/* Windows SYSTEMTIME only has 10 millisecond resolution, so we
 * also need to use a high resolution timer to get nanoseconds.
 * This is pretty clunky.
 */
static LARGE_INTEGER _ldap_pvt_gt_freq;
static LARGE_INTEGER _ldap_pvt_gt_prev;
static int _ldap_pvt_gt_offset;

#define SEC_TO_UNIX_EPOCH 11644473600LL
#define TICKS_PER_SECOND 10000000
#define BILLION	1000000000L

static int
ldap_pvt_gettimensec(int *sec)
{
	LARGE_INTEGER count;

	QueryPerformanceCounter( &count );

	/* It shouldn't ever go backwards, but multiple CPUs might
	 * be able to hit in the same tick.
	 */
	LDAP_MUTEX_LOCK( &ldap_int_gettime_mutex );
	/* We assume Windows has at least a vague idea of
	 * when a second begins. So we align our nanosecond count
	 * with the Windows millisecond count using this offset.
	 * We retain the submillisecond portion of our own count.
	 *
	 * Note - this also assumes that the relationship between
	 * the PerformanceCounter and SystemTime stays constant;
	 * that assumption breaks if the SystemTime is adjusted by
	 * an external action.
	 */
	if ( !_ldap_pvt_gt_freq.QuadPart ) {
		LARGE_INTEGER c2;
		ULARGE_INTEGER ut;
		FILETIME ft0, ft1;
		long long t;
		int nsec;

		/* Initialize our offset */
		QueryPerformanceFrequency( &_ldap_pvt_gt_freq );

		/* Wait for a tick of the system time: 10-15ms */
		GetSystemTimeAsFileTime( &ft0 );
		do {
			GetSystemTimeAsFileTime( &ft1 );
		} while ( ft1.dwLowDateTime == ft0.dwLowDateTime );

		ut.LowPart = ft1.dwLowDateTime;
		ut.HighPart = ft1.dwHighDateTime;
		QueryPerformanceCounter( &c2 );

		/* get second and fraction portion of counter */
		t = c2.QuadPart % (_ldap_pvt_gt_freq.QuadPart*10);

		/* convert to nanoseconds */
		t *= BILLION;
		nsec = t / _ldap_pvt_gt_freq.QuadPart;

		ut.QuadPart /= 10;
		ut.QuadPart %= (10 * BILLION);
		_ldap_pvt_gt_offset = nsec - ut.QuadPart;
		count = c2;
	}
	if ( count.QuadPart <= _ldap_pvt_gt_prev.QuadPart ) {
		_ldap_pvt_gt_subs++;
	} else {
		_ldap_pvt_gt_subs = 0;
		_ldap_pvt_gt_prev = count;
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_gettime_mutex );

	/* convert to nanoseconds */
	count.QuadPart %= _ldap_pvt_gt_freq.QuadPart*10;
	count.QuadPart *= BILLION;
	count.QuadPart /= _ldap_pvt_gt_freq.QuadPart;
	count.QuadPart -= _ldap_pvt_gt_offset;

	/* We've extracted the 1s and nanoseconds.
	 * The 1sec digit is used to detect wraparound in nanosecnds.
	 */
	if (count.QuadPart < 0)
		count.QuadPart += (10 * BILLION);
	else if (count.QuadPart >= (10 * BILLION))
		count.QuadPart -= (10 * BILLION);

	*sec = count.QuadPart / BILLION;
	return count.QuadPart % BILLION;
}


/* emulate POSIX clock_gettime */
int
ldap_pvt_clock_gettime( int clk_id, struct timespec *tv )
{
	FILETIME ft;
	ULARGE_INTEGER ut;
	int sec, sec0;

	GetSystemTimeAsFileTime( &ft );
	ut.LowPart = ft.dwLowDateTime;
	ut.HighPart = ft.dwHighDateTime;

	/* convert to sec */
	ut.QuadPart /= TICKS_PER_SECOND;

	tv->tv_nsec = ldap_pvt_gettimensec(&sec);
	tv->tv_sec = ut.QuadPart - SEC_TO_UNIX_EPOCH;

	/* check for carry from microseconds */
	sec0 = tv->tv_sec % 10;
	if (sec0 < sec || (sec0 == 9 && !sec))
		tv->tv_sec++;

	return 0;
}

/* emulate POSIX gettimeofday */
int
ldap_pvt_gettimeofday( struct timeval *tv, void *unused )
{
	struct timespec ts;
	ldap_pvt_clock_gettime( 0, &ts );
	tv->tv_sec = ts.tv_sec;
	tv->tv_usec = ts.tv_nsec / 1000;
	return 0;
}


/* return a broken out time, with nanoseconds
 */
void
ldap_pvt_gettime( struct lutil_tm *tm )
{
	SYSTEMTIME st;
	int sec, sec0;
	static const char daysPerMonth[] = {
	31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	GetSystemTime( &st );
	tm->tm_nsec = ldap_pvt_gettimensec(&sec);
	tm->tm_usub = _ldap_pvt_gt_subs;

	/* any difference larger than nanoseconds is
	 * already reflected in st
	 */
	tm->tm_sec = st.wSecond;
	tm->tm_min = st.wMinute;
	tm->tm_hour = st.wHour;
	tm->tm_mday = st.wDay;
	tm->tm_mon = st.wMonth - 1;
	tm->tm_year = st.wYear - 1900;

	/* check for carry from nanoseconds */
	sec0 = tm->tm_sec % 10;
	if (sec0 < sec || (sec0 == 9 && !sec)) {
		tm->tm_sec++;
		/* FIXME: we don't handle leap seconds */
		if (tm->tm_sec > 59) {
			tm->tm_sec = 0;
			tm->tm_min++;
			if (tm->tm_min > 59) {
				tm->tm_min = 0;
				tm->tm_hour++;
				if (tm->tm_hour > 23) {
					int days = daysPerMonth[tm->tm_mon];
					tm->tm_hour = 0;
					tm->tm_mday++;

					/* if it's February of a leap year,
					 * add 1 day to this month
					 */
					if (tm->tm_mon == 1 &&
						((!(st.wYear % 4) && (st.wYear % 100)) ||
						!(st.wYear % 400)))
						days++;

					if (tm->tm_mday > days) {
						tm->tm_mday = 1;
						tm->tm_mon++;
						if (tm->tm_mon > 11) {
							tm->tm_mon = 0;
							tm->tm_year++;
						}
					}
				}
			}
		}
	}
}
#else

#ifdef HAVE_CLOCK_GETTIME
static struct timespec _ldap_pvt_gt_prevTv;
#else
static struct timeval _ldap_pvt_gt_prevTv;
#endif

void
ldap_pvt_gettime( struct lutil_tm *ltm )
{
	struct tm tm;
	time_t t;
#ifdef HAVE_CLOCK_GETTIME
#define	FRAC	tv_nsec
#define	NSECS(x)	x
	struct timespec tv;

	clock_gettime( CLOCK_REALTIME, &tv );
#else
#define	FRAC	tv_usec
#define	NSECS(x)	x * 1000
	struct timeval tv;

	gettimeofday( &tv, NULL );
#endif
	t = tv.tv_sec;

	LDAP_MUTEX_LOCK( &ldap_int_gettime_mutex );
	if ( tv.tv_sec < _ldap_pvt_gt_prevTv.tv_sec
		|| ( tv.tv_sec == _ldap_pvt_gt_prevTv.tv_sec
		&& tv.FRAC <= _ldap_pvt_gt_prevTv.FRAC )) {
		_ldap_pvt_gt_subs++;
	} else {
		_ldap_pvt_gt_subs = 0;
		_ldap_pvt_gt_prevTv = tv;
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_gettime_mutex );

	ltm->tm_usub = _ldap_pvt_gt_subs;

	ldap_pvt_gmtime( &t, &tm );

	ltm->tm_sec = tm.tm_sec;
	ltm->tm_min = tm.tm_min;
	ltm->tm_hour = tm.tm_hour;
	ltm->tm_mday = tm.tm_mday;
	ltm->tm_mon = tm.tm_mon;
	ltm->tm_year = tm.tm_year;
	ltm->tm_nsec = NSECS(tv.FRAC);
}
#endif

size_t
ldap_pvt_csnstr(char *buf, size_t len, unsigned int replica, unsigned int mod)
{
	struct lutil_tm tm;
	int n;

	ldap_pvt_gettime( &tm );

	n = snprintf( buf, len,
		"%4d%02d%02d%02d%02d%02d.%06dZ#%06x#%03x#%06x",
		tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
		tm.tm_min, tm.tm_sec, tm.tm_nsec / 1000, tm.tm_usub, replica, mod );

	if( n < 0 ) return 0;
	return ( (size_t) n < len ) ? n : 0;
}

#define BUFSTART (1024-32)
#define BUFMAX (32*1024-32)

#if defined(LDAP_R_COMPILE)
static char *safe_realloc( char **buf, int len );

#if !(defined(HAVE_GETHOSTBYNAME_R) && defined(HAVE_GETHOSTBYADDR_R))
static int copy_hostent( struct hostent *res,
	char **buf, struct hostent * src );
#endif
#endif

int ldap_pvt_gethostbyname_a(
	const char *name, 
	struct hostent *resbuf,
	char **buf,
	struct hostent **result,
	int *herrno_ptr )
{
#if defined( HAVE_GETHOSTBYNAME_R )

# define NEED_SAFE_REALLOC 1   
	int r=-1;
	int buflen=BUFSTART;
	*buf = NULL;
	for(;buflen<BUFMAX;) {
		if (safe_realloc( buf, buflen )==NULL)
			return r;

#if (GETHOSTBYNAME_R_NARGS < 6)
		*result=gethostbyname_r( name, resbuf, *buf, buflen, herrno_ptr );
		r = (*result == NULL) ?  -1 : 0;
#else
		while((r = gethostbyname_r( name, resbuf, *buf, buflen, result, herrno_ptr )) == ERANGE) {
			/* Increase the buffer */
			buflen*=2;
			if (safe_realloc(buf, buflen) == NULL)
				return -1;
		}
#endif

		Debug2( LDAP_DEBUG_TRACE, "ldap_pvt_gethostbyname_a: host=%s, r=%d\n",
		       name, r );

#ifdef NETDB_INTERNAL
		if ((r<0) &&
			(*herrno_ptr==NETDB_INTERNAL) &&
			(errno==ERANGE))
		{
			buflen*=2;
			continue;
	 	}
#endif
		return r;
	}
	return -1;
#elif defined( LDAP_R_COMPILE )
# define NEED_COPY_HOSTENT   
	struct hostent *he;
	int	retval;
	*buf = NULL;
	
	LDAP_MUTEX_LOCK( &ldap_int_resolv_mutex );
	
	he = gethostbyname( name );
	
	if (he==NULL) {
		*herrno_ptr = h_errno;
		retval = -1;
	} else if (copy_hostent( resbuf, buf, he )<0) {
		*herrno_ptr = -1;
		retval = -1;
	} else {
		*result = resbuf;
		retval = 0;
	}
	
	LDAP_MUTEX_UNLOCK( &ldap_int_resolv_mutex );
	
	return retval;
#else	
	*buf = NULL;
	*result = gethostbyname( name );

	if (*result!=NULL) {
		return 0;
	}

	*herrno_ptr = h_errno;
	
	return -1;
#endif	
}

#if !defined( HAVE_GETNAMEINFO ) && !defined( HAVE_HSTRERROR )
static const char *
hp_strerror( int err )
{
	switch (err) {
	case HOST_NOT_FOUND:	return _("Host not found (authoritative)");
	case TRY_AGAIN:			return _("Host not found (server fail?)");
	case NO_RECOVERY:		return _("Non-recoverable failure");
	case NO_DATA:			return _("No data of requested type");
#ifdef NETDB_INTERNAL
	case NETDB_INTERNAL:	return STRERROR( errno );
#endif
	}
	return _("Unknown resolver error");
}
#endif

int ldap_pvt_get_hname(
	const struct sockaddr *sa,
	int len,
	char *name,
	int namelen,
	char **err )
{
	int rc;
#if defined( HAVE_GETNAMEINFO )

	LDAP_MUTEX_LOCK( &ldap_int_resolv_mutex );
	rc = getnameinfo( sa, len, name, namelen, NULL, 0, 0 );
	LDAP_MUTEX_UNLOCK( &ldap_int_resolv_mutex );
	if ( rc ) *err = (char *)AC_GAI_STRERROR( rc );
	return rc;

#else /* !HAVE_GETNAMEINFO */
	char *addr;
	int alen;
	struct hostent *hp = NULL;
#ifdef HAVE_GETHOSTBYADDR_R
	struct hostent hb;
	int buflen=BUFSTART, h_errno;
	char *buf=NULL;
#endif

#ifdef LDAP_PF_INET6
	if (sa->sa_family == AF_INET6) {
		struct sockaddr_in6 *sin = (struct sockaddr_in6 *)sa;
		addr = (char *)&sin->sin6_addr;
		alen = sizeof(sin->sin6_addr);
	} else
#endif
	if (sa->sa_family == AF_INET) {
		struct sockaddr_in *sin = (struct sockaddr_in *)sa;
		addr = (char *)&sin->sin_addr;
		alen = sizeof(sin->sin_addr);
	} else {
		rc = NO_RECOVERY;
		*err = (char *)HSTRERROR( rc );
		return rc;
	}
#if defined( HAVE_GETHOSTBYADDR_R )
	for(;buflen<BUFMAX;) {
		if (safe_realloc( &buf, buflen )==NULL) {
			*err = (char *)STRERROR( ENOMEM );
			return ENOMEM;
		}
#if (GETHOSTBYADDR_R_NARGS < 8)
		hp=gethostbyaddr_r( addr, alen, sa->sa_family,
			&hb, buf, buflen, &h_errno );
		rc = (hp == NULL) ? -1 : 0;
#else
		rc = gethostbyaddr_r( addr, alen, sa->sa_family,
			&hb, buf, buflen, 
			&hp, &h_errno );
#endif
#ifdef NETDB_INTERNAL
		if ((rc<0) &&
			(h_errno==NETDB_INTERNAL) &&
			(errno==ERANGE))
		{
			buflen*=2;
			continue;
		}
#endif
		break;
	}
	if (hp) {
		strncpy( name, hp->h_name, namelen );
	} else {
		*err = (char *)HSTRERROR( h_errno );
	}
	LDAP_FREE(buf);
#else /* HAVE_GETHOSTBYADDR_R */

	LDAP_MUTEX_LOCK( &ldap_int_resolv_mutex );
	hp = gethostbyaddr( addr, alen, sa->sa_family );
	if (hp) {
		strncpy( name, hp->h_name, namelen );
		rc = 0;
	} else {
		rc = h_errno;
		*err = (char *)HSTRERROR( h_errno );
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_resolv_mutex );

#endif	/* !HAVE_GETHOSTBYADDR_R */
	return rc;
#endif	/* !HAVE_GETNAMEINFO */
}

int ldap_pvt_gethostbyaddr_a(
	const char *addr,
	int len,
	int type,
	struct hostent *resbuf,
	char **buf,
	struct hostent **result,
	int *herrno_ptr )
{
#if defined( HAVE_GETHOSTBYADDR_R )

# undef NEED_SAFE_REALLOC
# define NEED_SAFE_REALLOC   
	int r=-1;
	int buflen=BUFSTART;
	*buf = NULL;   
	for(;buflen<BUFMAX;) {
		if (safe_realloc( buf, buflen )==NULL)
			return r;
#if (GETHOSTBYADDR_R_NARGS < 8)
		*result=gethostbyaddr_r( addr, len, type,
			resbuf, *buf, buflen, herrno_ptr );
		r = (*result == NULL) ? -1 : 0;
#else
		r = gethostbyaddr_r( addr, len, type,
			resbuf, *buf, buflen, 
			result, herrno_ptr );
#endif

#ifdef NETDB_INTERNAL
		if ((r<0) &&
			(*herrno_ptr==NETDB_INTERNAL) &&
			(errno==ERANGE))
		{
			buflen*=2;
			continue;
		}
#endif
		return r;
	}
	return -1;
#elif defined( LDAP_R_COMPILE )
# undef NEED_COPY_HOSTENT
# define NEED_COPY_HOSTENT   
	struct hostent *he;
	int	retval;
	*buf = NULL;   
	
	LDAP_MUTEX_LOCK( &ldap_int_resolv_mutex );
	he = gethostbyaddr( addr, len, type );
	
	if (he==NULL) {
		*herrno_ptr = h_errno;
		retval = -1;
	} else if (copy_hostent( resbuf, buf, he )<0) {
		*herrno_ptr = -1;
		retval = -1;
	} else {
		*result = resbuf;
		retval = 0;
	}
	LDAP_MUTEX_UNLOCK( &ldap_int_resolv_mutex );
	
	return retval;

#else /* gethostbyaddr() */
	*buf = NULL;   
	*result = gethostbyaddr( addr, len, type );

	if (*result!=NULL) {
		return 0;
	}
	return -1;
#endif	
}
/* 
 * ldap_int_utils_init() should be called before any other function.
 */

void ldap_int_utils_init( void )
{
	static int done=0;
	if (done)
	  return;
	done=1;

#ifdef LDAP_R_COMPILE
#if !defined( USE_CTIME_R ) && !defined( HAVE_REENTRANT_FUNCTIONS )
	ldap_pvt_thread_mutex_init( &ldap_int_ctime_mutex );
#endif
#if !defined( USE_GMTIME_R ) && !defined( USE_LOCALTIME_R )
	ldap_pvt_thread_mutex_init( &ldap_int_gmtime_mutex );
#endif
	ldap_pvt_thread_mutex_init( &ldap_int_resolv_mutex );

	ldap_pvt_thread_mutex_init( &ldap_int_hostname_mutex );

	ldap_pvt_thread_mutex_init( &ldap_int_gettime_mutex );

#endif

	/* call other module init functions here... */
}

#if defined( NEED_COPY_HOSTENT )
# undef NEED_SAFE_REALLOC
#define NEED_SAFE_REALLOC

static char *cpy_aliases(
	char ***tgtio,
	char *buf,
	char **src )
{
	int len;
	char **tgt=*tgtio;
	for( ; (*src) ; src++ ) {
		len = strlen( *src ) + 1;
		AC_MEMCPY( buf, *src, len );
		*tgt++=buf;
		buf+=len;
	}
	*tgtio=tgt;   
	return buf;
}

static char *cpy_addresses(
	char ***tgtio,
	char *buf,
	char **src,
	int len )
{
   	char **tgt=*tgtio;
	for( ; (*src) ; src++ ) {
		AC_MEMCPY( buf, *src, len );
		*tgt++=buf;
		buf+=len;
	}
	*tgtio=tgt;      
	return buf;
}

static int copy_hostent(
	struct hostent *res,
	char **buf,
	struct hostent * src )
{
	char	**p;
	char	**tp;
	char	*tbuf;
	int	name_len;
	int	n_alias=0;
	int	total_alias_len=0;
	int	n_addr=0;
	int	total_addr_len=0;
	int	total_len;
	  
	/* calculate the size needed for the buffer */
	name_len = strlen( src->h_name ) + 1;
	
	if( src->h_aliases != NULL ) {
		for( p = src->h_aliases; (*p) != NULL; p++ ) {
			total_alias_len += strlen( *p ) + 1;
			n_alias++; 
		}
	}

	if( src->h_addr_list != NULL ) {
		for( p = src->h_addr_list; (*p) != NULL; p++ ) {
			n_addr++;
		}
		total_addr_len = n_addr * src->h_length;
	}
	
	total_len = (n_alias + n_addr + 2) * sizeof( char * ) +
		total_addr_len + total_alias_len + name_len;
	
	if (safe_realloc( buf, total_len )) {			 
		tp = (char **) *buf;
		tbuf = *buf + (n_alias + n_addr + 2) * sizeof( char * );
		AC_MEMCPY( res, src, sizeof( struct hostent ) );
		/* first the name... */
		AC_MEMCPY( tbuf, src->h_name, name_len );
		res->h_name = tbuf; tbuf+=name_len;
		/* now the aliases */
		res->h_aliases = tp;
		if ( src->h_aliases != NULL ) {
			tbuf = cpy_aliases( &tp, tbuf, src->h_aliases );
		}
		*tp++=NULL;
		/* finally the addresses */
		res->h_addr_list = tp;
		if ( src->h_addr_list != NULL ) {
			tbuf = cpy_addresses( &tp, tbuf, src->h_addr_list, src->h_length );
		}
		*tp++=NULL;
		return 0;
	}
	return -1;
}
#endif

#if defined( NEED_SAFE_REALLOC )
static char *safe_realloc( char **buf, int len )
{
	char *tmpbuf;
	tmpbuf = LDAP_REALLOC( *buf, len );
	if (tmpbuf) {
		*buf=tmpbuf;
	} 
	return tmpbuf;
}
#endif

char * ldap_pvt_get_fqdn( char *name )
{
#ifdef HAVE_GETADDRINFO
	struct addrinfo hints, *res;
#else
	char *ha_buf;
	struct hostent *hp, he_buf;
	int local_h_errno;
#endif
	int rc;
	char *fqdn, hostbuf[MAXHOSTNAMELEN+1];

	if( name == NULL ) {
		if( gethostname( hostbuf, MAXHOSTNAMELEN ) == 0 ) {
			hostbuf[MAXHOSTNAMELEN] = '\0';
			name = hostbuf;
		} else {
			name = "localhost";
		}
	}

#ifdef HAVE_GETADDRINFO
	memset( &hints, 0, sizeof( hints ));
	hints.ai_family = AF_UNSPEC;
	hints.ai_flags = AI_CANONNAME;

	LDAP_MUTEX_LOCK( &ldap_int_resolv_mutex );
	rc = getaddrinfo( name, NULL, &hints, &res );
	LDAP_MUTEX_UNLOCK( &ldap_int_resolv_mutex );
	if ( rc == 0 && res->ai_canonname ) {
		fqdn = LDAP_STRDUP( res->ai_canonname );
	} else {
		fqdn = LDAP_STRDUP( name );
	}
	if ( rc == 0 )
		freeaddrinfo( res );
#else
	rc = ldap_pvt_gethostbyname_a( name,
		&he_buf, &ha_buf, &hp, &local_h_errno );

	if( rc < 0 || hp == NULL || hp->h_name == NULL ) {
		fqdn = LDAP_STRDUP( name );
	} else {
		fqdn = LDAP_STRDUP( hp->h_name );
	}

	LDAP_FREE( ha_buf );
#endif
	return fqdn;
}

#if ( defined( HAVE_GETADDRINFO ) || defined( HAVE_GETNAMEINFO ) ) \
	&& !defined( HAVE_GAI_STRERROR )
char *ldap_pvt_gai_strerror (int code) {
	static struct {
		int code;
		const char *msg;
	} values[] = {
#ifdef EAI_ADDRFAMILY
		{ EAI_ADDRFAMILY, N_("Address family for hostname not supported") },
#endif
		{ EAI_AGAIN, N_("Temporary failure in name resolution") },
		{ EAI_BADFLAGS, N_("Bad value for ai_flags") },
		{ EAI_FAIL, N_("Non-recoverable failure in name resolution") },
		{ EAI_FAMILY, N_("ai_family not supported") },
		{ EAI_MEMORY, N_("Memory allocation failure") },
#ifdef EAI_NODATA
		{ EAI_NODATA, N_("No address associated with hostname") },
#endif    
		{ EAI_NONAME, N_("Name or service not known") },
		{ EAI_SERVICE, N_("Servname not supported for ai_socktype") },
		{ EAI_SOCKTYPE, N_("ai_socktype not supported") },
#ifdef EAI_SYSTEM
		{ EAI_SYSTEM, N_("System error") },
#endif
		{ 0, NULL }
	};

	int i;

	for ( i = 0; values[i].msg != NULL; i++ ) {
		if ( values[i].code == code ) {
			return (char *) _(values[i].msg);
		}
	}
	
	return _("Unknown error");
}
#endif

/* format a socket address as a string */

#ifdef HAVE_TCPD
# include <tcpd.h>
# define SOCKADDR_STRING_UNKNOWN	STRING_UNKNOWN
#else /* ! TCP Wrappers */
# define SOCKADDR_STRING_UNKNOWN	"unknown"
#endif /* ! TCP Wrappers */

void
ldap_pvt_sockaddrstr( Sockaddr *sa, struct berval *addrbuf )
{
	char *addr;
	switch( sa->sa_addr.sa_family ) {
#ifdef LDAP_PF_LOCAL
	case AF_LOCAL:
		addrbuf->bv_len = snprintf( addrbuf->bv_val, addrbuf->bv_len,
			"PATH=%s", sa->sa_un_addr.sun_path );
		break;
#endif
#ifdef LDAP_PF_INET6
	case AF_INET6:
		strcpy(addrbuf->bv_val, "IP=");
		if ( IN6_IS_ADDR_V4MAPPED(&sa->sa_in6_addr.sin6_addr) ) {
#if defined( HAVE_GETADDRINFO ) && defined( HAVE_INET_NTOP )
			addr = (char *)inet_ntop( AF_INET,
			   ((struct in_addr *)&sa->sa_in6_addr.sin6_addr.s6_addr[12]),
			   addrbuf->bv_val+3, addrbuf->bv_len-3 );
#else /* ! HAVE_GETADDRINFO || ! HAVE_INET_NTOP */
			addr = inet_ntoa( *((struct in_addr *)
					&sa->sa_in6_addr.sin6_addr.s6_addr[12]) );
#endif /* ! HAVE_GETADDRINFO || ! HAVE_INET_NTOP */
			if ( !addr ) addr = SOCKADDR_STRING_UNKNOWN;
			if ( addr != addrbuf->bv_val+3 ) {
				addrbuf->bv_len = sprintf( addrbuf->bv_val+3, "%s:%d", addr,
				 (unsigned) ntohs( sa->sa_in6_addr.sin6_port ) ) + 3;
			} else {
				int len = strlen( addr );
				addrbuf->bv_len = sprintf( addr+len, ":%d",
				 (unsigned) ntohs( sa->sa_in6_addr.sin6_port ) ) + len + 3;
			}
		} else {
			addr = (char *)inet_ntop( AF_INET6,
				      &sa->sa_in6_addr.sin6_addr,
				      addrbuf->bv_val+4, addrbuf->bv_len-4 );
			if ( !addr ) addr = SOCKADDR_STRING_UNKNOWN;
			if ( addr != addrbuf->bv_val+4 ) {
				addrbuf->bv_len = sprintf( addrbuf->bv_val+3, "[%s]:%d", addr,
				 (unsigned) ntohs( sa->sa_in6_addr.sin6_port ) ) + 3;
			} else {
				int len = strlen( addr );
				addrbuf->bv_val[3] = '[';
				addrbuf->bv_len = sprintf( addr+len, "]:%d",
				 (unsigned) ntohs( sa->sa_in6_addr.sin6_port ) ) + len + 4;
			}
		}
		break;
#endif /* LDAP_PF_INET6 */
	case AF_INET:
		strcpy(addrbuf->bv_val, "IP=");
#if defined( HAVE_GETADDRINFO ) && defined( HAVE_INET_NTOP )
		addr = (char *)inet_ntop( AF_INET, &sa->sa_in_addr.sin_addr,
			   addrbuf->bv_val+3, addrbuf->bv_len-3 );
#else /* ! HAVE_GETADDRINFO || ! HAVE_INET_NTOP */
		addr = inet_ntoa( sa->sa_in_addr.sin_addr );
#endif /* ! HAVE_GETADDRINFO || ! HAVE_INET_NTOP */
		if ( !addr ) addr = SOCKADDR_STRING_UNKNOWN;
		if ( addr != addrbuf->bv_val+3 ) {
			addrbuf->bv_len = sprintf( addrbuf->bv_val+3, "%s:%d", addr,
			 (unsigned) ntohs( sa->sa_in_addr.sin_port ) ) + 3;
		} else {
			int len = strlen( addr );
			addrbuf->bv_len = sprintf( addr+len, ":%d",
			 (unsigned) ntohs( sa->sa_in_addr.sin_port ) ) + len + 3;
		}
		break;
	default:
		addrbuf->bv_val[0] = '\0';
	}
}

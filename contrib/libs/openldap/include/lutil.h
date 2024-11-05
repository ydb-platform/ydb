/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
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

#ifndef _LUTIL_H
#define _LUTIL_H 1

#include <ldap_cdefs.h>
#include <lber_types.h>
#include <ac/socket.h>

#ifdef HAVE_TCPD
# include <tcpd.h>
# define LUTIL_STRING_UNKNOWN	STRING_UNKNOWN
#else /* ! TCP Wrappers */
# define LUTIL_STRING_UNKNOWN	"unknown"
#endif /* ! TCP Wrappers */

/*
 * Include file for LDAP utility routine
 */

LDAP_BEGIN_DECL

/* n octets encode into ceiling(n/3) * 4 bytes */
/* Avoid floating point math through extra padding */

#define LUTIL_BASE64_ENCODE_LEN(n)	(((n)+2)/3 * 4)
#define LUTIL_BASE64_DECODE_LEN(n)	((n)/4*3)

/* ISC Base64 Routines */
/* base64.c */

LDAP_LUTIL_F( int )
lutil_b64_ntop LDAP_P((
	unsigned char const *,
	size_t,
	char *,
	size_t));

LDAP_LUTIL_F( int )
lutil_b64_pton LDAP_P((
	char const *,
	unsigned char *,
	size_t));

/* detach.c */
LDAP_LUTIL_F( int )
lutil_detach LDAP_P((
	int debug,
	int do_close));

/* entropy.c */
LDAP_LUTIL_F( int )
lutil_entropy LDAP_P((
	unsigned char *buf,
	ber_len_t nbytes ));

/* passfile.c */
struct berval;	/* avoid pulling in lber.h */

LDAP_LUTIL_F( int )
lutil_get_filed_password LDAP_P((
	const char *filename,
	struct berval * ));

/* passwd.c */
struct lutil_pw_scheme;

#define LUTIL_PASSWD_OK		(0)
#define LUTIL_PASSWD_ERR	(-1)

typedef int (LUTIL_PASSWD_CHK_FUNC)(
	const struct berval *scheme,
	const struct berval *passwd,
	const struct berval *cred,
	const char **text );

typedef int (LUTIL_PASSWD_HASH_FUNC) (
	const struct berval *scheme,
	const struct berval *passwd,
	struct berval *hash, 
	const char **text );

LDAP_LUTIL_F( int )
lutil_passwd_add LDAP_P((
	struct berval *scheme,
	LUTIL_PASSWD_CHK_FUNC *chk_fn,
	LUTIL_PASSWD_HASH_FUNC *hash_fn ));

LDAP_LUTIL_F( void )
lutil_passwd_init LDAP_P(( void ));

LDAP_LUTIL_F( void )
lutil_passwd_destroy LDAP_P(( void ));

LDAP_LUTIL_F( int )
lutil_authpasswd LDAP_P((
	const struct berval *passwd,	/* stored password */
	const struct berval *cred,	/* user supplied value */
	const char **methods ));

LDAP_LUTIL_F( int )
lutil_authpasswd_hash LDAP_P((
	const struct berval *cred,
	struct berval **passwd,	/* password to store */
	struct berval **salt,	/* salt to store */
	const char *method ));

#ifdef SLAPD_CRYPT
typedef int (lutil_cryptfunc) LDAP_P((
	const char *key,
	const char *salt,
	char **hash ));
LDAP_LUTIL_V (lutil_cryptfunc *) lutil_cryptptr;
#endif

LDAP_LUTIL_F( int )
lutil_passwd LDAP_P((
	const struct berval *passwd,	/* stored password */
	const struct berval *cred,	/* user supplied value */
	const char **methods,
	const char **text ));			/* error message */

LDAP_LUTIL_F( int )
lutil_passwd_generate LDAP_P(( struct berval *pw, ber_len_t ));

LDAP_LUTIL_F( int )
lutil_passwd_hash LDAP_P((
	const struct berval *passwd,
	const char *method,
	struct berval *hash,
	const char **text ));

LDAP_LUTIL_F( int )
lutil_passwd_scheme LDAP_P((
	const char *scheme ));

LDAP_LUTIL_F( int )
lutil_salt_format LDAP_P((
	const char *format ));

LDAP_LUTIL_F( int )
lutil_passwd_string64 LDAP_P((
	const struct berval *sc,
	const struct berval *hash,
	struct berval *b64,
	const struct berval *salt ));

/* utils.c */
LDAP_LUTIL_F( char* )
lutil_progname LDAP_P((
	const char* name,
	int argc,
	char *argv[] ));

typedef struct lutil_tm {
	int tm_sec;	/* seconds 0-60 (1 leap second) */
	int tm_min;	/* minutes 0-59 */
	int tm_hour;	/* hours 0-23 */
	int tm_mday;	/* day 1-31 */
	int tm_mon;	/* month 0-11 */
	int tm_year;	/* year - 1900 */
	int tm_nsec;	/* nanoseconds */
	int tm_usub;	/* submicro */
} lutil_tm;

typedef struct lutil_timet {
	unsigned int tt_sec;	/* seconds since epoch, 0000 or 1970 */
	int tt_gsec;		/* seconds since epoch, high 7 bits, maybe sign-flipped */
						/* sign flipped to sort properly as unsigned ints */
	unsigned int tt_nsec;	/* nanoseconds */
} lutil_timet;

/* Parse a timestamp string into a structure */
LDAP_LUTIL_F( int )
lutil_parsetime LDAP_P((
	char *atm, struct lutil_tm * ));

/* Convert structured time to time in seconds since 1970 (Unix epoch) */
LDAP_LUTIL_F( int )
lutil_tm2time LDAP_P((
	struct lutil_tm *, struct lutil_timet * ));

/* Convert structured time to time in seconds since 0000 (Proleptic Gregorian) */
LDAP_LUTIL_F( int )
lutil_tm2gtime LDAP_P((
	struct lutil_tm *, struct lutil_timet * ));

#ifdef _WIN32
LDAP_LUTIL_F( void )
lutil_slashpath LDAP_P(( char* path ));
#define	LUTIL_SLASHPATH(p)	lutil_slashpath(p)
#else
#define	LUTIL_SLASHPATH(p)
#endif

LDAP_LUTIL_F( char* )
lutil_strcopy LDAP_P(( char *dst, const char *src ));

LDAP_LUTIL_F( char* )
lutil_strncopy LDAP_P(( char *dst, const char *src, size_t n ));

LDAP_LUTIL_F( char* )
lutil_memcopy LDAP_P(( char *dst, const char *src, size_t n ));

#define lutil_strbvcopy(a, bv) lutil_memcopy((a),(bv)->bv_val,(bv)->bv_len)

struct tm;

/* use this macro to statically allocate buffer for lutil_gentime */
#define LDAP_LUTIL_GENTIME_BUFSIZE	22
#define lutil_gentime(s,m,t)	lutil_localtime((s),(m),(t),0)
LDAP_LUTIL_F( size_t )
lutil_localtime LDAP_P(( char *s, size_t smax, const struct tm *tm,
			long delta ));

#ifndef HAVE_MKSTEMP
LDAP_LUTIL_F( int )
mkstemp LDAP_P (( char * template ));
#endif

/* sockpair.c */
LDAP_LUTIL_F( int )
lutil_pair( ber_socket_t sd[2] );

/* uuid.c */
/* use this macro to allocate buffer for lutil_uuidstr */
#define LDAP_LUTIL_UUIDSTR_BUFSIZE	40
LDAP_LUTIL_F( size_t )
lutil_uuidstr( char *buf, size_t len );

LDAP_LUTIL_F( int )
lutil_uuidstr_from_normalized(
	char		*uuid,
	size_t		uuidlen,
	char		*buf,
	size_t		buflen );

/*
 * Sometimes not all declarations in a header file are needed.
 * An indicator to this is whether or not the symbol's type has
 * been defined. Thus, we don't need to include a symbol if
 * its type has not been defined through another header file.
 */

#ifdef HAVE_NT_SERVICE_MANAGER
LDAP_LUTIL_V (int) is_NT_Service;

#ifdef _LDAP_PVT_THREAD_H
LDAP_LUTIL_V (ldap_pvt_thread_cond_t) started_event;
#endif /* _LDAP_PVT_THREAD_H */

/* macros are different between Windows and Mingw */
#if defined(_WINSVC_H) || defined(_WINSVC_)
LDAP_LUTIL_V (SERVICE_STATUS) lutil_ServiceStatus;
LDAP_LUTIL_V (SERVICE_STATUS_HANDLE) hlutil_ServiceStatus;
#endif /* _WINSVC_H */

LDAP_LUTIL_F (void)
lutil_CommenceStartupProcessing( char *serverName, void (*stopper)(int)) ;

LDAP_LUTIL_F (void)
lutil_ReportShutdownComplete( void );

LDAP_LUTIL_F (void *)
lutil_getRegParam( char *svc, char *value );

LDAP_LUTIL_F (int)
lutil_srv_install( char* service, char * displayName, char* filename,
		 int auto_start );
LDAP_LUTIL_F (int)
lutil_srv_remove ( char* service, char* filename );

#endif /* HAVE_NT_SERVICE_MANAGER */

#ifdef HAVE_NT_EVENT_LOG
LDAP_LUTIL_F (void)
lutil_LogStartedEvent( char *svc, int slap_debug, char *configfile, char *urls );

LDAP_LUTIL_F (void)
lutil_LogStoppedEvent( char *svc );
#endif

#ifdef HAVE_EBCDIC
/* Generally this has only been used to put '\n' to stdout. We need to
 * make sure it is output in EBCDIC.
 */
#undef putchar
#undef putc
#define putchar(c)     putc((c), stdout)
#define putc(c,fp)     do { char x=(c); __atoe_l(&x,1); putc(x,fp); } while(0)
#endif

LDAP_LUTIL_F (int)
lutil_atoix( int *v, const char *s, int x );

LDAP_LUTIL_F (int)
lutil_atoux( unsigned *v, const char *s, int x );

LDAP_LUTIL_F (int)
lutil_atolx( long *v, const char *s, int x );

LDAP_LUTIL_F (int)
lutil_atoulx( unsigned long *v, const char *s, int x );

#define lutil_atoi(v, s)	lutil_atoix((v), (s), 10)
#define lutil_atou(v, s)	lutil_atoux((v), (s), 10)
#define lutil_atol(v, s)	lutil_atolx((v), (s), 10)
#define lutil_atoul(v, s)	lutil_atoulx((v), (s), 10)

#ifdef HAVE_LONG_LONG
#if defined(HAVE_STRTOLL) || defined(HAVE_STRTOQ)
LDAP_LUTIL_F (int)
lutil_atollx( long long *v, const char *s, int x );
#define lutil_atoll(v, s)	lutil_atollx((v), (s), 10)
#endif /* HAVE_STRTOLL || HAVE_STRTOQ */

#if defined(HAVE_STRTOULL) || defined(HAVE_STRTOUQ)
LDAP_LUTIL_F (int)
lutil_atoullx( unsigned long long *v, const char *s, int x );
#define lutil_atoull(v, s)	lutil_atoullx((v), (s), 10)
#endif /* HAVE_STRTOULL || HAVE_STRTOUQ */
#endif /* HAVE_LONG_LONG */

LDAP_LUTIL_F (int)
lutil_str2bin( struct berval *in, struct berval *out, void *ctx );

/* Parse and unparse time intervals */
LDAP_LUTIL_F (int)
lutil_parse_time( const char *in, unsigned long *tp );

LDAP_LUTIL_F (int)
lutil_unparse_time( char *buf, size_t buflen, unsigned long t );

#ifdef timerdiv
#define lutil_timerdiv timerdiv
#else /* ! timerdiv */
/* works inplace (x == t) */
#define lutil_timerdiv(t,d,x) \
	do { \
		time_t s = (t)->tv_sec; \
		assert( d > 0 ); \
		(x)->tv_sec = s / d; \
		(x)->tv_usec = ( (t)->tv_usec + 1000000 * ( s % d ) ) / d; \
	} while ( 0 )
#endif /* ! timerdiv */

#ifdef timermul
#define lutil_timermul timermul
#else /* ! timermul */
/* works inplace (x == t) */
#define lutil_timermul(t,m,x) \
	do { \
		time_t u = (t)->tv_usec * m; \
		assert( m > 0 ); \
		(x)->tv_sec = (t)->tv_sec * m + u / 1000000; \
		(x)->tv_usec = u % 1000000; \
	} while ( 0 );
#endif /* ! timermul */

LDAP_END_DECL

#endif /* _LUTIL_H */

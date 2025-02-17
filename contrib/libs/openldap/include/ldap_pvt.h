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

/* ldap-pvt.h - Header for ldap_pvt_ functions.
 * These are meant to be internal to OpenLDAP Software.
 */

#ifndef _LDAP_PVT_H
#define _LDAP_PVT_H 1

#include <openldap.h>				/* get public interfaces */
#include <lber.h>				/* get ber_slen_t */
#include <lber_pvt.h>				/* get Sockbuf_Buf */

LDAP_BEGIN_DECL

LDAP_F ( int )
ldap_pvt_url_scheme2proto LDAP_P((
	const char * ));
LDAP_F ( int )
ldap_pvt_url_scheme2tls LDAP_P((
	const char * ));
LDAP_F ( int )
ldap_pvt_url_scheme2proxied LDAP_P((
	const char * ));

LDAP_F ( int )
ldap_pvt_url_scheme_port LDAP_P((
	const char *, int ));

struct ldap_url_desc; /* avoid pulling in <ldap.h> */

#define LDAP_PVT_URL_PARSE_NONE			(0x00U)
#define LDAP_PVT_URL_PARSE_NOEMPTY_HOST		(0x01U)
#define LDAP_PVT_URL_PARSE_DEF_PORT		(0x02U)
#define LDAP_PVT_URL_PARSE_NOEMPTY_DN		(0x04U)
#define LDAP_PVT_URL_PARSE_NODEF_SCOPE		(0x08U)
#define	LDAP_PVT_URL_PARSE_HISTORIC		(LDAP_PVT_URL_PARSE_NODEF_SCOPE | \
						 LDAP_PVT_URL_PARSE_NOEMPTY_HOST | \
						 LDAP_PVT_URL_PARSE_DEF_PORT)

LDAP_F( int )
ldap_url_parse_ext LDAP_P((
	LDAP_CONST char *url,
	struct ldap_url_desc **ludpp,
	unsigned flags ));

LDAP_F (int) ldap_url_parselist LDAP_P((	/* deprecated, use ldap_url_parselist_ext() */
	struct ldap_url_desc **ludlist,
	const char *url ));

LDAP_F (int) ldap_url_parselist_ext LDAP_P((
	struct ldap_url_desc **ludlist,
	const char *url,
	const char *sep,
	unsigned flags ));

LDAP_F (char *) ldap_url_list2urls LDAP_P((
	struct ldap_url_desc *ludlist ));

LDAP_F (void) ldap_free_urllist LDAP_P((
	struct ldap_url_desc *ludlist ));

LDAP_F (int) ldap_pvt_scope2bv LDAP_P ((
	int scope, struct berval *bv ));

LDAP_F (LDAP_CONST char *) ldap_pvt_scope2str LDAP_P ((
	int scope ));

LDAP_F (int) ldap_pvt_bv2scope LDAP_P ((
	struct berval *bv ));

LDAP_F (int) ldap_pvt_str2scope LDAP_P ((
	LDAP_CONST char * ));

LDAP_F( char * )
ldap_pvt_ctime LDAP_P((
	const time_t *tp,
	char *buf ));

# if defined( HAVE_GMTIME_R )
#   define USE_GMTIME_R
#   define ldap_pvt_gmtime(timep, result) gmtime_r((timep), (result))
# else
LDAP_F( struct tm * )
ldap_pvt_gmtime LDAP_P((
	LDAP_CONST time_t *timep,
	struct tm *result ));
#endif

# if defined( HAVE_LOCALTIME_R )
#   define USE_LOCALTIME_R
#   define ldap_pvt_localtime(timep, result) localtime_r((timep), (result))
# else
LDAP_F( struct tm * )
ldap_pvt_localtime LDAP_P((
	LDAP_CONST time_t *timep,
	struct tm *result ));
# endif

#if defined( USE_GMTIME_R ) && defined( USE_LOCALTIME_R )
#   define ldap_pvt_gmtime_lock() (0)
#   define ldap_pvt_gmtime_unlock() (0)
#else
LDAP_F( int )
ldap_pvt_gmtime_lock LDAP_P(( void ));

LDAP_F( int )
ldap_pvt_gmtime_unlock LDAP_P(( void ));
#endif /* USE_GMTIME_R && USE_LOCALTIME_R */

/* Get current time as a structured time */
struct lutil_tm;
LDAP_F( void )
ldap_pvt_gettime LDAP_P(( struct lutil_tm * ));

#ifdef _WIN32
#define gettimeofday(tv,tz)	ldap_pvt_gettimeofday(tv,tz)
struct timeval;
LDAP_F( int )
ldap_pvt_gettimeofday LDAP_P(( struct timeval *tv, void *unused ));
#ifndef CLOCK_REALTIME
#define CLOCK_REALTIME	0
#endif
#define clock_gettime(clkid,tv)	ldap_pvt_clock_gettime(clkid,tv)
struct timespec;
LDAP_F( int )
ldap_pvt_clock_gettime LDAP_P(( int clkid, struct timespec *tv ));
#endif

/* use this macro to allocate buffer for ldap_pvt_csnstr */
#define LDAP_PVT_CSNSTR_BUFSIZE	64
LDAP_F( size_t )
ldap_pvt_csnstr( char *buf, size_t len, unsigned int replica, unsigned int mod );

LDAP_F( char *) ldap_pvt_get_fqdn LDAP_P(( char * ));

struct hostent;	/* avoid pulling in <netdb.h> */

LDAP_F( int )
ldap_pvt_gethostbyname_a LDAP_P((
	const char *name,
	struct hostent *resbuf,
	char **buf,
	struct hostent **result,
	int *herrno_ptr ));

LDAP_F( int )
ldap_pvt_gethostbyaddr_a LDAP_P((
	const char *addr,
	int len,
	int type,
	struct hostent *resbuf,
	char **buf,
	struct hostent **result,
	int *herrno_ptr ));

struct sockaddr;

LDAP_F( int )
ldap_pvt_get_hname LDAP_P((
	const struct sockaddr * sa,
	int salen,
	char *name,
	int namelen,
	char **herr ));

#ifdef LDAP_PF_LOCAL
#define LDAP_IPADDRLEN	(MAXPATHLEN + sizeof("PATH="))
#elif defined(LDAP_PF_INET6)
#define LDAP_IPADDRLEN	sizeof("IP=[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535")
#else
#define LDAP_IPADDRLEN	sizeof("IP=255.255.255.255:65336")
#endif

union Sockaddr;

LDAP_F (void)
ldap_pvt_sockaddrstr LDAP_P((
	union Sockaddr *sa,
	struct berval * ));


/* charray.c */

LDAP_F( int )
ldap_charray_add LDAP_P((
    char	***a,
    const char *s ));

LDAP_F( int )
ldap_charray_merge LDAP_P((
    char	***a,
    char	**s ));

LDAP_F( void )
ldap_charray_free LDAP_P(( char **a ));

LDAP_F( int )
ldap_charray_inlist LDAP_P((
    char	**a,
    const char *s ));

LDAP_F( char ** )
ldap_charray_dup LDAP_P(( char **a ));

LDAP_F( char ** )
ldap_str2charray LDAP_P((
	const char *str,
	const char *brkstr ));

LDAP_F( char * )
ldap_charray2str LDAP_P((
	char **array, const char* sep ));

/* getdn.c */

#ifdef LDAP_AVA_NULL	/* in ldap.h */
LDAP_F( void ) ldap_rdnfree_x LDAP_P(( LDAPRDN rdn, void *ctx ));
LDAP_F( void ) ldap_dnfree_x LDAP_P(( LDAPDN dn, void *ctx ));

LDAP_F( int ) ldap_bv2dn_x LDAP_P(( 
	struct berval *bv, LDAPDN *dn, unsigned flags, void *ctx ));
LDAP_F( int ) ldap_dn2bv_x LDAP_P(( 
	LDAPDN dn, struct berval *bv, unsigned flags, void *ctx ));
LDAP_F( int ) ldap_bv2rdn_x LDAP_P(( 
	struct berval *, LDAPRDN *, char **, unsigned flags, void *ctx ));
LDAP_F( int ) ldap_rdn2bv_x LDAP_P(( 
	LDAPRDN rdn, struct berval *bv, unsigned flags, void *ctx ));
#endif /* LDAP_AVA_NULL */

/* url.c */
LDAP_F (void) ldap_pvt_hex_unescape LDAP_P(( char *s ));

/*
 * these macros assume 'x' is an ASCII x
 * and assume the "C" locale
 */
#define LDAP_ASCII(c)		(!((c) & 0x80))
#define LDAP_SPACE(c)		((c) == ' ' || (c) == '\t' || (c) == '\n')
#define LDAP_DIGIT(c)		((c) >= '0' && (c) <= '9')
#define LDAP_LOWER(c)		((c) >= 'a' && (c) <= 'z')
#define LDAP_UPPER(c)		((c) >= 'A' && (c) <= 'Z')
#define LDAP_ALPHA(c)		(LDAP_LOWER(c) || LDAP_UPPER(c))
#define LDAP_ALNUM(c)		(LDAP_ALPHA(c) || LDAP_DIGIT(c))

#define LDAP_LDH(c)			(LDAP_ALNUM(c) || (c) == '-')

#define LDAP_HEXLOWER(c)	((c) >= 'a' && (c) <= 'f')
#define LDAP_HEXUPPER(c)	((c) >= 'A' && (c) <= 'F')
#define LDAP_HEX(c)			(LDAP_DIGIT(c) || \
								LDAP_HEXLOWER(c) || LDAP_HEXUPPER(c))

/* controls.c */
struct ldapcontrol;
LDAP_F (int)
ldap_pvt_put_control LDAP_P((
	const struct ldapcontrol *c,
	BerElement *ber ));
LDAP_F (int) ldap_pvt_get_controls LDAP_P((
	BerElement *be,
	struct ldapcontrol ***ctrlsp));

#ifdef HAVE_CYRUS_SASL
/* cyrus.c */
struct sasl_security_properties; /* avoid pulling in <sasl.h> */
LDAP_F (int) ldap_pvt_sasl_secprops LDAP_P((
	const char *in,
	struct sasl_security_properties *secprops ));
LDAP_F (void) ldap_pvt_sasl_secprops_unparse LDAP_P((
	struct sasl_security_properties *secprops,
	struct berval *out ));

LDAP_F (void *) ldap_pvt_sasl_mutex_new LDAP_P((void));
LDAP_F (int) ldap_pvt_sasl_mutex_lock LDAP_P((void *mutex));
LDAP_F (int) ldap_pvt_sasl_mutex_unlock LDAP_P((void *mutex));
LDAP_F (void) ldap_pvt_sasl_mutex_dispose LDAP_P((void *mutex));

LDAP_F (int) ldap_pvt_sasl_cbinding_parse LDAP_P(( const char *arg ));
LDAP_F (void *) ldap_pvt_sasl_cbinding LDAP_P(( void *ssl, int type,
					        int is_server ));
#endif /* HAVE_CYRUS_SASL */

struct sockbuf; /* avoid pulling in <lber.h> */
LDAP_F (int) ldap_pvt_sasl_install LDAP_P(( struct sockbuf *, void * ));
LDAP_F (void) ldap_pvt_sasl_remove LDAP_P(( struct sockbuf * ));

/*
 * SASL encryption support for LBER Sockbufs
 */

struct sb_sasl_generic_data;

struct sb_sasl_generic_ops {
	void (*init)(struct sb_sasl_generic_data *p,
		     ber_len_t *min_send,
		     ber_len_t *max_send,
		     ber_len_t *max_recv);
	ber_int_t (*encode)(struct sb_sasl_generic_data *p,
			    unsigned char *buf,
			    ber_len_t len,
			    Sockbuf_Buf *dst);
	ber_int_t (*decode)(struct sb_sasl_generic_data *p,
			    const Sockbuf_Buf *src,
			    Sockbuf_Buf *dst);
	void (*reset_buf)(struct sb_sasl_generic_data *p,
			  Sockbuf_Buf *buf);
	void (*fini)(struct sb_sasl_generic_data *p);
};

struct sb_sasl_generic_install {
	const struct sb_sasl_generic_ops 	*ops;
	void					*ops_private;
};

struct sb_sasl_generic_data {
	const struct sb_sasl_generic_ops 	*ops;
	void					*ops_private;
	Sockbuf_IO_Desc				*sbiod;
	ber_len_t				min_send;
	ber_len_t				max_send;
	ber_len_t				max_recv;
	Sockbuf_Buf				sec_buf_in;
	Sockbuf_Buf				buf_in;
	Sockbuf_Buf				buf_out;
	unsigned int				flags;
#define LDAP_PVT_SASL_PARTIAL_WRITE	1
};
 
#ifndef LDAP_PVT_SASL_LOCAL_SSF
#define LDAP_PVT_SASL_LOCAL_SSF	71	/* SSF for Unix Domain Sockets */
#endif /* ! LDAP_PVT_SASL_LOCAL_SSF */

struct ldap;
struct ldapmsg;
struct ldifrecord;

/* abandon */
LDAP_F ( int ) ldap_pvt_discard LDAP_P((
	struct ldap *ld, ber_int_t msgid ));

/* init.c */
LDAP_F( int )
ldap_pvt_conf_option LDAP_P((
	char *cmd, char *opt, int userconf ));

/* ldifutil.c */
LDAP_F( int )
ldap_parse_ldif_record_x LDAP_P((
	struct berval *rbuf,
	unsigned long linenum,
	struct ldifrecord *lr,
	const char *errstr,
	unsigned int flags,
	void *ctx ));

/* messages.c */
LDAP_F( BerElement * )
ldap_get_message_ber LDAP_P((
	struct ldapmsg * ));

/* open */
LDAP_F (int) ldap_open_internal_connection LDAP_P((
	struct ldap **ldp, ber_socket_t *fdp ));

/* sasl.c */
LDAP_F (int) ldap_pvt_sasl_generic_install LDAP_P(( Sockbuf *sb,
	struct sb_sasl_generic_install *install_arg ));
LDAP_F (void) ldap_pvt_sasl_generic_remove LDAP_P(( Sockbuf *sb ));

/* search.c */
LDAP_F( int ) ldap_pvt_put_filter LDAP_P((
	BerElement *ber,
	const char *str ));

LDAP_F( char * )
ldap_pvt_find_wildcard LDAP_P((	const char *s ));

LDAP_F( ber_slen_t )
ldap_pvt_filter_value_unescape LDAP_P(( char *filter ));

LDAP_F( ber_len_t )
ldap_bv2escaped_filter_value_len LDAP_P(( struct berval *in ));

LDAP_F( int )
ldap_bv2escaped_filter_value_x LDAP_P(( struct berval *in, struct berval *out,
	int inplace, void *ctx ));

LDAP_F (int) ldap_pvt_search LDAP_P((
	struct ldap *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	struct ldapcontrol **sctrls,
	struct ldapcontrol **cctrls,
	struct timeval *timeout,
	int sizelimit,
	int deref,
	int *msgidp ));

LDAP_F(int) ldap_pvt_search_s LDAP_P((
	struct ldap *ld,
	LDAP_CONST char *base,
	int scope,
	LDAP_CONST char *filter,
	char **attrs,
	int attrsonly,
	struct ldapcontrol **sctrls,
	struct ldapcontrol **cctrls,
	struct timeval *timeout,
	int sizelimit,
	int deref,
	struct ldapmsg **res ));

/* string.c */
LDAP_F( char * )
ldap_pvt_str2upper LDAP_P(( char *str ));

LDAP_F( char * )
ldap_pvt_str2lower LDAP_P(( char *str ));

LDAP_F( struct berval * )
ldap_pvt_str2upperbv LDAP_P(( char *str, struct berval *bv ));

LDAP_F( struct berval * )
ldap_pvt_str2lowerbv LDAP_P(( char *str, struct berval *bv ));

/* tls.c */
LDAP_F (int) ldap_pvt_tls_config LDAP_P(( struct ldap *ld,
	int option, const char *arg ));
LDAP_F (int) ldap_pvt_tls_get_option LDAP_P(( struct ldap *ld,
	int option, void *arg ));
LDAP_F (int) ldap_pvt_tls_set_option LDAP_P(( struct ldap *ld,
	int option, void *arg ));

LDAP_F (void) ldap_pvt_tls_destroy LDAP_P(( void ));
LDAP_F (int) ldap_pvt_tls_init LDAP_P(( int do_threads ));
LDAP_F (int) ldap_pvt_tls_init_def_ctx LDAP_P(( int is_server ));
LDAP_F (int) ldap_pvt_tls_accept LDAP_P(( Sockbuf *sb, void *ctx_arg ));
LDAP_F (int) ldap_pvt_tls_connect LDAP_P(( struct ldap *ld, Sockbuf *sb, const char *host ));
LDAP_F (int) ldap_pvt_tls_inplace LDAP_P(( Sockbuf *sb ));
LDAP_F (void *) ldap_pvt_tls_sb_ctx LDAP_P(( Sockbuf *sb ));
LDAP_F (void) ldap_pvt_tls_ctx_free LDAP_P(( void * ));

typedef int LDAPDN_rewrite_dummy LDAP_P (( void *dn, unsigned flags ));

typedef int (LDAP_TLS_CONNECT_CB) LDAP_P (( struct ldap *ld, void *ssl,
	void *ctx, void *arg ));

LDAP_F (int) ldap_pvt_tls_get_my_dn LDAP_P(( void *ctx, struct berval *dn,
	LDAPDN_rewrite_dummy *func, unsigned flags ));
LDAP_F (int) ldap_pvt_tls_get_peer_dn LDAP_P(( void *ctx, struct berval *dn,
	LDAPDN_rewrite_dummy *func, unsigned flags ));
LDAP_F (int) ldap_pvt_tls_get_strength LDAP_P(( void *ctx ));
LDAP_F (int) ldap_pvt_tls_get_unique LDAP_P(( void *ctx, struct berval *buf, int is_server ));
LDAP_F (int) ldap_pvt_tls_get_endpoint LDAP_P(( void *ctx, struct berval *buf, int is_server ));
LDAP_F (const char *) ldap_pvt_tls_get_version LDAP_P(( void *ctx ));
LDAP_F (const char *) ldap_pvt_tls_get_cipher LDAP_P(( void *ctx ));

LDAP_END_DECL

/*
 * Multiple precision stuff
 * 
 * May use OpenSSL's BIGNUM if built with TLS,
 * or GNU's multiple precision library. But if
 * long long is available, that's big enough
 * and much more efficient.
 *
 * If none is available, unsigned long data is used.
 */

LDAP_BEGIN_DECL

#ifdef USE_MP_BIGNUM
/*
 * Use OpenSSL's BIGNUM
 */
#error #include <openssl/crypto.h>
#error #include <openssl/bn.h>

typedef	BIGNUM* ldap_pvt_mp_t;
#define	LDAP_PVT_MP_INIT	(NULL)

#define	ldap_pvt_mp_init(mp) \
	do { (mp) = BN_new(); } while (0)

/* FIXME: we rely on mpr being initialized */
#define	ldap_pvt_mp_init_set(mpr,mpv) \
	do { ldap_pvt_mp_init((mpr)); BN_add((mpr), (mpr), (mpv)); } while (0)

#define	ldap_pvt_mp_add(mpr,mpv) \
	BN_add((mpr), (mpr), (mpv))

#define	ldap_pvt_mp_add_ulong(mp,v) \
	BN_add_word((mp), (v))

#define ldap_pvt_mp_clear(mp) \
	do { BN_free((mp)); (mp) = 0; } while (0)

#elif defined(USE_MP_GMP)
/*
 * Use GNU's multiple precision library
 */
#include <gmp.h>

typedef mpz_t		ldap_pvt_mp_t;
#define	LDAP_PVT_MP_INIT	{ 0 }

#define ldap_pvt_mp_init(mp) \
	mpz_init((mp))

#define	ldap_pvt_mp_init_set(mpr,mpv) \
	mpz_init_set((mpr), (mpv))

#define	ldap_pvt_mp_add(mpr,mpv) \
	mpz_add((mpr), (mpr), (mpv))

#define	ldap_pvt_mp_add_ulong(mp,v)	\
	mpz_add_ui((mp), (mp), (v))

#define ldap_pvt_mp_clear(mp) \
	mpz_clear((mp))

#else
/*
 * Use unsigned long long
 */

#ifdef USE_MP_LONG_LONG
typedef	unsigned long long	ldap_pvt_mp_t;
#define	LDAP_PVT_MP_INIT	(0LL)
#elif defined(USE_MP_LONG)
typedef	unsigned long		ldap_pvt_mp_t;
#define	LDAP_PVT_MP_INIT	(0L)
#elif defined(HAVE_LONG_LONG)
typedef	unsigned long long	ldap_pvt_mp_t;
#define	LDAP_PVT_MP_INIT	(0LL)
#else
typedef	unsigned long		ldap_pvt_mp_t;
#define	LDAP_PVT_MP_INIT	(0L)
#endif

#define ldap_pvt_mp_init(mp) \
	do { (mp) = 0; } while (0)

#define	ldap_pvt_mp_init_set(mpr,mpv) \
	do { (mpr) = (mpv); } while (0)

#define	ldap_pvt_mp_add(mpr,mpv) \
	do { (mpr) += (mpv); } while (0)

#define	ldap_pvt_mp_add_ulong(mp,v) \
	do { (mp) += (v); } while (0)

#define ldap_pvt_mp_clear(mp) \
	do { (mp) = 0; } while (0)

#endif /* MP */

#include "ldap_pvt_uc.h"

LDAP_END_DECL

LDAP_BEGIN_DECL

#include <limits.h>				/* get CHAR_BIT */

/* Buffer space for sign, decimal digits and \0. Note: log10(2) < 146/485. */
#define LDAP_PVT_INTTYPE_CHARS(type) (((sizeof(type)*CHAR_BIT-1)*146)/485 + 3)

LDAP_END_DECL

#endif /* _LDAP_PVT_H */

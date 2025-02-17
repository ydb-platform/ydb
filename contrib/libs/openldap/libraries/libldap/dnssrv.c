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
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/*
 * locate LDAP servers using DNS SRV records.
 * Location code based on MIT Kerberos KDC location code.
 */
#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/param.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

#ifdef HAVE_ARPA_NAMESER_H
#include <arpa/nameser.h>
#endif
#ifdef HAVE_RESOLV_H
#include <resolv.h>
#endif

int ldap_dn2domain(
	LDAP_CONST char *dn_in,
	char **domainp)
{
	int i, j;
	char *ndomain;
	LDAPDN dn = NULL;
	LDAPRDN rdn = NULL;
	LDAPAVA *ava = NULL;
	struct berval domain = BER_BVNULL;
	static const struct berval DC = BER_BVC("DC");
	static const struct berval DCOID = BER_BVC("0.9.2342.19200300.100.1.25");

	assert( dn_in != NULL );
	assert( domainp != NULL );

	*domainp = NULL;

	if ( ldap_str2dn( dn_in, &dn, LDAP_DN_FORMAT_LDAP ) != LDAP_SUCCESS ) {
		return -2;
	}

	if( dn ) for( i=0; dn[i] != NULL; i++ ) {
		rdn = dn[i];

		for( j=0; rdn[j] != NULL; j++ ) {
			ava = rdn[j];

			if( rdn[j+1] == NULL &&
				(ava->la_flags & LDAP_AVA_STRING) &&
				ava->la_value.bv_len &&
				( ber_bvstrcasecmp( &ava->la_attr, &DC ) == 0
				|| ber_bvcmp( &ava->la_attr, &DCOID ) == 0 ) )
			{
				if( domain.bv_len == 0 ) {
					ndomain = LDAP_REALLOC( domain.bv_val,
						ava->la_value.bv_len + 1);

					if( ndomain == NULL ) {
						goto return_error;
					}

					domain.bv_val = ndomain;

					AC_MEMCPY( domain.bv_val, ava->la_value.bv_val,
						ava->la_value.bv_len );

					domain.bv_len = ava->la_value.bv_len;
					domain.bv_val[domain.bv_len] = '\0';

				} else {
					ndomain = LDAP_REALLOC( domain.bv_val,
						ava->la_value.bv_len + sizeof(".") + domain.bv_len );

					if( ndomain == NULL ) {
						goto return_error;
					}

					domain.bv_val = ndomain;
					domain.bv_val[domain.bv_len++] = '.';
					AC_MEMCPY( &domain.bv_val[domain.bv_len],
						ava->la_value.bv_val, ava->la_value.bv_len );
					domain.bv_len += ava->la_value.bv_len;
					domain.bv_val[domain.bv_len] = '\0';
				}
			} else {
				domain.bv_len = 0;
			}
		} 
	}


	if( domain.bv_len == 0 && domain.bv_val != NULL ) {
		LDAP_FREE( domain.bv_val );
		domain.bv_val = NULL;
	}

	ldap_dnfree( dn );
	*domainp = domain.bv_val;
	return 0;

return_error:
	ldap_dnfree( dn );
	LDAP_FREE( domain.bv_val );
	return -1;
}

int ldap_domain2dn(
	LDAP_CONST char *domain_in,
	char **dnp)
{
	char *domain, *s, *tok_r, *dn, *dntmp;
	size_t loc;

	assert( domain_in != NULL );
	assert( dnp != NULL );

	domain = LDAP_STRDUP(domain_in);
	if (domain == NULL) {
		return LDAP_NO_MEMORY;
	}
	dn = NULL;
	loc = 0;

	for (s = ldap_pvt_strtok(domain, ".", &tok_r);
		s != NULL;
		s = ldap_pvt_strtok(NULL, ".", &tok_r))
	{
		size_t len = strlen(s);

		dntmp = (char *) LDAP_REALLOC(dn, loc + sizeof(",dc=") + len );
		if (dntmp == NULL) {
		    if (dn != NULL)
			LDAP_FREE(dn);
		    LDAP_FREE(domain);
		    return LDAP_NO_MEMORY;
		}

		dn = dntmp;

		if (loc > 0) {
		    /* not first time. */
		    strcpy(dn + loc, ",");
		    loc++;
		}
		strcpy(dn + loc, "dc=");
		loc += sizeof("dc=")-1;

		strcpy(dn + loc, s);
		loc += len;
    }

	LDAP_FREE(domain);
	*dnp = dn;
	return LDAP_SUCCESS;
}

#ifdef HAVE_RES_QUERY
#define DNSBUFSIZ (64*1024)
#define MAXHOST	254	/* RFC 1034, max length is 253 chars */
typedef struct srv_record {
    u_short priority;
    u_short weight;
    u_short port;
    char hostname[MAXHOST];
} srv_record;

/* Linear Congruential Generator - we don't need
 * high quality randomness, and we don't want to
 * interfere with anyone else's use of srand().
 *
 * The PRNG here cycles thru 941,955 numbers.
 */
static float srv_seed;

static void srv_srand(int seed) {
	srv_seed = (float)seed / (float)RAND_MAX;
}

static float srv_rand() {
	float val = 9821.0 * srv_seed + .211327;
	srv_seed = val - (int)val;
	return srv_seed;
}

static int srv_cmp(const void *aa, const void *bb){
	srv_record *a=(srv_record *)aa;
	srv_record *b=(srv_record *)bb;
	int i = a->priority - b->priority;
	if (i) return i;
	return b->weight - a->weight;
}

static void srv_shuffle(srv_record *a, int n) {
	int i, j, total = 0, r, p;

	for (i=0; i<n; i++)
		total += a[i].weight;

	/* Do a shuffle per RFC2782 Page 4 */
	for (p=n; p>1; a++, p--) {
		if (!total) {
			/* all remaining weights are zero,
			   do a straight Fisher-Yates shuffle */
			j = srv_rand() * p;
		} else {
			r = srv_rand() * total;
			for (j=0; j<p; j++) {
				r -= a[j].weight;
				if (r < 0) {
					total -= a[j].weight;
					break;
				}
			}
		}
		if (j && j<p) {
			srv_record t = a[0];
			a[0] = a[j];
			a[j] = t;
		}
	}
}
#endif /* HAVE_RES_QUERY */

/*
 * Lookup and return LDAP servers for domain (using the DNS
 * SRV record _ldap._tcp.domain).
 */
int ldap_domain2hostlist(
	LDAP_CONST char *domain,
	char **list )
{
#ifdef HAVE_RES_QUERY
    char *request;
    char *hostlist = NULL;
    srv_record *hostent_head=NULL;
    int i, j;
    int rc, len, cur = 0;
    unsigned char reply[DNSBUFSIZ];
    int hostent_count=0;

	assert( domain != NULL );
	assert( list != NULL );
	if( *domain == '\0' ) {
		return LDAP_PARAM_ERROR;
	}

    request = LDAP_MALLOC(strlen(domain) + sizeof("_ldap._tcp."));
    if (request == NULL) {
		return LDAP_NO_MEMORY;
    }
    sprintf(request, "_ldap._tcp.%s", domain);

    LDAP_MUTEX_LOCK(&ldap_int_resolv_mutex);

    rc = LDAP_UNAVAILABLE;
#ifdef NS_HFIXEDSZ
	/* Bind 8/9 interface */
    len = res_query(request, ns_c_in, ns_t_srv, reply, sizeof(reply));
#	ifndef T_SRV
#		define T_SRV ns_t_srv
#	endif
#else
	/* Bind 4 interface */
#	ifndef T_SRV
#		define T_SRV 33
#	endif

    len = res_query(request, C_IN, T_SRV, reply, sizeof(reply));
#endif
    if (len >= 0) {
	unsigned char *p;
	char host[DNSBUFSIZ];
	int status;
	u_short port, priority, weight;

	/* Parse out query */
	p = reply;

#ifdef NS_HFIXEDSZ
	/* Bind 8/9 interface */
	p += NS_HFIXEDSZ;
#elif defined(HFIXEDSZ)
	/* Bind 4 interface w/ HFIXEDSZ */
	p += HFIXEDSZ;
#else
	/* Bind 4 interface w/o HFIXEDSZ */
	p += sizeof(HEADER);
#endif

	status = dn_expand(reply, reply + len, p, host, sizeof(host));
	if (status < 0) {
	    goto out;
	}
	p += status;
	p += 4;

	while (p < reply + len) {
	    int type, class, ttl, size;
	    status = dn_expand(reply, reply + len, p, host, sizeof(host));
	    if (status < 0) {
		goto out;
	    }
	    p += status;
	    type = (p[0] << 8) | p[1];
	    p += 2;
	    class = (p[0] << 8) | p[1];
	    p += 2;
	    ttl = (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
	    p += 4;
	    size = (p[0] << 8) | p[1];
	    p += 2;
	    if (type == T_SRV) {
		status = dn_expand(reply, reply + len, p + 6, host, sizeof(host));
		if (status < 0) {
		    goto out;
		}

		/* Get priority weight and port */
		priority = (p[0] << 8) | p[1];
		weight = (p[2] << 8) | p[3];
		port = (p[4] << 8) | p[5];

		if ( port == 0 || host[ 0 ] == '\0' ) {
		    goto add_size;
		}

		hostent_head = (srv_record *) LDAP_REALLOC(hostent_head, (hostent_count+1)*(sizeof(srv_record)));
		if(hostent_head==NULL){
		    rc=LDAP_NO_MEMORY;
		    goto out;
		}
		hostent_head[hostent_count].priority=priority;
		hostent_head[hostent_count].weight=weight;
		hostent_head[hostent_count].port=port;
		strncpy(hostent_head[hostent_count].hostname, host, MAXHOST-1);
		hostent_head[hostent_count].hostname[MAXHOST-1] = '\0';
		hostent_count++;
	    }
add_size:;
	    p += size;
	}
	if (!hostent_head) goto out;
    qsort(hostent_head, hostent_count, sizeof(srv_record), srv_cmp);

	if (!srv_seed)
		srv_srand(time(0L));

	/* shuffle records of same priority */
	j = 0;
	priority = hostent_head[0].priority;
	for (i=1; i<hostent_count; i++) {
		if (hostent_head[i].priority != priority) {
			priority = hostent_head[i].priority;
			if (i-j > 1)
				srv_shuffle(hostent_head+j, i-j);
			j = i;
		}
	}
	if (i-j > 1)
		srv_shuffle(hostent_head+j, i-j);

    for(i=0; i<hostent_count; i++){
	int buflen;
        buflen = strlen(hostent_head[i].hostname) + STRLENOF(":65535 ");
        hostlist = (char *) LDAP_REALLOC(hostlist, cur+buflen+1);
        if (hostlist == NULL) {
            rc = LDAP_NO_MEMORY;
            goto out;
        }
        if(cur>0){
            hostlist[cur++]=' ';
        }
        cur += sprintf(&hostlist[cur], "%s:%hu", hostent_head[i].hostname, hostent_head[i].port);
    }
    }

    if (hostlist == NULL) {
	/* No LDAP servers found in DNS. */
	rc = LDAP_UNAVAILABLE;
	goto out;
    }

    rc = LDAP_SUCCESS;
	*list = hostlist;

  out:
    LDAP_MUTEX_UNLOCK(&ldap_int_resolv_mutex);

    if (request != NULL) {
	LDAP_FREE(request);
    }
    if (hostent_head != NULL) {
	LDAP_FREE(hostent_head);
    }
    if (rc != LDAP_SUCCESS && hostlist != NULL) {
	LDAP_FREE(hostlist);
    }
    return rc;
#else
    return LDAP_NOT_SUPPORTED;
#endif /* HAVE_RES_QUERY */
}

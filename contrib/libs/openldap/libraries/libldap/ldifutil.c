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
/* Portions Copyright (c) 1990 Regents of the University of Michigan.
 * All rights reserved.
 */

/*
 * This file contains public API to help with parsing LDIF
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>
#include <ac/ctype.h>
#include <ac/string.h>
#include <ac/unistd.h>
#include <ac/socket.h>
#include <ac/time.h>

#include "ldap-int.h"
#include "ldif.h"

#define	M_SEP	0x7f

/* strings found in LDIF entries */
static struct berval BV_VERSION = BER_BVC("version");
static struct berval BV_DN = BER_BVC("dn");
static struct berval BV_CONTROL = BER_BVC("control");
static struct berval BV_CHANGETYPE = BER_BVC("changetype");
static struct berval BV_ADDCT = BER_BVC("add");
static struct berval BV_MODIFYCT = BER_BVC("modify");
static struct berval BV_DELETECT = BER_BVC("delete");
static struct berval BV_MODRDNCT = BER_BVC("modrdn");
static struct berval BV_MODDNCT = BER_BVC("moddn");
static struct berval BV_RENAMECT = BER_BVC("rename");
static struct berval BV_MODOPADD = BER_BVC("add");
static struct berval BV_MODOPREPLACE = BER_BVC("replace");
static struct berval BV_MODOPDELETE = BER_BVC("delete");
static struct berval BV_MODOPINCREMENT = BER_BVC("increment");
static struct berval BV_NEWRDN = BER_BVC("newrdn");
static struct berval BV_DELETEOLDRDN = BER_BVC("deleteoldrdn");
static struct berval BV_NEWSUP = BER_BVC("newsuperior");

#define	BV_CASEMATCH(a, b) \
	((a)->bv_len == (b)->bv_len && 0 == strcasecmp((a)->bv_val, (b)->bv_val))

static int parse_ldif_control LDAP_P(( struct berval *bval, LDAPControl ***ppctrls ));

void
ldap_ldif_record_done( LDIFRecord *lr )
{
	int i;

	/* the LDAPControl stuff does not allow the use of memory contexts */
	if (lr->lr_ctrls != NULL) {
		ldap_controls_free( lr->lr_ctrls );
	}
	if ( lr->lr_lm != NULL ) {
		ber_memfree_x( lr->lr_lm, lr->lr_ctx );
	}
	if ( lr->lr_mops != NULL ) {
		ber_memfree_x( lr->lr_mops, lr->lr_ctx );
	}
	for (i=lr->lr_lines-1; i>=0; i--)
		if ( lr->lr_freeval[i] ) ber_memfree_x( lr->lr_vals[i].bv_val, lr->lr_ctx );
	ber_memfree_x( lr->lr_btype, lr->lr_ctx );

	memset( lr, 0, sizeof(LDIFRecord) );
}

/*
 * ldap_parse_ldif_record_x() will convert an LDIF record read with ldif_read_record()
 * into an array of LDAPMod* and an array of LDAPControl*, suitable for passing
 * directly to any other LDAP API function that takes LDAPMod** and LDAPControl**
 * arguments, such as ldap_modify_s().
 *
 * rbuf - the ldif record buffer returned from ldif_read_record - rbuf.bv_val must be
 *        writable - will use ldif_getline to read from it
 * linenum - the ldif line number returned from ldif_read_record
 *         - used for logging errors (e.g. error at line N)
 * lr - holds the data to return
 * errstr - a string used for logging (usually the program name e.g. "ldapmodify"
 * flags - 0 or some combination of LDIF_DEFAULT_ADD LDIF_ENTRIES_ONLY LDIF_NO_CONTROLS
 * ctx is the memory allocation context - if NULL, use the standard memory allocator
 */
int
ldap_parse_ldif_record_x(
	struct berval *rbuf,
	unsigned long linenum,
	LDIFRecord *lr,
	const char *errstr,
	unsigned int flags,
	void *ctx )
{
	char	*line, *dn;
	int		rc, modop;
	int		expect_modop, expect_sep;
	int		ldapadd, new_entry, delete_entry, got_all, no_dn;
	LDAPMod	**pmods;
	int version;
	LDAPControl **pctrls;
	int i, j, k, idn, nmods;
	struct berval **bvl, bv;

	assert( lr != NULL );
	assert( rbuf != NULL );
	memset( lr, 0, sizeof(LDIFRecord) );
	lr->lr_ctx = ctx; /* save memory context for later */
	ldapadd = flags & LDIF_DEFAULT_ADD;
	no_dn = flags & LDIF_NO_DN;
	expect_modop = flags & LDIF_MODS_ONLY;
	new_entry = ldapadd;

	rc = got_all = delete_entry = modop = 0;
	expect_sep = 0;
	version = 0;
	pmods = NULL;
	pctrls = NULL;
	dn = NULL;

	lr->lr_lines = ldif_countlines( rbuf->bv_val );
	lr->lr_btype = ber_memcalloc_x( 1, (lr->lr_lines+1)*2*sizeof(struct berval)+lr->lr_lines, ctx );
	if ( !lr->lr_btype )
		return LDAP_NO_MEMORY;

	lr->lr_vals = lr->lr_btype+lr->lr_lines+1;
	lr->lr_freeval = (char *)(lr->lr_vals+lr->lr_lines+1);
	i = -1;

	while ( rc == 0 && ( line = ldif_getline( &rbuf->bv_val )) != NULL ) {
		int freev;

		if ( *line == '\n' || *line == '\0' ) {
			break;
		}

		++i;

		if ( line[0] == '-' && !line[1] ) {
			BER_BVZERO( lr->lr_btype+i );
			lr->lr_freeval[i] = 0;
			continue;
		}
	
		if ( ( rc = ldif_parse_line2( line, lr->lr_btype+i, lr->lr_vals+i, &freev ) ) < 0 ) {
			fprintf( stderr, _("%s: invalid format (line %lu) entry: \"%s\"\n"),
				errstr, linenum+i, dn == NULL ? "" : dn );
			rc = LDAP_PARAM_ERROR;
			goto leave;
		}
		lr->lr_freeval[i] = freev;

		if ( dn == NULL && !no_dn ) {
			if ( linenum+i == 1 && BV_CASEMATCH( lr->lr_btype+i, &BV_VERSION )) {
				/* lutil_atoi() introduces a dependence of libldap
				 * on liblutil; we only allow version 1 by now (ITS#6654)
				 */
#if 0
				int	v;
				if( lr->lr_vals[i].bv_len == 0 || lutil_atoi( &v, lr->lr_vals[i].bv_val) != 0 || v != 1 )
#endif
				static const struct berval version1 = { 1, "1" };
				if ( lr->lr_vals[i].bv_len != version1.bv_len || strncmp( lr->lr_vals[i].bv_val, version1.bv_val, version1.bv_len ) != 0 )
				{
					fprintf( stderr,
						_("%s: invalid version %s, line %lu (ignored)\n"),
						errstr, lr->lr_vals[i].bv_val, linenum );
				}
				version++;

			} else if ( BV_CASEMATCH( lr->lr_btype+i, &BV_DN )) {
				lr->lr_dn = lr->lr_vals[i];
				dn = lr->lr_dn.bv_val; /* primarily for logging */
				idn = i;
			}
			/* skip all lines until we see "dn:" */
		}
	}

	/* check to make sure there was a dn: line */
	if ( !dn && !no_dn ) {
		rc = 0;
		goto leave;
	}

	lr->lr_lines = i+1;

	if( lr->lr_lines == 0 ) {
		rc = 0;
		goto leave;
	}

	if( version && lr->lr_lines == 1 ) {
		rc = 0;
		goto leave;
	}

	if ( no_dn ) {
		i = 0;
	} else {
		i = idn+1;
		/* Check for "control" tag after dn and before changetype. */
		if ( BV_CASEMATCH( lr->lr_btype+i, &BV_CONTROL )) {
			/* Parse and add it to the list of controls */
			if ( !( flags & LDIF_NO_CONTROLS ) ) {
				rc = parse_ldif_control( lr->lr_vals+i, &pctrls );
				if (rc != 0) {
					fprintf( stderr,
							 _("%s: Error processing %s line, line %lu: %s\n"),
							 errstr, BV_CONTROL.bv_val, linenum+i, ldap_err2string(rc) );
				}
			}
			i++;
			if ( i>= lr->lr_lines ) {
short_input:
				fprintf( stderr,
					_("%s: Expecting more input after %s line, line %lu\n"),
					errstr, lr->lr_btype[i-1].bv_val, linenum+i );

				rc = LDAP_PARAM_ERROR;
				goto leave;
			}
		}
	}

	/* Check for changetype */
	if ( BV_CASEMATCH( lr->lr_btype+i, &BV_CHANGETYPE )) {
#ifdef LIBERAL_CHANGETYPE_MODOP
		/* trim trailing spaces (and log warning ...) */
		int icnt;
		for ( icnt = lr->lr_vals[i].bv_len; --icnt > 0; ) {
			if ( !isspace( (unsigned char) lr->lr_vals[i].bv_val[icnt] ) ) {
				break;
			}
		}

		if ( ++icnt != lr->lr_vals[i].bv_len ) {
			fprintf( stderr, _("%s: illegal trailing space after"
				" \"%s: %s\" trimmed (line %lu, entry \"%s\")\n"),
				errstr, BV_CHANGETYPE.bv_val, lr->lr_vals[i].bv_val, linenum+i, dn );
			lr->lr_vals[i].bv_val[icnt] = '\0';
		}
#endif /* LIBERAL_CHANGETYPE_MODOP */

		/* if LDIF_ENTRIES_ONLY, then either the changetype must be add, or
		   there must be no changetype, and the flag LDIF_DEFAULT_ADD must be set */
		if ( flags & LDIF_ENTRIES_ONLY ) {
			if ( !( BV_CASEMATCH( lr->lr_vals+i, &BV_ADDCT )) ) {
				ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
									_("%s: skipping LDIF record beginning at line %lu: "
									  "changetype '%.*s' found but entries only was requested\n"),
									errstr, linenum,
									(int)lr->lr_vals[i].bv_len,
									(const char *)lr->lr_vals[i].bv_val );
				goto leave;
			}
		}

		if ( BV_CASEMATCH( lr->lr_vals+i, &BV_MODIFYCT )) {
			new_entry = 0;
			expect_modop = 1;
		} else if ( BV_CASEMATCH( lr->lr_vals+i, &BV_ADDCT )) {
			new_entry = 1;
			modop = LDAP_MOD_ADD;
		} else if ( BV_CASEMATCH( lr->lr_vals+i, &BV_MODRDNCT )
			|| BV_CASEMATCH( lr->lr_vals+i, &BV_MODDNCT )
			|| BV_CASEMATCH( lr->lr_vals+i, &BV_RENAMECT ))
		{
			i++;
			if ( i >= lr->lr_lines )
				goto short_input;
			if ( !BV_CASEMATCH( lr->lr_btype+i, &BV_NEWRDN )) {
				fprintf( stderr, _("%s: expecting \"%s:\" but saw"
					" \"%s:\" (line %lu, entry \"%s\")\n"),
					errstr, BV_NEWRDN.bv_val, lr->lr_btype[i].bv_val, linenum+i, dn );
				rc = LDAP_PARAM_ERROR;
				goto leave;
			}
			lr->lrop_newrdn = lr->lr_vals[i];
			i++;
			if ( i >= lr->lr_lines )
				goto short_input;
			if ( !BV_CASEMATCH( lr->lr_btype+i, &BV_DELETEOLDRDN )) {
				fprintf( stderr, _("%s: expecting \"%s:\" but saw"
					" \"%s:\" (line %lu, entry \"%s\")\n"),
					errstr, BV_DELETEOLDRDN.bv_val, lr->lr_btype[i].bv_val, linenum+i, dn );
				rc = LDAP_PARAM_ERROR;
				goto leave;
			}
			lr->lrop_delold = ( lr->lr_vals[i].bv_val[0] == '0' ) ? 0 : 1;
			i++;
			if ( i < lr->lr_lines ) {
				if ( !BV_CASEMATCH( lr->lr_btype+i, &BV_NEWSUP )) {
					fprintf( stderr, _("%s: expecting \"%s:\" but saw"
						" \"%s:\" (line %lu, entry \"%s\")\n"),
						errstr, BV_NEWSUP.bv_val, lr->lr_btype[i].bv_val, linenum+i, dn );
					rc = LDAP_PARAM_ERROR;
					goto leave;
				}
				lr->lrop_newsup = lr->lr_vals[i];
				i++;
			}
			got_all = 1;
		} else if ( BV_CASEMATCH( lr->lr_vals+i, &BV_DELETECT )) {
			got_all = delete_entry = 1;
		} else {
			fprintf( stderr,
				_("%s:  unknown %s \"%s\" (line %lu, entry \"%s\")\n"),
				errstr, BV_CHANGETYPE.bv_val, lr->lr_vals[i].bv_val, linenum+i, dn );
			rc = LDAP_PARAM_ERROR;
			goto leave;
		}
		i++;
	} else if ( ldapadd ) {		/*  missing changetype => add */
		new_entry = 1;
		modop = LDAP_MOD_ADD;
	} else {
		/* if LDIF_ENTRIES_ONLY, then either the changetype must be add, or
		   there must be no changetype, and the flag LDIF_DEFAULT_ADD must be set */
		if ( flags & LDIF_ENTRIES_ONLY ) {
			ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
								_("%s: skipping LDIF record beginning at line %lu: "
								  "no changetype found but entries only was requested and "
								  "the default setting for missing changetype is modify\n"),
								errstr, linenum );
			goto leave;
		}
		expect_modop = 1;	/* missing changetype => modify */
	}

	if ( got_all ) {
		if ( i < lr->lr_lines ) {
			fprintf( stderr,
				_("%s: extra lines at end (line %lu, entry \"%s\")\n"),
				errstr, linenum+i, dn );
			rc = LDAP_PARAM_ERROR;
			goto leave;
		}
		goto doit;
	}

	nmods = lr->lr_lines - i;
	idn = i;

	if ( new_entry ) {
		int fv;

		/* Make sure all attributes with multiple values are contiguous */
		for (; i<lr->lr_lines; i++) {
			for (j=i+1; j<lr->lr_lines; j++) {
				if ( !lr->lr_btype[j].bv_val ) {
					fprintf( stderr,
						_("%s: missing attributeDescription (line %lu, entry \"%s\")\n"),
						errstr, linenum+j, dn );
					rc = LDAP_PARAM_ERROR;
					goto leave;
				}
				if ( BV_CASEMATCH( lr->lr_btype+i, lr->lr_btype+j )) {
					nmods--;
					/* out of order, move intervening attributes down */
					if ( j != i+1 ) {
						bv = lr->lr_vals[j];
						fv = lr->lr_freeval[j];
						for (k=j; k>i; k--) {
							lr->lr_btype[k] = lr->lr_btype[k-1];
							lr->lr_vals[k] = lr->lr_vals[k-1];
							lr->lr_freeval[k] = lr->lr_freeval[k-1];
						}
						k++;
						lr->lr_btype[k] = lr->lr_btype[i];
						lr->lr_vals[k] = bv;
						lr->lr_freeval[k] = fv;
					}
					i++;
				}
			}
		}
		/* Allocate space for array of mods, array of pointers to mods,
		 * and array of pointers to values, allowing for NULL terminators
		 * for the pointer arrays...
		 */
		lr->lr_lm = ber_memalloc_x( nmods * sizeof(LDAPMod) +
			(nmods+1) * sizeof(LDAPMod*) +
			(lr->lr_lines + nmods - idn) * sizeof(struct berval *), ctx );
		if ( lr->lr_lm == NULL ) {
			rc = LDAP_NO_MEMORY;
			goto leave;
		}

		pmods = (LDAPMod **)(lr->lr_lm+nmods);
		bvl = (struct berval **)(pmods+nmods+1);

		j = 0;
		k = -1;
		BER_BVZERO(&bv);
		for (i=idn; i<lr->lr_lines; i++) {
			if ( BV_CASEMATCH( lr->lr_btype+i, &BV_DN )) {
				fprintf( stderr, _("%s: attributeDescription \"%s\":"
					" (possible missing newline"
						" after line %lu, entry \"%s\"?)\n"),
					errstr, lr->lr_btype[i].bv_val, linenum+i - 1, dn );
			}
			if ( !BV_CASEMATCH( lr->lr_btype+i, &bv )) {
				bvl[k++] = NULL;
				bv = lr->lr_btype[i];
				lr->lr_lm[j].mod_op = LDAP_MOD_ADD | LDAP_MOD_BVALUES;
				lr->lr_lm[j].mod_type = bv.bv_val;
				lr->lr_lm[j].mod_bvalues = bvl+k;
				pmods[j] = lr->lr_lm+j;
				j++;
			}
			bvl[k++] = lr->lr_vals+i;
		}
		bvl[k] = NULL;
		pmods[j] = NULL;
		goto doit;
	}

	lr->lr_mops = ber_memalloc_x( lr->lr_lines+1, ctx );
	if ( lr->lr_mops == NULL ) {
		rc = LDAP_NO_MEMORY;
		goto leave;
	}

	lr->lr_mops[lr->lr_lines] = M_SEP;
	if ( i > 0 )
		lr->lr_mops[i-1] = M_SEP;

	for ( ; i<lr->lr_lines; i++ ) {
		if ( expect_modop ) {
#ifdef LIBERAL_CHANGETYPE_MODOP
			/* trim trailing spaces (and log warning ...) */
		    int icnt;
		    for ( icnt = lr->lr_vals[i].bv_len; --icnt > 0; ) {
				if ( !isspace( (unsigned char) lr->lr_vals[i].bv_val[icnt] ) ) break;
			}
    
			if ( ++icnt != lr->lr_vals[i].bv_len ) {
				fprintf( stderr, _("%s: illegal trailing space after"
					" \"%s: %s\" trimmed (line %lu, entry \"%s\")\n"),
					errstr, type, lr->lr_vals[i].bv_val, linenum+i, dn );
				lr->lr_vals[i].bv_val[icnt] = '\0';
			}
#endif /* LIBERAL_CHANGETYPE_MODOP */

			expect_modop = 0;
			expect_sep = 1;
			if ( BV_CASEMATCH( lr->lr_btype+i, &BV_MODOPADD )) {
				modop = LDAP_MOD_ADD;
				lr->lr_mops[i] = M_SEP;
				nmods--;
			} else if ( BV_CASEMATCH( lr->lr_btype+i, &BV_MODOPREPLACE )) {
			/* defer handling these since they might have no values.
			 * Use the BVALUES flag to signal that these were
			 * deferred. If values are provided later, this
			 * flag will be switched off.
			 */
				modop = LDAP_MOD_REPLACE;
				lr->lr_mops[i] = modop | LDAP_MOD_BVALUES;
				lr->lr_btype[i] = lr->lr_vals[i];
			} else if ( BV_CASEMATCH( lr->lr_btype+i, &BV_MODOPDELETE )) {
				modop = LDAP_MOD_DELETE;
				lr->lr_mops[i] = modop | LDAP_MOD_BVALUES;
				lr->lr_btype[i] = lr->lr_vals[i];
			} else if ( BV_CASEMATCH( lr->lr_btype+i, &BV_MODOPINCREMENT )) {
				modop = LDAP_MOD_INCREMENT;
				lr->lr_mops[i] = M_SEP;
				nmods--;
			} else {	/* no modify op: invalid LDIF */
				fprintf( stderr, _("%s: modify operation type is missing at"
					" line %lu, entry \"%s\"\n"),
					errstr, linenum+i, dn );
				rc = LDAP_PARAM_ERROR;
				goto leave;
			}
			bv = lr->lr_vals[i];
		} else if ( expect_sep && BER_BVISEMPTY( lr->lr_btype+i )) {
			lr->lr_mops[i] = M_SEP;
			expect_sep = 0;
			expect_modop = 1;
			nmods--;
		} else {
			if ( !BV_CASEMATCH( lr->lr_btype+i, &bv )) {
				fprintf( stderr, _("%s: wrong attributeType at"
					" line %lu, entry \"%s\"\n"),
					errstr, linenum+i, dn );
				rc = LDAP_PARAM_ERROR;
				goto leave;
			}
			lr->lr_mops[i] = modop;
			/* If prev op was deferred and matches this type,
			 * clear the flag
			 */
			if ( (lr->lr_mops[i-1] & LDAP_MOD_BVALUES)
				&& BV_CASEMATCH( lr->lr_btype+i, lr->lr_btype+i-1 ))
			{
				lr->lr_mops[i-1] = M_SEP;
				nmods--;
			}
		}
	}

	/* Allocate space for array of mods, array of pointers to mods,
	 * and array of pointers to values, allowing for NULL terminators
	 * for the pointer arrays...
	 */
	lr->lr_lm = ber_memalloc_x( nmods * sizeof(LDAPMod) +
		(nmods+1) * sizeof(LDAPMod*) +
		(lr->lr_lines + nmods - idn) * sizeof(struct berval *), ctx );
	if ( lr->lr_lm == NULL ) {
		rc = LDAP_NO_MEMORY;
		goto leave;
	}

	pmods = (LDAPMod **)(lr->lr_lm+nmods);
	bvl = (struct berval **)(pmods+nmods+1);

	j = 0;
	k = -1;
	BER_BVZERO(&bv);
	if ( idn > 0 )
		lr->lr_mops[idn-1] = M_SEP;
	for (i=idn; i<lr->lr_lines; i++) {
		if ( lr->lr_mops[i] == M_SEP )
			continue;
		if ( lr->lr_mops[i] != lr->lr_mops[i-1] || !BV_CASEMATCH( lr->lr_btype+i, &bv )) {
			bvl[k++] = NULL;
			bv = lr->lr_btype[i];
			lr->lr_lm[j].mod_op = lr->lr_mops[i] | LDAP_MOD_BVALUES;
			lr->lr_lm[j].mod_type = bv.bv_val;
			if ( lr->lr_mops[i] & LDAP_MOD_BVALUES ) {
				lr->lr_lm[j].mod_bvalues = NULL;
			} else {
				lr->lr_lm[j].mod_bvalues = bvl+k;
			}
			pmods[j] = lr->lr_lm+j;
			j++;
		}
		bvl[k++] = lr->lr_vals+i;
	}
	bvl[k] = NULL;
	pmods[j] = NULL;

doit:
	/* first, set the common fields */
	lr->lr_ctrls = pctrls;
	/* next, set the op */
	if ( delete_entry ) {
		lr->lr_op = LDAP_REQ_DELETE;
	} else if ( lr->lrop_newrdn.bv_val != NULL ) {
		lr->lr_op = LDAP_REQ_MODDN;
	} else {
		/* for now, either add or modify */
		lr->lrop_mods = pmods;
		if ( new_entry ) {
			lr->lr_op = LDAP_REQ_ADD;
		} else {
			lr->lr_op = LDAP_REQ_MODIFY;
		}
	}

leave:
	if ( rc != LDAP_SUCCESS ) {
		ldap_ldif_record_done( lr );
	}

	return( rc );
}

/* Same as ldap_parse_ldif_record_x()
 * public API does not expose memory context
 */
int
ldap_parse_ldif_record(
	struct berval *rbuf,
	unsigned long linenum,
	LDIFRecord *lr,
	const char *errstr,
	unsigned int flags )
{
	return ldap_parse_ldif_record_x( rbuf, linenum, lr, errstr, flags, NULL );
}

/* Parse an LDIF control line of the form
      control:  oid  [true/false]  [: value]              or
      control:  oid  [true/false]  [:: base64-value]      or
      control:  oid  [true/false]  [:< url]
   The control is added to the list of controls in *ppctrls.
*/      
static int
parse_ldif_control(
	struct berval *bval,
	LDAPControl ***ppctrls)
{
	char *oid = NULL;
	int criticality = 0;   /* Default is false if not present */
	int i, rc=0;
	char *s, *oidStart;
	LDAPControl *newctrl = NULL;
	LDAPControl **pctrls = NULL;
	struct berval type, bv = BER_BVNULL;
	int freeval = 0;

	if (ppctrls) pctrls = *ppctrls;
	/* OID should come first. Validate and extract it. */
	s = bval->bv_val;
	if (*s == 0) return ( LDAP_PARAM_ERROR );
	oidStart = s;
	while (isdigit((unsigned char)*s) || *s == '.') {
		s++;                           /* OID should be digits or . */
	}
	if (s == oidStart) { 
		return ( LDAP_PARAM_ERROR );   /* OID was not present */
	}
	if (*s) {                          /* End of OID should be space or NULL */
		if (!isspace((unsigned char)*s)) {
			return ( LDAP_PARAM_ERROR ); /* else OID contained invalid chars */
		}
		*s++ = 0;                    /* Replace space with null to terminate */
	}

	oid = ber_strdup(oidStart);
	if (oid == NULL) return ( LDAP_NO_MEMORY );

	/* Optional Criticality field is next. */
	while (*s && isspace((unsigned char)*s)) {
		s++;                         /* Skip white space before criticality */
	}
	if (strncasecmp(s, "true", 4) == 0) {
		criticality = 1;
		s += 4;
	} 
	else if (strncasecmp(s, "false", 5) == 0) {
		criticality = 0;
		s += 5;
	}

	/* Optional value field is next */
	while (*s && isspace((unsigned char)*s)) {
		s++;                         /* Skip white space before value */
	}
	if (*s) {
		if (*s != ':') {           /* If value is present, must start with : */
			rc = LDAP_PARAM_ERROR;
			goto cleanup;
		}

		/* Back up so value is in the form
		     a: value
		     a:: base64-value
		     a:< url
		   Then we can use ldif_parse_line2 to extract and decode the value
		*/
		s--;
		*s = 'a';

		rc = ldif_parse_line2(s, &type, &bv, &freeval);
		if (rc < 0) {
			rc = LDAP_PARAM_ERROR;
			goto cleanup;
		}
    }

	/* Create a new LDAPControl structure. */
	newctrl = (LDAPControl *)ber_memalloc(sizeof(LDAPControl));
	if ( newctrl == NULL ) {
		rc = LDAP_NO_MEMORY;
		goto cleanup;
	}
	newctrl->ldctl_oid = oid;
	oid = NULL;
	newctrl->ldctl_iscritical = criticality;
	if ( freeval )
		newctrl->ldctl_value = bv;
	else
		ber_dupbv( &newctrl->ldctl_value, &bv );

	/* Add the new control to the passed-in list of controls. */
	i = 0;
	if (pctrls) {
		while ( pctrls[i] ) {    /* Count the # of controls passed in */
			i++;
		}
	}
	/* Allocate 1 more slot for the new control and 1 for the NULL. */
	pctrls = (LDAPControl **) ber_memrealloc(pctrls,
		(i+2)*(sizeof(LDAPControl *)));
	if (pctrls == NULL) {
		rc = LDAP_NO_MEMORY;
		goto cleanup;
	}
	pctrls[i] = newctrl;
	newctrl = NULL;
	pctrls[i+1] = NULL;
	*ppctrls = pctrls;

cleanup:
	if (newctrl) {
		if (newctrl->ldctl_oid) ber_memfree(newctrl->ldctl_oid);
		if (newctrl->ldctl_value.bv_val) {
			ber_memfree(newctrl->ldctl_value.bv_val);
		}
		ber_memfree(newctrl);
	}
	if (oid) ber_memfree(oid);

	return( rc );
}



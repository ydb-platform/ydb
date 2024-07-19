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
 * schema.c:  parsing routines used by servers and clients to process
 *	schema definitions
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdlib.h>

#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

#include <ldap_schema.h>

static const char EndOfInput[] = "end of input";

static const char *
choose_name( char *names[], const char *fallback )
{
	return (names != NULL && names[0] != NULL) ? names[0] : fallback;
}

LDAP_CONST char *
ldap_syntax2name( LDAPSyntax * syn )
{
	if (!syn) return NULL;
	return( syn->syn_oid );
}

LDAP_CONST char *
ldap_matchingrule2name( LDAPMatchingRule * mr )
{
	if (!mr) return NULL;
	return( choose_name( mr->mr_names, mr->mr_oid ) );
}

LDAP_CONST char *
ldap_matchingruleuse2name( LDAPMatchingRuleUse * mru )
{
	if (!mru) return NULL;
	return( choose_name( mru->mru_names, mru->mru_oid ) );
}

LDAP_CONST char *
ldap_attributetype2name( LDAPAttributeType * at )
{
	if (!at) return NULL;
	return( choose_name( at->at_names, at->at_oid ) );
}

LDAP_CONST char *
ldap_objectclass2name( LDAPObjectClass * oc )
{
	if (!oc) return NULL;
	return( choose_name( oc->oc_names, oc->oc_oid ) );
}

LDAP_CONST char *
ldap_contentrule2name( LDAPContentRule * cr )
{
	if (!cr) return NULL;
	return( choose_name( cr->cr_names, cr->cr_oid ) );
}

LDAP_CONST char *
ldap_nameform2name( LDAPNameForm * nf )
{
	if (!nf) return NULL;
	return( choose_name( nf->nf_names, nf->nf_oid ) );
}

LDAP_CONST char *
ldap_structurerule2name( LDAPStructureRule * sr )
{
	if (!sr) return NULL;
	return( choose_name( sr->sr_names, NULL ) );
}

/*
 * When pretty printing the entities we will be appending to a buffer.
 * Since checking for overflow, realloc'ing and checking if no error
 * is extremely boring, we will use a protection layer that will let
 * us blissfully ignore the error until the end.  This layer is
 * implemented with the help of the next type.
 */

typedef struct safe_string {
	char * val;
	ber_len_t size;
	ber_len_t pos;
	int at_whsp;
} safe_string;

static safe_string *
new_safe_string(int size)
{
	safe_string * ss;
	
	ss = LDAP_MALLOC(sizeof(safe_string));
	if ( !ss )
		return(NULL);

	ss->val = LDAP_MALLOC(size);
	if ( !ss->val ) {
		LDAP_FREE(ss);
		return(NULL);
	}

	ss->size = size;
	ss->pos = 0;
	ss->at_whsp = 0;

	return ss;
}

static void
safe_string_free(safe_string * ss)
{
	if ( !ss )
		return;
	LDAP_FREE(ss->val);
	LDAP_FREE(ss);
}

#if 0	/* unused */
static char *
safe_string_val(safe_string * ss)
{
	ss->val[ss->pos] = '\0';
	return(ss->val);
}
#endif

static char *
safe_strdup(safe_string * ss)
{
	char *ret = LDAP_MALLOC(ss->pos+1);
	if (!ret)
		return NULL;
	AC_MEMCPY(ret, ss->val, ss->pos);
	ret[ss->pos] = '\0';
	return ret;
}

static int
append_to_safe_string(safe_string * ss, char * s)
{
	int l = strlen(s);
	char * temp;

	/*
	 * Some runaway process is trying to append to a string that
	 * overflowed and we could not extend.
	 */
	if ( !ss->val )
		return -1;

	/* We always make sure there is at least one position available */
	if ( ss->pos + l >= ss->size-1 ) {
		ss->size *= 2;
		if ( ss->pos + l >= ss->size-1 ) {
			ss->size = ss->pos + l + 1;
		}

		temp = LDAP_REALLOC(ss->val, ss->size);
		if ( !temp ) {
			/* Trouble, out of memory */
			LDAP_FREE(ss->val);
			return -1;
		}
		ss->val = temp;
	}
	strncpy(&ss->val[ss->pos], s, l);
	ss->pos += l;
	if ( ss->pos > 0 && LDAP_SPACE(ss->val[ss->pos-1]) )
		ss->at_whsp = 1;
	else
		ss->at_whsp = 0;

	return 0;
}

static int
print_literal(safe_string *ss, char *s)
{
	return(append_to_safe_string(ss,s));
}

static int
print_whsp(safe_string *ss)
{
	if ( ss->at_whsp )
		return(append_to_safe_string(ss,""));
	else
		return(append_to_safe_string(ss," "));
}

static int
print_numericoid(safe_string *ss, char *s)
{
	if ( s )
		return(append_to_safe_string(ss,s));
	else
		return(append_to_safe_string(ss,""));
}

/* This one is identical to print_qdescr */
static int
print_qdstring(safe_string *ss, char *s)
{
	print_whsp(ss);
	print_literal(ss,"'");
	append_to_safe_string(ss,s);
	print_literal(ss,"'");
	return(print_whsp(ss));
}

static int
print_qdescr(safe_string *ss, char *s)
{
	print_whsp(ss);
	print_literal(ss,"'");
	append_to_safe_string(ss,s);
	print_literal(ss,"'");
	return(print_whsp(ss));
}

static int
print_qdescrlist(safe_string *ss, char **sa)
{
	char **sp;
	int ret = 0;
	
	for (sp=sa; *sp; sp++) {
		ret = print_qdescr(ss,*sp);
	}
	/* If the list was empty, we return zero that is potentially
	 * incorrect, but since we will be still appending things, the
	 * overflow will be detected later.  Maybe FIX.
	 */
	return(ret);
}

static int
print_qdescrs(safe_string *ss, char **sa)
{
	/* The only way to represent an empty list is as a qdescrlist
	 * so, if the list is empty we treat it as a long list.
	 * Really, this is what the syntax mandates.  We should not
	 * be here if the list was empty, but if it happens, a label
	 * has already been output and we cannot undo it.
	 */
	if ( !sa[0] || ( sa[0] && sa[1] ) ) {
		print_whsp(ss);
		print_literal(ss,"("/*)*/);
		print_qdescrlist(ss,sa);
		print_literal(ss,/*(*/")");
		return(print_whsp(ss));
	} else {
	  return(print_qdescr(ss,*sa));
	}
}

static int
print_woid(safe_string *ss, char *s)
{
	print_whsp(ss);
	append_to_safe_string(ss,s);
	return print_whsp(ss);
}

static int
print_oidlist(safe_string *ss, char **sa)
{
	char **sp;

	for (sp=sa; *(sp+1); sp++) {
		print_woid(ss,*sp);
		print_literal(ss,"$");
	}
	return(print_woid(ss,*sp));
}

static int
print_oids(safe_string *ss, char **sa)
{
	if ( sa[0] && sa[1] ) {
		print_literal(ss,"("/*)*/);
		print_oidlist(ss,sa);
		print_whsp(ss);
		return(print_literal(ss,/*(*/")"));
	} else {
		return(print_woid(ss,*sa));
	}
}

static int
print_noidlen(safe_string *ss, char *s, int l)
{
	char buf[64];
	int ret;

	ret = print_numericoid(ss,s);
	if ( l ) {
		snprintf(buf, sizeof buf, "{%d}",l);
		ret = print_literal(ss,buf);
	}
	return(ret);
}

static int
print_ruleid(safe_string *ss, int rid)
{
	char buf[64];
	snprintf(buf, sizeof buf, "%d", rid);
	return print_literal(ss,buf);
}

static int
print_ruleids(safe_string *ss, int n, int *rids)
{
	int i;

	if( n == 1 ) {
		print_ruleid(ss,rids[0]);
		return print_whsp(ss);
	} else {
		print_literal(ss,"("/*)*/);
		for( i=0; i<n; i++ ) {
			print_whsp(ss);
			print_ruleid(ss,rids[i]);
		}
		print_whsp(ss);
		return print_literal(ss,/*(*/")");
	}
}


static int
print_extensions(safe_string *ss, LDAPSchemaExtensionItem **extensions)
{
	LDAPSchemaExtensionItem **ext;

	if ( extensions ) {
		print_whsp(ss);
		for ( ext = extensions; *ext != NULL; ext++ ) {
			print_literal(ss, (*ext)->lsei_name);
			print_whsp(ss);
			/* Should be print_qdstrings */
			print_qdescrs(ss, (*ext)->lsei_values);
			print_whsp(ss);
		}
	}

	return 0;
}

char *
ldap_syntax2str( LDAPSyntax * syn )
{
	struct berval bv;
	if (ldap_syntax2bv( syn, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_syntax2bv( LDAPSyntax * syn, struct berval *bv )
{
	safe_string * ss;

	if ( !syn || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"("/*)*/);
	print_whsp(ss);

	print_numericoid(ss, syn->syn_oid);
	print_whsp(ss);

	if ( syn->syn_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,syn->syn_desc);
	}

	print_whsp(ss);

	print_extensions(ss, syn->syn_extensions);

	print_literal(ss,/*(*/ ")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

char *
ldap_matchingrule2str( LDAPMatchingRule * mr )
{
	struct berval bv;
	if (ldap_matchingrule2bv( mr, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_matchingrule2bv( LDAPMatchingRule * mr, struct berval *bv )
{
	safe_string * ss;

	if ( !mr || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"(" /*)*/);
	print_whsp(ss);

	print_numericoid(ss, mr->mr_oid);
	print_whsp(ss);

	if ( mr->mr_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,mr->mr_names);
	}

	if ( mr->mr_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,mr->mr_desc);
	}

	if ( mr->mr_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	if ( mr->mr_syntax_oid ) {
		print_literal(ss,"SYNTAX");
		print_whsp(ss);
		print_literal(ss, mr->mr_syntax_oid);
		print_whsp(ss);
	}

	print_whsp(ss);

	print_extensions(ss, mr->mr_extensions);

	print_literal(ss,/*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

char *
ldap_matchingruleuse2str( LDAPMatchingRuleUse * mru )
{
	struct berval bv;
	if (ldap_matchingruleuse2bv( mru, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_matchingruleuse2bv( LDAPMatchingRuleUse * mru, struct berval *bv )
{
	safe_string * ss;

	if ( !mru || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"(" /*)*/);
	print_whsp(ss);

	print_numericoid(ss, mru->mru_oid);
	print_whsp(ss);

	if ( mru->mru_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,mru->mru_names);
	}

	if ( mru->mru_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,mru->mru_desc);
	}

	if ( mru->mru_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	if ( mru->mru_applies_oids ) {
		print_literal(ss,"APPLIES");
		print_whsp(ss);
		print_oids(ss, mru->mru_applies_oids);
		print_whsp(ss);
	}

	print_whsp(ss);

	print_extensions(ss, mru->mru_extensions);

	print_literal(ss,/*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

char *
ldap_objectclass2str( LDAPObjectClass * oc )
{
	struct berval bv;
	if (ldap_objectclass2bv( oc, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_objectclass2bv( LDAPObjectClass * oc, struct berval *bv )
{
	safe_string * ss;

	if ( !oc || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"("/*)*/);
	print_whsp(ss);

	print_numericoid(ss, oc->oc_oid);
	print_whsp(ss);

	if ( oc->oc_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,oc->oc_names);
	}

	if ( oc->oc_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,oc->oc_desc);
	}

	if ( oc->oc_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	if ( oc->oc_sup_oids ) {
		print_literal(ss,"SUP");
		print_whsp(ss);
		print_oids(ss,oc->oc_sup_oids);
		print_whsp(ss);
	}

	switch (oc->oc_kind) {
	case LDAP_SCHEMA_ABSTRACT:
		print_literal(ss,"ABSTRACT");
		break;
	case LDAP_SCHEMA_STRUCTURAL:
		print_literal(ss,"STRUCTURAL");
		break;
	case LDAP_SCHEMA_AUXILIARY:
		print_literal(ss,"AUXILIARY");
		break;
	default:
		print_literal(ss,"KIND-UNKNOWN");
		break;
	}
	print_whsp(ss);
	
	if ( oc->oc_at_oids_must ) {
		print_literal(ss,"MUST");
		print_whsp(ss);
		print_oids(ss,oc->oc_at_oids_must);
		print_whsp(ss);
	}

	if ( oc->oc_at_oids_may ) {
		print_literal(ss,"MAY");
		print_whsp(ss);
		print_oids(ss,oc->oc_at_oids_may);
		print_whsp(ss);
	}

	print_whsp(ss);

	print_extensions(ss, oc->oc_extensions);

	print_literal(ss, /*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

char *
ldap_contentrule2str( LDAPContentRule * cr )
{
	struct berval bv;
	if (ldap_contentrule2bv( cr, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_contentrule2bv( LDAPContentRule * cr, struct berval *bv )
{
	safe_string * ss;

	if ( !cr || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"("/*)*/);
	print_whsp(ss);

	print_numericoid(ss, cr->cr_oid);
	print_whsp(ss);

	if ( cr->cr_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,cr->cr_names);
	}

	if ( cr->cr_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,cr->cr_desc);
	}

	if ( cr->cr_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	if ( cr->cr_oc_oids_aux ) {
		print_literal(ss,"AUX");
		print_whsp(ss);
		print_oids(ss,cr->cr_oc_oids_aux);
		print_whsp(ss);
	}

	if ( cr->cr_at_oids_must ) {
		print_literal(ss,"MUST");
		print_whsp(ss);
		print_oids(ss,cr->cr_at_oids_must);
		print_whsp(ss);
	}

	if ( cr->cr_at_oids_may ) {
		print_literal(ss,"MAY");
		print_whsp(ss);
		print_oids(ss,cr->cr_at_oids_may);
		print_whsp(ss);
	}

	if ( cr->cr_at_oids_not ) {
		print_literal(ss,"NOT");
		print_whsp(ss);
		print_oids(ss,cr->cr_at_oids_not);
		print_whsp(ss);
	}

	print_whsp(ss);
	print_extensions(ss, cr->cr_extensions);

	print_literal(ss, /*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

char *
ldap_structurerule2str( LDAPStructureRule * sr )
{
	struct berval bv;
	if (ldap_structurerule2bv( sr, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_structurerule2bv( LDAPStructureRule * sr, struct berval *bv )
{
	safe_string * ss;

	if ( !sr || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"("/*)*/);
	print_whsp(ss);

	print_ruleid(ss, sr->sr_ruleid);
	print_whsp(ss);

	if ( sr->sr_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,sr->sr_names);
	}

	if ( sr->sr_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,sr->sr_desc);
	}

	if ( sr->sr_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	print_literal(ss,"FORM");
	print_whsp(ss);
	print_woid(ss,sr->sr_nameform);
	print_whsp(ss);

	if ( sr->sr_nsup_ruleids ) {
		print_literal(ss,"SUP");
		print_whsp(ss);
		print_ruleids(ss,sr->sr_nsup_ruleids,sr->sr_sup_ruleids);
		print_whsp(ss);
	}

	print_whsp(ss);
	print_extensions(ss, sr->sr_extensions);

	print_literal(ss, /*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}


char *
ldap_nameform2str( LDAPNameForm * nf )
{
	struct berval bv;
	if (ldap_nameform2bv( nf, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_nameform2bv( LDAPNameForm * nf, struct berval *bv )
{
	safe_string * ss;

	if ( !nf || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"("/*)*/);
	print_whsp(ss);

	print_numericoid(ss, nf->nf_oid);
	print_whsp(ss);

	if ( nf->nf_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,nf->nf_names);
	}

	if ( nf->nf_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,nf->nf_desc);
	}

	if ( nf->nf_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	print_literal(ss,"OC");
	print_whsp(ss);
	print_woid(ss,nf->nf_objectclass);
	print_whsp(ss);

	print_literal(ss,"MUST");
	print_whsp(ss);
	print_oids(ss,nf->nf_at_oids_must);
	print_whsp(ss);


	if ( nf->nf_at_oids_may ) {
		print_literal(ss,"MAY");
		print_whsp(ss);
		print_oids(ss,nf->nf_at_oids_may);
		print_whsp(ss);
	}

	print_whsp(ss);
	print_extensions(ss, nf->nf_extensions);

	print_literal(ss, /*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

char *
ldap_attributetype2str( LDAPAttributeType * at )
{
	struct berval bv;
	if (ldap_attributetype2bv( at, &bv ))
		return(bv.bv_val);
	else
		return NULL;
}

struct berval *
ldap_attributetype2bv(  LDAPAttributeType * at, struct berval *bv )
{
	safe_string * ss;

	if ( !at || !bv )
		return NULL;

	ss = new_safe_string(256);
	if ( !ss )
		return NULL;

	print_literal(ss,"("/*)*/);
	print_whsp(ss);

	print_numericoid(ss, at->at_oid);
	print_whsp(ss);

	if ( at->at_names ) {
		print_literal(ss,"NAME");
		print_qdescrs(ss,at->at_names);
	}

	if ( at->at_desc ) {
		print_literal(ss,"DESC");
		print_qdstring(ss,at->at_desc);
	}

	if ( at->at_obsolete ) {
		print_literal(ss, "OBSOLETE");
		print_whsp(ss);
	}

	if ( at->at_sup_oid ) {
		print_literal(ss,"SUP");
		print_woid(ss,at->at_sup_oid);
	}

	if ( at->at_equality_oid ) {
		print_literal(ss,"EQUALITY");
		print_woid(ss,at->at_equality_oid);
	}

	if ( at->at_ordering_oid ) {
		print_literal(ss,"ORDERING");
		print_woid(ss,at->at_ordering_oid);
	}

	if ( at->at_substr_oid ) {
		print_literal(ss,"SUBSTR");
		print_woid(ss,at->at_substr_oid);
	}

	if ( at->at_syntax_oid ) {
		print_literal(ss,"SYNTAX");
		print_whsp(ss);
		print_noidlen(ss,at->at_syntax_oid,at->at_syntax_len);
		print_whsp(ss);
	}

	if ( at->at_single_value == LDAP_SCHEMA_YES ) {
		print_literal(ss,"SINGLE-VALUE");
		print_whsp(ss);
	}

	if ( at->at_collective == LDAP_SCHEMA_YES ) {
		print_literal(ss,"COLLECTIVE");
		print_whsp(ss);
	}

	if ( at->at_no_user_mod == LDAP_SCHEMA_YES ) {
		print_literal(ss,"NO-USER-MODIFICATION");
		print_whsp(ss);
	}

	if ( at->at_usage != LDAP_SCHEMA_USER_APPLICATIONS ) {
		print_literal(ss,"USAGE");
		print_whsp(ss);
		switch (at->at_usage) {
		case LDAP_SCHEMA_DIRECTORY_OPERATION:
			print_literal(ss,"directoryOperation");
			break;
		case LDAP_SCHEMA_DISTRIBUTED_OPERATION:
			print_literal(ss,"distributedOperation");
			break;
		case LDAP_SCHEMA_DSA_OPERATION:
			print_literal(ss,"dSAOperation");
			break;
		default:
			print_literal(ss,"UNKNOWN");
			break;
		}
	}
	
	print_whsp(ss);

	print_extensions(ss, at->at_extensions);

	print_literal(ss,/*(*/")");

	bv->bv_val = safe_strdup(ss);
	bv->bv_len = ss->pos;
	safe_string_free(ss);
	return(bv);
}

/*
 * Now come the parsers.  There is one parser for each entity type:
 * objectclasses, attributetypes, etc.
 *
 * Each of them is written as a recursive-descent parser, except that
 * none of them is really recursive.  But the idea is kept: there
 * is one routine per non-terminal that either gobbles lexical tokens
 * or calls lower-level routines, etc.
 *
 * The scanner is implemented in the routine get_token.  Actually,
 * get_token is more than a scanner and will return tokens that are
 * in fact non-terminals in the grammar.  So you can see the whole
 * approach as the combination of a low-level bottom-up recognizer
 * combined with a scanner and a number of top-down parsers.  Or just
 * consider that the real grammars recognized by the parsers are not
 * those of the standards.  As a matter of fact, our parsers are more
 * liberal than the spec when there is no ambiguity.
 *
 * The difference is pretty academic (modulo bugs or incorrect
 * interpretation of the specs).
 */

typedef enum tk_t {
	TK_NOENDQUOTE	= -2,
	TK_OUTOFMEM	= -1,
	TK_EOS		= 0,
	TK_UNEXPCHAR	= 1,
	TK_BAREWORD	= 2,
	TK_QDSTRING	= 3,
	TK_LEFTPAREN	= 4,
	TK_RIGHTPAREN	= 5,
	TK_DOLLAR	= 6,
	TK_QDESCR	= TK_QDSTRING
} tk_t;

static tk_t
get_token( const char ** sp, char ** token_val )
{
	tk_t kind;
	const char * p;
	const char * q;
	char * res;

	*token_val = NULL;
	switch (**sp) {
	case '\0':
		kind = TK_EOS;
		(*sp)++;
		break;
	case '(':
		kind = TK_LEFTPAREN;
		(*sp)++;
		break;
	case ')':
		kind = TK_RIGHTPAREN;
		(*sp)++;
		break;
	case '$':
		kind = TK_DOLLAR;
		(*sp)++;
		break;
	case '\'':
		kind = TK_QDSTRING;
		(*sp)++;
		p = *sp;
		while ( **sp != '\'' && **sp != '\0' )
			(*sp)++;
		if ( **sp == '\'' ) {
			q = *sp;
			res = LDAP_MALLOC(q-p+1);
			if ( !res ) {
				kind = TK_OUTOFMEM;
			} else {
				strncpy(res,p,q-p);
				res[q-p] = '\0';
				*token_val = res;
			}
			(*sp)++;
		} else {
			kind = TK_NOENDQUOTE;
		}
		break;
	default:
		kind = TK_BAREWORD;
		p = *sp;
		while ( !LDAP_SPACE(**sp) &&
			**sp != '(' &&
			**sp != ')' &&
			**sp != '$' &&
			**sp != '\'' &&
			/* for suggested minimum upper bound on the number
			 * of characters (RFC 4517) */
			**sp != '{' &&
			**sp != '\0' )
			(*sp)++;
		q = *sp;
		res = LDAP_MALLOC(q-p+1);
		if ( !res ) {
			kind = TK_OUTOFMEM;
		} else {
			strncpy(res,p,q-p);
			res[q-p] = '\0';
			*token_val = res;
		}
		break;
/*  		kind = TK_UNEXPCHAR; */
/*  		break; */
	}
	
	return kind;
}

/* Gobble optional whitespace */
static void
parse_whsp(const char **sp)
{
	while (LDAP_SPACE(**sp))
		(*sp)++;
}

/* TBC:!!
 * General note for all parsers: to guarantee the algorithm halts they
 * must always advance the pointer even when an error is found.  For
 * this one is not that important since an error here is fatal at the
 * upper layers, but it is a simple strategy that will not get in
 * endless loops.
 */

/* Parse a sequence of dot-separated decimal strings */
char *
ldap_int_parse_numericoid(const char **sp, int *code, const int flags)
{
	char * res = NULL;
	const char * start = *sp;
	int len;
	int quoted = 0;

	/* Netscape puts the SYNTAX value in quotes (incorrectly) */
	if ( flags & LDAP_SCHEMA_ALLOW_QUOTED && **sp == '\'' ) {
		quoted = 1;
		(*sp)++;
		start++;
	}
	/* Each iteration of this loop gets one decimal string */
	while (**sp) {
		if ( !LDAP_DIGIT(**sp) ) {
			/*
			 * Initial char is not a digit or char after dot is
			 * not a digit
			 */
			*code = LDAP_SCHERR_NODIGIT;
			return NULL;
		}
		(*sp)++;
		while ( LDAP_DIGIT(**sp) )
			(*sp)++;
		if ( **sp != '.' )
			break;
		/* Otherwise, gobble the dot and loop again */
		(*sp)++;
	}
	/* Now *sp points at the char past the numericoid. Perfect. */
	len = *sp - start;
	if ( flags & LDAP_SCHEMA_ALLOW_QUOTED && quoted ) {
		if ( **sp == '\'' ) {
			(*sp)++;
		} else {
			*code = LDAP_SCHERR_UNEXPTOKEN;
			return NULL;
		}
	}
	if (flags & LDAP_SCHEMA_SKIP) {
		res = (char *)start;
	} else {
		res = LDAP_MALLOC(len+1);
		if (!res) {
			*code = LDAP_SCHERR_OUTOFMEM;
			return(NULL);
		}
		strncpy(res,start,len);
		res[len] = '\0';
	}
	return(res);
}

/* Parse a sequence of dot-separated decimal strings */
int
ldap_int_parse_ruleid(const char **sp, int *code, const int flags, int *ruleid)
{
	*ruleid=0;

	if ( !LDAP_DIGIT(**sp) ) {
		*code = LDAP_SCHERR_NODIGIT;
		return -1;
	}
	*ruleid = (**sp) - '0';
	(*sp)++;

	while ( LDAP_DIGIT(**sp) ) {
		*ruleid *= 10;
		*ruleid += (**sp) - '0';
		(*sp)++;
	}

	return 0;
}

/* Parse a qdescr or a list of them enclosed in () */
static char **
parse_qdescrs(const char **sp, int *code)
{
	char ** res;
	char ** res1;
	tk_t kind;
	char * sval;
	int size;
	int pos;

	parse_whsp(sp);
	kind = get_token(sp,&sval);
	if ( kind == TK_LEFTPAREN ) {
		/* Let's presume there will be at least 2 entries */
		size = 3;
		res = LDAP_CALLOC(3,sizeof(char *));
		if ( !res ) {
			*code = LDAP_SCHERR_OUTOFMEM;
			return NULL;
		}
		pos = 0;
		while (1) {
			parse_whsp(sp);
			kind = get_token(sp,&sval);
			if ( kind == TK_RIGHTPAREN )
				break;
			if ( kind == TK_QDESCR ) {
				if ( pos == size-2 ) {
					size++;
					res1 = LDAP_REALLOC(res,size*sizeof(char *));
					if ( !res1 ) {
						LDAP_VFREE(res);
						LDAP_FREE(sval);
						*code = LDAP_SCHERR_OUTOFMEM;
						return(NULL);
					}
					res = res1;
				}
				res[pos++] = sval;
				res[pos] = NULL;
				parse_whsp(sp);
			} else {
				LDAP_VFREE(res);
				LDAP_FREE(sval);
				*code = LDAP_SCHERR_UNEXPTOKEN;
				return(NULL);
			}
		}
		parse_whsp(sp);
		return(res);
	} else if ( kind == TK_QDESCR ) {
		res = LDAP_CALLOC(2,sizeof(char *));
		if ( !res ) {
			*code = LDAP_SCHERR_OUTOFMEM;
			return NULL;
		}
		res[0] = sval;
		res[1] = NULL;
		parse_whsp(sp);
		return res;
	} else {
		LDAP_FREE(sval);
		*code = LDAP_SCHERR_BADNAME;
		return NULL;
	}
}

/* Parse a woid */
static char *
parse_woid(const char **sp, int *code)
{
	char * sval;
	tk_t kind;

	parse_whsp(sp);
	kind = get_token(sp, &sval);
	if ( kind != TK_BAREWORD ) {
		LDAP_FREE(sval);
		*code = LDAP_SCHERR_UNEXPTOKEN;
		return NULL;
	}
	parse_whsp(sp);
	return sval;
}

/* Parse a noidlen */
static char *
parse_noidlen(const char **sp, int *code, int *len, int flags)
{
	char * sval;
	const char *savepos;
	int quoted = 0;
	int allow_quoted = ( flags & LDAP_SCHEMA_ALLOW_QUOTED );
	int allow_oidmacro = ( flags & LDAP_SCHEMA_ALLOW_OID_MACRO );

	*len = 0;
	/* Netscape puts the SYNTAX value in quotes (incorrectly) */
	if ( allow_quoted && **sp == '\'' ) {
		quoted = 1;
		(*sp)++;
	}
	savepos = *sp;
	sval = ldap_int_parse_numericoid(sp, code, 0);
	if ( !sval ) {
		if ( allow_oidmacro
			&& *sp == savepos
			&& *code == LDAP_SCHERR_NODIGIT )
		{
			if ( get_token(sp, &sval) != TK_BAREWORD ) {
				if ( sval != NULL ) {
					LDAP_FREE(sval);
				}
				return NULL;
			}
		} else {
			return NULL;
		}
	}
	if ( **sp == '{' /*}*/ ) {
		(*sp)++;
		*len = atoi(*sp);
		while ( LDAP_DIGIT(**sp) )
			(*sp)++;
		if ( **sp != /*{*/ '}' ) {
			*code = LDAP_SCHERR_UNEXPTOKEN;
			LDAP_FREE(sval);
			return NULL;
		}
		(*sp)++;
	}		
	if ( allow_quoted && quoted ) {
		if ( **sp == '\'' ) {
			(*sp)++;
		} else {
			*code = LDAP_SCHERR_UNEXPTOKEN;
			LDAP_FREE(sval);
			return NULL;
		}
	}
	return sval;
}

/*
 * Next routine will accept a qdstring in place of an oid if
 * allow_quoted is set.  This is necessary to interoperate with
 * Netscape Directory server that will improperly quote each oid (at
 * least those of the descr kind) in the SUP clause.
 */

/* Parse a woid or a $-separated list of them enclosed in () */
static char **
parse_oids(const char **sp, int *code, const int allow_quoted)
{
	char ** res;
	char ** res1;
	tk_t kind;
	char * sval;
	int size;
	int pos;

	/*
	 * Strictly speaking, doing this here accepts whsp before the
	 * ( at the beginning of an oidlist, but this is harmless.  Also,
	 * we are very liberal in what we accept as an OID.  Maybe
	 * refine later.
	 */
	parse_whsp(sp);
	kind = get_token(sp,&sval);
	if ( kind == TK_LEFTPAREN ) {
		/* Let's presume there will be at least 2 entries */
		size = 3;
		res = LDAP_CALLOC(3,sizeof(char *));
		if ( !res ) {
			*code = LDAP_SCHERR_OUTOFMEM;
			return NULL;
		}
		pos = 0;
		parse_whsp(sp);
		kind = get_token(sp,&sval);
		if ( kind == TK_BAREWORD ||
		     ( allow_quoted && kind == TK_QDSTRING ) ) {
			res[pos++] = sval;
			res[pos] = NULL;
		} else if ( kind == TK_RIGHTPAREN ) {
			/* FIXME: be liberal in what we accept... */
			parse_whsp(sp);
			LDAP_FREE(res);
			return NULL;
		} else {
			*code = LDAP_SCHERR_UNEXPTOKEN;
			LDAP_FREE(sval);
			LDAP_VFREE(res);
			return NULL;
		}
		parse_whsp(sp);
		while (1) {
			kind = get_token(sp,&sval);
			if ( kind == TK_RIGHTPAREN )
				break;
			if ( kind == TK_DOLLAR ) {
				parse_whsp(sp);
				kind = get_token(sp,&sval);
				if ( kind == TK_BAREWORD ||
				     ( allow_quoted &&
				       kind == TK_QDSTRING ) ) {
					if ( pos == size-2 ) {
						size++;
						res1 = LDAP_REALLOC(res,size*sizeof(char *));
						if ( !res1 ) {
							LDAP_FREE(sval);
							LDAP_VFREE(res);
							*code = LDAP_SCHERR_OUTOFMEM;
							return(NULL);
						}
						res = res1;
					}
					res[pos++] = sval;
					res[pos] = NULL;
				} else {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					LDAP_FREE(sval);
					LDAP_VFREE(res);
					return NULL;
				}
				parse_whsp(sp);
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				LDAP_FREE(sval);
				LDAP_VFREE(res);
				return NULL;
			}
		}
		parse_whsp(sp);
		return(res);
	} else if ( kind == TK_BAREWORD ||
		    ( allow_quoted && kind == TK_QDSTRING ) ) {
		res = LDAP_CALLOC(2,sizeof(char *));
		if ( !res ) {
			LDAP_FREE(sval);
			*code = LDAP_SCHERR_OUTOFMEM;
			return NULL;
		}
		res[0] = sval;
		res[1] = NULL;
		parse_whsp(sp);
		return res;
	} else {
		LDAP_FREE(sval);
		*code = LDAP_SCHERR_BADNAME;
		return NULL;
	}
}

static int
add_extension(LDAPSchemaExtensionItem ***extensions,
	      char * name, char ** values)
{
	int n;
	LDAPSchemaExtensionItem **tmp, *ext;

	ext = LDAP_CALLOC(1, sizeof(LDAPSchemaExtensionItem));
	if ( !ext )
		return 1;
	ext->lsei_name = name;
	ext->lsei_values = values;

	if ( !*extensions ) {
		*extensions =
		  LDAP_CALLOC(2, sizeof(LDAPSchemaExtensionItem *));
		if ( !*extensions ) {
			LDAP_FREE( ext );
			return 1;
		}
		n = 0;
	} else {
		for ( n=0; (*extensions)[n] != NULL; n++ )
	  		;
		tmp = LDAP_REALLOC(*extensions,
				   (n+2)*sizeof(LDAPSchemaExtensionItem *));
		if ( !tmp ) {
			LDAP_FREE( ext );
			return 1;
		}
		*extensions = tmp;
	}
	(*extensions)[n] = ext;
	(*extensions)[n+1] = NULL;
	return 0;
}

static void
free_extensions(LDAPSchemaExtensionItem **extensions)
{
	LDAPSchemaExtensionItem **ext;

	if ( extensions ) {
		for ( ext = extensions; *ext != NULL; ext++ ) {
			LDAP_FREE((*ext)->lsei_name);
			LDAP_VFREE((*ext)->lsei_values);
			LDAP_FREE(*ext);
		}
		LDAP_FREE(extensions);
	}
}

void
ldap_syntax_free( LDAPSyntax * syn )
{
	if ( !syn ) return;
	LDAP_FREE(syn->syn_oid);
	if (syn->syn_names) LDAP_VFREE(syn->syn_names);
	if (syn->syn_desc) LDAP_FREE(syn->syn_desc);
	free_extensions(syn->syn_extensions);
	LDAP_FREE(syn);
}

LDAPSyntax *
ldap_str2syntax( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	LDAPSyntax * syn;
	char ** ext_vals;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	syn = LDAP_CALLOC(1,sizeof(LDAPSyntax));

	if ( !syn ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		LDAP_FREE(sval);
		*code = LDAP_SCHERR_NOLEFTPAREN;
		ldap_syntax_free(syn);
		return NULL;
	}

	parse_whsp(&ss);
	syn->syn_oid = ldap_int_parse_numericoid(&ss,code,0);
	if ( !syn->syn_oid ) {
		*errp = ss;
		ldap_syntax_free(syn);
		return NULL;
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal and accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_syntax_free(syn);
			return NULL;
		case TK_RIGHTPAREN:
			return syn;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_syntax_free(syn);
					return(NULL);
				}
				seen_name = 1;
				syn->syn_names = parse_qdescrs(&ss,code);
				if ( !syn->syn_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_syntax_free(syn);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_syntax_free(syn);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_syntax_free(syn);
					return NULL;
				}
				syn->syn_desc = sval;
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_syntax_free(syn);
					return NULL;
				}
				if ( add_extension(&syn->syn_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_syntax_free(syn);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_syntax_free(syn);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_syntax_free(syn);
			return NULL;
		}
	}
}

void
ldap_matchingrule_free( LDAPMatchingRule * mr )
{
	if (!mr) return;
	LDAP_FREE(mr->mr_oid);
	if (mr->mr_names) LDAP_VFREE(mr->mr_names);
	if (mr->mr_desc) LDAP_FREE(mr->mr_desc);
	if (mr->mr_syntax_oid) LDAP_FREE(mr->mr_syntax_oid);
	free_extensions(mr->mr_extensions);
	LDAP_FREE(mr);
}

LDAPMatchingRule *
ldap_str2matchingrule( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_syntax = 0;
	LDAPMatchingRule * mr;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	mr = LDAP_CALLOC(1,sizeof(LDAPMatchingRule));

	if ( !mr ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_matchingrule_free(mr);
		return NULL;
	}

	parse_whsp(&ss);
	savepos = ss;
	mr->mr_oid = ldap_int_parse_numericoid(&ss,code,flags);
	if ( !mr->mr_oid ) {
		if ( flags & LDAP_SCHEMA_ALLOW_NO_OID ) {
			/* Backtracking */
			ss = savepos;
			kind = get_token(&ss,&sval);
			if ( kind == TK_BAREWORD ) {
				if ( !strcasecmp(sval, "NAME") ||
				     !strcasecmp(sval, "DESC") ||
				     !strcasecmp(sval, "OBSOLETE") ||
				     !strcasecmp(sval, "SYNTAX") ||
				     !strncasecmp(sval, "X-", 2) ) {
					/* Missing OID, backtrack */
					ss = savepos;
				} else {
					/* Non-numerical OID, ignore */
				}
			}
			LDAP_FREE(sval);
		} else {
			*errp = ss;
			ldap_matchingrule_free(mr);
			return NULL;
		}
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal and accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_matchingrule_free(mr);
			return NULL;
		case TK_RIGHTPAREN:
			if( !seen_syntax ) {
				*code = LDAP_SCHERR_MISSING;
				ldap_matchingrule_free(mr);
				return NULL;
			}
			return mr;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingrule_free(mr);
					return(NULL);
				}
				seen_name = 1;
				mr->mr_names = parse_qdescrs(&ss,code);
				if ( !mr->mr_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_matchingrule_free(mr);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingrule_free(mr);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_matchingrule_free(mr);
					return NULL;
				}
				mr->mr_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingrule_free(mr);
					return(NULL);
				}
				seen_obsolete = 1;
				mr->mr_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"SYNTAX") ) {
				LDAP_FREE(sval);
				if ( seen_syntax ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingrule_free(mr);
					return(NULL);
				}
				seen_syntax = 1;
				parse_whsp(&ss);
				mr->mr_syntax_oid =
					ldap_int_parse_numericoid(&ss,code,flags);
				if ( !mr->mr_syntax_oid ) {
					*errp = ss;
					ldap_matchingrule_free(mr);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_matchingrule_free(mr);
					return NULL;
				}
				if ( add_extension(&mr->mr_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_matchingrule_free(mr);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_matchingrule_free(mr);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_matchingrule_free(mr);
			return NULL;
		}
	}
}

void
ldap_matchingruleuse_free( LDAPMatchingRuleUse * mru )
{
	if (!mru) return;
	LDAP_FREE(mru->mru_oid);
	if (mru->mru_names) LDAP_VFREE(mru->mru_names);
	if (mru->mru_desc) LDAP_FREE(mru->mru_desc);
	if (mru->mru_applies_oids) LDAP_VFREE(mru->mru_applies_oids);
	free_extensions(mru->mru_extensions);
	LDAP_FREE(mru);
}

LDAPMatchingRuleUse *
ldap_str2matchingruleuse( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_applies = 0;
	LDAPMatchingRuleUse * mru;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	mru = LDAP_CALLOC(1,sizeof(LDAPMatchingRuleUse));

	if ( !mru ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_matchingruleuse_free(mru);
		return NULL;
	}

	parse_whsp(&ss);
	savepos = ss;
	mru->mru_oid = ldap_int_parse_numericoid(&ss,code,flags);
	if ( !mru->mru_oid ) {
		if ( flags & LDAP_SCHEMA_ALLOW_NO_OID ) {
			/* Backtracking */
			ss = savepos;
			kind = get_token(&ss,&sval);
			if ( kind == TK_BAREWORD ) {
				if ( !strcasecmp(sval, "NAME") ||
				     !strcasecmp(sval, "DESC") ||
				     !strcasecmp(sval, "OBSOLETE") ||
				     !strcasecmp(sval, "APPLIES") ||
				     !strncasecmp(sval, "X-", 2) ) {
					/* Missing OID, backtrack */
					ss = savepos;
				} else {
					/* Non-numerical OID, ignore */
				}
			}
			LDAP_FREE(sval);
		} else {
			*errp = ss;
			ldap_matchingruleuse_free(mru);
			return NULL;
		}
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal and accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_matchingruleuse_free(mru);
			return NULL;
		case TK_RIGHTPAREN:
			if( !seen_applies ) {
				*code = LDAP_SCHERR_MISSING;
				ldap_matchingruleuse_free(mru);
				return NULL;
			}
			return mru;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return(NULL);
				}
				seen_name = 1;
				mru->mru_names = parse_qdescrs(&ss,code);
				if ( !mru->mru_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_matchingruleuse_free(mru);
					return NULL;
				}
				mru->mru_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return(NULL);
				}
				seen_obsolete = 1;
				mru->mru_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"APPLIES") ) {
				LDAP_FREE(sval);
				if ( seen_applies ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return(NULL);
				}
				seen_applies = 1;
				mru->mru_applies_oids = parse_oids(&ss,
							     code,
							     flags);
				if ( !mru->mru_applies_oids && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return NULL;
				}
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_matchingruleuse_free(mru);
					return NULL;
				}
				if ( add_extension(&mru->mru_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_matchingruleuse_free(mru);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_matchingruleuse_free(mru);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_matchingruleuse_free(mru);
			return NULL;
		}
	}
}

void
ldap_attributetype_free(LDAPAttributeType * at)
{
	if (!at) return;
	LDAP_FREE(at->at_oid);
	if (at->at_names) LDAP_VFREE(at->at_names);
	if (at->at_desc) LDAP_FREE(at->at_desc);
	if (at->at_sup_oid) LDAP_FREE(at->at_sup_oid);
	if (at->at_equality_oid) LDAP_FREE(at->at_equality_oid);
	if (at->at_ordering_oid) LDAP_FREE(at->at_ordering_oid);
	if (at->at_substr_oid) LDAP_FREE(at->at_substr_oid);
	if (at->at_syntax_oid) LDAP_FREE(at->at_syntax_oid);
	free_extensions(at->at_extensions);
	LDAP_FREE(at);
}

LDAPAttributeType *
ldap_str2attributetype( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_sup = 0;
	int seen_equality = 0;
	int seen_ordering = 0;
	int seen_substr = 0;
	int seen_syntax = 0;
	int seen_usage = 0;
	LDAPAttributeType * at;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	at = LDAP_CALLOC(1,sizeof(LDAPAttributeType));

	if ( !at ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_attributetype_free(at);
		return NULL;
	}

	/*
	 * Definitions MUST begin with an OID in the numericoid format.
	 * However, this routine is used by clients to parse the response
	 * from servers and very well known servers will provide an OID
	 * in the wrong format or even no OID at all.  We do our best to
	 * extract info from those servers.
	 */
	parse_whsp(&ss);
	savepos = ss;
	at->at_oid = ldap_int_parse_numericoid(&ss,code,0);
	if ( !at->at_oid ) {
		if ( ( flags & ( LDAP_SCHEMA_ALLOW_NO_OID
				| LDAP_SCHEMA_ALLOW_OID_MACRO ) )
			    && (ss == savepos) )
		{
			/* Backtracking */
			ss = savepos;
			kind = get_token(&ss,&sval);
			if ( kind == TK_BAREWORD ) {
				if ( !strcasecmp(sval, "NAME") ||
				     !strcasecmp(sval, "DESC") ||
				     !strcasecmp(sval, "OBSOLETE") ||
				     !strcasecmp(sval, "SUP") ||
				     !strcasecmp(sval, "EQUALITY") ||
				     !strcasecmp(sval, "ORDERING") ||
				     !strcasecmp(sval, "SUBSTR") ||
				     !strcasecmp(sval, "SYNTAX") ||
				     !strcasecmp(sval, "SINGLE-VALUE") ||
				     !strcasecmp(sval, "COLLECTIVE") ||
				     !strcasecmp(sval, "NO-USER-MODIFICATION") ||
				     !strcasecmp(sval, "USAGE") ||
				     !strncasecmp(sval, "X-", 2) )
				{
					/* Missing OID, backtrack */
					ss = savepos;
				} else if ( flags
					& LDAP_SCHEMA_ALLOW_OID_MACRO)
				{
					/* Non-numerical OID ... */
					int len = ss-savepos;
					at->at_oid = LDAP_MALLOC(len+1);
					if ( !at->at_oid ) {
						ldap_attributetype_free(at);
						return NULL;
					}

					strncpy(at->at_oid, savepos, len);
					at->at_oid[len] = 0;
				}
			}
			LDAP_FREE(sval);
		} else {
			*errp = ss;
			ldap_attributetype_free(at);
			return NULL;
		}
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal and accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_attributetype_free(at);
			return NULL;
		case TK_RIGHTPAREN:
			return at;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_name = 1;
				at->at_names = parse_qdescrs(&ss,code);
				if ( !at->at_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_attributetype_free(at);
					return NULL;
				}
				at->at_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_obsolete = 1;
				at->at_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"SUP") ) {
				LDAP_FREE(sval);
				if ( seen_sup ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_sup = 1;
				at->at_sup_oid = parse_woid(&ss,code);
				if ( !at->at_sup_oid ) {
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"EQUALITY") ) {
				LDAP_FREE(sval);
				if ( seen_equality ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_equality = 1;
				at->at_equality_oid = parse_woid(&ss,code);
				if ( !at->at_equality_oid ) {
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"ORDERING") ) {
				LDAP_FREE(sval);
				if ( seen_ordering ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_ordering = 1;
				at->at_ordering_oid = parse_woid(&ss,code);
				if ( !at->at_ordering_oid ) {
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"SUBSTR") ) {
				LDAP_FREE(sval);
				if ( seen_substr ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_substr = 1;
				at->at_substr_oid = parse_woid(&ss,code);
				if ( !at->at_substr_oid ) {
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"SYNTAX") ) {
				LDAP_FREE(sval);
				if ( seen_syntax ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_syntax = 1;
				parse_whsp(&ss);
				savepos = ss;
				at->at_syntax_oid =
					parse_noidlen(&ss,
						      code,
						      &at->at_syntax_len,
						      flags);
				if ( !at->at_syntax_oid ) {
				    if ( flags & LDAP_SCHEMA_ALLOW_OID_MACRO ) {
					kind = get_token(&ss,&sval);
					if (kind == TK_BAREWORD)
					{
					    char *sp = strchr(sval, '{');
					    at->at_syntax_oid = sval;
					    if (sp)
					    {
						*sp++ = 0;
					    	at->at_syntax_len = atoi(sp);
						while ( LDAP_DIGIT(*sp) )
							sp++;
						if ( *sp != '}' ) {
						    *code = LDAP_SCHERR_UNEXPTOKEN;
						    *errp = ss;
						    ldap_attributetype_free(at);
						    return NULL;
						}
					    }
					}
				    } else {
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				    }
				}
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"SINGLE-VALUE") ) {
				LDAP_FREE(sval);
				if ( at->at_single_value ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				at->at_single_value = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"COLLECTIVE") ) {
				LDAP_FREE(sval);
				if ( at->at_collective ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				at->at_collective = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"NO-USER-MODIFICATION") ) {
				LDAP_FREE(sval);
				if ( at->at_no_user_mod ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				at->at_no_user_mod = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"USAGE") ) {
				LDAP_FREE(sval);
				if ( seen_usage ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_attributetype_free(at);
					return(NULL);
				}
				seen_usage = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_BAREWORD ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_attributetype_free(at);
					return NULL;
				}
				if ( !strcasecmp(sval,"userApplications") )
					at->at_usage =
					    LDAP_SCHEMA_USER_APPLICATIONS;
				else if ( !strcasecmp(sval,"directoryOperation") )
					at->at_usage =
					    LDAP_SCHEMA_DIRECTORY_OPERATION;
				else if ( !strcasecmp(sval,"distributedOperation") )
					at->at_usage =
					    LDAP_SCHEMA_DISTRIBUTED_OPERATION;
				else if ( !strcasecmp(sval,"dSAOperation") )
					at->at_usage =
					    LDAP_SCHEMA_DSA_OPERATION;
				else {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_attributetype_free(at);
					return NULL;
				}
				LDAP_FREE(sval);
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_attributetype_free(at);
					return NULL;
				}
				if ( add_extension(&at->at_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_attributetype_free(at);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_attributetype_free(at);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_attributetype_free(at);
			return NULL;
		}
	}
}

void
ldap_objectclass_free(LDAPObjectClass * oc)
{
	if (!oc) return;
	LDAP_FREE(oc->oc_oid);
	if (oc->oc_names) LDAP_VFREE(oc->oc_names);
	if (oc->oc_desc) LDAP_FREE(oc->oc_desc);
	if (oc->oc_sup_oids) LDAP_VFREE(oc->oc_sup_oids);
	if (oc->oc_at_oids_must) LDAP_VFREE(oc->oc_at_oids_must);
	if (oc->oc_at_oids_may) LDAP_VFREE(oc->oc_at_oids_may);
	free_extensions(oc->oc_extensions);
	LDAP_FREE(oc);
}

LDAPObjectClass *
ldap_str2objectclass( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_sup = 0;
	int seen_kind = 0;
	int seen_must = 0;
	int seen_may = 0;
	LDAPObjectClass * oc;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	oc = LDAP_CALLOC(1,sizeof(LDAPObjectClass));

	if ( !oc ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}
	oc->oc_kind = LDAP_SCHEMA_STRUCTURAL;

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_objectclass_free(oc);
		return NULL;
	}

	/*
	 * Definitions MUST begin with an OID in the numericoid format.
	 * However, this routine is used by clients to parse the response
	 * from servers and very well known servers will provide an OID
	 * in the wrong format or even no OID at all.  We do our best to
	 * extract info from those servers.
	 */
	parse_whsp(&ss);
	savepos = ss;
	oc->oc_oid = ldap_int_parse_numericoid(&ss,code,0);
	if ( !oc->oc_oid ) {
		if ( (flags & LDAP_SCHEMA_ALLOW_ALL) && (ss == savepos) ) {
			/* Backtracking */
			ss = savepos;
			kind = get_token(&ss,&sval);
			if ( kind == TK_BAREWORD ) {
				if ( !strcasecmp(sval, "NAME") ||
				     !strcasecmp(sval, "DESC") ||
				     !strcasecmp(sval, "OBSOLETE") ||
				     !strcasecmp(sval, "SUP") ||
				     !strcasecmp(sval, "ABSTRACT") ||
				     !strcasecmp(sval, "STRUCTURAL") ||
				     !strcasecmp(sval, "AUXILIARY") ||
				     !strcasecmp(sval, "MUST") ||
				     !strcasecmp(sval, "MAY") ||
				     !strncasecmp(sval, "X-", 2) ) {
					/* Missing OID, backtrack */
					ss = savepos;
				} else if ( flags &
					LDAP_SCHEMA_ALLOW_OID_MACRO ) {
					/* Non-numerical OID, ignore */
					int len = ss-savepos;
					oc->oc_oid = LDAP_MALLOC(len+1);
					if ( !oc->oc_oid ) {
						ldap_objectclass_free(oc);
						return NULL;
					}

					strncpy(oc->oc_oid, savepos, len);
					oc->oc_oid[len] = 0;
				}
			}
			LDAP_FREE(sval);
			*code = 0;
		} else {
			*errp = ss;
			ldap_objectclass_free(oc);
			return NULL;
		}
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal an accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_objectclass_free(oc);
			return NULL;
		case TK_RIGHTPAREN:
			return oc;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_name = 1;
				oc->oc_names = parse_qdescrs(&ss,code);
				if ( !oc->oc_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_objectclass_free(oc);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_objectclass_free(oc);
					return NULL;
				}
				oc->oc_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_obsolete = 1;
				oc->oc_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"SUP") ) {
				LDAP_FREE(sval);
				if ( seen_sup ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_sup = 1;
				oc->oc_sup_oids = parse_oids(&ss,
							     code,
							     flags);
				if ( !oc->oc_sup_oids && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_objectclass_free(oc);
					return NULL;
				}
				*code = 0;
			} else if ( !strcasecmp(sval,"ABSTRACT") ) {
				LDAP_FREE(sval);
				if ( seen_kind ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_kind = 1;
				oc->oc_kind = LDAP_SCHEMA_ABSTRACT;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"STRUCTURAL") ) {
				LDAP_FREE(sval);
				if ( seen_kind ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_kind = 1;
				oc->oc_kind = LDAP_SCHEMA_STRUCTURAL;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"AUXILIARY") ) {
				LDAP_FREE(sval);
				if ( seen_kind ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_kind = 1;
				oc->oc_kind = LDAP_SCHEMA_AUXILIARY;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"MUST") ) {
				LDAP_FREE(sval);
				if ( seen_must ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_must = 1;
				oc->oc_at_oids_must = parse_oids(&ss,code,0);
				if ( !oc->oc_at_oids_must && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_objectclass_free(oc);
					return NULL;
				}
				*code = 0;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"MAY") ) {
				LDAP_FREE(sval);
				if ( seen_may ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_objectclass_free(oc);
					return(NULL);
				}
				seen_may = 1;
				oc->oc_at_oids_may = parse_oids(&ss,code,0);
				if ( !oc->oc_at_oids_may && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_objectclass_free(oc);
					return NULL;
				}
				*code = 0;
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				*code = 0;
				if ( !ext_vals ) {
					*errp = ss;
					ldap_objectclass_free(oc);
					return NULL;
				}
				if ( add_extension(&oc->oc_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_objectclass_free(oc);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_objectclass_free(oc);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_objectclass_free(oc);
			return NULL;
		}
	}
}

void
ldap_contentrule_free(LDAPContentRule * cr)
{
	if (!cr) return;
	LDAP_FREE(cr->cr_oid);
	if (cr->cr_names) LDAP_VFREE(cr->cr_names);
	if (cr->cr_desc) LDAP_FREE(cr->cr_desc);
	if (cr->cr_oc_oids_aux) LDAP_VFREE(cr->cr_oc_oids_aux);
	if (cr->cr_at_oids_must) LDAP_VFREE(cr->cr_at_oids_must);
	if (cr->cr_at_oids_may) LDAP_VFREE(cr->cr_at_oids_may);
	if (cr->cr_at_oids_not) LDAP_VFREE(cr->cr_at_oids_not);
	free_extensions(cr->cr_extensions);
	LDAP_FREE(cr);
}

LDAPContentRule *
ldap_str2contentrule( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_aux = 0;
	int seen_must = 0;
	int seen_may = 0;
	int seen_not = 0;
	LDAPContentRule * cr;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	cr = LDAP_CALLOC(1,sizeof(LDAPContentRule));

	if ( !cr ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_contentrule_free(cr);
		return NULL;
	}

	/*
	 * Definitions MUST begin with an OID in the numericoid format.
	 */
	parse_whsp(&ss);
	savepos = ss;
	cr->cr_oid = ldap_int_parse_numericoid(&ss,code,0);
	if ( !cr->cr_oid ) {
		if ( (flags & LDAP_SCHEMA_ALLOW_ALL) && (ss == savepos) ) {
			/* Backtracking */
			ss = savepos;
			kind = get_token(&ss,&sval);
			if ( kind == TK_BAREWORD ) {
				if ( !strcasecmp(sval, "NAME") ||
				     !strcasecmp(sval, "DESC") ||
				     !strcasecmp(sval, "OBSOLETE") ||
				     !strcasecmp(sval, "AUX") ||
				     !strcasecmp(sval, "MUST") ||
				     !strcasecmp(sval, "MAY") ||
				     !strcasecmp(sval, "NOT") ||
				     !strncasecmp(sval, "X-", 2) ) {
					/* Missing OID, backtrack */
					ss = savepos;
				} else if ( flags &
					LDAP_SCHEMA_ALLOW_OID_MACRO ) {
					/* Non-numerical OID, ignore */
					int len = ss-savepos;
					cr->cr_oid = LDAP_MALLOC(len+1);
					if ( !cr->cr_oid ) {
						ldap_contentrule_free(cr);
						return NULL;
					}

					strncpy(cr->cr_oid, savepos, len);
					cr->cr_oid[len] = 0;
				}
			}
			LDAP_FREE(sval);
		} else {
			*errp = ss;
			ldap_contentrule_free(cr);
			return NULL;
		}
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal an accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_contentrule_free(cr);
			return NULL;
		case TK_RIGHTPAREN:
			return cr;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_name = 1;
				cr->cr_names = parse_qdescrs(&ss,code);
				if ( !cr->cr_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_contentrule_free(cr);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_contentrule_free(cr);
					return NULL;
				}
				cr->cr_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_obsolete = 1;
				cr->cr_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"AUX") ) {
				LDAP_FREE(sval);
				if ( seen_aux ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_aux = 1;
				cr->cr_oc_oids_aux = parse_oids(&ss,code,0);
				if ( !cr->cr_oc_oids_aux ) {
					*errp = ss;
					ldap_contentrule_free(cr);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"MUST") ) {
				LDAP_FREE(sval);
				if ( seen_must ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_must = 1;
				cr->cr_at_oids_must = parse_oids(&ss,code,0);
				if ( !cr->cr_at_oids_must && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_contentrule_free(cr);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"MAY") ) {
				LDAP_FREE(sval);
				if ( seen_may ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_may = 1;
				cr->cr_at_oids_may = parse_oids(&ss,code,0);
				if ( !cr->cr_at_oids_may && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_contentrule_free(cr);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"NOT") ) {
				LDAP_FREE(sval);
				if ( seen_not ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_contentrule_free(cr);
					return(NULL);
				}
				seen_not = 1;
				cr->cr_at_oids_not = parse_oids(&ss,code,0);
				if ( !cr->cr_at_oids_not && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_contentrule_free(cr);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_contentrule_free(cr);
					return NULL;
				}
				if ( add_extension(&cr->cr_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_contentrule_free(cr);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_contentrule_free(cr);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_contentrule_free(cr);
			return NULL;
		}
	}
}

void
ldap_structurerule_free(LDAPStructureRule * sr)
{
	if (!sr) return;
	if (sr->sr_names) LDAP_VFREE(sr->sr_names);
	if (sr->sr_desc) LDAP_FREE(sr->sr_desc);
	if (sr->sr_nameform) LDAP_FREE(sr->sr_nameform);
	if (sr->sr_sup_ruleids) LDAP_FREE(sr->sr_sup_ruleids);
	free_extensions(sr->sr_extensions);
	LDAP_FREE(sr);
}

LDAPStructureRule *
ldap_str2structurerule( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	int ret;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_nameform = 0;
	LDAPStructureRule * sr;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	sr = LDAP_CALLOC(1,sizeof(LDAPStructureRule));

	if ( !sr ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_structurerule_free(sr);
		return NULL;
	}

	/*
	 * Definitions MUST begin with a ruleid.
	 */
	parse_whsp(&ss);
	savepos = ss;
	ret = ldap_int_parse_ruleid(&ss,code,0,&sr->sr_ruleid);
	if ( ret ) {
		*errp = ss;
		ldap_structurerule_free(sr);
		return NULL;
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal an accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_structurerule_free(sr);
			return NULL;
		case TK_RIGHTPAREN:
			if( !seen_nameform ) {
				*code = LDAP_SCHERR_MISSING;
				ldap_structurerule_free(sr);
				return NULL;
			}
			return sr;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_structurerule_free(sr);
					return(NULL);
				}
				seen_name = 1;
				sr->sr_names = parse_qdescrs(&ss,code);
				if ( !sr->sr_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_structurerule_free(sr);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_structurerule_free(sr);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_structurerule_free(sr);
					return NULL;
				}
				sr->sr_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_structurerule_free(sr);
					return(NULL);
				}
				seen_obsolete = 1;
				sr->sr_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"FORM") ) {
				LDAP_FREE(sval);
				if ( seen_nameform ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_structurerule_free(sr);
					return(NULL);
				}
				seen_nameform = 1;
				sr->sr_nameform = parse_woid(&ss,code);
				if ( !sr->sr_nameform ) {
					*errp = ss;
					ldap_structurerule_free(sr);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_structurerule_free(sr);
					return NULL;
				}
				if ( add_extension(&sr->sr_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_structurerule_free(sr);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_structurerule_free(sr);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_structurerule_free(sr);
			return NULL;
		}
	}
}

void
ldap_nameform_free(LDAPNameForm * nf)
{
	if (!nf) return;
	LDAP_FREE(nf->nf_oid);
	if (nf->nf_names) LDAP_VFREE(nf->nf_names);
	if (nf->nf_desc) LDAP_FREE(nf->nf_desc);
	if (nf->nf_objectclass) LDAP_FREE(nf->nf_objectclass);
	if (nf->nf_at_oids_must) LDAP_VFREE(nf->nf_at_oids_must);
	if (nf->nf_at_oids_may) LDAP_VFREE(nf->nf_at_oids_may);
	free_extensions(nf->nf_extensions);
	LDAP_FREE(nf);
}

LDAPNameForm *
ldap_str2nameform( LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags )
{
	tk_t kind;
	const char * ss = s;
	char * sval;
	int seen_name = 0;
	int seen_desc = 0;
	int seen_obsolete = 0;
	int seen_class = 0;
	int seen_must = 0;
	int seen_may = 0;
	LDAPNameForm * nf;
	char ** ext_vals;
	const char * savepos;

	if ( !s ) {
		*code = LDAP_SCHERR_EMPTY;
		*errp = "";
		return NULL;
	}

	*errp = s;
	nf = LDAP_CALLOC(1,sizeof(LDAPNameForm));

	if ( !nf ) {
		*code = LDAP_SCHERR_OUTOFMEM;
		return NULL;
	}

	kind = get_token(&ss,&sval);
	if ( kind != TK_LEFTPAREN ) {
		*code = LDAP_SCHERR_NOLEFTPAREN;
		LDAP_FREE(sval);
		ldap_nameform_free(nf);
		return NULL;
	}

	/*
	 * Definitions MUST begin with an OID in the numericoid format.
	 * However, this routine is used by clients to parse the response
	 * from servers and very well known servers will provide an OID
	 * in the wrong format or even no OID at all.  We do our best to
	 * extract info from those servers.
	 */
	parse_whsp(&ss);
	savepos = ss;
	nf->nf_oid = ldap_int_parse_numericoid(&ss,code,0);
	if ( !nf->nf_oid ) {
		*errp = ss;
		ldap_nameform_free(nf);
		return NULL;
	}
	parse_whsp(&ss);

	/*
	 * Beyond this point we will be liberal an accept the items
	 * in any order.
	 */
	while (1) {
		kind = get_token(&ss,&sval);
		switch (kind) {
		case TK_EOS:
			*code = LDAP_SCHERR_NORIGHTPAREN;
			*errp = EndOfInput;
			ldap_nameform_free(nf);
			return NULL;
		case TK_RIGHTPAREN:
			if( !seen_class || !seen_must ) {
				*code = LDAP_SCHERR_MISSING;
				ldap_nameform_free(nf);
				return NULL;
			}
			return nf;
		case TK_BAREWORD:
			if ( !strcasecmp(sval,"NAME") ) {
				LDAP_FREE(sval);
				if ( seen_name ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_nameform_free(nf);
					return(NULL);
				}
				seen_name = 1;
				nf->nf_names = parse_qdescrs(&ss,code);
				if ( !nf->nf_names ) {
					if ( *code != LDAP_SCHERR_OUTOFMEM )
						*code = LDAP_SCHERR_BADNAME;
					*errp = ss;
					ldap_nameform_free(nf);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"DESC") ) {
				LDAP_FREE(sval);
				if ( seen_desc ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_nameform_free(nf);
					return(NULL);
				}
				seen_desc = 1;
				parse_whsp(&ss);
				kind = get_token(&ss,&sval);
				if ( kind != TK_QDSTRING ) {
					*code = LDAP_SCHERR_UNEXPTOKEN;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_nameform_free(nf);
					return NULL;
				}
				nf->nf_desc = sval;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OBSOLETE") ) {
				LDAP_FREE(sval);
				if ( seen_obsolete ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_nameform_free(nf);
					return(NULL);
				}
				seen_obsolete = 1;
				nf->nf_obsolete = LDAP_SCHEMA_YES;
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"OC") ) {
				LDAP_FREE(sval);
				if ( seen_class ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_nameform_free(nf);
					return(NULL);
				}
				seen_class = 1;
				nf->nf_objectclass = parse_woid(&ss,code);
				if ( !nf->nf_objectclass ) {
					*errp = ss;
					ldap_nameform_free(nf);
					return NULL;
				}
			} else if ( !strcasecmp(sval,"MUST") ) {
				LDAP_FREE(sval);
				if ( seen_must ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_nameform_free(nf);
					return(NULL);
				}
				seen_must = 1;
				nf->nf_at_oids_must = parse_oids(&ss,code,0);
				if ( !nf->nf_at_oids_must && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_nameform_free(nf);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( !strcasecmp(sval,"MAY") ) {
				LDAP_FREE(sval);
				if ( seen_may ) {
					*code = LDAP_SCHERR_DUPOPT;
					*errp = ss;
					ldap_nameform_free(nf);
					return(NULL);
				}
				seen_may = 1;
				nf->nf_at_oids_may = parse_oids(&ss,code,0);
				if ( !nf->nf_at_oids_may && *code != LDAP_SUCCESS ) {
					*errp = ss;
					ldap_nameform_free(nf);
					return NULL;
				}
				parse_whsp(&ss);
			} else if ( sval[0] == 'X' && sval[1] == '-' ) {
				/* Should be parse_qdstrings */
				ext_vals = parse_qdescrs(&ss, code);
				if ( !ext_vals ) {
					*errp = ss;
					ldap_nameform_free(nf);
					return NULL;
				}
				if ( add_extension(&nf->nf_extensions,
						    sval, ext_vals) ) {
					*code = LDAP_SCHERR_OUTOFMEM;
					*errp = ss;
					LDAP_FREE(sval);
					ldap_nameform_free(nf);
					return NULL;
				}
			} else {
				*code = LDAP_SCHERR_UNEXPTOKEN;
				*errp = ss;
				LDAP_FREE(sval);
				ldap_nameform_free(nf);
				return NULL;
			}
			break;
		default:
			*code = LDAP_SCHERR_UNEXPTOKEN;
			*errp = ss;
			LDAP_FREE(sval);
			ldap_nameform_free(nf);
			return NULL;
		}
	}
}

static char *const err2text[] = {
	N_("Success"),
	N_("Out of memory"),
	N_("Unexpected token"),
	N_("Missing opening parenthesis"),
	N_("Missing closing parenthesis"),
	N_("Expecting digit"),
	N_("Expecting a name"),
	N_("Bad description"),
	N_("Bad superiors"),
	N_("Duplicate option"),
	N_("Unexpected end of data"),
	N_("Missing required field"),
	N_("Out of order field")
};

char *
ldap_scherr2str(int code)
{
	if ( code < 0 || code >= (int)(sizeof(err2text)/sizeof(char *)) ) {
		return _("Unknown error");
	} else {
		return _(err2text[code]);
	}
}

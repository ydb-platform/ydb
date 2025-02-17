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

/* ldap-schema.h - Header for basic schema handling functions that can be
 *		used by both clients and servers.
 * these routines should be renamed ldap_x_...
 */

#ifndef _LDAP_SCHEMA_H
#define _LDAP_SCHEMA_H 1

#include <ldap_cdefs.h>

LDAP_BEGIN_DECL

/* Codes for parsing errors */

#define LDAP_SCHERR_OUTOFMEM		1
#define LDAP_SCHERR_UNEXPTOKEN		2
#define LDAP_SCHERR_NOLEFTPAREN		3
#define LDAP_SCHERR_NORIGHTPAREN	4
#define LDAP_SCHERR_NODIGIT			5
#define LDAP_SCHERR_BADNAME			6
#define LDAP_SCHERR_BADDESC			7
#define LDAP_SCHERR_BADSUP			8
#define LDAP_SCHERR_DUPOPT			9
#define LDAP_SCHERR_EMPTY			10
#define LDAP_SCHERR_MISSING			11
#define LDAP_SCHERR_OUT_OF_ORDER	12

typedef struct ldap_schema_extension_item {
	char *lsei_name;
	char **lsei_values;
} LDAPSchemaExtensionItem;

typedef struct ldap_syntax {
	char *syn_oid;		/* REQUIRED */
	char **syn_names;	/* OPTIONAL */
	char *syn_desc;		/* OPTIONAL */
	LDAPSchemaExtensionItem **syn_extensions; /* OPTIONAL */
} LDAPSyntax;

typedef struct ldap_matchingrule {
	char *mr_oid;		/* REQUIRED */
	char **mr_names;	/* OPTIONAL */
	char *mr_desc;		/* OPTIONAL */
	int  mr_obsolete;	/* OPTIONAL */
	char *mr_syntax_oid;	/* REQUIRED */
	LDAPSchemaExtensionItem **mr_extensions; /* OPTIONAL */
} LDAPMatchingRule;

typedef struct ldap_matchingruleuse {
	char *mru_oid;		/* REQUIRED */
	char **mru_names;	/* OPTIONAL */
	char *mru_desc;		/* OPTIONAL */
	int  mru_obsolete;	/* OPTIONAL */
	char **mru_applies_oids;	/* REQUIRED */
	LDAPSchemaExtensionItem **mru_extensions; /* OPTIONAL */
} LDAPMatchingRuleUse;

typedef struct ldap_attributetype {
	char *at_oid;		/* REQUIRED */
	char **at_names;	/* OPTIONAL */
	char *at_desc;		/* OPTIONAL */
	int  at_obsolete;	/* 0=no, 1=yes */
	char *at_sup_oid;	/* OPTIONAL */
	char *at_equality_oid;	/* OPTIONAL */
	char *at_ordering_oid;	/* OPTIONAL */
	char *at_substr_oid;	/* OPTIONAL */
	char *at_syntax_oid;	/* OPTIONAL */
	int  at_syntax_len;	/* OPTIONAL */
	int  at_single_value;	/* 0=no, 1=yes */
	int  at_collective;	/* 0=no, 1=yes */
	int  at_no_user_mod;	/* 0=no, 1=yes */
	int  at_usage;		/* 0=userApplications, 1=directoryOperation,
				   2=distributedOperation, 3=dSAOperation */
	LDAPSchemaExtensionItem **at_extensions; /* OPTIONAL */
} LDAPAttributeType;

typedef struct ldap_objectclass {
	char *oc_oid;		/* REQUIRED */
	char **oc_names;	/* OPTIONAL */
	char *oc_desc;		/* OPTIONAL */
	int  oc_obsolete;	/* 0=no, 1=yes */
	char **oc_sup_oids;	/* OPTIONAL */
	int  oc_kind;		/* 0=ABSTRACT, 1=STRUCTURAL, 2=AUXILIARY */
	char **oc_at_oids_must;	/* OPTIONAL */
	char **oc_at_oids_may;	/* OPTIONAL */
	LDAPSchemaExtensionItem **oc_extensions; /* OPTIONAL */
} LDAPObjectClass;

typedef struct ldap_contentrule {
	char *cr_oid;		/* REQUIRED */
	char **cr_names;	/* OPTIONAL */
	char *cr_desc;		/* OPTIONAL */
	char **cr_sup_oids;	/* OPTIONAL */
	int  cr_obsolete;	/* 0=no, 1=yes */
	char **cr_oc_oids_aux;	/* OPTIONAL */
	char **cr_at_oids_must;	/* OPTIONAL */
	char **cr_at_oids_may;	/* OPTIONAL */
	char **cr_at_oids_not;	/* OPTIONAL */
	LDAPSchemaExtensionItem **cr_extensions; /* OPTIONAL */
} LDAPContentRule;

typedef struct ldap_nameform {
	char *nf_oid;		/* REQUIRED */
	char **nf_names;	/* OPTIONAL */
	char *nf_desc;		/* OPTIONAL */
	int  nf_obsolete;	/* 0=no, 1=yes */
	char *nf_objectclass;	/* REQUIRED */
	char **nf_at_oids_must;	/* REQUIRED */
	char **nf_at_oids_may;	/* OPTIONAL */
	LDAPSchemaExtensionItem **nf_extensions; /* OPTIONAL */
} LDAPNameForm;

typedef struct ldap_structurerule {
	int sr_ruleid;		/* REQUIRED */
	char **sr_names;	/* OPTIONAL */
	char *sr_desc;		/* OPTIONAL */
	int  sr_obsolete;	/* 0=no, 1=yes */
	char *sr_nameform;	/* REQUIRED */
	int sr_nsup_ruleids;/* number of sr_sup_ruleids */
	int *sr_sup_ruleids;/* OPTIONAL */
	LDAPSchemaExtensionItem **sr_extensions; /* OPTIONAL */
} LDAPStructureRule;

/*
 * Misc macros
 */
#define LDAP_SCHEMA_NO				0
#define LDAP_SCHEMA_YES				1

#define LDAP_SCHEMA_USER_APPLICATIONS		0
#define LDAP_SCHEMA_DIRECTORY_OPERATION		1
#define LDAP_SCHEMA_DISTRIBUTED_OPERATION	2
#define LDAP_SCHEMA_DSA_OPERATION		3

#define LDAP_SCHEMA_ABSTRACT			0
#define LDAP_SCHEMA_STRUCTURAL			1
#define LDAP_SCHEMA_AUXILIARY			2


/*
 * Flags that control how liberal the parsing routines are.
 */
#define LDAP_SCHEMA_ALLOW_NONE		0x00U /* Strict parsing               */
#define LDAP_SCHEMA_ALLOW_NO_OID	0x01U /* Allow missing oid            */
#define LDAP_SCHEMA_ALLOW_QUOTED	0x02U /* Allow bogus extra quotes     */
#define LDAP_SCHEMA_ALLOW_DESCR		0x04U /* Allow descr instead of OID   */
#define LDAP_SCHEMA_ALLOW_DESCR_PREFIX	0x08U /* Allow descr as OID prefix    */
#define LDAP_SCHEMA_ALLOW_OID_MACRO	0x10U /* Allow OID macros in slapd    */
#define LDAP_SCHEMA_ALLOW_OUT_OF_ORDER_FIELDS 0x20U /* Allow fields in most any order */
#define LDAP_SCHEMA_ALLOW_ALL		0x3fU /* Be very liberal in parsing   */
#define	LDAP_SCHEMA_SKIP			0x80U /* Don't malloc any result      */


LDAP_F( LDAP_CONST char * )
ldap_syntax2name LDAP_P((
	LDAPSyntax * syn ));

LDAP_F( LDAP_CONST char * )
ldap_matchingrule2name LDAP_P((
	LDAPMatchingRule * mr ));

LDAP_F( LDAP_CONST char * )
ldap_matchingruleuse2name LDAP_P((
	LDAPMatchingRuleUse * mru ));

LDAP_F( LDAP_CONST char * )
ldap_attributetype2name LDAP_P((
	LDAPAttributeType * at ));

LDAP_F( LDAP_CONST char * )
ldap_objectclass2name LDAP_P((
	LDAPObjectClass * oc ));

LDAP_F( LDAP_CONST char * )
ldap_contentrule2name LDAP_P((
	LDAPContentRule * cr ));

LDAP_F( LDAP_CONST char * )
ldap_nameform2name LDAP_P((
	LDAPNameForm * nf ));

LDAP_F( LDAP_CONST char * )
ldap_structurerule2name LDAP_P((
	LDAPStructureRule * sr ));

LDAP_F( void )
ldap_syntax_free LDAP_P((
	LDAPSyntax * syn ));

LDAP_F( void )
ldap_matchingrule_free LDAP_P((
	LDAPMatchingRule * mr ));

LDAP_F( void )
ldap_matchingruleuse_free LDAP_P((
	LDAPMatchingRuleUse * mr ));

LDAP_F( void )
ldap_attributetype_free LDAP_P((
	LDAPAttributeType * at ));

LDAP_F( void )
ldap_objectclass_free LDAP_P((
	LDAPObjectClass * oc ));

LDAP_F( void )
ldap_contentrule_free LDAP_P((
	LDAPContentRule * cr ));

LDAP_F( void )
ldap_nameform_free LDAP_P((
	LDAPNameForm * nf ));

LDAP_F( void )
ldap_structurerule_free LDAP_P((
	LDAPStructureRule * sr ));

LDAP_F( LDAPStructureRule * )
ldap_str2structurerule LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPNameForm * )
ldap_str2nameform LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPContentRule * )
ldap_str2contentrule LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPObjectClass * )
ldap_str2objectclass LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPAttributeType * )
ldap_str2attributetype LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPSyntax * )
ldap_str2syntax LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPMatchingRule * )
ldap_str2matchingrule LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( LDAPMatchingRuleUse * )
ldap_str2matchingruleuse LDAP_P((
	LDAP_CONST char * s,
	int * code,
	LDAP_CONST char ** errp,
	LDAP_CONST unsigned flags ));

LDAP_F( char * )
ldap_structurerule2str LDAP_P((
	LDAPStructureRule * sr ));

LDAP_F( struct berval * )
ldap_structurerule2bv LDAP_P((
	LDAPStructureRule * sr, struct berval *bv ));

LDAP_F( char * )
ldap_nameform2str LDAP_P((
	LDAPNameForm * nf ));

LDAP_F( struct berval * )
ldap_nameform2bv LDAP_P((
	LDAPNameForm * nf, struct berval *bv ));

LDAP_F( char * )
ldap_contentrule2str LDAP_P((
	LDAPContentRule * cr ));

LDAP_F( struct berval * )
ldap_contentrule2bv LDAP_P((
	LDAPContentRule * cr, struct berval *bv ));

LDAP_F( char * )
ldap_objectclass2str LDAP_P((
	LDAPObjectClass * oc ));

LDAP_F( struct berval * )
ldap_objectclass2bv LDAP_P((
	LDAPObjectClass * oc, struct berval *bv ));

LDAP_F( char * )
ldap_attributetype2str LDAP_P((
	LDAPAttributeType * at ));

LDAP_F( struct berval * )
ldap_attributetype2bv LDAP_P((
	LDAPAttributeType * at, struct berval *bv ));

LDAP_F( char * )
ldap_syntax2str LDAP_P((
	LDAPSyntax * syn ));

LDAP_F( struct berval * )
ldap_syntax2bv LDAP_P((
	LDAPSyntax * syn, struct berval *bv ));

LDAP_F( char * )
ldap_matchingrule2str LDAP_P((
	LDAPMatchingRule * mr ));

LDAP_F( struct berval * )
ldap_matchingrule2bv LDAP_P((
	LDAPMatchingRule * mr, struct berval *bv ));

LDAP_F( char * )
ldap_matchingruleuse2str LDAP_P((
	LDAPMatchingRuleUse * mru ));

LDAP_F( struct berval * )
ldap_matchingruleuse2bv LDAP_P((
	LDAPMatchingRuleUse * mru, struct berval *bv ));

LDAP_F( char * )
ldap_scherr2str LDAP_P((
	int code )) LDAP_GCCATTR((const));

LDAP_END_DECL

#endif


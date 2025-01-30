/* ldap_avl.h - avl tree definitions */
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
/* Portions Copyright (c) 1993 Regents of the University of Michigan.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that this notice is preserved and that due credit is given
 * to the University of Michigan at Ann Arbor. The name of the University
 * may not be used to endorse or promote products derived from this
 * software without specific prior written permission. This software
 * is provided ``as is'' without express or implied warranty.
 */


#ifndef _AVL
#define _AVL

#include <ldap_cdefs.h>

/*
 * this structure represents a generic avl tree node.
 */

LDAP_BEGIN_DECL

typedef struct avlnode Avlnode;

struct avlnode {
	void*		avl_data;
	struct avlnode	*avl_link[2];
	char		avl_bits[2];
	signed char		avl_bf;
};

#define avl_left	avl_link[0]
#define avl_right	avl_link[1]
#define avl_lbit	avl_bits[0]
#define avl_rbit	avl_bits[1]

typedef struct tavlnode TAvlnode;

struct tavlnode {
	void*		avl_data;
	struct tavlnode	*avl_link[2];
	char		avl_bits[2];
	signed char		avl_bf;
};

#ifdef AVL_INTERNAL

/* balance factor values */
#define LH 	(-1)
#define EH 	0
#define RH 	1

#define avl_bf2str(bf)	((bf) == -1 ? "LH" : (bf) == 0 ? "EH" : (bf) == 1 ? "RH" : "(unknown)" )

/* thread bits */
#define	AVL_CHILD	0
#define	AVL_THREAD	1

/* avl routines */
#define ldap_avl_getone(x)	((x) == 0 ? 0 : (x)->avl_data)
#define ldap_avl_onenode(x)	((x) == 0 || ((x)->avl_left == 0 && (x)->avl_right == 0))

#endif /* AVL_INTERNALS */

#define	ldap_avl_child(x,dir)	((x)->avl_bits[dir]) == AVL_CHILD ? \
	(x)->avl_link[dir] : NULL
#define	ldap_avl_lchild(x)	ldap_avl_child(x,0)
#define	ldap_avl_rchild(x)	ldap_avl_child(x,1)

typedef int		(*AVL_APPLY) LDAP_P((void *, void*));
typedef int		(*AVL_CMP) LDAP_P((const void*, const void*));
typedef int		(*AVL_DUP) LDAP_P((void*, void*));
typedef void	(*AVL_FREE) LDAP_P((void*));

LDAP_AVL_F( int )
ldap_avl_free LDAP_P(( Avlnode *root, AVL_FREE dfree ));

LDAP_AVL_F( int )
ldap_avl_insert LDAP_P((Avlnode **, void*, AVL_CMP, AVL_DUP));

LDAP_AVL_F( void* )
ldap_avl_delete LDAP_P((Avlnode **, void*, AVL_CMP));

LDAP_AVL_F( void* )
ldap_avl_find LDAP_P((Avlnode *, const void*, AVL_CMP));

LDAP_AVL_F( Avlnode* )
ldap_avl_find2 LDAP_P((Avlnode *, const void*, AVL_CMP));

LDAP_AVL_F( void* )
ldap_avl_find_lin LDAP_P((Avlnode *, const void*, AVL_CMP));

#ifdef AVL_NONREENTRANT
LDAP_AVL_F( void* )
ldap_avl_getfirst LDAP_P((Avlnode *));

LDAP_AVL_F( void* )
ldap_avl_getnext LDAP_P((void));
#endif

LDAP_AVL_F( int )
ldap_avl_dup_error LDAP_P((void*, void*));

LDAP_AVL_F( int )
ldap_avl_dup_ok LDAP_P((void*, void*));

LDAP_AVL_F( int )
ldap_avl_apply LDAP_P((Avlnode *, AVL_APPLY, void*, int, int));

LDAP_AVL_F( int )
ldap_avl_prefixapply LDAP_P((Avlnode *, void*, AVL_CMP, void*, AVL_CMP, void*, int));

LDAP_AVL_F( int )
ldap_tavl_free LDAP_P(( TAvlnode *root, AVL_FREE dfree ));

LDAP_AVL_F( int )
ldap_tavl_insert LDAP_P((TAvlnode **, void*, AVL_CMP, AVL_DUP));

LDAP_AVL_F( void* )
ldap_tavl_delete LDAP_P((TAvlnode **, void*, AVL_CMP));

LDAP_AVL_F( void* )
ldap_tavl_find LDAP_P((TAvlnode *, const void*, AVL_CMP));

LDAP_AVL_F( TAvlnode* )
ldap_tavl_find2 LDAP_P((TAvlnode *, const void*, AVL_CMP));

LDAP_AVL_F( TAvlnode* )
ldap_tavl_find3 LDAP_P((TAvlnode *, const void*, AVL_CMP, int *ret));

#define	TAVL_DIR_LEFT	0
#define	TAVL_DIR_RIGHT	1

LDAP_AVL_F( TAvlnode* )
ldap_tavl_end LDAP_P((TAvlnode *, int direction));

LDAP_AVL_F( TAvlnode* )
ldap_tavl_next LDAP_P((TAvlnode *, int direction));

/* apply traversal types */
#define AVL_PREORDER	1
#define AVL_INORDER	2
#define AVL_POSTORDER	3
/* what apply returns if it ran out of nodes */
#define AVL_NOMORE	(-6)

LDAP_END_DECL

#endif /* _AVL */

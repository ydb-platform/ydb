/* avl.c - routines to implement an avl tree */
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
/* ACKNOWLEDGEMENTS:
 * This work was originally developed by the University of Michigan
 * (as part of U-MICH LDAP).  Additional significant contributors
 * include:
 *   Howard Y. Chu
 *   Hallvard B. Furuseth
 *   Kurt D. Zeilenga
 */

#include "portable.h"

#include <limits.h>
#include <stdio.h>
#include <ac/stdlib.h>

#ifdef CSRIMALLOC
#define ber_memalloc malloc
#define ber_memrealloc realloc
#define ber_memfree free
#else
#include "lber.h"
#endif

#define AVL_INTERNAL
#include "ldap_avl.h"

/* Maximum tree depth this host's address space could support */
#define MAX_TREE_DEPTH	(sizeof(void *) * CHAR_BIT)

static const int avl_bfs[] = {LH, RH};

/*
 * ldap_avl_insert -- insert a node containing data data into the avl tree
 * with root root.  fcmp is a function to call to compare the data portion
 * of two nodes.  it should take two arguments and return <, >, or == 0,
 * depending on whether its first argument is <, >, or == its second
 * argument (like strcmp, e.g.).  fdup is a function to call when a duplicate
 * node is inserted.  it should return 0, or -1 and its return value
 * will be the return value from ldap_avl_insert in the case of a duplicate node.
 * the function will be called with the original node's data as its first
 * argument and with the incoming duplicate node's data as its second
 * argument.  this could be used, for example, to keep a count with each
 * node.
 *
 * NOTE: this routine may malloc memory
 */
int
ldap_avl_insert( Avlnode ** root, void *data, AVL_CMP fcmp, AVL_DUP fdup )
{
    Avlnode *t, *p, *s, *q, *r;
    int a, cmp, ncmp;

	if ( *root == NULL ) {
		if (( r = (Avlnode *) ber_memalloc( sizeof( Avlnode ))) == NULL ) {
			return( -1 );
		}
		r->avl_link[0] = r->avl_link[1] = NULL;
		r->avl_data = data;
		r->avl_bits[0] = r->avl_bits[1] = AVL_CHILD;
		r->avl_bf = EH;
		*root = r;

		return( 0 );
	}

    t = NULL;
    s = p = *root;

	/* find insertion point */
    while (1) {
		cmp = fcmp( data, p->avl_data );
		if ( cmp == 0 )
			return (*fdup)( p->avl_data, data );

		cmp = (cmp > 0);
		q = p->avl_link[cmp];
		if (q == NULL) {
			/* insert */
			if (( q = (Avlnode *) ber_memalloc( sizeof( Avlnode ))) == NULL ) {
				return( -1 );
			}
			q->avl_link[0] = q->avl_link[1] = NULL;
			q->avl_data = data;
			q->avl_bits[0] = q->avl_bits[1] = AVL_CHILD;
			q->avl_bf = EH;

			p->avl_link[cmp] = q;
			break;
		} else if ( q->avl_bf ) {
			t = p;
			s = q;
		}
		p = q;
    }

    /* adjust balance factors */
    cmp = fcmp( data, s->avl_data ) > 0;
	r = p = s->avl_link[cmp];
	a = avl_bfs[cmp];

	while ( p != q ) {
		cmp = fcmp( data, p->avl_data ) > 0;
		p->avl_bf = avl_bfs[cmp];
		p = p->avl_link[cmp];
	}

	/* checks and balances */

	if ( s->avl_bf == EH ) {
		s->avl_bf = a;
		return 0;
	} else if ( s->avl_bf == -a ) {
		s->avl_bf = EH;
		return 0;
    } else if ( s->avl_bf == a ) {
		cmp = (a > 0);
		ncmp = !cmp;
		if ( r->avl_bf == a ) {
			/* single rotation */
			p = r;
			s->avl_link[cmp] = r->avl_link[ncmp];
			r->avl_link[ncmp] = s;
			s->avl_bf = 0;
			r->avl_bf = 0;
		} else if ( r->avl_bf == -a ) {
			/* double rotation */
			p = r->avl_link[ncmp];
			r->avl_link[ncmp] = p->avl_link[cmp];
			p->avl_link[cmp] = r;
			s->avl_link[cmp] = p->avl_link[ncmp];
			p->avl_link[ncmp] = s;

			if ( p->avl_bf == a ) {
				s->avl_bf = -a;
				r->avl_bf = 0;
			} else if ( p->avl_bf == -a ) {
				s->avl_bf = 0;
				r->avl_bf = a;
			} else {
				s->avl_bf = 0;
				r->avl_bf = 0;
			}
			p->avl_bf = 0;
		}
		/* Update parent */
		if ( t == NULL )
			*root = p;
		else if ( s == t->avl_right )
			t->avl_right = p;
		else
			t->avl_left = p;
    }

  return 0;
}

void*
ldap_avl_delete( Avlnode **root, void* data, AVL_CMP fcmp )
{
	Avlnode *p, *q, *r, *top;
	int side, side_bf, shorter, nside;

	/* parent stack */
	Avlnode *pptr[MAX_TREE_DEPTH];
	unsigned char pdir[MAX_TREE_DEPTH];
	int depth = 0;

	if ( *root == NULL )
		return NULL;

	p = *root;

	while (1) {
		side = fcmp( data, p->avl_data );
		if ( !side )
			break;
		side = ( side > 0 );
		pdir[depth] = side;
		pptr[depth++] = p;

		p = p->avl_link[side];
		if ( p == NULL )
			return p;
	}
	data = p->avl_data;

	/* If this node has two children, swap so we are deleting a node with
	 * at most one child.
	 */
	if ( p->avl_link[0] && p->avl_link[1] ) {

		/* find the immediate predecessor <q> */
		q = p->avl_link[0];
		side = depth;
		pdir[depth++] = 0;
		while (q->avl_link[1]) {
			pdir[depth] = 1;
			pptr[depth++] = q;
			q = q->avl_link[1];
		}
		/* swap links */
		r = p->avl_link[0];
		p->avl_link[0] = q->avl_link[0];
		q->avl_link[0] = r;

		q->avl_link[1] = p->avl_link[1];
		p->avl_link[1] = NULL;

		q->avl_bf = p->avl_bf;

		/* fix stack positions: old parent of p points to q */
		pptr[side] = q;
		if ( side ) {
			r = pptr[side-1];
			r->avl_link[pdir[side-1]] = q;
		} else {
			*root = q;
		}
		/* new parent of p points to p */
		if ( depth-side > 1 ) {
			r = pptr[depth-1];
			r->avl_link[1] = p;
		} else {
			q->avl_link[0] = p;
		}
	}

	/* now <p> has at most one child, get it */
	q = p->avl_link[0] ? p->avl_link[0] : p->avl_link[1];

	ber_memfree( p );

	if ( !depth ) {
		*root = q;
		return data;
	}

	/* set the child into p's parent */
	depth--;
	p = pptr[depth];
	side = pdir[depth];
	p->avl_link[side] = q;

	top = NULL;
	shorter = 1;

	while ( shorter ) {
		p = pptr[depth];
		side = pdir[depth];
		nside = !side;
		side_bf = avl_bfs[side];

		/* case 1: height unchanged */
		if ( p->avl_bf == EH ) {
			/* Tree is now heavier on opposite side */
			p->avl_bf = avl_bfs[nside];
			shorter = 0;

		} else if ( p->avl_bf == side_bf ) {
		/* case 2: taller subtree shortened, height reduced */
			p->avl_bf = EH;
		} else {
		/* case 3: shorter subtree shortened */
			if ( depth )
				top = pptr[depth-1]; /* p->parent; */
			else
				top = NULL;
			/* set <q> to the taller of the two subtrees of <p> */
			q = p->avl_link[nside];
			if ( q->avl_bf == EH ) {
				/* case 3a: height unchanged, single rotate */
				p->avl_link[nside] = q->avl_link[side];
				q->avl_link[side] = p;
				shorter = 0;
				q->avl_bf = side_bf;
				p->avl_bf = (- side_bf);

			} else if ( q->avl_bf == p->avl_bf ) {
				/* case 3b: height reduced, single rotate */
				p->avl_link[nside] = q->avl_link[side];
				q->avl_link[side] = p;
				shorter = 1;
				q->avl_bf = EH;
				p->avl_bf = EH;

			} else {
				/* case 3c: height reduced, balance factors opposite */
				r = q->avl_link[side];
				q->avl_link[side] = r->avl_link[nside];
				r->avl_link[nside] = q;

				p->avl_link[nside] = r->avl_link[side];
				r->avl_link[side] = p;

				if ( r->avl_bf == side_bf ) {
					q->avl_bf = (- side_bf);
					p->avl_bf = EH;
				} else if ( r->avl_bf == (- side_bf)) {
					q->avl_bf = EH;
					p->avl_bf = side_bf;
				} else {
					q->avl_bf = EH;
					p->avl_bf = EH;
				}
				r->avl_bf = EH;
				q = r;
			}
			/* a rotation has caused <q> (or <r> in case 3c) to become
			 * the root.  let <p>'s former parent know this.
			 */
			if ( top == NULL ) {
				*root = q;
			} else if (top->avl_link[0] == p) {
				top->avl_link[0] = q;
			} else {
				top->avl_link[1] = q;
			}
			/* end case 3 */
			p = q;
		}
		if ( !depth )
			break;
		depth--;
	} /* end while(shorter) */

	return data;
}

static int
avl_inapply( Avlnode *root, AVL_APPLY fn, void* arg, int stopflag )
{
	if ( root == 0 )
		return( AVL_NOMORE );

	if ( root->avl_left != 0 )
		if ( avl_inapply( root->avl_left, fn, arg, stopflag )
		    == stopflag )
			return( stopflag );

	if ( (*fn)( root->avl_data, arg ) == stopflag )
		return( stopflag );

	if ( root->avl_right == 0 )
		return( AVL_NOMORE );
	else
		return( avl_inapply( root->avl_right, fn, arg, stopflag ) );
}

static int
avl_postapply( Avlnode *root, AVL_APPLY fn, void* arg, int stopflag )
{
	if ( root == 0 )
		return( AVL_NOMORE );

	if ( root->avl_left != 0 )
		if ( avl_postapply( root->avl_left, fn, arg, stopflag )
		    == stopflag )
			return( stopflag );

	if ( root->avl_right != 0 )
		if ( avl_postapply( root->avl_right, fn, arg, stopflag )
		    == stopflag )
			return( stopflag );

	return( (*fn)( root->avl_data, arg ) );
}

static int
avl_preapply( Avlnode *root, AVL_APPLY fn, void* arg, int stopflag )
{
	if ( root == 0 )
		return( AVL_NOMORE );

	if ( (*fn)( root->avl_data, arg ) == stopflag )
		return( stopflag );

	if ( root->avl_left != 0 )
		if ( avl_preapply( root->avl_left, fn, arg, stopflag )
		    == stopflag )
			return( stopflag );

	if ( root->avl_right == 0 )
		return( AVL_NOMORE );
	else
		return( avl_preapply( root->avl_right, fn, arg, stopflag ) );
}

/*
 * ldap_avl_apply -- avl tree root is traversed, function fn is called with
 * arguments arg and the data portion of each node.  if fn returns stopflag,
 * the traversal is cut short, otherwise it continues.  Do not use -6 as
 * a stopflag, as this is what is used to indicate the traversal ran out
 * of nodes.
 */

int
ldap_avl_apply( Avlnode *root, AVL_APPLY fn, void* arg, int stopflag, int type )
{
	switch ( type ) {
	case AVL_INORDER:
		return( avl_inapply( root, fn, arg, stopflag ) );
	case AVL_PREORDER:
		return( avl_preapply( root, fn, arg, stopflag ) );
	case AVL_POSTORDER:
		return( avl_postapply( root, fn, arg, stopflag ) );
	default:
		fprintf( stderr, "Invalid traversal type %d\n", type );
		return( -1 );
	}

	/* NOTREACHED */
}

/*
 * ldap_avl_prefixapply - traverse avl tree root, applying function fprefix
 * to any nodes that match.  fcmp is called with data as its first arg
 * and the current node's data as its second arg.  it should return
 * 0 if they match, < 0 if data is less, and > 0 if data is greater.
 * the idea is to efficiently find all nodes that are prefixes of
 * some key...  Like ldap_avl_apply, this routine also takes a stopflag
 * and will return prematurely if fmatch returns this value.  Otherwise,
 * AVL_NOMORE is returned.
 */

int
ldap_avl_prefixapply(
    Avlnode	*root,
    void*	data,
    AVL_CMP		fmatch,
    void*	marg,
    AVL_CMP		fcmp,
    void*	carg,
    int		stopflag
)
{
	int	cmp;

	if ( root == 0 )
		return( AVL_NOMORE );

	cmp = (*fcmp)( data, root->avl_data /* , carg */);
	if ( cmp == 0 ) {
		if ( (*fmatch)( root->avl_data, marg ) == stopflag )
			return( stopflag );

		if ( root->avl_left != 0 )
			if ( ldap_avl_prefixapply( root->avl_left, data, fmatch,
			    marg, fcmp, carg, stopflag ) == stopflag )
				return( stopflag );

		if ( root->avl_right != 0 )
			return( ldap_avl_prefixapply( root->avl_right, data, fmatch,
			    marg, fcmp, carg, stopflag ) );
		else
			return( AVL_NOMORE );

	} else if ( cmp < 0 ) {
		if ( root->avl_left != 0 )
			return( ldap_avl_prefixapply( root->avl_left, data, fmatch,
			    marg, fcmp, carg, stopflag ) );
	} else {
		if ( root->avl_right != 0 )
			return( ldap_avl_prefixapply( root->avl_right, data, fmatch,
			    marg, fcmp, carg, stopflag ) );
	}

	return( AVL_NOMORE );
}

/*
 * ldap_avl_free -- traverse avltree root, freeing the memory it is using.
 * the dfree() is called to free the data portion of each node.  The
 * number of items actually freed is returned.
 */

int
ldap_avl_free( Avlnode *root, AVL_FREE dfree )
{
	int	nleft, nright;

	if ( root == 0 )
		return( 0 );

	nleft = nright = 0;
	if ( root->avl_left != 0 )
		nleft = ldap_avl_free( root->avl_left, dfree );

	if ( root->avl_right != 0 )
		nright = ldap_avl_free( root->avl_right, dfree );

	if ( dfree )
		(*dfree)( root->avl_data );
	ber_memfree( root );

	return( nleft + nright + 1 );
}

/*
 * ldap_avl_find -- search avltree root for a node with data data.  the function
 * cmp is used to compare things.  it is called with data as its first arg
 * and the current node data as its second.  it should return 0 if they match,
 * < 0 if arg1 is less than arg2 and > 0 if arg1 is greater than arg2.
 */

Avlnode *
ldap_avl_find2( Avlnode *root, const void *data, AVL_CMP fcmp )
{
	int	cmp;

	while ( root != 0 && (cmp = (*fcmp)( data, root->avl_data )) != 0 ) {
		cmp = cmp > 0;
		root = root->avl_link[cmp];
	}
	return root;
}

void*
ldap_avl_find( Avlnode *root, const void* data, AVL_CMP fcmp )
{
	int	cmp;

	while ( root != 0 && (cmp = (*fcmp)( data, root->avl_data )) != 0 ) {
		cmp = cmp > 0;
		root = root->avl_link[cmp];
	}

	return( root ? root->avl_data : 0 );
}

/*
 * ldap_avl_find_lin -- search avltree root linearly for a node with data data.
 * the function cmp is used to compare things.  it is called with data as its
 * first arg and the current node data as its second.  it should return 0 if
 * they match, non-zero otherwise.
 */

void*
ldap_avl_find_lin( Avlnode *root, const void* data, AVL_CMP fcmp )
{
	void*	res;

	if ( root == 0 )
		return( NULL );

	if ( (*fcmp)( data, root->avl_data ) == 0 )
		return( root->avl_data );

	if ( root->avl_left != 0 )
		if ( (res = ldap_avl_find_lin( root->avl_left, data, fcmp ))
		    != NULL )
			return( res );

	if ( root->avl_right == 0 )
		return( NULL );
	else
		return( ldap_avl_find_lin( root->avl_right, data, fcmp ) );
}

/* NON-REENTRANT INTERFACE */

static void*	*avl_list;
static int	avl_maxlist;
static int	ldap_avl_nextlist;

#define AVL_GRABSIZE	100

/* ARGSUSED */
static int
avl_buildlist( void* data, void* arg )
{
	static int	slots;

	if ( avl_list == (void* *) 0 ) {
		avl_list = (void* *) ber_memalloc(AVL_GRABSIZE * sizeof(void*));
		slots = AVL_GRABSIZE;
		avl_maxlist = 0;
	} else if ( avl_maxlist == slots ) {
		slots += AVL_GRABSIZE;
		avl_list = (void* *) ber_memrealloc( (char *) avl_list,
		    (unsigned) slots * sizeof(void*));
	}

	avl_list[ avl_maxlist++ ] = data;

	return( 0 );
}

/*
 * ldap_avl_getfirst() and ldap_avl_getnext() are provided as alternate tree
 * traversal methods, to be used when a single function cannot be
 * provided to be called with every node in the tree.  ldap_avl_getfirst()
 * traverses the tree and builds a linear list of all the nodes,
 * returning the first node.  ldap_avl_getnext() returns the next thing
 * on the list built by ldap_avl_getfirst().  This means that ldap_avl_getfirst()
 * can take a while, and that the tree should not be messed with while
 * being traversed in this way, and that multiple traversals (even of
 * different trees) cannot be active at once.
 */

void*
ldap_avl_getfirst( Avlnode *root )
{
	if ( avl_list ) {
		ber_memfree( (char *) avl_list);
		avl_list = (void* *) 0;
	}
	avl_maxlist = 0;
	ldap_avl_nextlist = 0;

	if ( root == 0 )
		return( 0 );

	(void) ldap_avl_apply( root, avl_buildlist, (void*) 0, -1, AVL_INORDER );

	return( avl_list[ ldap_avl_nextlist++ ] );
}

void*
ldap_avl_getnext( void )
{
	if ( avl_list == 0 )
		return( 0 );

	if ( ldap_avl_nextlist == avl_maxlist ) {
		ber_memfree( (void*) avl_list);
		avl_list = (void* *) 0;
		return( 0 );
	}

	return( avl_list[ ldap_avl_nextlist++ ] );
}

/* end non-reentrant code */


int
ldap_avl_dup_error( void* left, void* right )
{
	return( -1 );
}

int
ldap_avl_dup_ok( void* left, void* right )
{
	return( 0 );
}

/* avl.c - routines to implement an avl tree */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2005-2024 The OpenLDAP Foundation.
 * Portions Copyright (c) 2005 by Howard Chu, Symas Corp.
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
 * This work was initially developed by Howard Chu for inclusion
 * in OpenLDAP software.
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
 * Threaded AVL trees - for fast in-order traversal of nodes.
 */
/*
 * ldap_tavl_insert -- insert a node containing data data into the avl tree
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
ldap_tavl_insert( TAvlnode ** root, void *data, AVL_CMP fcmp, AVL_DUP fdup )
{
    TAvlnode *t, *p, *s, *q, *r;
    int a, cmp, ncmp;

	if ( *root == NULL ) {
		if (( r = (TAvlnode *) ber_memalloc( sizeof( TAvlnode ))) == NULL ) {
			return( -1 );
		}
		r->avl_link[0] = r->avl_link[1] = NULL;
		r->avl_data = data;
		r->avl_bf = EH;
		r->avl_bits[0] = r->avl_bits[1] = AVL_THREAD;
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
		q = ldap_avl_child( p, cmp );
		if (q == NULL) {
			/* insert */
			if (( q = (TAvlnode *) ber_memalloc( sizeof( TAvlnode ))) == NULL ) {
				return( -1 );
			}
			q->avl_link[cmp] = p->avl_link[cmp];
			q->avl_link[!cmp] = p;
			q->avl_data = data;
			q->avl_bf = EH;
			q->avl_bits[0] = q->avl_bits[1] = AVL_THREAD;

			p->avl_link[cmp] = q;
			p->avl_bits[cmp] = AVL_CHILD;
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
			if ( r->avl_bits[ncmp] == AVL_THREAD ) {
				r->avl_bits[ncmp] = AVL_CHILD;
				s->avl_bits[cmp] = AVL_THREAD;
			} else {
				s->avl_link[cmp] = r->avl_link[ncmp];
				r->avl_link[ncmp] = s;
			}
			s->avl_bf = 0;
			r->avl_bf = 0;
		} else if ( r->avl_bf == -a ) {
			/* double rotation */
			p = r->avl_link[ncmp];
			if ( p->avl_bits[cmp] == AVL_THREAD ) {
				p->avl_bits[cmp] = AVL_CHILD;
				r->avl_bits[ncmp] = AVL_THREAD;
			} else {
				r->avl_link[ncmp] = p->avl_link[cmp];
				p->avl_link[cmp] = r;
			}
			if ( p->avl_bits[ncmp] == AVL_THREAD ) {
				p->avl_bits[ncmp] = AVL_CHILD;
				s->avl_link[cmp] = p;
				s->avl_bits[cmp] = AVL_THREAD;
			} else {
				s->avl_link[cmp] = p->avl_link[ncmp];
				p->avl_link[ncmp] = s;
			}
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
ldap_tavl_delete( TAvlnode **root, void* data, AVL_CMP fcmp )
{
	TAvlnode *p, *q, *r, *top;
	int side, side_bf, shorter, nside = -1;

	/* parent stack */
	TAvlnode *pptr[MAX_TREE_DEPTH];
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

		if ( p->avl_bits[side] == AVL_THREAD )
			return NULL;
		p = p->avl_link[side];
	}
	data = p->avl_data;

	/* If this node has two children, swap so we are deleting a node with
	 * at most one child.
	 */
	if ( p->avl_bits[0] == AVL_CHILD && p->avl_bits[1] == AVL_CHILD &&
		p->avl_link[0] && p->avl_link[1] ) {

		/* find the immediate predecessor <q> */
		q = p->avl_link[0];
		side = depth;
		pdir[depth++] = 0;
		while (q->avl_bits[1] == AVL_CHILD && q->avl_link[1]) {
			pdir[depth] = 1;
			pptr[depth++] = q;
			q = q->avl_link[1];
		}
		/* swap links */
		r = p->avl_link[0];
		p->avl_link[0] = q->avl_link[0];
		q->avl_link[0] = r;

		q->avl_link[1] = p->avl_link[1];
		p->avl_link[1] = q;

		p->avl_bits[0] = q->avl_bits[0];
		p->avl_bits[1] = q->avl_bits[1];
		q->avl_bits[0] = q->avl_bits[1] = AVL_CHILD;

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

		/* fix right subtree: successor of p points to q */
		r = q->avl_link[1];
		while ( r->avl_bits[0] == AVL_CHILD && r->avl_link[0] )
			r = r->avl_link[0];
		r->avl_link[0] = q;
	}

	/* now <p> has at most one child, get it */
	if ( p->avl_link[0] && p->avl_bits[0] == AVL_CHILD ) {
		q = p->avl_link[0];
		/* Preserve thread continuity */
		r = p->avl_link[1];
		nside = 1;
	} else if ( p->avl_link[1] && p->avl_bits[1] == AVL_CHILD ) {
		q = p->avl_link[1];
		r = p->avl_link[0];
		nside = 0;
	} else {
		q = NULL;
		if ( depth > 0 )
			r = p->avl_link[pdir[depth-1]];
		else
			r = NULL;
	}

	ber_memfree( p );

	/* Update child thread */
	if ( q ) {
		for ( ; q->avl_bits[nside] == AVL_CHILD && q->avl_link[nside];
			q = q->avl_link[nside] ) ;
		q->avl_link[nside] = r;
	}
	
	if ( !depth ) {
		*root = q;
		return data;
	}

	/* set the child into p's parent */
	depth--;
	p = pptr[depth];
	side = pdir[depth];
	p->avl_link[side] = q;

	if ( !q ) {
		p->avl_bits[side] = AVL_THREAD;
		p->avl_link[side] = r;
	}

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
				if ( q->avl_bits[side] == AVL_THREAD ) {
					q->avl_bits[side] = AVL_CHILD;
					p->avl_bits[nside] = AVL_THREAD;
				} else {
					p->avl_link[nside] = q->avl_link[side];
					q->avl_link[side] = p;
				}
				shorter = 0;
				q->avl_bf = side_bf;
				p->avl_bf = (- side_bf);

			} else if ( q->avl_bf == p->avl_bf ) {
				/* case 3b: height reduced, single rotate */
				if ( q->avl_bits[side] == AVL_THREAD ) {
					q->avl_bits[side] = AVL_CHILD;
					p->avl_bits[nside] = AVL_THREAD;
				} else {
					p->avl_link[nside] = q->avl_link[side];
					q->avl_link[side] = p;
				}
				shorter = 1;
				q->avl_bf = EH;
				p->avl_bf = EH;

			} else {
				/* case 3c: height reduced, balance factors opposite */
				r = q->avl_link[side];
				if ( r->avl_bits[nside] == AVL_THREAD ) {
					r->avl_bits[nside] = AVL_CHILD;
					q->avl_bits[side] = AVL_THREAD;
				} else {
					q->avl_link[side] = r->avl_link[nside];
					r->avl_link[nside] = q;
				}

				if ( r->avl_bits[side] == AVL_THREAD ) {
					r->avl_bits[side] = AVL_CHILD;
					p->avl_bits[nside] = AVL_THREAD;
					p->avl_link[nside] = r;
				} else {
					p->avl_link[nside] = r->avl_link[side];
					r->avl_link[side] = p;
				}

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

/*
 * ldap_tavl_free -- traverse avltree root, freeing the memory it is using.
 * the dfree() is called to free the data portion of each node.  The
 * number of items actually freed is returned.
 */

int
ldap_tavl_free( TAvlnode *root, AVL_FREE dfree )
{
	int	nleft, nright;

	if ( root == 0 )
		return( 0 );

	nleft = ldap_tavl_free( ldap_avl_lchild( root ), dfree );

	nright = ldap_tavl_free( ldap_avl_rchild( root ), dfree );

	if ( dfree )
		(*dfree)( root->avl_data );
	ber_memfree( root );

	return( nleft + nright + 1 );
}

/*
 * ldap_tavl_find -- search avltree root for a node with data data.  the function
 * cmp is used to compare things.  it is called with data as its first arg
 * and the current node data as its second.  it should return 0 if they match,
 * < 0 if arg1 is less than arg2 and > 0 if arg1 is greater than arg2.
 */

/*
 * ldap_tavl_find2 - returns TAvlnode instead of data pointer.
 * ldap_tavl_find3 - as above, but returns TAvlnode even if no match is found.
 *				also set *ret = last comparison result, or -1 if root == NULL.
 */
TAvlnode *
ldap_tavl_find3( TAvlnode *root, const void *data, AVL_CMP fcmp, int *ret )
{
	int	cmp = -1, dir;
	TAvlnode *prev = root;

	while ( root != 0 && (cmp = (*fcmp)( data, root->avl_data )) != 0 ) {
		prev = root;
		dir = cmp > 0;
		root = ldap_avl_child( root, dir );
	}
	*ret = cmp;
	return root ? root : prev;
}

TAvlnode *
ldap_tavl_find2( TAvlnode *root, const void *data, AVL_CMP fcmp )
{
	int	cmp;

	while ( root != 0 && (cmp = (*fcmp)( data, root->avl_data )) != 0 ) {
		cmp = cmp > 0;
		root = ldap_avl_child( root, cmp );
	}
	return root;
}

void*
ldap_tavl_find( TAvlnode *root, const void* data, AVL_CMP fcmp )
{
	int	cmp;

	while ( root != 0 && (cmp = (*fcmp)( data, root->avl_data )) != 0 ) {
		cmp = cmp > 0;
		root = ldap_avl_child( root, cmp );
	}

	return( root ? root->avl_data : 0 );
}

/* Return the leftmost or rightmost node in the tree */
TAvlnode *
ldap_tavl_end( TAvlnode *root, int dir )
{
	if ( root ) {
		while ( root->avl_bits[dir] == AVL_CHILD )
			root = root->avl_link[dir];
	}
	return root;
}

/* Return the next node in the given direction */
TAvlnode *
ldap_tavl_next( TAvlnode *root, int dir )
{
	if ( root ) {
		int c = root->avl_bits[dir];

		root = root->avl_link[dir];
		if ( c == AVL_CHILD ) {
			dir ^= 1;
			while ( root->avl_bits[dir] == AVL_CHILD )
				root = root->avl_link[dir];
		}
	}
	return root;
}

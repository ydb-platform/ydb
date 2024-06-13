/* ldap_queue.h -- queue macros */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2001-2024 The OpenLDAP Foundation.
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
/* Copyright (c) 1991, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *	This product includes software developed by the University of
 *	California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 *	@(#)queue.h	8.5 (Berkeley) 8/20/94
 * $FreeBSD: src/sys/sys/queue.h,v 1.32.2.5 2001/09/30 21:12:54 luigi Exp $
 *
 * See also: ftp://ftp.cs.berkeley.edu/pub/4bsd/README.Impt.License.Change
 */
/* ACKNOWLEDGEMENTS:
 * This work is derived from FreeBSD queue.h work.  Adapted for use in
 * OpenLDAP Software by Kurt D. Zeilenga.
 */

#ifndef _LDAP_QUEUE_H_
#define	_LDAP_QUEUE_H_

/*
 * This file defines five types of data structures: singly-linked lists,
 * singly-linked tail queues, lists, tail queues, and circular queues.
 *
 * A singly-linked list is headed by a single forward pointer. The elements
 * are singly linked for minimum space and pointer manipulation overhead at
 * the expense of O(n) removal for arbitrary elements. New elements can be
 * added to the list after an existing element or at the head of the list.
 * Elements being removed from the head of the list should use the explicit
 * macro for this purpose for optimum efficiency. A singly-linked list may
 * only be traversed in the forward direction.  Singly-linked lists are ideal
 * for applications with large datasets and few or no removals or for
 * implementing a LIFO queue.
 *
 * A singly-linked tail queue is headed by a pair of pointers, one to the
 * head of the list and the other to the tail of the list. The elements are
 * singly linked for minimum space and pointer manipulation overhead at the
 * expense of O(n) removal for arbitrary elements. New elements can be added
 * to the list after an existing element, at the head of the list, or at the
 * end of the list. Elements being removed from the head of the tail queue
 * should use the explicit macro for this purpose for optimum efficiency.
 * A singly-linked tail queue may only be traversed in the forward direction.
 * Singly-linked tail queues are ideal for applications with large datasets
 * and few or no removals or for implementing a FIFO queue.
 *
 * A list is headed by a single forward pointer (or an array of forward
 * pointers for a hash table header). The elements are doubly linked
 * so that an arbitrary element can be removed without a need to
 * traverse the list. New elements can be added to the list before
 * or after an existing element or at the head of the list. A list
 * may only be traversed in the forward direction.
 *
 * A tail queue is headed by a pair of pointers, one to the head of the
 * list and the other to the tail of the list. The elements are doubly
 * linked so that an arbitrary element can be removed without a need to
 * traverse the list. New elements can be added to the list before or
 * after an existing element, at the head of the list, or at the end of
 * the list. A tail queue may be traversed in either direction.
 *
 * A circle queue is headed by a pair of pointers, one to the head of the
 * list and the other to the tail of the list. The elements are doubly
 * linked so that an arbitrary element can be removed without a need to
 * traverse the list. New elements can be added to the list before or after
 * an existing element, at the head of the list, or at the end of the list.
 * A circle queue may be traversed in either direction, but has a more
 * complex end of list detection. Also, it is possible to rotate the queue,
 * rejoining the ends and splitting it so that a given element becomes the
 * new head or tail.
 *
 * For details on the use of these macros, see the queue(3) manual page.
 * All macros are prefixed with LDAP_.
 *
 *			SLIST_	LIST_	STAILQ_	TAILQ_	CIRCLEQ_
 * _HEAD		+	+	+	+	+
 * _ENTRY		+	+	+	+	+
 * _INIT		+	+	+	+	+
 * _ENTRY_INIT		+	+	+	+	+
 * _EMPTY		+	+	+	+	+
 * _FIRST		+	+	+	+	+
 * _NEXT		+	+	+	+	+
 * _PREV		-	-	-	+	+
 * _LAST		-	-	+	+	+
 * _FOREACH		+	+	+	+	+
 * _FOREACH_REVERSE	-	-	-	+	+
 * _INSERT_HEAD		+	+	+	+	+
 * _INSERT_BEFORE	-	+	-	+	+
 * _INSERT_AFTER	+	+	+	+	+
 * _INSERT_TAIL		-	-	+	+	+
 * _REMOVE_HEAD		+	-	+	-	-
 * _REMOVE		+	+	+	+	+
 *
 */

/*
 * Singly-linked List definitions.
 */
#define LDAP_SLIST_HEAD(name, type)					\
struct name {								\
	struct type *slh_first;	/* first element */			\
}

#define LDAP_SLIST_HEAD_INITIALIZER(head)				\
	{ NULL }

#define LDAP_SLIST_ENTRY(type)						\
struct {								\
	struct type *sle_next;	/* next element */			\
}

#define LDAP_SLIST_ENTRY_INITIALIZER(entry)				\
	{ NULL }

/*
 * Singly-linked List functions.
 */
#define	LDAP_SLIST_EMPTY(head)	((head)->slh_first == NULL)

#define	LDAP_SLIST_FIRST(head)	((head)->slh_first)

#define LDAP_SLIST_FOREACH(var, head, field)				\
	for((var) = (head)->slh_first; (var); (var) = (var)->field.sle_next)

#define LDAP_SLIST_INIT(head) {						\
	(head)->slh_first = NULL;					\
}

#define LDAP_SLIST_ENTRY_INIT(var, field) {				\
	(var)->field.sle_next = NULL;					\
}

#define LDAP_SLIST_INSERT_AFTER(slistelm, elm, field) do {		\
	(elm)->field.sle_next = (slistelm)->field.sle_next;		\
	(slistelm)->field.sle_next = (elm);				\
} while (0)

#define LDAP_SLIST_INSERT_HEAD(head, elm, field) do {			\
	(elm)->field.sle_next = (head)->slh_first;			\
	(head)->slh_first = (elm);					\
} while (0)

#define LDAP_SLIST_NEXT(elm, field)	((elm)->field.sle_next)

#define LDAP_SLIST_REMOVE_HEAD(head, field) do {			\
	(head)->slh_first = (head)->slh_first->field.sle_next;		\
} while (0)

#define LDAP_SLIST_REMOVE(head, elm, type, field) do {			\
	if ((head)->slh_first == (elm)) {				\
		LDAP_SLIST_REMOVE_HEAD((head), field);			\
	}								\
	else {								\
		struct type *curelm = (head)->slh_first;		\
		while( curelm->field.sle_next != (elm) )		\
			curelm = curelm->field.sle_next;		\
		curelm->field.sle_next =				\
		    curelm->field.sle_next->field.sle_next;		\
	}								\
} while (0)

/*
 * Singly-linked Tail queue definitions.
 */
#define LDAP_STAILQ_HEAD(name, type)					\
struct name {								\
	struct type *stqh_first;/* first element */			\
	struct type **stqh_last;/* addr of last next element */		\
}

#define LDAP_STAILQ_HEAD_INITIALIZER(head)				\
	{ NULL, &(head).stqh_first }

#define LDAP_STAILQ_ENTRY(type)						\
struct {								\
	struct type *stqe_next;	/* next element */			\
}

#define LDAP_STAILQ_ENTRY_INITIALIZER(entry)				\
	{ NULL }

/*
 * Singly-linked Tail queue functions.
 */
#define LDAP_STAILQ_EMPTY(head) ((head)->stqh_first == NULL)

#define	LDAP_STAILQ_INIT(head) do {					\
	(head)->stqh_first = NULL;					\
	(head)->stqh_last = &(head)->stqh_first;			\
} while (0)

#define LDAP_STAILQ_ENTRY_INIT(var, field) {				\
	(var)->field.stqe_next = NULL;					\
}

#define LDAP_STAILQ_FIRST(head)	((head)->stqh_first)

#define	LDAP_STAILQ_LAST(head, type, field)				\
	(LDAP_STAILQ_EMPTY(head) ?					\
		NULL :							\
	        ((struct type *)					\
		((char *)((head)->stqh_last) - offsetof(struct type, field))))

#define LDAP_STAILQ_FOREACH(var, head, field)				\
	for((var) = (head)->stqh_first; (var); (var) = (var)->field.stqe_next)

#define LDAP_STAILQ_INSERT_HEAD(head, elm, field) do {			\
	if (((elm)->field.stqe_next = (head)->stqh_first) == NULL)	\
		(head)->stqh_last = &(elm)->field.stqe_next;		\
	(head)->stqh_first = (elm);					\
} while (0)

#define LDAP_STAILQ_INSERT_TAIL(head, elm, field) do {			\
	(elm)->field.stqe_next = NULL;					\
	*(head)->stqh_last = (elm);					\
	(head)->stqh_last = &(elm)->field.stqe_next;			\
} while (0)

#define LDAP_STAILQ_INSERT_AFTER(head, tqelm, elm, field) do {		\
	if (((elm)->field.stqe_next = (tqelm)->field.stqe_next) == NULL)\
		(head)->stqh_last = &(elm)->field.stqe_next;		\
	(tqelm)->field.stqe_next = (elm);				\
} while (0)

#define LDAP_STAILQ_NEXT(elm, field)	((elm)->field.stqe_next)

#define LDAP_STAILQ_REMOVE_HEAD(head, field) do {			\
	if (((head)->stqh_first =					\
	     (head)->stqh_first->field.stqe_next) == NULL)		\
		(head)->stqh_last = &(head)->stqh_first;		\
} while (0)

#define LDAP_STAILQ_REMOVE_HEAD_UNTIL(head, elm, field) do {		\
	if (((head)->stqh_first = (elm)->field.stqe_next) == NULL)	\
		(head)->stqh_last = &(head)->stqh_first;		\
} while (0)

#define LDAP_STAILQ_REMOVE(head, elm, type, field) do {			\
	if ((head)->stqh_first == (elm)) {				\
		LDAP_STAILQ_REMOVE_HEAD(head, field);			\
	}								\
	else {								\
		struct type *curelm = (head)->stqh_first;		\
		while( curelm->field.stqe_next != (elm) )		\
			curelm = curelm->field.stqe_next;		\
		if((curelm->field.stqe_next =				\
		    curelm->field.stqe_next->field.stqe_next) == NULL)	\
			(head)->stqh_last = &(curelm)->field.stqe_next;	\
	}								\
} while (0)

/*
 * List definitions.
 */
#define LDAP_LIST_HEAD(name, type)					\
struct name {								\
	struct type *lh_first;	/* first element */			\
}

#define LDAP_LIST_HEAD_INITIALIZER(head)				\
	{ NULL }

#define LDAP_LIST_ENTRY(type)						\
struct {								\
	struct type *le_next;	/* next element */			\
	struct type **le_prev;	/* address of previous next element */	\
}

#define LDAP_LIST_ENTRY_INITIALIZER(entry)			\
	{ NULL, NULL }

/*
 * List functions.
 */

#define	LDAP_LIST_EMPTY(head) ((head)->lh_first == NULL)

#define LDAP_LIST_FIRST(head)	((head)->lh_first)

#define LDAP_LIST_FOREACH(var, head, field)				\
	for((var) = (head)->lh_first; (var); (var) = (var)->field.le_next)

#define	LDAP_LIST_INIT(head) do {					\
	(head)->lh_first = NULL;					\
} while (0)

#define LDAP_LIST_ENTRY_INIT(var, field) do {				\
	(var)->field.le_next = NULL;					\
	(var)->field.le_prev = NULL;					\
} while (0)

#define LDAP_LIST_INSERT_AFTER(listelm, elm, field) do {		\
	if (((elm)->field.le_next = (listelm)->field.le_next) != NULL)	\
		(listelm)->field.le_next->field.le_prev =		\
		    &(elm)->field.le_next;				\
	(listelm)->field.le_next = (elm);				\
	(elm)->field.le_prev = &(listelm)->field.le_next;		\
} while (0)

#define LDAP_LIST_INSERT_BEFORE(listelm, elm, field) do {		\
	(elm)->field.le_prev = (listelm)->field.le_prev;		\
	(elm)->field.le_next = (listelm);				\
	*(listelm)->field.le_prev = (elm);				\
	(listelm)->field.le_prev = &(elm)->field.le_next;		\
} while (0)

#define LDAP_LIST_INSERT_HEAD(head, elm, field) do {			\
	if (((elm)->field.le_next = (head)->lh_first) != NULL)		\
		(head)->lh_first->field.le_prev = &(elm)->field.le_next;\
	(head)->lh_first = (elm);					\
	(elm)->field.le_prev = &(head)->lh_first;			\
} while (0)

#define LDAP_LIST_NEXT(elm, field)	((elm)->field.le_next)

#define LDAP_LIST_REMOVE(elm, field) do {				\
	if ((elm)->field.le_next != NULL)				\
		(elm)->field.le_next->field.le_prev = 			\
		    (elm)->field.le_prev;				\
	*(elm)->field.le_prev = (elm)->field.le_next;			\
} while (0)

/*
 * Tail queue definitions.
 */
#define LDAP_TAILQ_HEAD(name, type)					\
struct name {								\
	struct type *tqh_first;	/* first element */			\
	struct type **tqh_last;	/* addr of last next element */		\
}

#define LDAP_TAILQ_HEAD_INITIALIZER(head)				\
	{ NULL, &(head).tqh_first }

#define LDAP_TAILQ_ENTRY(type)						\
struct {								\
	struct type *tqe_next;	/* next element */			\
	struct type **tqe_prev;	/* address of previous next element */	\
}

#define LDAP_TAILQ_ENTRY_INITIALIZER(entry)				\
	{ NULL, NULL }

/*
 * Tail queue functions.
 */
#define	LDAP_TAILQ_EMPTY(head) ((head)->tqh_first == NULL)

#define LDAP_TAILQ_FOREACH(var, head, field)				\
	for (var = LDAP_TAILQ_FIRST(head); var; var = LDAP_TAILQ_NEXT(var, field))

#define LDAP_TAILQ_FOREACH_REVERSE(var, head, headname, field)		\
	for ((var) = LDAP_TAILQ_LAST((head), headname);			\
	     (var);							\
	     (var) = LDAP_TAILQ_PREV((var), headname, field))

#define	LDAP_TAILQ_FIRST(head) ((head)->tqh_first)

#define	LDAP_TAILQ_LAST(head, headname) \
	(*(((struct headname *)((head)->tqh_last))->tqh_last))

#define	LDAP_TAILQ_NEXT(elm, field) ((elm)->field.tqe_next)

#define LDAP_TAILQ_PREV(elm, headname, field) \
	(*(((struct headname *)((elm)->field.tqe_prev))->tqh_last))

#define	LDAP_TAILQ_INIT(head) do {					\
	(head)->tqh_first = NULL;					\
	(head)->tqh_last = &(head)->tqh_first;				\
} while (0)

#define LDAP_TAILQ_ENTRY_INIT(var, field) do {				\
	(var)->field.tqe_next = NULL;					\
	(var)->field.tqe_prev = NULL;					\
} while (0)

#define LDAP_TAILQ_INSERT_HEAD(head, elm, field) do {			\
	if (((elm)->field.tqe_next = (head)->tqh_first) != NULL)	\
		(head)->tqh_first->field.tqe_prev =			\
		    &(elm)->field.tqe_next;				\
	else								\
		(head)->tqh_last = &(elm)->field.tqe_next;		\
	(head)->tqh_first = (elm);					\
	(elm)->field.tqe_prev = &(head)->tqh_first;			\
} while (0)

#define LDAP_TAILQ_INSERT_TAIL(head, elm, field) do {			\
	(elm)->field.tqe_next = NULL;					\
	(elm)->field.tqe_prev = (head)->tqh_last;			\
	*(head)->tqh_last = (elm);					\
	(head)->tqh_last = &(elm)->field.tqe_next;			\
} while (0)

#define LDAP_TAILQ_INSERT_AFTER(head, listelm, elm, field) do {		\
	if (((elm)->field.tqe_next = (listelm)->field.tqe_next) != NULL)\
		(elm)->field.tqe_next->field.tqe_prev = 		\
		    &(elm)->field.tqe_next;				\
	else								\
		(head)->tqh_last = &(elm)->field.tqe_next;		\
	(listelm)->field.tqe_next = (elm);				\
	(elm)->field.tqe_prev = &(listelm)->field.tqe_next;		\
} while (0)

#define LDAP_TAILQ_INSERT_BEFORE(listelm, elm, field) do {		\
	(elm)->field.tqe_prev = (listelm)->field.tqe_prev;		\
	(elm)->field.tqe_next = (listelm);				\
	*(listelm)->field.tqe_prev = (elm);				\
	(listelm)->field.tqe_prev = &(elm)->field.tqe_next;		\
} while (0)

#define LDAP_TAILQ_REMOVE(head, elm, field) do {			\
	if (((elm)->field.tqe_next) != NULL)				\
		(elm)->field.tqe_next->field.tqe_prev = 		\
		    (elm)->field.tqe_prev;				\
	else								\
		(head)->tqh_last = (elm)->field.tqe_prev;		\
	*(elm)->field.tqe_prev = (elm)->field.tqe_next;			\
} while (0)

/*
 * Circular queue definitions.
 */
#define LDAP_CIRCLEQ_HEAD(name, type)					\
struct name {								\
	struct type *cqh_first;		/* first element */		\
	struct type *cqh_last;		/* last element */		\
}

#define LDAP_CIRCLEQ_HEAD_INITIALIZER(head)				\
	{ (void *)&(head), (void *)&(head) }

#define LDAP_CIRCLEQ_ENTRY(type)					\
struct {								\
	struct type *cqe_next;		/* next element */		\
	struct type *cqe_prev;		/* previous element */		\
}

/*
 * Circular queue functions.
 */
#define LDAP_CIRCLEQ_EMPTY(head) ((head)->cqh_first == (void *)(head))

#define LDAP_CIRCLEQ_FIRST(head) ((head)->cqh_first)

#define LDAP_CIRCLEQ_FOREACH(var, head, field)				\
	for((var) = (head)->cqh_first;					\
	    (var) != (void *)(head);					\
	    (var) = (var)->field.cqe_next)

#define LDAP_CIRCLEQ_FOREACH_REVERSE(var, head, field)			\
	for((var) = (head)->cqh_last;					\
	    (var) != (void *)(head);					\
	    (var) = (var)->field.cqe_prev)

#define	LDAP_CIRCLEQ_INIT(head) do {					\
	(head)->cqh_first = (void *)(head);				\
	(head)->cqh_last = (void *)(head);				\
} while (0)

#define LDAP_CIRCLEQ_ENTRY_INIT(var, field) do {			\
	(var)->field.cqe_next = NULL;					\
	(var)->field.cqe_prev = NULL;					\
} while (0)

#define LDAP_CIRCLEQ_INSERT_AFTER(head, listelm, elm, field) do {	\
	(elm)->field.cqe_next = (listelm)->field.cqe_next;		\
	(elm)->field.cqe_prev = (listelm);				\
	if ((listelm)->field.cqe_next == (void *)(head))		\
		(head)->cqh_last = (elm);				\
	else								\
		(listelm)->field.cqe_next->field.cqe_prev = (elm);	\
	(listelm)->field.cqe_next = (elm);				\
} while (0)

#define LDAP_CIRCLEQ_INSERT_BEFORE(head, listelm, elm, field) do {	\
	(elm)->field.cqe_next = (listelm);				\
	(elm)->field.cqe_prev = (listelm)->field.cqe_prev;		\
	if ((listelm)->field.cqe_prev == (void *)(head))		\
		(head)->cqh_first = (elm);				\
	else								\
		(listelm)->field.cqe_prev->field.cqe_next = (elm);	\
	(listelm)->field.cqe_prev = (elm);				\
} while (0)

#define LDAP_CIRCLEQ_INSERT_HEAD(head, elm, field) do {			\
	(elm)->field.cqe_next = (head)->cqh_first;			\
	(elm)->field.cqe_prev = (void *)(head);				\
	if ((head)->cqh_last == (void *)(head))				\
		(head)->cqh_last = (elm);				\
	else								\
		(head)->cqh_first->field.cqe_prev = (elm);		\
	(head)->cqh_first = (elm);					\
} while (0)

#define LDAP_CIRCLEQ_INSERT_TAIL(head, elm, field) do {			\
	(elm)->field.cqe_next = (void *)(head);				\
	(elm)->field.cqe_prev = (head)->cqh_last;			\
	if ((head)->cqh_first == (void *)(head))			\
		(head)->cqh_first = (elm);				\
	else								\
		(head)->cqh_last->field.cqe_next = (elm);		\
	(head)->cqh_last = (elm);					\
} while (0)

#define LDAP_CIRCLEQ_LAST(head) ((head)->cqh_last)

#define LDAP_CIRCLEQ_NEXT(elm,field) ((elm)->field.cqe_next)

#define LDAP_CIRCLEQ_PREV(elm,field) ((elm)->field.cqe_prev)

#define	LDAP_CIRCLEQ_REMOVE(head, elm, field) do {			\
	if ((elm)->field.cqe_next == (void *)(head))			\
		(head)->cqh_last = (elm)->field.cqe_prev;		\
	else								\
		(elm)->field.cqe_next->field.cqe_prev =			\
		    (elm)->field.cqe_prev;				\
	if ((elm)->field.cqe_prev == (void *)(head))			\
		(head)->cqh_first = (elm)->field.cqe_next;		\
	else								\
		(elm)->field.cqe_prev->field.cqe_next =			\
		    (elm)->field.cqe_next;				\
} while (0)

#define LDAP_CIRCLEQ_LOOP_NEXT(head, elm, field)			\
	(((elm)->field.cqe_next == (void *)(head))			\
		? ((head)->cqh_first)					\
		: ((elm)->field.cqe_next))

#define LDAP_CIRCLEQ_LOOP_PREV(head, elm, field)			\
	(((elm)->field.cqe_prev == (void *)(head))			\
		? ((head)->cqh_last)					\
		: ((elm)->field.cqe_prev))

#define LDAP_CIRCLEQ_MAKE_HEAD(head, elm, field) do {			\
	if ((elm)->field.cqe_prev != (void *)(head)) {			\
		(head)->cqh_first->field.cqe_prev = (head)->cqh_last;	\
		(head)->cqh_last->field.cqe_next = (head)->cqh_first;	\
		(head)->cqh_first = elm;				\
		(head)->cqh_last = (elm)->field.cqe_prev;		\
		(elm)->field.cqe_prev->field.cqe_next = (void *)(head);	\
		(elm)->field.cqe_prev = (void *)(head);			\
	}								\
} while (0)

#define LDAP_CIRCLEQ_MAKE_TAIL(head, elm, field) do {			\
	if ((elm)->field.cqe_next != (void *)(head)) {			\
		(head)->cqh_first->field.cqe_prev = (head)->cqh_last;	\
		(head)->cqh_last->field.cqe_next = (head)->cqh_first;	\
		(head)->cqh_first = (elm)->field.cqe_next;		\
		(head)->cqh_last = elm;					\
		(elm)->field.cqe_next->field.cqe_prev = (void *)(head);	\
		(elm)->field.cqe_next = (void *)(head);			\
	}								\
} while (0)

#endif /* !_LDAP_QUEUE_H_ */

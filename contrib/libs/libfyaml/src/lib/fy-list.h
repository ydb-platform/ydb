/*
 * fy-list.h - simple doubly linked list implementation
 *
 * Copyright (c) 2022 Innokentii Mokin <iam@justregular.dev>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_LIST_H
#define FY_LIST_H

#include <stddef.h>

#define fy_container_of(ptr, type, member) \
    ( (void)sizeof(0 ? (ptr) : &((type *)0)->member), \
      (type *)((char*)(ptr) - offsetof(type, member)) )

struct fy_list_head {
	struct fy_list_head *prev;
	struct fy_list_head *next;
};

static inline void fy_list_init_head(struct fy_list_head *lh) 
{
	lh->prev = lh;
	lh->next = lh;
}

static inline void fy_list_add_head(struct fy_list_head *ln, struct fy_list_head *lh)
{
	struct fy_list_head *second = lh->next;

	second->prev = ln;
	ln->next = second;
	lh->next = ln;
	ln->prev = lh;
}

static inline void fy_list_add_tail(struct fy_list_head *ln, struct fy_list_head *lh)
{
	struct fy_list_head *tail = lh->prev;

	lh->prev = ln;
	ln->next = lh;
	tail->next = ln;
	ln->prev = tail;
}

static inline bool fy_list_is_empty(struct fy_list_head *lh)
{
	return lh == lh->next;
}

static inline bool fy_list_is_singular(struct fy_list_head *lh)
{
	return lh != lh->next && lh == lh->next->next;
}

static inline void fy_list_del(struct fy_list_head *ln) {
	ln->prev->next = ln->next;
	ln->next->prev = ln->prev;
	ln->prev = NULL;
	ln->next = NULL;
}

static inline void fy_list_splice(struct fy_list_head *nlh, struct fy_list_head *lh) {
	struct fy_list_head *prev = lh, *next = lh->next,
						*head = nlh->next, *tail = nlh->prev;

	if (nlh == nlh->next) {
		return;
	}

	head->prev = prev;
	tail->next = next;
	prev->next = head;
	next->prev = tail;
}

#endif

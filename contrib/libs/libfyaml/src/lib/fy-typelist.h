/*
 * fy-typelist.h - typed list method builders
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_TYPELIST_H
#define FY_TYPELIST_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

#include <libfyaml.h>

#include "fy-list.h"

/* declare type methods */

#define FY_TYPE_FWD_DECL_LIST(_type) \
/* type safe list wrapper */ \
struct fy_ ## _type ## _list { struct fy_list_head _lh; }; \
\
struct __useless_struct_to_allow_semicolon

#define FY_TYPE_DECL_LIST(_type) \
static inline void fy_ ## _type ## _list_init(struct fy_ ## _type ## _list *_l) \
{ \
	fy_list_init_head(&_l->_lh); \
} \
static inline void fy_ ## _type ## _list_add(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (_l && _n) \
		fy_list_add_head(&_n->node, &_l->_lh); \
} \
static inline void fy_ ## _type ## _list_add_tail(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (_l && _n) \
		fy_list_add_tail(&_n->node, &_l->_lh); \
} \
static inline void fy_ ## _type ## _list_push(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (_l && _n) \
		fy_ ## _type ## _list_add(_l, _n); \
} \
static inline void fy_ ## _type ## _list_push_tail(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (_l && _n) \
		fy_ ## _type ## _list_add_tail(_l, _n); \
} \
static inline bool fy_ ## _type ## _list_empty(struct fy_ ## _type ## _list *_l) \
{ \
	return _l ? fy_list_is_empty(&_l->_lh) : true; \
} \
static inline bool fy_ ## _type ## _list_is_singular(struct fy_ ## _type ## _list *_l) \
{ \
	return _l ? fy_list_is_singular(&_l->_lh) : true; \
} \
static inline void fy_ ## _type ## _list_del(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (_l && _n) { \
		fy_list_del(&_n->node); \
		fy_list_init_head(&_n->node); \
	} \
} \
static inline void fy_ ## _type ## _list_insert_after(struct fy_ ## _type ## _list *_l, \
		struct fy_ ## _type *_p, struct fy_ ## _type *_n) \
{ \
	if (_l && _p && _n) \
		fy_list_add_head(&_n->node, &_p->node); \
} \
static inline void fy_ ## _type ## _list_insert_before(struct fy_ ## _type ## _list *_l, \
		struct fy_ ## _type *_p, struct fy_ ## _type *_n) \
{ \
	if (_l && _p && _n) \
		fy_list_add_tail(&_n->node, &_p->node); \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _list_head(struct fy_ ## _type ## _list *_l) \
{ \
	return !fy_ ## _type ## _list_empty(_l) ? fy_container_of(_l->_lh.next, struct fy_ ## _type, node) : NULL; \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _list_tail(struct fy_ ## _type ## _list *_l) \
{ \
	return !fy_ ## _type ## _list_empty(_l) ? fy_container_of(_l->_lh.prev, struct fy_ ## _type, node) : NULL; \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _list_first(struct fy_ ## _type ## _list *_l) \
{ \
	return fy_ ## _type ## _list_head(_l); \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _list_last(struct fy_ ## _type ## _list *_l) \
{ \
	return fy_ ## _type ## _list_tail(_l); \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _list_pop(struct fy_ ## _type ## _list *_l) \
{ \
	struct fy_ ## _type *_n; \
	\
	_n = fy_ ## _type ## _list_head(_l); \
	if (!_n) \
		return NULL; \
	fy_ ## _type ## _list_del(_l, _n); \
	return _n; \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _list_pop_tail(struct fy_ ## _type ## _list *_l) \
{ \
	struct fy_ ## _type *_n; \
	\
	_n = fy_ ## _type ## _list_tail(_l); \
	if (!_n) \
		return NULL; \
	fy_ ## _type ## _list_del(_l, _n); \
	return _n; \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _next(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (!_n || !_l || _n->node.next == &_l->_lh) \
		return NULL; \
	return fy_container_of(_n->node.next, struct fy_ ## _type, node); \
} \
static inline struct fy_ ## _type *fy_ ## _type ## _prev(struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n) \
{ \
	if (!_n || !_l || _n->node.prev == &_l->_lh) \
		return NULL; \
	return fy_container_of(_n->node.prev, struct fy_ ## _type, node); \
} \
static inline void fy_ ## _type ## _lists_splice( \
		struct fy_ ## _type ## _list *_l, \
		struct fy_ ## _type ## _list *_lfrom) \
{ \
	/* check arguments for sanity and lists are not empty */ \
	if (!_l || !_lfrom || \
		fy_ ## _type ## _list_empty(_lfrom)) \
		return; \
	fy_list_splice(&_lfrom->_lh, &_l->_lh); \
} \
static inline void fy_ ## _type ## _list_splice_after( \
		struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n, \
		struct fy_ ## _type ## _list *_lfrom) \
{ \
	/* check arguments for sanity and lists are not empty */ \
	if (!_l || !_n || !_lfrom || \
		fy_ ## _type ## _list_empty(_lfrom)) \
		return; \
	fy_list_splice(&_lfrom->_lh, &_n->node); \
} \
static inline void fy_ ## _type ## _list_splice_before( \
		struct fy_ ## _type ## _list *_l, struct fy_ ## _type *_n, \
		struct fy_ ## _type ## _list *_lfrom) \
{ \
	/* check arguments for sanity and lists are not empty */ \
	 if (!_l || !_n || !_lfrom || \
		fy_ ## _type ## _list_empty(_lfrom)) \
		return; \
	fy_list_splice(&_lfrom->_lh, _n->node.prev); \
} \
struct __useless_struct_to_allow_semicolon

#endif

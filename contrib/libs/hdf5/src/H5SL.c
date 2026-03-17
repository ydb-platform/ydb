/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:	Provides a skip list abstract data type.
 *
 *              (See "Deterministic Skip Lists" by Munro, Papadakis & Sedgewick)
 *
 *              (Implementation changed to a deterministic skip list from a
 *               probabilistic one.  This implementation uses a 1-2-3 skip list
 *               using arrays, as described by Munro, Papadakis & Sedgewick.
 *
 *               Arrays are allocated using a free list factory for each size
 *               that is a power of two.  Factories are created as soon as they
 *               are needed, and are never destroyed until the package is shut
 *               down.  There is no longer a maximum level or "p" value.
 *               -NAF 2008/11/05)
 *
 *              (See "Skip Lists: A Probabilistic Alternative to Balanced Trees"
 *               by William Pugh for additional information)
 *
 *              (This implementation has the optimization for reducing key
 *               key comparisons mentioned in section 3.5 of "A Skip List
 *               Cookbook" by William Pugh
 *              -Removed as our implementation of this was useless for a 1-2-3
 *               skip list.  The implementation in that document hurts
 *               performance, at least for integer keys. -NAF)
 *
 *              (Note: This implementation does not have the information for
 *               implementing the "Linear List Operations" (like insert/delete/
 *               search by position) in section 3.4 of "A Skip List Cookbook",
 *               but they shouldn't be that hard to add, if necessary)
 *
 *              (This implementation has an additional backward pointer, which
 *               allows the list to be iterated in reverse)
 *
 *              (There's also an article on "Alternating Skip Lists", which
 *              are similar to deterministic skip lists, in the August 2000
 *              issue of Dr. Dobb's Journal)
 *
 */

#include "H5SLmodule.h" /* This source code file is part of the H5SL module */

/* Private headers needed */
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5SLprivate.h" /* Skip list routines			*/

/* Local Macros */

/* Define the code template for searches for the "OP" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_SEARCH_FOUND(SLIST, X, I)                                                                \
    {                                                                                                        \
        HGOTO_DONE(X->item);                                                                                 \
    }

/* Define the code template for finds for the "OP" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_FIND_FOUND(SLIST, X, I)                                                                  \
    {                                                                                                        \
        HGOTO_DONE(X);                                                                                       \
    }

/* Define a code template for comparing scalar keys for the "CMP" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_SCALAR_CMP(SLIST, TYPE, PNODE, PKEY, HASHVAL) (*(TYPE *)((PNODE)->key) < *(TYPE *)PKEY)

/* Define a code template for comparing string keys for the "CMP" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_STRING_CMP(SLIST, TYPE, PNODE, PKEY, HASHVAL)                                            \
    (((PNODE)->hashval == HASHVAL) ? (strcmp((const char *)(PNODE)->key, (const char *)PKEY) < 0)            \
                                   : ((PNODE)->hashval < HASHVAL))

/* Define a code template for comparing H5_obj_t keys for the "CMP" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_OBJ_CMP(SLIST, TYPE, PNODE, PKEY, HASHVAL)                                               \
    ((((TYPE *)((PNODE)->key))->fileno == ((TYPE *)PKEY)->fileno)                                            \
         ? (((TYPE *)((PNODE)->key))->addr < ((TYPE *)PKEY)->addr)                                           \
         : (((TYPE *)((PNODE)->key))->fileno < ((TYPE *)PKEY)->fileno))

/* Define a code template for comparing generic keys for the "CMP" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_GENERIC_CMP(SLIST, TYPE, PNODE, PKEY, HASHVAL)                                           \
    ((SLIST)->cmp((TYPE *)((PNODE)->key), (TYPE *)PKEY) < 0)

/* Define a code template for comparing scalar keys for the "EQ" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_SCALAR_EQ(SLIST, TYPE, PNODE, PKEY, HASHVAL) (*(TYPE *)((PNODE)->key) == *(TYPE *)PKEY)

/* Define a code template for comparing string keys for the "EQ" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_STRING_EQ(SLIST, TYPE, PNODE, PKEY, HASHVAL)                                             \
    (((PNODE)->hashval == HASHVAL) && (strcmp((const char *)(PNODE)->key, (const char *)PKEY) == 0))

/* Define a code template for comparing H5_obj_t keys for the "EQ" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_OBJ_EQ(SLIST, TYPE, PNODE, PKEY, HASHVAL)                                                \
    ((((TYPE *)((PNODE)->key))->fileno == ((TYPE *)PKEY)->fileno) &&                                         \
     (((TYPE *)((PNODE)->key))->addr == ((TYPE *)PKEY)->addr))

/* Define a code template for comparing generic keys for the "EQ" in the H5SL_LOCATE macro */
#define H5SL_LOCATE_GENERIC_EQ(SLIST, TYPE, PNODE, PKEY, HASHVAL)                                            \
    ((SLIST)->cmp((TYPE *)((PNODE)->key), (TYPE *)PKEY) == 0)

/* Define a code template for initializing the hash value for scalar keys for the "HASHINIT" in the
 * H5SL_LOCATE macro */
#define H5SL_LOCATE_SCALAR_HASHINIT(KEY, HASHVAL)

/* Define a code template for initializing the hash value for string keys for the "HASHINIT" in the
 * H5SL_LOCATE macro */
#define H5SL_LOCATE_STRING_HASHINIT(KEY, HASHVAL) HASHVAL = H5_hash_string((const char *)KEY);

/* Define a code template for initializing the hash value for H5_obj_t keys for the "HASHINIT" in the
 * H5SL_LOCATE macro */
#define H5SL_LOCATE_OBJ_HASHINIT(KEY, HASHVAL)

/* Define a code template for initializing the hash value for generic keys for the "HASHINIT" in the
 * H5SL_LOCATE macro */
#define H5SL_LOCATE_GENERIC_HASHINIT(KEY, HASHVAL)

/* Macro used to find node for operation, if all keys are valid */
#define H5SL_LOCATE_OPT(OP, CMP, SLIST, X, TYPE, KEY, HASHVAL)                                               \
    {                                                                                                        \
        int      _i;     /* Local index variable */                                                          \
        unsigned _count; /* Num nodes searched at this height */                                             \
                                                                                                             \
        H5_GLUE3(H5SL_LOCATE_, CMP, _HASHINIT)                                                               \
        (KEY, HASHVAL) for (_i = (int)SLIST->curr_level; _i >= 0; _i--)                                      \
        {                                                                                                    \
            _count = 0;                                                                                      \
            while (_count < 3 && X->forward[_i] &&                                                           \
                   H5_GLUE3(H5SL_LOCATE_, CMP, _CMP)(SLIST, TYPE, X->forward[_i], KEY, HASHVAL)) {           \
                X = X->forward[_i];                                                                          \
                _count++;                                                                                    \
            }                                                                                                \
        }                                                                                                    \
        X = X->forward[0];                                                                                   \
        if (X != NULL && H5_GLUE3(H5SL_LOCATE_, CMP, _EQ)(SLIST, TYPE, X, KEY, HASHVAL)) {                   \
            /* What to do when a node is found */                                                            \
            H5_GLUE3(H5SL_LOCATE_, OP, _FOUND)(SLIST, X, _i)                                                 \
        }                                                                                                    \
    }

/* Macro used to find node for operation */
#define H5SL_LOCATE(OP, CMP, SLIST, X, TYPE, KEY, HASHVAL)                                                   \
    {                                                                                                        \
        H5SL_LOCATE_OPT(OP, CMP, SLIST, X, TYPE, KEY, HASHVAL)                                               \
    }

/* Macro used to grow a node by 1.  Does not update pointers. LVL is the current
 * level of X.  Does not update LVL but does update X->lvl. */
#define H5SL_GROW(X, LVL, ERR)                                                                               \
    {                                                                                                        \
        /* Check if we need to increase allocation of forward pointers */                                    \
        if (LVL + 1 >= ((size_t)1) << X->log_nalloc) {                                                       \
            H5SL_node_t **_tmp;                                                                              \
            assert(LVL + 1 == ((size_t)1) << X->log_nalloc);                                                 \
            /* Double the amount of allocated space */                                                       \
            X->log_nalloc++;                                                                                 \
                                                                                                             \
            /* Check if we need to create a new factory */                                                   \
            if (X->log_nalloc >= H5SL_fac_nused_g) {                                                         \
                assert(X->log_nalloc == H5SL_fac_nused_g);                                                   \
                                                                                                             \
                /* Check if we need to allocate space for the factory pointer*/                              \
                if (H5SL_fac_nused_g >= H5SL_fac_nalloc_g) {                                                 \
                    assert(H5SL_fac_nused_g == H5SL_fac_nalloc_g);                                           \
                    /* Double the size of the array of factory pointers */                                   \
                    H5SL_fac_nalloc_g *= 2;                                                                  \
                    if (NULL == (H5SL_fac_g = (H5FL_fac_head_t **)H5MM_realloc(                              \
                                     (void *)H5SL_fac_g, H5SL_fac_nalloc_g * sizeof(H5FL_fac_head_t *))))    \
                        HGOTO_ERROR(H5E_SLIST, H5E_CANTALLOC, ERR, "memory allocation failed");              \
                }                                                                                            \
                                                                                                             \
                /* Create the new factory */                                                                 \
                H5SL_fac_g[H5SL_fac_nused_g] =                                                               \
                    H5FL_fac_init((((size_t)1) << H5SL_fac_nused_g) * sizeof(H5SL_node_t *));                \
                H5SL_fac_nused_g++;                                                                          \
            }                                                                                                \
                                                                                                             \
            /* Allocate space for new forward pointers */                                                    \
            if (NULL == (_tmp = (H5SL_node_t **)H5FL_FAC_MALLOC(H5SL_fac_g[X->log_nalloc])))                 \
                HGOTO_ERROR(H5E_SLIST, H5E_CANTALLOC, ERR, "memory allocation failed");                      \
            H5MM_memcpy((void *)_tmp, (const void *)X->forward, (LVL + 1) * sizeof(H5SL_node_t *));          \
            X->forward = (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[X->log_nalloc - 1], (void *)X->forward);   \
            X->forward = _tmp;                                                                               \
        }                                                                                                    \
                                                                                                             \
        X->level++;                                                                                          \
    }

/* Macro used to shrink a node by 1.  Does not update pointers.  LVL is the
 * current level of X.  Does not update LVL but does update X->level. */
#define H5SL_SHRINK(X, LVL)                                                                                  \
    {                                                                                                        \
        /* Check if we can reduce the allocation of forward pointers */                                      \
        if (LVL <= ((size_t)1) << (X->log_nalloc - 1)) {                                                     \
            H5SL_node_t **_tmp;                                                                              \
            assert(LVL == ((size_t)1) << (X->log_nalloc - 1));                                               \
            X->log_nalloc--;                                                                                 \
                                                                                                             \
            /* Allocate space for new forward pointers */                                                    \
            if (NULL == (_tmp = (H5SL_node_t **)H5FL_FAC_MALLOC(H5SL_fac_g[X->log_nalloc])))                 \
                HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, NULL, "memory allocation failed");                       \
            H5MM_memcpy((void *)_tmp, (const void *)X->forward, (LVL) * sizeof(H5SL_node_t *));              \
            X->forward = (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[X->log_nalloc + 1], (void *)X->forward);   \
            X->forward = _tmp;                                                                               \
        }                                                                                                    \
                                                                                                             \
        X->level--;                                                                                          \
    }

/* Macro used to grow the level of a node by 1, with appropriate changes to the
 * head node if necessary.  PREV is the previous node of the height that X is to
 * grow to. */
#define H5SL_PROMOTE(SLIST, X, PREV, ERR)                                                                    \
    do {                                                                                                     \
        size_t _lvl = X->level;                                                                              \
                                                                                                             \
        H5SL_GROW(X, _lvl, ERR);                                                                             \
                                                                                                             \
        if (_lvl == (size_t)SLIST->curr_level) {                                                             \
            assert(PREV == SLIST->header);                                                                   \
            /* Grow the head */                                                                              \
            H5SL_GROW(PREV, _lvl, ERR)                                                                       \
            SLIST->curr_level++;                                                                             \
            X->forward[_lvl + 1] = NULL;                                                                     \
        }                                                                                                    \
        else {                                                                                               \
            assert(_lvl < (size_t)SLIST->curr_level);                                                        \
            X->forward[_lvl + 1] = PREV->forward[_lvl + 1];                                                  \
        }                                                                                                    \
        PREV->forward[_lvl + 1] = X;                                                                         \
    } while (0)

/* Macro used to reduce the level of a node by 1.  Does not update the head node
 * "current level".  PREV is the previous node of the current height of X. */
#define H5SL_DEMOTE(X, PREV)                                                                                 \
    {                                                                                                        \
        size_t _lvl = X->level;                                                                              \
                                                                                                             \
        assert(PREV->forward[_lvl] == X);                                                                    \
        PREV->forward[_lvl] = X->forward[_lvl];                                                              \
        H5SL_SHRINK(X, _lvl);                                                                                \
    }

/* Macro used to insert node.  Does not actually insert the node.  After running
 * this macro, X will contain the node before where the new node should be
 * inserted (at level 0). */
#define H5SL_INSERT(CMP, SLIST, X, TYPE, KEY, HASHVAL)                                                       \
    {                                                                                                        \
        H5SL_node_t *_last = X;    /* Lowest node in the current gap */                                      \
        H5SL_node_t *_next = NULL; /* Highest node in the current gap */                                     \
        H5SL_node_t *_drop;        /* Low node of the gap to drop into */                                    \
        int          _count;       /* Number of nodes in the current gap */                                  \
        int          _i;                                                                                     \
                                                                                                             \
        H5_GLUE3(H5SL_LOCATE_, CMP, _HASHINIT)                                                               \
        (KEY, HASHVAL) for (_i = (int)SLIST->curr_level; _i >= 0; _i--)                                      \
        {                                                                                                    \
            /* Search for the node to drop into, also count the number of nodes */                           \
            /* of height _i in this gap */                                                                   \
            _drop = NULL;                                                                                    \
            for (_count = 0;; _count++) {                                                                    \
                /* Terminate if this is the last node in the gap */                                          \
                if (X->forward[_i] == _next) {                                                               \
                    if (!_drop)                                                                              \
                        _drop = X;                                                                           \
                    break;                                                                                   \
                }                                                                                            \
                                                                                                             \
                /* Check if this node is the start of the next gap */                                        \
                if (!_drop && !H5_GLUE3(H5SL_LOCATE_, CMP, _CMP)(SLIST, TYPE, X->forward[_i], KEY, HASHVAL)) \
                    _drop = X;                                                                               \
                                                                                                             \
                /* No need to check the last node in the gap if there are 3, as */                           \
                /* there cannot be a fourth */                                                               \
                if (_count == 2) {                                                                           \
                    if (!_drop)                                                                              \
                        _drop = X->forward[_i];                                                              \
                    _count = 3;                                                                              \
                    break;                                                                                   \
                }                                                                                            \
                X = X->forward[_i];                                                                          \
            }                                                                                                \
            assert(!_drop->forward[_i] ||                                                                    \
                   !H5_GLUE3(H5SL_LOCATE_, CMP, _CMP)(SLIST, TYPE, _drop->forward[_i], KEY, HASHVAL));       \
                                                                                                             \
            /* Promote the middle node if necessary */                                                       \
            if (_count == 3) {                                                                               \
                assert(X == _last->forward[_i]->forward[_i]);                                                \
                H5SL_PROMOTE(SLIST, X, _last, NULL);                                                         \
            }                                                                                                \
                                                                                                             \
            /* Prepare to drop down */                                                                       \
            X = _last = _drop;                                                                               \
            _next     = _drop->forward[_i];                                                                  \
        }                                                                                                    \
                                                                                                             \
        if (_next && H5_GLUE3(H5SL_LOCATE_, CMP, _EQ)(SLIST, TYPE, _next, KEY, HASHVAL))                     \
            HGOTO_ERROR(H5E_SLIST, H5E_CANTINSERT, NULL, "can't insert duplicate key");                      \
    }

/* Macro used to remove node */
#define H5SL_REMOVE(CMP, SLIST, X, TYPE, KEY, HASHVAL)                                                       \
    {                                                                                                        \
        H5SL_node_t *_last  = X;             /* Lowest node in the current gap */                            \
        H5SL_node_t *_llast = X;             /* Lowest node in the previous gap */                           \
        H5SL_node_t *_next  = NULL;          /* Highest node in the current gap */                           \
        H5SL_node_t *_drop  = NULL;          /* Low node of the gap to drop into */                          \
        H5SL_node_t *_ldrop = NULL;          /* Low node of gap before the one to drop into */               \
        H5SL_node_t *_head  = SLIST->header; /* Head of the skip list */                                     \
        int          _count;                 /* Number of nodes in the current gap */                        \
        int          _i = (int)SLIST->curr_level;                                                            \
                                                                                                             \
        if (_i < 0)                                                                                          \
            HGOTO_DONE(NULL);                                                                                \
                                                                                                             \
        H5_GLUE3(H5SL_LOCATE_, CMP, _HASHINIT)                                                               \
        (KEY, HASHVAL)                                                                                       \
                                                                                                             \
            /* Find the gap to drop in to at the highest level */                                            \
            while (X && (!X->key || H5_GLUE3(H5SL_LOCATE_, CMP, _CMP)(SLIST, TYPE, X, KEY, HASHVAL)))        \
        {                                                                                                    \
            _llast = _last;                                                                                  \
            _last  = X;                                                                                      \
            X      = X->forward[_i];                                                                         \
        }                                                                                                    \
        _next = X;                                                                                           \
                                                                                                             \
        /* Main loop */                                                                                      \
        for (_i--; _i >= 0; _i--) {                                                                          \
            /* Search for the node to drop into, also count the number of */                                 \
            /* nodes of height _i in this gap and keep track of of the node */                               \
            /* before the one to drop into (_ldrop will become _llast, */                                    \
            /* _drop will become _last). */                                                                  \
            X = _ldrop = _last;                                                                              \
            _drop      = NULL;                                                                               \
            for (_count = 0;; _count++) {                                                                    \
                /* Terminate if this is the last node in the gap */                                          \
                if (X->forward[_i] == _next) {                                                               \
                    if (!_drop)                                                                              \
                        _drop = X;                                                                           \
                    break;                                                                                   \
                }                                                                                            \
                                                                                                             \
                /* If we have already found the node to drop into and there */                               \
                /* is more than one node in this gap, we can stop searching */                               \
                if (_drop) {                                                                                 \
                    assert(_count >= 1);                                                                     \
                    _count = 2;                                                                              \
                    break;                                                                                   \
                }                                                                                            \
                else { /* !_drop */                                                                          \
                    /* Check if this node is the start of the next gap */                                    \
                    if (!H5_GLUE3(H5SL_LOCATE_, CMP, _CMP)(SLIST, TYPE, X->forward[_i], KEY, HASHVAL)) {     \
                        _drop = X;                                                                           \
                        /* Again check if we can stop searching */                                           \
                        if (_count) {                                                                        \
                            _count = 2;                                                                      \
                            break;                                                                           \
                        }                                                                                    \
                    }                                                                                        \
                    else                                                                                     \
                        _ldrop = X;                                                                          \
                }                                                                                            \
                                                                                                             \
                /* No need to check the last node in the gap if there are */                                 \
                /* 3, as there cannot be a fourth */                                                         \
                if (_count == 2) {                                                                           \
                    if (!_drop)                                                                              \
                        _drop = X->forward[_i];                                                              \
                    break;                                                                                   \
                }                                                                                            \
                X = X->forward[_i];                                                                          \
            }                                                                                                \
            assert(_count >= 1 && _count <= 3);                                                              \
            assert(!_drop->forward[_i] ||                                                                    \
                   !H5_GLUE3(H5SL_LOCATE_, CMP, _CMP)(SLIST, TYPE, _drop->forward[_i], KEY, HASHVAL));       \
                                                                                                             \
            /* Check if we need to adjust node heights */                                                    \
            if (_count == 1) {                                                                               \
                /* Check if we are in the first gap */                                                       \
                if (_llast == _last) {                                                                       \
                    /* We are in the first gap, count the number of nodes */                                 \
                    /* of height _i in the next gap.  We need only check */                                  \
                    /* onenode to see if we should promote the first node */                                 \
                    /* in the next gap */                                                                    \
                    _llast = _next->forward[_i + 1];                                                         \
                                                                                                             \
                    /* Demote the separator node */                                                          \
                    H5SL_DEMOTE(_next, _last)                                                                \
                                                                                                             \
                    /* If there are 2 or more nodes, promote the first */                                    \
                    if (_next->forward[_i]->forward[_i] != _llast) {                                         \
                        X = _next->forward[_i];                                                              \
                        H5SL_PROMOTE(SLIST, X, _last, NULL);                                                 \
                    }                                                                                        \
                    else if (!_head->forward[_i + 1]) {                                                      \
                        /* shrink the header */                                                              \
                        assert(_i == SLIST->curr_level - 1);                                                 \
                        assert((size_t)SLIST->curr_level == _head->level);                                   \
                                                                                                             \
                        H5SL_SHRINK(_head, (size_t)(_i + 1))                                                 \
                        SLIST->curr_level--;                                                                 \
                    }                                                                                        \
                }                                                                                            \
                else {                                                                                       \
                    /* We are not in the first gap, count the number of */                                   \
                    /* nodes of height _i in the previous gap.  Note we */                                   \
                    /* "look ahead" in this loop so X has the value of the */                                \
                    /* last node in the previous gap. */                                                     \
                    X = _llast->forward[_i];                                                                 \
                    for (_count = 1; _count < 3 && X->forward[_i] != _last; _count++)                        \
                        X = X->forward[_i];                                                                  \
                    assert(X->forward[_i] == _last);                                                         \
                                                                                                             \
                    /* Demote the separator node */                                                          \
                    H5SL_DEMOTE(_last, _llast)                                                               \
                                                                                                             \
                    /* If there are 2 or more nodes, promote the last */                                     \
                    if (_count >= 2)                                                                         \
                        H5SL_PROMOTE(SLIST, X, _llast, NULL);                                                \
                    else if (!_head->forward[_i + 1]) {                                                      \
                        /* shrink the header */                                                              \
                        assert(_i == SLIST->curr_level - 1);                                                 \
                        assert((size_t)SLIST->curr_level == _head->level);                                   \
                                                                                                             \
                        H5SL_SHRINK(_head, (size_t)(_i + 1))                                                 \
                        SLIST->curr_level--;                                                                 \
                    }                                                                                        \
                }                                                                                            \
            }                                                                                                \
                                                                                                             \
            /* Prepare to drop down */                                                                       \
            _llast = _ldrop;                                                                                 \
            _last  = _drop;                                                                                  \
            _next  = _drop->forward[_i];                                                                     \
        }                                                                                                    \
                                                                                                             \
        /* Check if we've found the node */                                                                  \
        if (_next && H5_GLUE3(H5SL_LOCATE_, CMP, _EQ)(SLIST, TYPE, _next, KEY, HASHVAL)) {                   \
            void *tmp = _next->item;                                                                         \
            X         = _next;                                                                               \
                                                                                                             \
            /* If the node has a height > 0, swap it with its (lower) */                                     \
            /* neighbor */                                                                                   \
            if (X->level) {                                                                                  \
                X              = X->backward;                                                                \
                _next->key     = X->key;                                                                     \
                _next->item    = X->item;                                                                    \
                _next->hashval = X->hashval;                                                                 \
            }                                                                                                \
            assert(!X->level);                                                                               \
                                                                                                             \
            /* Remove the node */                                                                            \
            X->backward->forward[0] = X->forward[0];                                                         \
            if (SLIST->last == X)                                                                            \
                SLIST->last = X->backward;                                                                   \
            else                                                                                             \
                X->forward[0]->backward = X->backward;                                                       \
            SLIST->nobjs--;                                                                                  \
            X->forward = (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[0], X->forward);                           \
            X          = H5FL_FREE(H5SL_node_t, X);                                                          \
                                                                                                             \
            HGOTO_DONE(tmp);                                                                                 \
        }                                                                                                    \
    }

/* Macro used to search for node */
#define H5SL_SEARCH(CMP, SLIST, X, TYPE, KEY, HASHVAL) H5SL_LOCATE(SEARCH, CMP, SLIST, X, TYPE, KEY, HASHVAL)

/* Macro used to find a node */
#define H5SL_FIND(CMP, SLIST, X, TYPE, KEY, HASHVAL) H5SL_LOCATE(FIND, CMP, SLIST, X, TYPE, KEY, HASHVAL)

/* Private typedefs & structs */

/* Skip list node data structure */
struct H5SL_node_t {
    const void          *key;        /* Pointer to node's key */
    void                *item;       /* Pointer to node's item */
    size_t               level;      /* The level of this node */
    size_t               log_nalloc; /* log2(Number of slots allocated in forward) */
    uint32_t             hashval;    /* Hash value for key (only for strings, currently) */
    struct H5SL_node_t **forward;    /* Array of forward pointers from this node */
    struct H5SL_node_t  *backward;   /* Backward pointer from this node */
};

/* Main skip list data structure */
struct H5SL_t {
    /* Static values for each list */
    H5SL_type_t type; /* Type of skip list */
    H5SL_cmp_t  cmp;  /* Comparison callback, if type is H5SL_TYPE_GENERIC */

    /* Dynamic values for each list */
    int          curr_level; /* Current top level used in list */
    size_t       nobjs;      /* Number of active objects in skip list */
    H5SL_node_t *header;     /* Header for nodes in skip list */
    H5SL_node_t *last;       /* Pointer to last node in skip list */
};

/* Static functions */
static H5SL_node_t *H5SL__new_node(void *item, const void *key, uint32_t hashval);
static H5SL_node_t *H5SL__insert_common(H5SL_t *slist, void *item, const void *key);
static herr_t       H5SL__release_common(H5SL_t *slist, H5SL_operator_t op, void *op_data);
static herr_t       H5SL__close_common(H5SL_t *slist, H5SL_operator_t op, void *op_data);

/* Declare a free list to manage the H5SL_t struct */
H5FL_DEFINE_STATIC(H5SL_t);

/* Declare a free list to manage the H5SL_node_t struct */
H5FL_DEFINE_STATIC(H5SL_node_t);

/* Global variables */
static H5FL_fac_head_t **H5SL_fac_g;
static size_t            H5SL_fac_nused_g;
static size_t            H5SL_fac_nalloc_g;

/*-------------------------------------------------------------------------
 * Function:    H5SL_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5SL_init(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOERR

    /* Allocate space for array of factories */
    H5SL_fac_g = (H5FL_fac_head_t **)H5MM_malloc(sizeof(H5FL_fac_head_t *));
    assert(H5SL_fac_g);
    H5SL_fac_nalloc_g = 1;

    /* Initialize first factory */
    H5SL_fac_g[0] = H5FL_fac_init(sizeof(H5SL_node_t *));
    assert(H5SL_fac_g[0]);
    H5SL_fac_nused_g = 1;

    FUNC_LEAVE_NOAPI(ret_value)
}

/*--------------------------------------------------------------------------
 NAME
    H5SL_term_package
 PURPOSE
    Terminate all the H5FL factories used in this package, and clear memory
 USAGE
    int H5SL_term_package()
 RETURNS
    Success:	Positive if any action might have caused a change in some
                other interface; zero otherwise.
        Failure:	Negative
 DESCRIPTION
    Release any resources allocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Can't report errors...
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5SL_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Terminate all the factories */
    if (H5SL_fac_nused_g > 0) {
        size_t                       i;
        herr_t H5_ATTR_NDEBUG_UNUSED ret;

        for (i = 0; i < H5SL_fac_nused_g; i++) {
            ret = H5FL_fac_term(H5SL_fac_g[i]);
            assert(ret >= 0);
        }
        H5SL_fac_nused_g = 0;

        n++;
    }

    /* Free the list of factories */
    if (H5SL_fac_g) {
        H5SL_fac_g        = (H5FL_fac_head_t **)H5MM_xfree((void *)H5SL_fac_g);
        H5SL_fac_nalloc_g = 0;

        n++;
    }

    FUNC_LEAVE_NOAPI(n)
} /* H5SL_term_package() */

/*--------------------------------------------------------------------------
 NAME
    H5SL__new_node
 PURPOSE
    Create a new skip list node of level 0
 USAGE
    H5SL_node_t *H5SL__new_node(item,key,hasval)
        void *item;             IN: Pointer to item info for node
        void *key;              IN: Pointer to key info for node
        uint32_t hashval;       IN: Hash value for node

 RETURNS
    Returns a pointer to a skip list node on success, NULL on failure.
 DESCRIPTION
    Create a new skip list node of the height 0, setting the item
    and key values internally.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine does _not_ initialize the 'forward' pointers
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5SL_node_t *
H5SL__new_node(void *item, const void *key, uint32_t hashval)
{
    H5SL_node_t *ret_value = NULL; /* New skip list node */

    FUNC_ENTER_PACKAGE

    /* Allocate the node */
    if (NULL == (ret_value = (H5SL_node_t *)H5FL_MALLOC(H5SL_node_t)))
        HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Initialize node */
    ret_value->key     = key;
    ret_value->item    = item;
    ret_value->level   = 0;
    ret_value->hashval = hashval;
    if (NULL == (ret_value->forward = (H5SL_node_t **)H5FL_FAC_MALLOC(H5SL_fac_g[0]))) {
        ret_value = H5FL_FREE(H5SL_node_t, ret_value);
        HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, NULL, "memory allocation failed");
    }
    ret_value->log_nalloc = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL__new_node() */

/*--------------------------------------------------------------------------
 NAME
    H5SL__insert_common
 PURPOSE
    Common code for inserting an object into a skip list
 USAGE
    H5SL_node_t *H5SL__insert_common(slist,item,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *item;             IN: Item to insert
        void *key;              IN: Key for item to insert

 RETURNS
    Returns pointer to new node on success, NULL on failure.
 DESCRIPTION
    Common code for inserting an element into a skip list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Inserting an item with the same key as an existing object fails.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5SL_node_t *
H5SL__insert_common(H5SL_t *slist, void *item, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    H5SL_node_t *prev;             /* Node before the new node */
    uint32_t     hashval   = 0;    /* Hash value for key */
    H5SL_node_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    prev = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_INSERT(SCALAR, slist, prev, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_INSERT(SCALAR, slist, prev, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_INSERT(STRING, slist, prev, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_INSERT(SCALAR, slist, prev, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_INSERT(SCALAR, slist, prev, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_INSERT(SCALAR, slist, prev, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_INSERT(OBJ, slist, prev, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_INSERT(SCALAR, slist, prev, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_INSERT(GENERIC, slist, prev, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* 'key' must not have been found in existing list, if we get here */

    if (slist->curr_level < 0)
        slist->curr_level = 0;

    /* Create new node of level 0 */
    if (NULL == (x = H5SL__new_node(item, key, hashval)))
        HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, NULL, "can't create new skip list node");

    /* Update the links */
    x->backward      = prev;
    x->forward[0]    = prev->forward[0];
    prev->forward[0] = x;
    if (x->forward[0])
        x->forward[0]->backward = x;
    else {
        assert(slist->last == prev);
        slist->last = x;
    }

    /* Increment the number of nodes in the skip list */
    slist->nobjs++;

    /* Set return value */
    ret_value = x;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL__insert_common() */

/*--------------------------------------------------------------------------
 NAME
    H5SL__release_common
 PURPOSE
    Release all nodes from a skip list, optionally calling a 'free' operator
 USAGE
    herr_t H5SL__release_common(slist,op,opdata)
        H5SL_t *slist;            IN/OUT: Pointer to skip list to release nodes
        H5SL_operator_t op;     IN: Callback function to free item & key
        void *op_data;          IN/OUT: Pointer to application data for callback

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Release all the nodes in a skip list.  The 'op' routine is called for
    each node in the list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    The return value from the 'op' routine is ignored.

    The skip list itself is still valid, it just has all its nodes removed.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5SL__release_common(H5SL_t *slist, H5SL_operator_t op, void *op_data)
{
    H5SL_node_t *node, *next_node; /* Pointers to skip list nodes */
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Free skip list nodes */
    node = slist->header->forward[0];
    while (node) {
        next_node = node->forward[0];

        /* Call callback, if one is given.
         *
         * Ignoring const here is fine as we only need the value to be const
         * with respect to the list code, which should never modify the
         * elements. The library code that is making use of the skip list
         * container can do what it likes with the elements.
         */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        if (op)
            (void)(op)(node->item, (void *)node->key, op_data);
        H5_GCC_CLANG_DIAG_ON("cast-qual")

        node->forward = (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[node->log_nalloc], node->forward);
        node          = H5FL_FREE(H5SL_node_t, node);
        node          = next_node;
    }

    /* Reset the header pointers */
    slist->header->forward =
        (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[slist->header->log_nalloc], slist->header->forward);
    if (NULL == (slist->header->forward = (H5SL_node_t **)H5FL_FAC_MALLOC(H5SL_fac_g[0])))
        HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, FAIL, "memory allocation failed");
    slist->header->forward[0] = NULL;
    slist->header->log_nalloc = 0;
    slist->header->level      = 0;

    /* Reset the last pointer */
    slist->last = slist->header;

    /* Reset the dynamic internal fields */
    slist->curr_level = -1;
    slist->nobjs      = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL__release_common() */

/*--------------------------------------------------------------------------
 NAME
    H5SL__close_common
 PURPOSE
    Close a skip list, deallocating it and potentially freeing all its nodes.
 USAGE
    herr_t H5SL__close_common(slist,op,opdata)
        H5SL_t *slist;          IN/OUT: Pointer to skip list to close
        H5SL_operator_t op;     IN: Callback function to free item & key
        void *op_data;          IN/OUT: Pointer to application data for callback

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Close a skip list, freeing all internal information.  Any objects left in
    the skip list have the 'op' routine called for each.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    If the 'op' routine returns non-zero, only the nodes up to that
    point in the list are released and the list is still valid.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5SL__close_common(H5SL_t *slist, H5SL_operator_t op, void *op_data)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Free skip list nodes */
    if (H5SL__release_common(slist, op, op_data) < 0)
        HGOTO_ERROR(H5E_SLIST, H5E_CANTFREE, FAIL, "can't release skip list nodes");

    /* Release header node */
    slist->header->forward =
        (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[slist->header->log_nalloc], slist->header->forward);
    slist->header = H5FL_FREE(H5SL_node_t, slist->header);

    /* Free skip list object */
    slist = H5FL_FREE(H5SL_t, slist);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL__close_common() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_create
 PURPOSE
    Create a skip list
 USAGE
    H5SL_t *H5SL_create(H5SL_type_t type, H5SL_cmp_t cmp)

 RETURNS
    Returns a pointer to a skip list on success, NULL on failure.
 DESCRIPTION
    Create a skip list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_t *
H5SL_create(H5SL_type_t type, H5SL_cmp_t cmp)
{
    H5SL_t      *new_slist = NULL; /* Pointer to new skip list object created */
    H5SL_node_t *header;           /* Pointer to skip list header node */
    H5SL_t      *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Check args */
    assert(type >= H5SL_TYPE_INT && type <= H5SL_TYPE_GENERIC);

    /* Allocate skip list structure */
    if (NULL == (new_slist = H5FL_MALLOC(H5SL_t)))
        HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set the static internal fields */
    new_slist->type = type;
    assert((type == H5SL_TYPE_GENERIC) == !!cmp);
    new_slist->cmp = cmp;

    /* Set the dynamic internal fields */
    new_slist->curr_level = -1;
    new_slist->nobjs      = 0;

    /* Allocate the header node */
    if (NULL == (header = H5SL__new_node(NULL, NULL, (uint32_t)ULONG_MAX)))
        HGOTO_ERROR(H5E_SLIST, H5E_NOSPACE, NULL, "can't create new skip list node");

    /* Initialize header node's forward pointer */
    header->forward[0] = NULL;

    /* Initialize header node's backward pointer */
    header->backward = NULL;

    /* Attach the header */
    new_slist->header = header;
    new_slist->last   = header;

    /* Set the return value */
    ret_value = new_slist;

done:
    /* Error cleanup */
    if (ret_value == NULL) {
        if (new_slist != NULL)
            new_slist = H5FL_FREE(H5SL_t, new_slist);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_create() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_count
 PURPOSE
    Count the number of objects in a skip list
 USAGE
    size_t H5SL_count(slist)
        H5SL_t *slist;            IN: Pointer to skip list to count

 RETURNS
    Returns number of objects on success, can't fail
 DESCRIPTION
    Count elements in a skip list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
size_t
H5SL_count(H5SL_t *slist)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    FUNC_LEAVE_NOAPI(slist->nobjs)
} /* end H5SL_count() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_insert
 PURPOSE
    Insert an object into a skip list
 USAGE
    herr_t H5SL_insert(slist,item,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *item;             IN: Item to insert
        void *key;              IN: Key for item to insert

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Insert element into a skip list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Inserting an item with the same key as an existing object fails.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5SL_insert(H5SL_t *slist, void *item, const void *key)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */
    if (NULL == H5SL__insert_common(slist, item, key))
        HGOTO_ERROR(H5E_SLIST, H5E_CANTINSERT, FAIL, "can't create new skip list node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_insert() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_add
 PURPOSE
    Insert an object into a skip list
 USAGE
    H5SL_node_t *H5SL_add(slist,item,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *item;             IN: Item to insert
        void *key;              IN: Key for item to insert

 RETURNS
    Returns pointer to new skip list node on success, NULL on failure.
 DESCRIPTION
    Insert element into a skip list and return the skip list node for the
    new element in the list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Inserting an item with the same key as an existing object fails.

    This routine is a useful starting point for next/prev calls
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_add(H5SL_t *slist, void *item, const void *key)
{
    H5SL_node_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */
    if (NULL == (ret_value = H5SL__insert_common(slist, item, key)))
        HGOTO_ERROR(H5E_SLIST, H5E_CANTINSERT, NULL, "can't create new skip list node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_add() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_remove
 PURPOSE
    Removes an object from a skip list
 USAGE
    void *H5SL_remove(slist,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to remove

 RETURNS
    Returns pointer to item removed on success, NULL on failure.
 DESCRIPTION
    Remove element from a skip list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5SL_remove(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Remove item from skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to remove
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_REMOVE(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_REMOVE(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_REMOVE(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_REMOVE(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_REMOVE(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_REMOVE(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_REMOVE(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_REMOVE(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_REMOVE(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_remove() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_remove_first
 PURPOSE
    Removes the first object from a skip list
 USAGE
    void *H5SL_remove_first(slist)
        H5SL_t *slist;          IN/OUT: Pointer to skip list

 RETURNS
    Returns pointer to item removed on success, NULL on failure.
 DESCRIPTION
    Remove first element from a skip list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5SL_remove_first(H5SL_t *slist)
{
    void        *ret_value = NULL;                      /* Return value             */
    H5SL_node_t *head      = slist->header;             /* Skip list header         */
    H5SL_node_t *tmp       = slist->header->forward[0]; /* Temporary node pointer   */
    H5SL_node_t *next;                                  /* Next node to search for  */
    size_t       level;                                 /* Skip list level          */
    size_t       i;                                     /* Index                    */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(slist);

    /* Assign level */
    H5_CHECK_OVERFLOW(slist->curr_level, int, size_t);
    level = (size_t)slist->curr_level;

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Remove item from skip list */

    /* Check for empty list */
    if (slist->last != slist->header) {

        /* Assign return value */
        ret_value = tmp->item;
        assert(level == head->level);
        assert(0 == tmp->level);

        /* Remove the first node */
        head->forward[0] = tmp->forward[0];
        if (slist->last == tmp)
            slist->last = head;
        else
            tmp->forward[0]->backward = head;
        slist->nobjs--;
        /* Free memory */
        tmp->forward = (H5SL_node_t **)H5FL_FAC_FREE(H5SL_fac_g[0], tmp->forward);
        tmp          = H5FL_FREE(H5SL_node_t, tmp);

        /* Reshape the skip list as necessary to maintain 1-2-3 condition */
        for (i = 0; i < level; i++) {
            next = head->forward[i + 1];
            assert(next);

            /* Check if  head->forward[i] == head->forward[i+1] (illegal) */
            if (head->forward[i] == next) {
                tmp  = next;
                next = next->forward[i + 1];

                assert(tmp->level == i + 1);

                /* Demote head->forward[i] */
                H5SL_DEMOTE(tmp, head)

                /* Check if we need to promote the following node to maintain
                 * 1-2-3 condition */
                if (tmp->forward[i]->forward[i] != next) {
                    assert(tmp->forward[i]->forward[i]->forward[i] == next ||
                           tmp->forward[i]->forward[i]->forward[i]->forward[i] == next);
                    tmp = tmp->forward[i];
                    H5SL_PROMOTE(slist, tmp, head, NULL);
                    /* In this case, since there is a node of height = i+1 here
                     * now (tmp), we know the skip list must be valid and can
                     * break */
                    break;
                }
                else if (!head->forward[i + 1]) {
                    /* We just shrunk the largest node, shrink the header */
                    assert(i == level - 1);

                    H5SL_SHRINK(head, level)
                    slist->curr_level--;
                }
            }
            else
                break;
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_remove_first() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_search
 PURPOSE
    Search for object in a skip list
 USAGE
    void *H5SL_search(slist,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to search for

 RETURNS
    Returns pointer to item on success, NULL on failure
 DESCRIPTION
    Search for an object in a skip list, according to it's key
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5SL_search(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_SEARCH(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_SEARCH(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_SEARCH(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_SEARCH(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_SEARCH(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_SEARCH(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_SEARCH(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_SEARCH(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_SEARCH(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* 'key' must not have been found in list, if we get here */
    ret_value = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_search() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_less
 PURPOSE
    Search for object in a skip list that is less than or equal to 'key'
 USAGE
    void *H5SL_less(slist,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to search for

 RETURNS
    Returns pointer to item who key is less than or equal to 'key' on success,
        NULL on failure
 DESCRIPTION
    Search for an object in a skip list, according to it's key, returning the
    object itself (for an exact match), or the object with the next highest
    key that is less than 'key'
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5SL_less(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_SEARCH(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_SEARCH(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_SEARCH(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_SEARCH(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_SEARCH(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_SEARCH(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_SEARCH(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_SEARCH(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_SEARCH(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* An exact match for 'key' must not have been found in list, if we get here */
    /* Check for a node with a key that is less than the given 'key' */
    if (x == NULL) {
        /* Check for walking off the list */
        if (slist->last != slist->header)
            ret_value = slist->last->item;
        else
            ret_value = NULL;
    }
    else {
        if (x->backward != slist->header)
            ret_value = x->backward->item;
        else
            ret_value = NULL;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_less() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_greater
 PURPOSE
    Search for object in a skip list that is greater than or equal to 'key'
 USAGE
    void *H5SL_greater(slist, key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to search for

 RETURNS
    Returns pointer to item who key is greater than or equal to 'key' on success,
        NULL on failure
 DESCRIPTION
    Search for an object in a skip list, according to it's key, returning the
    object itself (for an exact match), or the object with the next lowest
    key that is greater than 'key'
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5SL_greater(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_SEARCH(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_SEARCH(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_SEARCH(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_SEARCH(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_SEARCH(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_SEARCH(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_SEARCH(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_SEARCH(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_SEARCH(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* An exact match for 'key' must not have been found in list, if we get here */
    /* ('x' must be the next node with a key greater than the 'key', or NULL) */
    if (x)
        ret_value = x->item;
    else
        ret_value = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_greater() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_find
 PURPOSE
    Search for _node_ in a skip list
 USAGE
    H5SL_node_t *H5SL_node(slist,key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to search for

 RETURNS
    Returns pointer to _node_ matching key on success, NULL on failure
 DESCRIPTION
    Search for an object in a skip list, according to it's key and returns
    the node that the object is attached to
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine is a useful starting point for next/prev calls
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_find(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    H5SL_node_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_FIND(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_FIND(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_FIND(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_FIND(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_FIND(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_FIND(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_FIND(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_FIND(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_FIND(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* 'key' must not have been found in list, if we get here */
    ret_value = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_find() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_below
 PURPOSE
    Search for _node_ in a skip list whose object is less than or equal to 'key'
 USAGE
    H5SL_node_t *H5SL_below(slist, key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to search for

 RETURNS
    Returns pointer to _node_ who key is less than or equal to 'key' on success,
        NULL on failure
 DESCRIPTION
    Search for a node with an object in a skip list, according to it's key,
    returning the node itself (for an exact match), or the node with the next
    highest key that is less than 'key'
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_below(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    H5SL_node_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_FIND(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_FIND(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_FIND(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_FIND(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_FIND(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_FIND(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_FIND(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_FIND(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_FIND(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* An exact match for 'key' must not have been found in list, if we get here */
    /* Check for a node with a key that is less than the given 'key' */
    if (NULL == x) {
        /* Check for walking off the list */
        if (slist->last != slist->header)
            ret_value = slist->last;
        else
            ret_value = NULL;
    }
    else {
        if (x->backward != slist->header)
            ret_value = x->backward;
        else
            ret_value = NULL;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_below() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_above
 PURPOSE
    Search for _node_ in a skip list whose object is greater than or equal to 'key'
 USAGE
    H5SL_node_t *H5SL_above(slist, key)
        H5SL_t *slist;          IN/OUT: Pointer to skip list
        void *key;              IN: Key for item to search for

 RETURNS
    Returns pointer to _node_ with object that has a key is greater than or
        equal to 'key' on success, NULL on failure
 DESCRIPTION
    Search for a node with an object in a skip list, according to it's key,
    returning the node itself (for an exact match), or the node with the next
    lowest key that is greater than 'key'
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_above(H5SL_t *slist, const void *key)
{
    H5SL_node_t *x;                /* Current node to examine */
    uint32_t     hashval   = 0;    /* Hash value for key */
    H5SL_node_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);
    assert(key);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Insert item into skip list */

    /* Work through the forward pointers for a node, finding the node at each
     * level that is before the location to insert
     */
    x = slist->header;
    switch (slist->type) {
        case H5SL_TYPE_INT:
            H5SL_FIND(SCALAR, slist, x, const int, key, -)
            break;

        case H5SL_TYPE_HADDR:
            H5SL_FIND(SCALAR, slist, x, const haddr_t, key, -)
            break;

        case H5SL_TYPE_STR:
            H5SL_FIND(STRING, slist, x, char *, key, hashval)
            break;

        case H5SL_TYPE_HSIZE:
            H5SL_FIND(SCALAR, slist, x, const hsize_t, key, -)
            break;

        case H5SL_TYPE_UNSIGNED:
            H5SL_FIND(SCALAR, slist, x, const unsigned, key, -)
            break;

        case H5SL_TYPE_SIZE:
            H5SL_FIND(SCALAR, slist, x, const size_t, key, -)
            break;

        case H5SL_TYPE_OBJ:
            H5SL_FIND(OBJ, slist, x, const H5_obj_t, key, -)
            break;

        case H5SL_TYPE_HID:
            H5SL_FIND(SCALAR, slist, x, const hid_t, key, -)
            break;

        case H5SL_TYPE_GENERIC:
            H5SL_FIND(GENERIC, slist, x, const void, key, -)
            break;

        default:
            assert(0 && "Unknown skiplist type!");
    }

    /* An exact match for 'key' must not have been found in list, if we get here */
    /* ('x' must be the next node with a key greater than the 'key', or NULL) */
    if (x)
        ret_value = x;
    else
        ret_value = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_above() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_first
 PURPOSE
    Gets a pointer to the first node in a skip list
 USAGE
    H5SL_node_t *H5SL_first(slist)
        H5SL_t *slist;          IN: Pointer to skip list

 RETURNS
    Returns pointer to first node in skip list on success, NULL on failure.
 DESCRIPTION
    Retrieves a pointer to the first node in a skip list, for iterating over
    the list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_first(H5SL_t *slist)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    FUNC_LEAVE_NOAPI(slist->header->forward[0])
} /* end H5SL_first() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_next
 PURPOSE
    Gets a pointer to the next node in a skip list
 USAGE
    H5SL_node_t *H5SL_next(slist_node)
        H5SL_node_t *slist_node;          IN: Pointer to skip list node

 RETURNS
    Returns pointer to node after slist_node in skip list on success, NULL on failure.
 DESCRIPTION
    Retrieves a pointer to the next node in a skip list, for iterating over
    the list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_next(H5SL_node_t *slist_node)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist_node);

    /* Check internal consistency */
    /* (Pre-condition) */

    FUNC_LEAVE_NOAPI(slist_node->forward[0])
} /* end H5SL_next() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_prev
 PURPOSE
    Gets a pointer to the previous node in a skip list
 USAGE
    H5SL_node_t *H5SL_prev(slist_node)
        H5SL_node_t *slist_node;          IN: Pointer to skip list node

 RETURNS
    Returns pointer to node before slist_node in skip list on success, NULL on failure.
 DESCRIPTION
    Retrieves a pointer to the previous node in a skip list, for iterating over
    the list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_prev(H5SL_node_t *slist_node)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist_node);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Walk backward, detecting the header node (which has it's key set to NULL) */
    FUNC_LEAVE_NOAPI(slist_node->backward->key == NULL ? NULL : slist_node->backward)
} /* end H5SL_prev() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_last
 PURPOSE
    Gets a pointer to the last node in a skip list
 USAGE
    H5SL_node_t *H5SL_last(slist)
        H5SL_t *slist;          IN: Pointer to skip list

 RETURNS
    Returns pointer to last node in skip list on success, NULL on failure.
 DESCRIPTION
    Retrieves a pointer to the last node in a skip list, for iterating over
    the list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5SL_node_t *
H5SL_last(H5SL_t *slist)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Find last node, avoiding the header node */
    FUNC_LEAVE_NOAPI(slist->last == slist->header ? NULL : slist->last)
} /* end H5SL_last() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_item
 PURPOSE
    Gets pointer to the 'item' for a skip list node
 USAGE
    void *H5SL_item(slist_node)
        H5SL_node_t *slist_node;          IN: Pointer to skip list node

 RETURNS
    Returns pointer to node 'item' on success, NULL on failure.
 DESCRIPTION
    Retrieves a node's 'item'
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
void *
H5SL_item(H5SL_node_t *slist_node)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist_node);

    /* Check internal consistency */
    /* (Pre-condition) */

    FUNC_LEAVE_NOAPI(slist_node->item)
} /* end H5SL_item() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_iterate
 PURPOSE
    Iterate over all nodes in a skip list
 USAGE
    herr_t H5SL_iterate(slist, op, op_data)
        H5SL_t *slist;          IN/OUT: Pointer to skip list to iterate over
        H5SL_operator_t op;     IN: Callback function for iteration
        void *op_data;          IN/OUT: Pointer to application data for callback

 RETURNS
    Returns a negative value if something is wrong, the return
        value of the last operator if it was non-zero, or zero if all
        nodes were processed.
 DESCRIPTION
    Iterate over all the nodes in a skip list, calling an application callback
    with the item, key and any operator data.

    The operator callback receives a pointer to the item and key for the list
    being iterated over ('mesg'), and the pointer to the operator data passed
    in to H5SL_iterate ('op_data').  The return values from an operator are:
        A. Zero causes the iterator to continue, returning zero when all
            nodes of that type have been processed.
        B. Positive causes the iterator to immediately return that positive
            value, indicating short-circuit success.
        C. Negative causes the iterator to immediately return that value,
            indicating failure.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5SL_iterate(H5SL_t *slist, H5SL_operator_t op, void *op_data)
{
    H5SL_node_t *node;          /* Pointer to current skip list node */
    H5SL_node_t *next;          /* Pointer to next skip list node */
    herr_t       ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Free skip list nodes */
    node = slist->header->forward[0];
    while (node != NULL) {
        /* Protect against the node being deleted by the callback */
        next = node->forward[0];

        /* Call the iterator callback
         *
         * Ignoring const here is fine as we only need the value to be const
         * with respect to the list code, which should never modify the
         * elements. The library code that is making use of the skip list
         * container can do what it likes with the elements.
         */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        if ((ret_value = (op)(node->item, (void *)node->key, op_data)) != 0)
            break;
        H5_GCC_CLANG_DIAG_ON("cast-qual")

        /* Advance to next node */
        node = next;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_iterate() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_release
 PURPOSE
    Release all nodes from a skip list
 USAGE
    herr_t H5SL_release(slist)
        H5SL_t *slist;            IN/OUT: Pointer to skip list to release nodes

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Release all the nodes in a skip list.  Any objects left in the skip list
    nodes are not deallocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    The skip list itself is still valid, it just has all its nodes removed.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5SL_release(H5SL_t *slist)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Free skip list nodes */
    if (H5SL__release_common(slist, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_SLIST, H5E_CANTFREE, FAIL, "can't release skip list nodes");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_release() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_free
 PURPOSE
    Release all nodes from a skip list, freeing all nodes
 USAGE
    herr_t H5SL_free(slist,op,op_data)
        H5SL_t *slist;          IN/OUT: Pointer to skip list to release nodes
        H5SL_operator_t op;     IN: Callback function to free item & key
        void *op_data;          IN/OUT: Pointer to application data for callback

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Release all the nodes in a skip list.  Any objects left in
    the skip list have the 'op' routine called for each.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    The skip list itself is still valid, it just has all its nodes removed.

    The return value from the 'op' routine is ignored.

    This routine is essentially a combination of iterating over all the nodes
    (where the iterator callback is supposed to free the items and/or keys)
    followed by a call to H5SL_release().
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5SL_free(H5SL_t *slist, H5SL_operator_t op, void *op_data)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Free skip list nodes */
    if (H5SL__release_common(slist, op, op_data) < 0)
        HGOTO_ERROR(H5E_SLIST, H5E_CANTFREE, FAIL, "can't release skip list nodes");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_free() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_destroy
 PURPOSE
    Close a skip list, deallocating it and freeing all its nodes.
 USAGE
    herr_t H5SL_destroy(slist,op,opdata)
        H5SL_t *slist;          IN/OUT: Pointer to skip list to close
        H5SL_operator_t op;     IN: Callback function to free item & key
        void *op_data;          IN/OUT: Pointer to application data for callback

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Close a skip list, freeing all internal information.  Any objects left in
    the skip list have the 'op' routine called for each.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    The return value from the 'op' routine is ignored.

    This routine is essentially a combination of iterating over all the nodes
    (where the iterator callback is supposed to free the items and/or keys)
    followed by a call to H5SL_close().
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5SL_destroy(H5SL_t *slist, H5SL_operator_t op, void *op_data)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Close skip list */
    if (H5SL__close_common(slist, op, op_data) < 0)
        HGOTO_ERROR(H5E_SLIST, H5E_CANTCLOSEOBJ, FAIL, "can't close skip list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_destroy() */

/*--------------------------------------------------------------------------
 NAME
    H5SL_close
 PURPOSE
    Close a skip list, deallocating it.
 USAGE
    herr_t H5SL_close(slist)
        H5SL_t *slist;            IN/OUT: Pointer to skip list to close

 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Close a skip list, freeing all internal information.  Any objects left in
    the skip list are not deallocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5SL_close(H5SL_t *slist)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(slist);

    /* Check internal consistency */
    /* (Pre-condition) */

    /* Close skip list */
    if (H5SL__close_common(slist, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_SLIST, H5E_CANTCLOSEOBJ, FAIL, "can't close skip list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SL_close() */

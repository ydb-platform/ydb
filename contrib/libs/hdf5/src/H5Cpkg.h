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
 * Purpose:	This file contains declarations which are visible only within
 *		the H5C package.  Source files outside the H5C package should
 *		include H5Cprivate.h instead.
 */

/* clang-format off */
/* Maintain current format by disabling format for this file */

#if !(defined H5C_FRIEND || defined H5C_MODULE)
#error "Do not include this file outside the H5C package!"
#endif

#ifndef H5Cpkg_H
#define H5Cpkg_H

/* Get package's private header */
#include "H5Cprivate.h"

/* Other private headers needed by this file */
#include "H5Clog.h"             /* Cache logging */
#include "H5SLprivate.h"        /* Skip lists */

/**************************/
/* Package Private Macros */
/**************************/

/* Number of epoch markers active */
#define H5C__MAX_EPOCH_MARKERS                  10

/* Cache configuration settings */
#define H5C__HASH_TABLE_LEN     (64 * 1024) /* must be a power of 2 */

/* Initial allocated size of the "flush_dep_parent" array */
#define H5C_FLUSH_DEP_PARENT_INIT 8


/****************************************************************************
 *
 * We maintain doubly linked lists of instances of H5C_cache_entry_t for a
 * variety of reasons -- protected list, LRU list, and the clean and dirty
 * LRU lists at present.  The following macros support linking and unlinking
 * of instances of H5C_cache_entry_t by both their regular and auxiliary next
 * and previous pointers.
 *
 * The size and length fields are also maintained.
 *
 * Note that the relevant pair of prev and next pointers are presumed to be
 * NULL on entry in the insertion macros.
 *
 * Finally, observe that the sanity checking macros evaluate to the empty
 * string when H5C_DO_SANITY_CHECKS is false.  They also contain calls
 * to the HGOTO_ERROR macro, which may not be appropriate in all cases.
 * If so, we will need versions of the insertion and deletion macros which
 * do not reference the sanity checking macros.
 *
 ****************************************************************************/

#ifdef H5C_DO_SANITY_CHECKS

#define H5C__GEN_DLL_PRE_REMOVE_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val) \
do { if ((head_ptr) == NULL || (tail_ptr) == NULL ||                              \
    (entry_ptr) == NULL || (len) <= 0 ||                                     \
    (list_size) < (entry_ptr)->size ||                                       \
    ((entry_ptr)->list_prev == NULL && (head_ptr) != (entry_ptr)) ||         \
    ((entry_ptr)->list_next == NULL && (tail_ptr) != (entry_ptr)) ||         \
    ((len) == 1 &&                                                           \
     !((head_ptr) == (entry_ptr) && (tail_ptr) == (entry_ptr) &&             \
       (entry_ptr)->list_next == NULL && (entry_ptr)->list_prev == NULL &&   \
       (list_size) == (entry_ptr)->size                                      \
      )                                                                      \
    )                                                                        \
   ) {                                                                       \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "DLL pre remove SC failed"); \
} } while (0)

#define H5C__GEN_DLL_PRE_INSERT_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val) \
if ((entry_ptr) == NULL || (entry_ptr)->list_next != NULL || (entry_ptr)->list_prev != NULL || \
    (((head_ptr) == NULL || (tail_ptr) == NULL) && (head_ptr) != (tail_ptr)) || \
    ((len) == 0 &&                                                            \
        ((list_size) > 0 || (head_ptr) != NULL || (tail_ptr) != NULL)         \
    ) ||                                                                      \
    ((len) == 1 &&                                                            \
        ((head_ptr) != (tail_ptr) || (head_ptr) == NULL ||                    \
        (head_ptr)->size != (list_size))                                      \
    ) ||                                                                      \
    ((len) >= 1 &&                                                            \
     ((head_ptr) == NULL || (head_ptr)->list_prev != NULL ||                  \
      (tail_ptr) == NULL || (tail_ptr)->list_next != NULL)                    \
    )                                                                         \
   ) {                                                                        \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "DLL pre insert SC failed"); \
}

#define H5C__GEN_DLL_PRE_SIZE_UPDATE_SC(dll_len, dll_size, old_size, new_size, fail_val) \
if ((dll_len) <= 0 || (dll_size) <= 0 || (old_size) <= 0 ||                   \
    (old_size) > (dll_size) || (new_size) <= 0 ||                             \
    ((dll_len) == 1 && (old_size) != (dll_size))                              \
   ) {                                                                        \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "DLL pre size update SC failed"); \
}

#define H5C__GEN_DLL_POST_SIZE_UPDATE_SC(dll_len, dll_size, old_size, new_size, fail_val) \
if ((new_size) > (dll_size) || ((dll_len) == 1 && (new_size) != (dll_size))) { \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "DLL post size update SC failed"); \
}
#else /* H5C_DO_SANITY_CHECKS */
#define H5C__GEN_DLL_PRE_REMOVE_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__GEN_DLL_PRE_INSERT_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__GEN_DLL_PRE_SIZE_UPDATE_SC(dll_len, dll_size, old_size, new_size, fail_val)
#define H5C__GEN_DLL_POST_SIZE_UPDATE_SC(dll_len, dll_size, old_size, new_size, fail_val)
#endif /* H5C_DO_SANITY_CHECKS */

#define H5C__GEN_DLL_APPEND(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val)    \
{                                                                              \
    H5C__GEN_DLL_PRE_INSERT_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val) \
    if ((head_ptr) == NULL) {                                                  \
       (head_ptr) = (entry_ptr);                                               \
       (tail_ptr) = (entry_ptr);                                               \
    }                                                                          \
    else {                                                                     \
       (tail_ptr)->list_next = (entry_ptr);                                    \
       (entry_ptr)->list_prev = (tail_ptr);                                    \
       (tail_ptr) = (entry_ptr);                                               \
    }                                                                          \
    (len)++;                                                                   \
    (list_size) += (entry_ptr)->size;                                          \
} /* H5C__GEN_DLL_APPEND() */

#define H5C__GEN_DLL_PREPEND(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val)    \
{                                                                              \
    H5C__GEN_DLL_PRE_INSERT_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val) \
    if ((head_ptr) == NULL) {                                                  \
       (head_ptr) = (entry_ptr);                                               \
       (tail_ptr) = (entry_ptr);                                               \
    }                                                                          \
    else {                                                                     \
       (head_ptr)->list_prev = (entry_ptr);                                    \
       (entry_ptr)->list_next = (head_ptr);                                    \
       (head_ptr) = (entry_ptr);                                               \
    }                                                                          \
    (len)++;                                                                   \
    (list_size) += (entry_ptr)->size;                                          \
} /* H5C__GEN_DLL_PREPEND() */

#define H5C__GEN_DLL_REMOVE(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val)    \
{                                                                              \
    H5C__GEN_DLL_PRE_REMOVE_SC(entry_ptr, list_next, list_prev, head_ptr, tail_ptr, len, list_size, fail_val); \
    if ((head_ptr) == (entry_ptr)) {                                           \
       (head_ptr) = (entry_ptr)->list_next;                                    \
       if ((head_ptr) != NULL)                                                 \
          (head_ptr)->list_prev = NULL;                                        \
    }                                                                          \
    else                                                                       \
       (entry_ptr)->list_prev->list_next = (entry_ptr)->list_next;             \
    if ((tail_ptr) == (entry_ptr)) {                                           \
       (tail_ptr) = (entry_ptr)->list_prev;                                    \
       if ((tail_ptr) != NULL)                                                 \
          (tail_ptr)->list_next = NULL;                                        \
    }                                                                          \
    else                                                                       \
       (entry_ptr)->list_next->list_prev = (entry_ptr)->list_prev;             \
    (entry_ptr)->list_next = NULL;                                             \
    (entry_ptr)->list_prev = NULL;                                             \
    (len)--;                                                                   \
    (list_size) -= (entry_ptr)->size;                                          \
} /* H5C__GEN_DLL_REMOVE() */

#define H5C__GEN_DLL_UPDATE_FOR_SIZE_CHANGE(dll_len, dll_size, old_size, new_size, fail_val) \
{                                                                              \
    H5C__GEN_DLL_PRE_SIZE_UPDATE_SC(dll_len, dll_size, old_size, new_size, fail_val) \
    (dll_size) -= (old_size);                                                  \
    (dll_size) += (new_size);                                                  \
    H5C__GEN_DLL_POST_SIZE_UPDATE_SC(dll_len, dll_size, old_size, new_size, fail_val) \
} /* H5C__GEN_DLL_UPDATE_FOR_SIZE_CHANGE() */


/* Macros that modify the LRU/protected/pinned lists */
#define H5C__DLL_APPEND(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val)    \
    H5C__GEN_DLL_APPEND(entry_ptr, next, prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__DLL_PREPEND(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val) \
    H5C__GEN_DLL_PREPEND(entry_ptr, next, prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__DLL_REMOVE(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val) \
    H5C__GEN_DLL_REMOVE(entry_ptr, next, prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__DLL_UPDATE_FOR_SIZE_CHANGE(dll_len, dll_size, old_size, new_size, fail_val) \
    H5C__GEN_DLL_UPDATE_FOR_SIZE_CHANGE(dll_len, dll_size, old_size, new_size, fail_val)

/* Macros that modify the "auxiliary" LRU list */
#define H5C__AUX_DLL_APPEND(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val)  \
    H5C__GEN_DLL_APPEND(entry_ptr, aux_next, aux_prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__AUX_DLL_PREPEND(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val) \
    H5C__GEN_DLL_PREPEND(entry_ptr, aux_next, aux_prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__AUX_DLL_REMOVE(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val)  \
    H5C__GEN_DLL_REMOVE(entry_ptr, aux_next, aux_prev, head_ptr, tail_ptr, len, list_size, fail_val)

/* Macros that modify the "index" list */
#define H5C__IL_DLL_APPEND(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val) \
    H5C__GEN_DLL_APPEND(entry_ptr, il_next, il_prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__IL_DLL_REMOVE(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val) \
    H5C__GEN_DLL_REMOVE(entry_ptr, il_next, il_prev, head_ptr, tail_ptr, len, list_size, fail_val)


/***********************************************************************
 *
 * Stats collection macros
 *
 * The following macros must handle stats collection when this collection
 * is enabled, and evaluate to the empty string when it is not.
 *
 * The sole exception to this rule is
 * H5C__UPDATE_CACHE_HIT_RATE_STATS(), which is always active as
 * the cache hit rate stats are always collected and available.
 *
 ***********************************************************************/

#define H5C__UPDATE_CACHE_HIT_RATE_STATS(cache_ptr, hit) \
do {                                                     \
    (cache_ptr)->cache_accesses++;                       \
    if (hit)                                             \
        (cache_ptr)->cache_hits++;                       \
} while (0)

#if H5C_COLLECT_CACHE_STATS

#define H5C__UPDATE_MAX_INDEX_SIZE_STATS(cache_ptr)                        \
do {                                                                       \
    if ((cache_ptr)->index_size > (cache_ptr)->max_index_size)             \
        (cache_ptr)->max_index_size = (cache_ptr)->index_size;             \
    if ((cache_ptr)->clean_index_size > (cache_ptr)->max_clean_index_size) \
        (cache_ptr)->max_clean_index_size = (cache_ptr)->clean_index_size; \
    if ((cache_ptr)->dirty_index_size > (cache_ptr)->max_dirty_index_size) \
        (cache_ptr)->max_dirty_index_size = (cache_ptr)->dirty_index_size; \
} while (0)

#define H5C__UPDATE_STATS_FOR_DIRTY_PIN(cache_ptr, entry_ptr)            \
do {                                                                     \
    (cache_ptr)->dirty_pins[(entry_ptr)->type->id]++;                    \
} while (0)

#define H5C__UPDATE_STATS_FOR_UNPROTECT(cache_ptr)             \
do {                                                           \
    if ((cache_ptr)->slist_len > (cache_ptr)->max_slist_len)   \
        (cache_ptr)->max_slist_len = (cache_ptr)->slist_len;   \
    if ((cache_ptr)->slist_size > (cache_ptr)->max_slist_size) \
        (cache_ptr)->max_slist_size = (cache_ptr)->slist_size; \
    if ((cache_ptr)->pel_len > (cache_ptr)->max_pel_len)       \
        (cache_ptr)->max_pel_len = (cache_ptr)->pel_len;       \
    if ((cache_ptr)->pel_size > (cache_ptr)->max_pel_size)     \
        (cache_ptr)->max_pel_size = (cache_ptr)->pel_size;     \
} while (0)

#define H5C__UPDATE_STATS_FOR_MOVE(cache_ptr, entry_ptr)              \
do {                                                                  \
    if ((cache_ptr)->flush_in_progress)                               \
        (cache_ptr)->cache_flush_moves[(entry_ptr)->type->id]++;      \
    if ((entry_ptr)->flush_in_progress)                               \
        (cache_ptr)->entry_flush_moves[(entry_ptr)->type->id]++;      \
    (cache_ptr)->moves[(entry_ptr)->type->id]++;                      \
    (cache_ptr)->entries_relocated_counter++;                         \
} while (0)

#define H5C__UPDATE_STATS_FOR_ENTRY_SIZE_CHANGE(cache_ptr, entry_ptr, new_size) \
do {                                                                     \
    if ((cache_ptr)->flush_in_progress)                                  \
        (cache_ptr)->cache_flush_size_changes[(entry_ptr)->type->id]++;  \
    if ((entry_ptr)->flush_in_progress)                                  \
        (cache_ptr)->entry_flush_size_changes[(entry_ptr)->type->id]++;  \
    if ((entry_ptr)->size < (new_size)) {                                \
        (cache_ptr)->size_increases[(entry_ptr)->type->id]++;            \
        H5C__UPDATE_MAX_INDEX_SIZE_STATS(cache_ptr);                     \
        if ((cache_ptr)->slist_size > (cache_ptr)->max_slist_size)       \
            (cache_ptr)->max_slist_size = (cache_ptr)->slist_size;       \
        if ((cache_ptr)->pl_size > (cache_ptr)->max_pl_size)             \
            (cache_ptr)->max_pl_size = (cache_ptr)->pl_size;             \
    } else if ((entry_ptr)->size > (new_size)) {                         \
        (cache_ptr)->size_decreases[(entry_ptr)->type->id]++;            \
    }                                                                    \
} while (0)

#define H5C__UPDATE_STATS_FOR_HT_INSERTION(cache_ptr) \
do {                                                                     \
    (cache_ptr)->total_ht_insertions++;                                  \
} while (0)

#define H5C__UPDATE_STATS_FOR_HT_DELETION(cache_ptr) \
do {                                                                     \
    (cache_ptr)->total_ht_deletions++;                                   \
} while (0)

#define H5C__UPDATE_STATS_FOR_HT_SEARCH(cache_ptr, success, depth) \
do {                                                               \
    if (success) {                                                 \
        (cache_ptr)->successful_ht_searches++;                     \
        (cache_ptr)->total_successful_ht_search_depth += depth;    \
    } else {                                                       \
        (cache_ptr)->failed_ht_searches++;                         \
        (cache_ptr)->total_failed_ht_search_depth += depth;        \
    }                                                              \
} while (0)

#define H5C__UPDATE_STATS_FOR_UNPIN(cache_ptr, entry_ptr) \
do {                                                      \
    (cache_ptr)->unpins[(entry_ptr)->type->id]++;         \
} while (0)

#define H5C__UPDATE_STATS_FOR_PREFETCH(cache_ptr, dirty) \
do {                                                     \
    (cache_ptr)->prefetches++;                           \
    if (dirty)                                           \
        (cache_ptr)->dirty_prefetches++;                 \
} while (0)

#define H5C__UPDATE_STATS_FOR_PREFETCH_HIT(cache_ptr) \
do {                                                     \
    (cache_ptr)->prefetch_hits++; \
} while (0)

#define H5C__UPDATE_STATS_FOR_SLIST_SCAN_RESTART(cache_ptr) \
do {                                                     \
    (cache_ptr)->slist_scan_restarts++; \
} while (0)

#define H5C__UPDATE_STATS_FOR_LRU_SCAN_RESTART(cache_ptr) \
do {                                                     \
    (cache_ptr)->LRU_scan_restarts++; \
} while (0)

#define H5C__UPDATE_STATS_FOR_INDEX_SCAN_RESTART(cache_ptr) \
do {                                                     \
    (cache_ptr)->index_scan_restarts++; \
} while (0)

#if H5C_COLLECT_CACHE_ENTRY_STATS

#define H5C__RESET_CACHE_ENTRY_STATS(entry_ptr) \
do {                                               \
    (entry_ptr)->accesses = 0;                  \
    (entry_ptr)->clears   = 0;                  \
    (entry_ptr)->flushes  = 0;                  \
    (entry_ptr)->pins     = 0;                  \
} while (0)

#define H5C__UPDATE_STATS_FOR_CLEAR(cache_ptr, entry_ptr)    \
do {                                                            \
    (cache_ptr)->clears[(entry_ptr)->type->id]++;            \
    if((entry_ptr)->is_pinned)                               \
        (cache_ptr)->pinned_clears[(entry_ptr)->type->id]++; \
    (entry_ptr)->clears++;                                   \
} while (0)

#define H5C__UPDATE_STATS_FOR_FLUSH(cache_ptr, entry_ptr)     \
do {                                                             \
    (cache_ptr)->flushes[(entry_ptr)->type->id]++;            \
    if((entry_ptr)->is_pinned)                                \
        (cache_ptr)->pinned_flushes[(entry_ptr)->type->id]++; \
    (entry_ptr)->flushes++;                                   \
} while (0)

#define H5C__UPDATE_STATS_FOR_EVICTION(cache_ptr, entry_ptr, take_ownership) \
do {                                                                            \
    if (take_ownership)                                                      \
        (cache_ptr)->take_ownerships[(entry_ptr)->type->id]++;               \
    else                                                                     \
        (cache_ptr)->evictions[(entry_ptr)->type->id]++;                     \
    if ((entry_ptr)->accesses > (cache_ptr)->max_accesses[(entry_ptr)->type->id]) \
        (cache_ptr)->max_accesses[(entry_ptr)->type->id] = (entry_ptr)->accesses; \
    if ((entry_ptr)->accesses < (cache_ptr)->min_accesses[(entry_ptr)->type->id]) \
        (cache_ptr)->min_accesses[(entry_ptr)->type->id] = (entry_ptr)->accesses; \
    if ((entry_ptr)->clears > (cache_ptr)->max_clears[(entry_ptr)->type->id]) \
        (cache_ptr)->max_clears[(entry_ptr)->type->id] = (entry_ptr)->clears; \
    if ((entry_ptr)->flushes > (cache_ptr)->max_flushes[(entry_ptr)->type->id]) \
        (cache_ptr)->max_flushes[(entry_ptr)->type->id] = (entry_ptr)->flushes; \
    if ((entry_ptr)->size > (cache_ptr)->max_size[(entry_ptr)->type->id])    \
        (cache_ptr)->max_size[(entry_ptr)->type->id] = (entry_ptr)->size;    \
    if ((entry_ptr)->pins > (cache_ptr)->max_pins[(entry_ptr)->type->id])    \
        (cache_ptr)->max_pins[(entry_ptr)->type->id] = (entry_ptr)->pins;    \
} while (0)

#define H5C__UPDATE_STATS_FOR_INSERTION(cache_ptr, entry_ptr)             \
do {                                                                         \
    (cache_ptr)->insertions[(entry_ptr)->type->id]++;                     \
    if ((entry_ptr)->is_pinned) {                                         \
        (cache_ptr)->pinned_insertions[(entry_ptr)->type->id]++;          \
        (cache_ptr)->pins[(entry_ptr)->type->id]++;                       \
        (entry_ptr)->pins++;                                              \
        if ((cache_ptr)->pel_len > (cache_ptr)->max_pel_len)              \
            (cache_ptr)->max_pel_len = (cache_ptr)->pel_len;              \
        if ((cache_ptr)->pel_size > (cache_ptr)->max_pel_size)            \
            (cache_ptr)->max_pel_size = (cache_ptr)->pel_size;            \
    }                                                                     \
    if ((cache_ptr)->index_len > (cache_ptr)->max_index_len)              \
        (cache_ptr)->max_index_len = (cache_ptr)->index_len;              \
    H5C__UPDATE_MAX_INDEX_SIZE_STATS(cache_ptr);                          \
    if ((cache_ptr)->slist_len > (cache_ptr)->max_slist_len)              \
        (cache_ptr)->max_slist_len = (cache_ptr)->slist_len;              \
    if ((cache_ptr)->slist_size > (cache_ptr)->max_slist_size)            \
        (cache_ptr)->max_slist_size = (cache_ptr)->slist_size;            \
    if ((entry_ptr)->size > (cache_ptr)->max_size[(entry_ptr)->type->id]) \
        (cache_ptr)->max_size[(entry_ptr)->type->id] = (entry_ptr)->size; \
    (cache_ptr)->entries_inserted_counter++;                              \
} while (0)

#define H5C__UPDATE_STATS_FOR_PROTECT(cache_ptr, entry_ptr, hit)          \
do {                                                                         \
    if (hit)                                                              \
        (cache_ptr)->hits[(entry_ptr)->type->id]++;                       \
    else                                                                  \
        (cache_ptr)->misses[(entry_ptr)->type->id]++;                     \
    if (!(entry_ptr)->is_read_only)                                       \
        (cache_ptr)->write_protects[(entry_ptr)->type->id]++;             \
    else {                                                                \
        (cache_ptr)->read_protects[(entry_ptr)->type->id]++;              \
        if ((entry_ptr)->ro_ref_count > (cache_ptr)->max_read_protects[(entry_ptr)->type->id]) \
            (cache_ptr)->max_read_protects[(entry_ptr)->type->id] = (entry_ptr)->ro_ref_count; \
    }                                                                     \
    if ((cache_ptr)->index_len > (cache_ptr)->max_index_len)              \
        (cache_ptr)->max_index_len = (cache_ptr)->index_len;              \
    H5C__UPDATE_MAX_INDEX_SIZE_STATS(cache_ptr);                          \
    if ((cache_ptr)->pl_len > (cache_ptr)->max_pl_len)                    \
        (cache_ptr)->max_pl_len = (cache_ptr)->pl_len;                    \
    if ((cache_ptr)->pl_size > (cache_ptr)->max_pl_size)                  \
        (cache_ptr)->max_pl_size = (cache_ptr)->pl_size;                  \
    if ((entry_ptr)->size > (cache_ptr)->max_size[(entry_ptr)->type->id]) \
        (cache_ptr)->max_size[(entry_ptr)->type->id] = (entry_ptr)->size; \
    (entry_ptr)->accesses++;                                              \
} while (0)

#define H5C__UPDATE_STATS_FOR_PIN(cache_ptr, entry_ptr)      \
do {                                                            \
    (cache_ptr)->pins[(entry_ptr)->type->id]++;              \
    (entry_ptr)->pins++;                                     \
    if ((cache_ptr)->pel_len > (cache_ptr)->max_pel_len)     \
        (cache_ptr)->max_pel_len = (cache_ptr)->pel_len;     \
    if ((cache_ptr)->pel_size > (cache_ptr)->max_pel_size)   \
        (cache_ptr)->max_pel_size = (cache_ptr)->pel_size;   \
} while (0)

#else /* H5C_COLLECT_CACHE_ENTRY_STATS */

#define H5C__RESET_CACHE_ENTRY_STATS(entry_ptr)

#define H5C__UPDATE_STATS_FOR_CLEAR(cache_ptr, entry_ptr)     \
do {                                                             \
    (cache_ptr)->clears[(entry_ptr)->type->id]++;             \
    if((entry_ptr)->is_pinned)                                \
        (cache_ptr)->pinned_clears[(entry_ptr)->type->id]++;  \
} while (0)

#define H5C__UPDATE_STATS_FOR_FLUSH(cache_ptr, entry_ptr)     \
do {                                                             \
    (cache_ptr)->flushes[(entry_ptr)->type->id]++;            \
    if ((entry_ptr)->is_pinned)                               \
        (cache_ptr)->pinned_flushes[(entry_ptr)->type->id]++; \
} while (0)

#define H5C__UPDATE_STATS_FOR_EVICTION(cache_ptr, entry_ptr, take_ownership) \
do {                                                              \
    if (take_ownership)                                        \
        (cache_ptr)->take_ownerships[(entry_ptr)->type->id]++; \
    else                                                       \
        (cache_ptr)->evictions[(entry_ptr)->type->id]++;       \
} while (0)

#define H5C__UPDATE_STATS_FOR_INSERTION(cache_ptr, entry_ptr)    \
do {                                                                \
    (cache_ptr)->insertions[(entry_ptr)->type->id]++;            \
    if ((entry_ptr)->is_pinned) {                                \
        (cache_ptr)->pinned_insertions[(entry_ptr)->type->id]++; \
        (cache_ptr)->pins[(entry_ptr)->type->id]++;              \
        if ((cache_ptr)->pel_len > (cache_ptr)->max_pel_len)     \
            (cache_ptr)->max_pel_len = (cache_ptr)->pel_len;     \
        if ((cache_ptr)->pel_size > (cache_ptr)->max_pel_size)   \
            (cache_ptr)->max_pel_size = (cache_ptr)->pel_size;   \
    }                                                            \
    if ((cache_ptr)->index_len > (cache_ptr)->max_index_len)     \
        (cache_ptr)->max_index_len = (cache_ptr)->index_len;     \
    H5C__UPDATE_MAX_INDEX_SIZE_STATS(cache_ptr)                  \
    if ((cache_ptr)->slist_len > (cache_ptr)->max_slist_len)     \
        (cache_ptr)->max_slist_len = (cache_ptr)->slist_len;     \
    if ((cache_ptr)->slist_size > (cache_ptr)->max_slist_size)   \
        (cache_ptr)->max_slist_size = (cache_ptr)->slist_size;   \
    (cache_ptr)->entries_inserted_counter++;                     \
} while (0)

#define H5C__UPDATE_STATS_FOR_PROTECT(cache_ptr, entry_ptr, hit)       \
do {                                                                      \
    if (hit)                                                           \
        (cache_ptr)->hits[(entry_ptr)->type->id]++;                    \
    else                                                               \
        (cache_ptr)->misses[(entry_ptr)->type->id]++;                  \
    if (!(entry_ptr)->is_read_only)                                    \
        (cache_ptr)->write_protects[(entry_ptr)->type->id]++;          \
    else {                                                             \
        (cache_ptr)->read_protects[(entry_ptr)->type->id]++;           \
        if ((entry_ptr)->ro_ref_count >                                \
                (cache_ptr)->max_read_protects[(entry_ptr)->type->id]) \
            (cache_ptr)->max_read_protects[(entry_ptr)->type->id] =    \
                    (entry_ptr)->ro_ref_count;                         \
    }                                                                  \
    if ((cache_ptr)->index_len > (cache_ptr)->max_index_len)           \
        (cache_ptr)->max_index_len = (cache_ptr)->index_len;           \
    H5C__UPDATE_MAX_INDEX_SIZE_STATS(cache_ptr)                        \
    if ((cache_ptr)->pl_len > (cache_ptr)->max_pl_len)                 \
        (cache_ptr)->max_pl_len = (cache_ptr)->pl_len;                 \
    if ((cache_ptr)->pl_size > (cache_ptr)->max_pl_size)               \
        (cache_ptr)->max_pl_size = (cache_ptr)->pl_size;               \
} while (0)

#define H5C__UPDATE_STATS_FOR_PIN(cache_ptr, entry_ptr)    \
do {                                                          \
    (cache_ptr)->pins[(entry_ptr)->type->id]++;            \
    if ((cache_ptr)->pel_len > (cache_ptr)->max_pel_len)   \
        (cache_ptr)->max_pel_len = (cache_ptr)->pel_len;   \
    if ((cache_ptr)->pel_size > (cache_ptr)->max_pel_size) \
        (cache_ptr)->max_pel_size = (cache_ptr)->pel_size; \
} while (0)

#endif /* H5C_COLLECT_CACHE_ENTRY_STATS */

#else /* H5C_COLLECT_CACHE_STATS */

#define H5C__RESET_CACHE_ENTRY_STATS(entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_DIRTY_PIN(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_UNPROTECT(cache_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_MOVE(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_ENTRY_SIZE_CHANGE(cache_ptr, entry_ptr, new_size) do {} while(0)
#define H5C__UPDATE_STATS_FOR_HT_INSERTION(cache_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_HT_DELETION(cache_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_HT_SEARCH(cache_ptr, success, depth) do {} while(0)
#define H5C__UPDATE_STATS_FOR_INSERTION(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_CLEAR(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_FLUSH(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_EVICTION(cache_ptr, entry_ptr, take_ownership) do {} while(0)
#define H5C__UPDATE_STATS_FOR_PROTECT(cache_ptr, entry_ptr, hit) do {} while(0)
#define H5C__UPDATE_STATS_FOR_PIN(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_UNPIN(cache_ptr, entry_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_PREFETCH(cache_ptr, dirty) do {} while(0)
#define H5C__UPDATE_STATS_FOR_PREFETCH_HIT(cache_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_SLIST_SCAN_RESTART(cache_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_LRU_SCAN_RESTART(cache_ptr) do {} while(0)
#define H5C__UPDATE_STATS_FOR_INDEX_SCAN_RESTART(cache_ptr) do {} while(0)

#endif /* H5C_COLLECT_CACHE_STATS */


/***********************************************************************
 *
 * Hash table access and manipulation macros:
 *
 * The following macros handle searches, insertions, and deletion in
 * the hash table.
 *
 ***********************************************************************/

#define H5C__HASH_MASK      ((size_t)(H5C__HASH_TABLE_LEN - 1) << 3)
#define H5C__HASH_FCN(x)    (int)((unsigned)((x) & H5C__HASH_MASK) >> 3)

#define H5C__POST_HT_SHIFT_TO_FRONT_SC_CMP(cache_ptr, entry_ptr, k) \
((cache_ptr) == NULL || (cache_ptr)->index[k] != (entry_ptr) ||     \
     (entry_ptr)->ht_prev != NULL                                   \
)
#define H5C__PRE_HT_SEARCH_SC_CMP(cache_ptr, entry_addr)                  \
((cache_ptr) == NULL ||                                                   \
    (cache_ptr)->index_size !=                                            \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
    !H5_addr_defined(entry_addr) ||                                      \
    H5C__HASH_FCN(entry_addr) < 0 ||                                      \
    H5C__HASH_FCN(entry_addr) >= H5C__HASH_TABLE_LEN                      \
)
#define H5C__POST_SUC_HT_SEARCH_SC_CMP(cache_ptr, entry_ptr, k)               \
((cache_ptr) == NULL || (cache_ptr)->index_len < 1 ||                         \
    (entry_ptr) == NULL ||                                                    \
    (cache_ptr)->index_size < (entry_ptr)->size ||                            \
    (cache_ptr)->index_size !=                                                \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) ||     \
    (entry_ptr)->size <= 0 ||                                                 \
    (cache_ptr)->index[k] == NULL ||                                          \
    ((cache_ptr)->index[k] != (entry_ptr) && (entry_ptr)->ht_prev == NULL) || \
    ((cache_ptr)->index[k] == (entry_ptr) && (entry_ptr)->ht_prev != NULL) || \
    ((entry_ptr)->ht_prev != NULL && (entry_ptr)->ht_prev->ht_next != (entry_ptr)) || \
    ((entry_ptr)->ht_next != NULL && (entry_ptr)->ht_next->ht_prev != (entry_ptr)) \
)

#ifdef H5C_DO_SANITY_CHECKS

#define H5C__PRE_HT_INSERT_SC(cache_ptr, entry_ptr, fail_val)           \
if ((cache_ptr) == NULL ||                                              \
     (entry_ptr) == NULL || !H5_addr_defined((entry_ptr)->addr) ||     \
     (entry_ptr)->ht_next != NULL || (entry_ptr)->ht_prev != NULL ||    \
     (entry_ptr)->size <= 0 ||                                          \
     H5C__HASH_FCN((entry_ptr)->addr) < 0 ||                            \
     H5C__HASH_FCN((entry_ptr)->addr) >= H5C__HASH_TABLE_LEN ||         \
     (cache_ptr)->index_size !=                                         \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
     (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||         \
     (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||         \
     (entry_ptr)->ring <= H5C_RING_UNDEFINED ||                         \
     (entry_ptr)->ring >= H5C_RING_NTYPES ||                            \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                 \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +         \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring]) ||       \
     (cache_ptr)->index_len != (cache_ptr)->il_len ||                   \
     (cache_ptr)->index_size != (cache_ptr)->il_size                    \
    ) {                                                                 \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, fail_val, "pre HT insert SC failed"); \
}

#define H5C__POST_HT_INSERT_SC(cache_ptr, entry_ptr, fail_val)          \
if ((cache_ptr) == NULL ||                                              \
     (cache_ptr)->index_size !=                                         \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
     (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||         \
     (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||         \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] == 0 ||             \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                 \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +         \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring]) ||       \
     (cache_ptr)->index_len != (cache_ptr)->il_len ||                   \
     (cache_ptr)->index_size != (cache_ptr)->il_size                    \
    ) {                                                                 \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, fail_val, "post HT insert SC failed"); \
}

#define H5C__PRE_HT_REMOVE_SC(cache_ptr, entry_ptr, fail_val)           \
if ( (cache_ptr) == NULL || (cache_ptr)->index_len < 1 ||               \
     (entry_ptr) == NULL ||                                             \
     (cache_ptr)->index_size < (entry_ptr)->size ||                     \
     !H5_addr_defined((entry_ptr)->addr) ||                            \
     (entry_ptr)->size <= 0 ||                                          \
     H5C__HASH_FCN((entry_ptr)->addr) < 0 ||                            \
     H5C__HASH_FCN((entry_ptr)->addr) >= H5C__HASH_TABLE_LEN ||         \
     (cache_ptr)->index[H5C__HASH_FCN((entry_ptr)->addr)] == NULL ||    \
     ((cache_ptr)->index[H5C__HASH_FCN((entry_ptr)->addr)] != (entry_ptr) && \
       (entry_ptr)->ht_prev == NULL) ||                                 \
     ((cache_ptr)->index[H5C__HASH_FCN((entry_ptr)->addr)] == (entry_ptr) && \
       (entry_ptr)->ht_prev != NULL) ||                                 \
     (cache_ptr)->index_size !=                                         \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
     (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||         \
     (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||         \
     (entry_ptr)->ring <= H5C_RING_UNDEFINED ||                         \
     (entry_ptr)->ring >= H5C_RING_NTYPES ||                            \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] <= 0 ||             \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] < (entry_ptr)->size || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                 \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +         \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring]) ||       \
     (cache_ptr)->index_len != (cache_ptr)->il_len ||                   \
     (cache_ptr)->index_size != (cache_ptr)->il_size                    \
   ) {                                                                  \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "pre HT remove SC failed"); \
}

#define H5C__POST_HT_REMOVE_SC(cache_ptr, entry_ptr, fail_val)            \
if ((cache_ptr) == NULL ||                                                \
     (entry_ptr) == NULL || !H5_addr_defined((entry_ptr)->addr) ||       \
     (entry_ptr)->size <= 0 ||                                            \
     (entry_ptr)->ht_next != NULL ||                                      \
     (entry_ptr)->ht_prev != NULL ||                                      \
     (cache_ptr)->index_size !=                                           \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
     (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||           \
     (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||           \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                   \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +           \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring]) ||         \
     (cache_ptr)->index_len != (cache_ptr)->il_len ||                     \
     (cache_ptr)->index_size != (cache_ptr)->il_size                      \
   ) {                                                                    \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "post HT remove SC failed"); \
}

#define H5C__PRE_HT_SEARCH_SC(cache_ptr, entry_addr, fail_val)            \
if (H5C__PRE_HT_SEARCH_SC_CMP(cache_ptr, entry_addr)) {                   \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "pre HT search SC failed"); \
}

#define H5C__POST_SUC_HT_SEARCH_SC(cache_ptr, entry_ptr, k, fail_val)       \
if(H5C__POST_SUC_HT_SEARCH_SC_CMP(cache_ptr, entry_ptr, k)) {               \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "post successful HT search SC failed"); \
}

#define H5C__POST_HT_SHIFT_TO_FRONT_SC(cache_ptr, entry_ptr, k, fail_val) \
if(H5C__POST_HT_SHIFT_TO_FRONT_SC_CMP(cache_ptr, entry_ptr, k)) {         \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "post HT shift to front SC failed"); \
}

#define H5C__PRE_HT_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size,   \
                                entry_ptr, was_clean, fail_val)           \
if ((cache_ptr) == NULL ||                                                \
     (cache_ptr)->index_len <= 0 || (cache_ptr)->index_size <= 0 ||       \
     (new_size) <= 0 || (old_size) > (cache_ptr)->index_size ||           \
     ((cache_ptr)->index_len == 1 && (cache_ptr)->index_size != (old_size)) || \
     (cache_ptr)->index_size !=                                           \
       ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
     (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||           \
     (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||           \
     ((!(was_clean) || (cache_ptr)->clean_index_size < (old_size)) &&     \
        ((was_clean) || (cache_ptr)->dirty_index_size < (old_size))) ||   \
     (entry_ptr) == NULL ||                                               \
     (entry_ptr)->ring <= H5C_RING_UNDEFINED ||                           \
     (entry_ptr)->ring >= H5C_RING_NTYPES ||                              \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] <= 0 ||               \
     (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
     (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                   \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +           \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring]) ||         \
     (cache_ptr)->index_len != (cache_ptr)->il_len ||                     \
     (cache_ptr)->index_size != (cache_ptr)->il_size                      \
   ) {                                                                    \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "pre HT entry size change SC failed"); \
}

#define H5C__POST_HT_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size,    \
                                entry_ptr, fail_val)                        \
if ((cache_ptr) == NULL ||                                                  \
    (cache_ptr)->index_len <= 0 || (cache_ptr)->index_size <= 0 ||          \
    (new_size) > (cache_ptr)->index_size ||                                 \
    (cache_ptr)->index_size !=                                              \
        ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) ||  \
    (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||              \
    (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||              \
    ((!((entry_ptr)->is_dirty ) || (cache_ptr)->dirty_index_size < (new_size)) && \
      ((entry_ptr)->is_dirty || (cache_ptr)->clean_index_size < (new_size)) \
    ) ||                                                                    \
    ((cache_ptr)->index_len == 1 && (cache_ptr)->index_size != (new_size)) || \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                      \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +             \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring]) ||           \
    (cache_ptr)->index_len != (cache_ptr)->il_len ||                        \
    (cache_ptr)->index_size != (cache_ptr)->il_size                         \
  ) {                                                                       \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "post HT entry size change SC failed"); \
}

#define H5C__PRE_HT_UPDATE_FOR_ENTRY_CLEAN_SC(cache_ptr, entry_ptr, fail_val) \
if ((cache_ptr) == NULL || (cache_ptr)->index_len <= 0 ||                     \
    (entry_ptr) == NULL || (entry_ptr)->is_dirty != false ||                  \
    (cache_ptr)->index_size < (entry_ptr)->size ||                            \
    (cache_ptr)->dirty_index_size < (entry_ptr)->size ||                      \
    (cache_ptr)->index_size != ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
    (cache_ptr)->index_size < ((cache_ptr)->clean_index_size) ||              \
    (cache_ptr)->index_size < ((cache_ptr)->dirty_index_size) ||              \
    (entry_ptr)->ring <= H5C_RING_UNDEFINED ||                                \
    (entry_ptr)->ring >= H5C_RING_NTYPES ||                                   \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] <= 0 ||                    \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                        \
      ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +                \
       (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring])                 \
  ) {                                                                         \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "pre HT update for entry clean SC failed"); \
}

#define H5C__PRE_HT_UPDATE_FOR_ENTRY_DIRTY_SC(cache_ptr, entry_ptr, fail_val) \
if ((cache_ptr) == NULL || (cache_ptr)->index_len <= 0 ||                     \
    (entry_ptr) == NULL || (entry_ptr)->is_dirty != true ||                   \
    (cache_ptr)->index_size < (entry_ptr)->size ||                            \
    (cache_ptr)->clean_index_size < (entry_ptr)->size ||                      \
    (cache_ptr)->index_size != ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) ||   \
    (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||                \
    (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||                \
    (entry_ptr)->ring <= H5C_RING_UNDEFINED ||                                \
    (entry_ptr)->ring >= H5C_RING_NTYPES ||                                   \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] <= 0 ||                    \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                        \
      ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +                \
       (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring])                 \
  ) {                                                                         \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "pre HT update for entry dirty SC failed"); \
}

#define H5C__POST_HT_UPDATE_FOR_ENTRY_CLEAN_SC(cache_ptr, entry_ptr, fail_val) \
if ((cache_ptr)->index_size != ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
    (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||                \
    (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||                \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                        \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +               \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring])                \
  ) {                                                                         \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "post HT update for entry clean SC failed"); \
}

#define H5C__POST_HT_UPDATE_FOR_ENTRY_DIRTY_SC(cache_ptr, entry_ptr, fail_val) \
if ((cache_ptr)->index_size != ((cache_ptr)->clean_index_size + (cache_ptr)->dirty_index_size) || \
    (cache_ptr)->index_size < (cache_ptr)->clean_index_size ||                \
    (cache_ptr)->index_size < (cache_ptr)->dirty_index_size ||                \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring] > (cache_ptr)->index_len || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] > (cache_ptr)->index_size || \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] !=                        \
       ((cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] +               \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring])                \
  ) {                                                                         \
    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, (fail_val), "post HT update for entry dirty SC failed"); \
}

#else /* H5C_DO_SANITY_CHECKS */

#define H5C__PRE_HT_INSERT_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__POST_HT_INSERT_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__PRE_HT_REMOVE_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__POST_HT_REMOVE_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__PRE_HT_SEARCH_SC(cache_ptr, entry_addr, fail_val)
#define H5C__POST_SUC_HT_SEARCH_SC(cache_ptr, entry_ptr, k, fail_val)
#define H5C__POST_HT_SHIFT_TO_FRONT_SC(cache_ptr, entry_ptr, k, fail_val)
#define H5C__PRE_HT_UPDATE_FOR_ENTRY_CLEAN_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__PRE_HT_UPDATE_FOR_ENTRY_DIRTY_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__PRE_HT_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size, entry_ptr, was_clean, fail_val)
#define H5C__POST_HT_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size, entry_ptr, fail_val)
#define H5C__POST_HT_UPDATE_FOR_ENTRY_CLEAN_SC(cache_ptr, entry_ptr, fail_val)
#define H5C__POST_HT_UPDATE_FOR_ENTRY_DIRTY_SC(cache_ptr, entry_ptr, fail_val)

#endif /* H5C_DO_SANITY_CHECKS */


#define H5C__INSERT_IN_INDEX(cache_ptr, entry_ptr, fail_val)                 \
do {                                                                         \
    int k;                                                                   \
    H5C__PRE_HT_INSERT_SC(cache_ptr, entry_ptr, fail_val)                    \
    k = H5C__HASH_FCN((entry_ptr)->addr);                                    \
    if((cache_ptr)->index[k] != NULL) {                                      \
        (entry_ptr)->ht_next = (cache_ptr)->index[k];                        \
        (entry_ptr)->ht_next->ht_prev = (entry_ptr);                         \
    }                                                                        \
    (cache_ptr)->index[k] = (entry_ptr);                                     \
    (cache_ptr)->index_len++;                                                \
    (cache_ptr)->index_size += (entry_ptr)->size;                            \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring]++;                        \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] += (entry_ptr)->size;    \
    if((entry_ptr)->is_dirty) {                                              \
        (cache_ptr)->dirty_index_size += (entry_ptr)->size;                  \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring] += (entry_ptr)->size; \
    } else {                                                                 \
        (cache_ptr)->clean_index_size += (entry_ptr)->size;                  \
        (cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] += (entry_ptr)->size; \
    }                                                                        \
    if((entry_ptr)->flush_me_last) {                                         \
        (cache_ptr)->num_last_entries++;                                     \
        assert((cache_ptr)->num_last_entries <= 2);                        \
    }                                                                        \
    H5C__IL_DLL_APPEND((entry_ptr), (cache_ptr)->il_head,                    \
                       (cache_ptr)->il_tail, (cache_ptr)->il_len,            \
                       (cache_ptr)->il_size, fail_val);                      \
    H5C__UPDATE_STATS_FOR_HT_INSERTION(cache_ptr);                           \
    H5C__POST_HT_INSERT_SC(cache_ptr, entry_ptr, fail_val);                  \
} while (0)

#define H5C__DELETE_FROM_INDEX(cache_ptr, entry_ptr, fail_val)               \
do {                                                                         \
    int k;                                                                   \
    H5C__PRE_HT_REMOVE_SC(cache_ptr, entry_ptr, fail_val)                    \
    k = H5C__HASH_FCN((entry_ptr)->addr);                                    \
    if((entry_ptr)->ht_next)                                                 \
        (entry_ptr)->ht_next->ht_prev = (entry_ptr)->ht_prev;                \
    if((entry_ptr)->ht_prev)                                                 \
        (entry_ptr)->ht_prev->ht_next = (entry_ptr)->ht_next;                \
    if((cache_ptr)->index[k] == (entry_ptr))                                 \
        (cache_ptr)->index[k] = (entry_ptr)->ht_next;                        \
    (entry_ptr)->ht_next = NULL;                                             \
    (entry_ptr)->ht_prev = NULL;                                             \
    (cache_ptr)->index_len--;                                                \
    (cache_ptr)->index_size -= (entry_ptr)->size;                            \
    (cache_ptr)->index_ring_len[(entry_ptr)->ring]--;                        \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] -= (entry_ptr)->size;    \
    if((entry_ptr)->is_dirty) {                                              \
        (cache_ptr)->dirty_index_size -= (entry_ptr)->size;                  \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring] -= (entry_ptr)->size; \
    } else {                                                                 \
        (cache_ptr)->clean_index_size -= (entry_ptr)->size;                  \
        (cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] -= (entry_ptr)->size; \
    }                                                                        \
    if((entry_ptr)->flush_me_last) {                                         \
        (cache_ptr)->num_last_entries--;                                     \
        assert((cache_ptr)->num_last_entries <= 1);                        \
    }                                                                        \
    H5C__IL_DLL_REMOVE((entry_ptr), (cache_ptr)->il_head,                    \
                       (cache_ptr)->il_tail, (cache_ptr)->il_len,            \
                       (cache_ptr)->il_size, fail_val);                      \
    H5C__UPDATE_STATS_FOR_HT_DELETION(cache_ptr);                            \
    H5C__POST_HT_REMOVE_SC(cache_ptr, entry_ptr, fail_val);                  \
} while (0)

#define H5C__SEARCH_INDEX(cache_ptr, entry_addr, entry_ptr, fail_val)        \
do {                                                                         \
    int k;                                                                   \
    int depth = 0;                                                           \
    H5C__PRE_HT_SEARCH_SC(cache_ptr, entry_addr, fail_val);                   \
    k = H5C__HASH_FCN(entry_addr);                                           \
    (entry_ptr) = (cache_ptr)->index[k];                                     \
    while(entry_ptr) {                                                       \
        if(H5_addr_eq(entry_addr, (entry_ptr)->addr)) {                      \
            H5C__POST_SUC_HT_SEARCH_SC(cache_ptr, entry_ptr, k, fail_val);    \
            if((entry_ptr) != (cache_ptr)->index[k]) {                       \
                if((entry_ptr)->ht_next)                                     \
                    (entry_ptr)->ht_next->ht_prev = (entry_ptr)->ht_prev;    \
                assert((entry_ptr)->ht_prev != NULL);                      \
                (entry_ptr)->ht_prev->ht_next = (entry_ptr)->ht_next;        \
                (cache_ptr)->index[k]->ht_prev = (entry_ptr);                \
                (entry_ptr)->ht_next = (cache_ptr)->index[k];                \
                (entry_ptr)->ht_prev = NULL;                                 \
                (cache_ptr)->index[k] = (entry_ptr);                         \
                H5C__POST_HT_SHIFT_TO_FRONT_SC(cache_ptr, entry_ptr, k, fail_val); \
            }                                                                \
            break;                                                           \
        }                                                                    \
        (entry_ptr) = (entry_ptr)->ht_next;                                  \
        (depth)++;                                                           \
    }                                                                        \
    H5C__UPDATE_STATS_FOR_HT_SEARCH(cache_ptr, ((entry_ptr) != NULL), depth); \
} while (0)

#define H5C__UPDATE_INDEX_FOR_ENTRY_CLEAN(cache_ptr, entry_ptr, fail_val)  \
do {                                                                          \
    H5C__PRE_HT_UPDATE_FOR_ENTRY_CLEAN_SC(cache_ptr, entry_ptr, fail_val);  \
    (cache_ptr)->dirty_index_size -= (entry_ptr)->size;                    \
    (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring] -= (entry_ptr)->size; \
    (cache_ptr)->clean_index_size += (entry_ptr)->size;                    \
    (cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] += (entry_ptr)->size; \
    H5C__POST_HT_UPDATE_FOR_ENTRY_CLEAN_SC(cache_ptr, entry_ptr, fail_val); \
} while (0)

#define H5C__UPDATE_INDEX_FOR_ENTRY_DIRTY(cache_ptr, entry_ptr, fail_val)  \
do {                                                                          \
    H5C__PRE_HT_UPDATE_FOR_ENTRY_DIRTY_SC(cache_ptr, entry_ptr, fail_val);  \
    (cache_ptr)->clean_index_size -= (entry_ptr)->size;                    \
    (cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] -= (entry_ptr)->size; \
    (cache_ptr)->dirty_index_size += (entry_ptr)->size;                    \
    (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring] += (entry_ptr)->size; \
    H5C__POST_HT_UPDATE_FOR_ENTRY_DIRTY_SC(cache_ptr, entry_ptr, fail_val); \
} while (0)

#define H5C__UPDATE_INDEX_FOR_SIZE_CHANGE(cache_ptr, old_size, new_size,    \
                                entry_ptr, was_clean, fail_val)             \
do {                                                                           \
    H5C__PRE_HT_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size,         \
                            entry_ptr, was_clean, fail_val);                 \
    (cache_ptr)->index_size -= (old_size);                                  \
    (cache_ptr)->index_size += (new_size);                                  \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] -= (old_size);          \
    (cache_ptr)->index_ring_size[(entry_ptr)->ring] += (new_size);          \
    if(was_clean) {                                                         \
        (cache_ptr)->clean_index_size -= (old_size);                        \
        (cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] -= (old_size); \
    } else {                                                                \
        (cache_ptr)->dirty_index_size -= (old_size);                        \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring] -= (old_size); \
    }                                                                       \
    if((entry_ptr)->is_dirty) {                                             \
        (cache_ptr)->dirty_index_size += (new_size);                        \
        (cache_ptr)->dirty_index_ring_size[(entry_ptr)->ring] += (new_size); \
    } else {                                                                \
        (cache_ptr)->clean_index_size += (new_size);                        \
        (cache_ptr)->clean_index_ring_size[(entry_ptr)->ring] += (new_size); \
    }                                                                       \
    H5C__DLL_UPDATE_FOR_SIZE_CHANGE((cache_ptr)->il_len,                    \
                                    (cache_ptr)->il_size,                   \
                                    (old_size), (new_size), (fail_val));     \
    H5C__POST_HT_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size,        \
                                      entry_ptr, fail_val);                  \
} while (0)


/**************************************************************************
 *
 * Skip list modification macros
 *
 **************************************************************************/

#ifdef H5C_DO_SLIST_SANITY_CHECKS

#define H5C__ENTRY_IN_SLIST(cache_ptr, entry_ptr) \
    H5C__entry_in_skip_list((cache_ptr), (entry_ptr))

#else /* H5C_DO_SLIST_SANITY_CHECKS */

#define H5C__ENTRY_IN_SLIST(cache_ptr, entry_ptr) false

#endif /* H5C_DO_SLIST_SANITY_CHECKS */


#ifdef H5C_DO_SANITY_CHECKS

#define H5C__SLIST_INSERT_ENTRY_SC(cache_ptr, entry_ptr)              \
do {                                                                     \
    (cache_ptr)->slist_len_increase++;                                \
    (cache_ptr)->slist_size_increase += (int64_t)((entry_ptr)->size); \
} while (0) /* H5C__SLIST_INSERT_ENTRY_SC() */
#define H5C__SLIST_REMOVE_ENTRY_SC(cache_ptr, entry_ptr)              \
do {                                                                     \
    (cache_ptr)->slist_len_increase--;                                \
    (cache_ptr)->slist_size_increase -= (int64_t)((entry_ptr)->size); \
} while (0) /* H5C__SLIST_REMOVE_ENTRY_SC() */
#define H5C__SLIST_UPDATE_FOR_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size) \
do {                                                                     \
    (cache_ptr)->slist_size_increase -= (int64_t)(old_size);          \
    (cache_ptr)->slist_size_increase += (int64_t)(new_size);          \
} while (0) /* H5C__SLIST_UPDATE_FOR_ENTRY_SIZE_CHANGE_SC() */

#else /* H5C_DO_SANITY_CHECKS */

#define H5C__SLIST_INSERT_ENTRY_SC(cache_ptr, entry_ptr)
#define H5C__SLIST_REMOVE_ENTRY_SC(cache_ptr, entry_ptr)
#define H5C__SLIST_UPDATE_FOR_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size)

#endif /* H5C_DO_SANITY_CHECKS */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__INSERT_ENTRY_IN_SLIST
 *
 * Purpose:     Insert a cache entry into a cache's skip list.  Updates
 *              the associated length and size fields.
 *
 * Note:        This macro is set up so that H5C_DO_SANITY_CHECKS and
 *              H5C_DO_SLIST_SANITY_CHECKS can be selected independently.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, entry_ptr, fail_val)             \
do {                                                                           \
    assert(cache_ptr);                                                       \
                                                                               \
    if((cache_ptr)->slist_enabled) {                                           \
        assert(entry_ptr);                                                   \
        assert((entry_ptr)->size > 0);                                       \
        assert(H5_addr_defined((entry_ptr)->addr));                         \
        assert(!(entry_ptr)->in_slist);                                      \
        assert(!H5C__ENTRY_IN_SLIST((cache_ptr), (entry_ptr)));              \
        assert((entry_ptr)->ring > H5C_RING_UNDEFINED);                      \
        assert((entry_ptr)->ring < H5C_RING_NTYPES);                         \
        assert((cache_ptr)->slist_ring_len[(entry_ptr)->ring] <=             \
                 (cache_ptr)->slist_len);                                      \
        assert((cache_ptr)->slist_ring_size[(entry_ptr)->ring] <=            \
                 (cache_ptr)->slist_size);                                     \
        assert((cache_ptr)->slist_ptr);                                      \
                                                                               \
        if(H5SL_insert((cache_ptr)->slist_ptr, entry_ptr, &((entry_ptr)->addr)) < 0) \
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, (fail_val), "can't insert entry in skip list"); \
                                                                               \
        (entry_ptr)->in_slist = true;                                          \
        (cache_ptr)->slist_changed = true;                                     \
        (cache_ptr)->slist_len++;                                              \
        (cache_ptr)->slist_size += (entry_ptr)->size;                          \
        ((cache_ptr)->slist_ring_len[(entry_ptr)->ring])++;                    \
        ((cache_ptr)->slist_ring_size[(entry_ptr)->ring]) += (entry_ptr)->size;\
        H5C__SLIST_INSERT_ENTRY_SC(cache_ptr, entry_ptr);                      \
                                                                               \
        assert((cache_ptr)->slist_len > 0);                                  \
        assert((cache_ptr)->slist_size > 0);                                 \
    } else { /* slist disabled */                                              \
        assert((cache_ptr)->slist_len == 0);                                 \
        assert((cache_ptr)->slist_size == 0);                                \
    }                                                                          \
} while (0) /* H5C__INSERT_ENTRY_IN_SLIST */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__REMOVE_ENTRY_FROM_SLIST
 *
 * Purpose:     Insert a cache entry into a cache's skip list.  Updates
 *              the associated length and size fields.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__REMOVE_ENTRY_FROM_SLIST(cache_ptr, entry_ptr, during_flush, fail_val) \
do {                                                                              \
    assert(cache_ptr);                                                       \
                                                                               \
    if((cache_ptr)->slist_enabled) {                                           \
        assert(entry_ptr);                                                   \
        assert(!(entry_ptr)->is_read_only);                                  \
        assert((entry_ptr)->ro_ref_count == 0);                              \
        assert((entry_ptr)->size > 0);                                       \
        assert((entry_ptr)->in_slist);                                       \
        assert((cache_ptr)->slist_ptr);                                      \
        assert((entry_ptr)->ring > H5C_RING_UNDEFINED);                      \
        assert((entry_ptr)->ring < H5C_RING_NTYPES);                         \
        assert((cache_ptr)->slist_ring_len[(entry_ptr)->ring] <=             \
                 (cache_ptr)->slist_len);                                      \
        assert((cache_ptr)->slist_ring_size[(entry_ptr)->ring] <=            \
                 (cache_ptr)->slist_size);                                     \
        assert((cache_ptr)->slist_size >= (entry_ptr)->size);                \
                                                                               \
        if(H5SL_remove((cache_ptr)->slist_ptr, &(entry_ptr)->addr) != (entry_ptr) ) \
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, (fail_val), "can't delete entry from skip list"); \
                                                                               \
        assert((cache_ptr)->slist_len > 0);                                  \
        if(!(during_flush))                                                    \
            (cache_ptr)->slist_changed = true;                                 \
        (cache_ptr)->slist_len--;                                              \
        assert((cache_ptr)->slist_size >= (entry_ptr)->size);                \
        (cache_ptr)->slist_size -= (entry_ptr)->size;                          \
        (cache_ptr)->slist_ring_len[(entry_ptr)->ring]--;                      \
        assert((cache_ptr)->slist_ring_size[(entry_ptr)->ring] >= (entry_ptr)->size); \
        ((cache_ptr)->slist_ring_size[(entry_ptr)->ring]) -= (entry_ptr)->size;\
        H5C__SLIST_REMOVE_ENTRY_SC(cache_ptr, entry_ptr);                       \
        (entry_ptr)->in_slist = false;                                         \
    } else { /* slist disabled */                                              \
        assert((cache_ptr)->slist_len == 0);                                 \
        assert((cache_ptr)->slist_size == 0);                                \
    }                                                                          \
} while (0) /* H5C__REMOVE_ENTRY_FROM_SLIST */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_SLIST_FOR_SIZE_CHANGE
 *
 * Purpose:     Update cache_ptr->slist_size for a change in the size of
 *              and entry in the slist.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_SLIST_FOR_SIZE_CHANGE(cache_ptr, old_size, new_size)      \
do {                                                                          \
    assert(cache_ptr);                                                      \
                                                                              \
    if((cache_ptr)->slist_enabled) {                                          \
        assert((old_size) > 0);                                             \
        assert((new_size) > 0);                                             \
        assert((old_size) <= (cache_ptr)->slist_size);                      \
        assert((cache_ptr)->slist_len > 0);                                 \
        assert((cache_ptr)->slist_len > 1 ||                                \
                 (cache_ptr)->slist_size == (old_size));                      \
        assert((entry_ptr)->ring > H5C_RING_UNDEFINED);                     \
        assert((entry_ptr)->ring < H5C_RING_NTYPES);                        \
        assert((cache_ptr)->slist_ring_len[(entry_ptr)->ring] <=            \
                 (cache_ptr)->slist_len);                                     \
        assert((cache_ptr)->slist_ring_size[(entry_ptr)->ring] <=           \
                 (cache_ptr)->slist_size);                                    \
                                                                              \
        (cache_ptr)->slist_size -= (old_size);                                \
        (cache_ptr)->slist_size += (new_size);                                \
                                                                              \
        assert((cache_ptr)->slist_ring_size[(entry_ptr)->ring] >= (old_size)); \
                                                                              \
        (cache_ptr)->slist_ring_size[(entry_ptr)->ring] -= (old_size);        \
        (cache_ptr)->slist_ring_size[(entry_ptr)->ring] += (new_size);        \
                                                                              \
        H5C__SLIST_UPDATE_FOR_ENTRY_SIZE_CHANGE_SC(cache_ptr, old_size, new_size); \
                                                                              \
        assert((new_size) <= (cache_ptr)->slist_size);                      \
        assert((cache_ptr)->slist_len > 1 ||                                \
                  (cache_ptr)->slist_size == (new_size));                     \
    } else { /* slist disabled */                                             \
        assert((cache_ptr)->slist_len == 0);                                \
        assert((cache_ptr)->slist_size == 0);                               \
    }                                                                         \
} while (0) /* H5C__UPDATE_SLIST_FOR_SIZE_CHANGE */


/**************************************************************************
 *
 * Replacement policy update macros
 *
 **************************************************************************/

#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS

#define H5C__UPDATE_RP_FOR_EVICTION_CD_LRU(cache_ptr, entry_ptr, fail_val)   \
do {                                                                            \
    /* If the entry is clean when it is evicted, it should be on the         \
     * clean LRU list, if it was dirty, it should be on the dirty LRU list.  \
     * Remove it from the appropriate list according to the value of the     \
     * dirty flag.                                                           \
     */                                                                      \
    if((entry_ptr)->is_dirty) {                                              \
        H5C__AUX_DLL_REMOVE((entry_ptr), (cache_ptr)->dLRU_head_ptr,         \
                            (cache_ptr)->dLRU_tail_ptr,                      \
                            (cache_ptr)->dLRU_list_len,                      \
                            (cache_ptr)->dLRU_list_size, (fail_val))         \
    } else {                                                                 \
        H5C__AUX_DLL_REMOVE((entry_ptr), (cache_ptr)->cLRU_head_ptr,         \
                            (cache_ptr)->cLRU_tail_ptr,                      \
                            (cache_ptr)->cLRU_list_len,                      \
                            (cache_ptr)->cLRU_list_size, (fail_val))         \
    }                                                                        \
} while (0) /* H5C__UPDATE_RP_FOR_EVICTION_CD_LRU() */

#define H5C__UPDATE_RP_FOR_FLUSH_CD_LRU(cache_ptr, entry_ptr, fail_val)      \
do {                                                                            \
    /* An entry being flushed or cleared, may not be dirty.  Use the         \
     * dirty flag to infer whether the entry is on the clean or dirty        \
     * LRU list, and remove it.  Then insert it at the head of the           \
     * clean LRU list.                                                       \
     *                                                                       \
     * The function presumes that a dirty entry will be either cleared       \
     * or flushed shortly, so it is OK if we put a dirty entry on the        \
     * clean LRU list.                                                       \
     */                                                                      \
    if((entry_ptr)->is_dirty) {                                              \
        H5C__AUX_DLL_REMOVE((entry_ptr), (cache_ptr)->dLRU_head_ptr,         \
                            (cache_ptr)->dLRU_tail_ptr,                      \
                            (cache_ptr)->dLRU_list_len,                      \
                            (cache_ptr)->dLRU_list_size, (fail_val))         \
    } else {                                                                 \
        H5C__AUX_DLL_REMOVE((entry_ptr), (cache_ptr)->cLRU_head_ptr,         \
                            (cache_ptr)->cLRU_tail_ptr,                      \
                            (cache_ptr)->cLRU_list_len,                      \
                            (cache_ptr)->cLRU_list_size, (fail_val))         \
    }                                                                        \
                                                                             \
    H5C__AUX_DLL_PREPEND((entry_ptr), (cache_ptr)->cLRU_head_ptr,            \
                         (cache_ptr)->cLRU_tail_ptr,                         \
                         (cache_ptr)->cLRU_list_len,                         \
                         (cache_ptr)->cLRU_list_size, (fail_val))            \
} while (0) /* H5C__UPDATE_RP_FOR_FLUSH_CD_LRU() */

#define H5C__UPDATE_RP_FOR_INSERT_APPEND_CD_LRU(cache_ptr, entry_ptr, fail_val) \
do {                                                                            \
    /* Insert the entry at the _tail_ of the clean or dirty LRU list as      \
     * appropriate.                                                          \
     */                                                                      \
    if((entry_ptr)->is_dirty) {                                              \
        H5C__AUX_DLL_APPEND((entry_ptr), (cache_ptr)->dLRU_head_ptr,         \
                            (cache_ptr)->dLRU_tail_ptr,                      \
                            (cache_ptr)->dLRU_list_len,                      \
                            (cache_ptr)->dLRU_list_size, (fail_val));        \
    } else {                                                                 \
        H5C__AUX_DLL_APPEND((entry_ptr), (cache_ptr)->cLRU_head_ptr,         \
                            (cache_ptr)->cLRU_tail_ptr,                      \
                            (cache_ptr)->cLRU_list_len,                      \
                            (cache_ptr)->cLRU_list_size, (fail_val));        \
    }                                                                        \
} while (0) /* H5C__UPDATE_RP_FOR_INSERT_APPEND_CD_LRU() */

#define H5C__UPDATE_RP_FOR_INSERTION_CD_LRU(cache_ptr, entry_ptr, fail_val) \
do {                                                                           \
    /* Insert the entry at the head of the clean or dirty LRU list as       \
     * appropriate.                                                         \
     */                                                                     \
    if((entry_ptr)->is_dirty) {                                             \
        H5C__AUX_DLL_PREPEND((entry_ptr), (cache_ptr)->dLRU_head_ptr,       \
                             (cache_ptr)->dLRU_tail_ptr,                    \
                             (cache_ptr)->dLRU_list_len,                    \
                             (cache_ptr)->dLRU_list_size, (fail_val))       \
    } else {                                                                \
        H5C__AUX_DLL_PREPEND((entry_ptr), (cache_ptr)->cLRU_head_ptr,       \
                             (cache_ptr)->cLRU_tail_ptr,                    \
                             (cache_ptr)->cLRU_list_len,                    \
                             (cache_ptr)->cLRU_list_size, (fail_val))       \
    }                                                                       \
} while(0) /* H5C__UPDATE_RP_FOR_INSERTION_CD_LRU() */

#define H5C__UPDATE_RP_FOR_PROTECT_CD_LRU(cache_ptr, entry_ptr, fail_val)   \
{                                                                           \
    /* Remove the entry from the clean or dirty LRU list as appropriate */  \
    if((entry_ptr)->is_dirty) {                                             \
        H5C__AUX_DLL_REMOVE((entry_ptr), (cache_ptr)->dLRU_head_ptr,        \
                            (cache_ptr)->dLRU_tail_ptr,                     \
                            (cache_ptr)->dLRU_list_len,                     \
                            (cache_ptr)->dLRU_list_size, (fail_val))        \
    } else {                                                                \
        H5C__AUX_DLL_REMOVE((entry_ptr), (cache_ptr)->cLRU_head_ptr,        \
                            (cache_ptr)->cLRU_tail_ptr,                     \
                            (cache_ptr)->cLRU_list_len,                     \
                            (cache_ptr)->cLRU_list_size, (fail_val))        \
    }                                                                       \
} while (0) /* H5C__UPDATE_RP_FOR_PROTECT_CD_LRU() */

#define H5C__UPDATE_RP_FOR_MOVE_CD_LRU(cache_ptr, entry_ptr, was_dirty, fail_val) \
do {                                                                      \
    /* Remove the entry from either the clean or dirty LRU list as     \
     * indicated by the was_dirty parameter                            \
     */                                                                \
    if(was_dirty) {                                                    \
        H5C__AUX_DLL_REMOVE((entry_ptr),                               \
                             (cache_ptr)->dLRU_head_ptr,               \
                             (cache_ptr)->dLRU_tail_ptr,               \
                             (cache_ptr)->dLRU_list_len,               \
                             (cache_ptr)->dLRU_list_size, (fail_val));  \
                                                                       \
    } else {                                                           \
        H5C__AUX_DLL_REMOVE((entry_ptr),                               \
                             (cache_ptr)->cLRU_head_ptr,               \
                             (cache_ptr)->cLRU_tail_ptr,               \
                             (cache_ptr)->cLRU_list_len,               \
                             (cache_ptr)->cLRU_list_size, (fail_val));  \
    }                                                                  \
                                                                       \
    /* Insert the entry at the head of either the clean or dirty       \
     * LRU list as appropriate.                                        \
     */                                                                \
    if((entry_ptr)->is_dirty) {                                        \
        H5C__AUX_DLL_PREPEND((entry_ptr),                              \
                              (cache_ptr)->dLRU_head_ptr,              \
                              (cache_ptr)->dLRU_tail_ptr,              \
                              (cache_ptr)->dLRU_list_len,              \
                              (cache_ptr)->dLRU_list_size, (fail_val)); \
    } else {                                                           \
        H5C__AUX_DLL_PREPEND((entry_ptr),                              \
                              (cache_ptr)->cLRU_head_ptr,              \
                              (cache_ptr)->cLRU_tail_ptr,              \
                              (cache_ptr)->cLRU_list_len,              \
                              (cache_ptr)->cLRU_list_size, (fail_val)); \
    }                                                                  \
} while (0) /* H5C__UPDATE_RP_FOR_MOVE_CD_LRU() */

#define H5C__UPDATE_RP_FOR_SIZE_CHANGE_CD_LRU(cache_ptr, entry_ptr, new_size, fail_val) \
do {                                                                     \
    /* Update the size of the clean or dirty LRU list as              \
     * appropriate.                                                   \
     */                                                               \
    if((entry_ptr)->is_dirty) {                                       \
        H5C__DLL_UPDATE_FOR_SIZE_CHANGE((cache_ptr)->dLRU_list_len,   \
                                        (cache_ptr)->dLRU_list_size,  \
                                        (entry_ptr)->size,            \
                                        (new_size), (fail_val))       \
                                                                      \
    } else {                                                          \
        H5C__DLL_UPDATE_FOR_SIZE_CHANGE((cache_ptr)->cLRU_list_len,   \
                                        (cache_ptr)->cLRU_list_size,  \
                                        (entry_ptr)->size,            \
                                        (new_size), (fail_val))       \
    }                                                                 \
} while (0) /* H5C__UPDATE_RP_FOR_SIZE_CHANGE_CD_LRU() */

#define H5C__UPDATE_RP_FOR_UNPIN_CD_LRU(cache_ptr, entry_ptr, fail_val) \
do {                                                                       \
    /* Insert the entry at the head of either the clean                 \
     * or dirty LRU list as appropriate.                                \
     */                                                                 \
    if((entry_ptr)->is_dirty) {                                         \
        H5C__AUX_DLL_PREPEND((entry_ptr),                               \
                              (cache_ptr)->dLRU_head_ptr,               \
                              (cache_ptr)->dLRU_tail_ptr,               \
                              (cache_ptr)->dLRU_list_len,               \
                              (cache_ptr)->dLRU_list_size, (fail_val))  \
    } else {                                                            \
        H5C__AUX_DLL_PREPEND((entry_ptr),                               \
                              (cache_ptr)->cLRU_head_ptr,               \
                              (cache_ptr)->cLRU_tail_ptr,               \
                              (cache_ptr)->cLRU_list_len,               \
                              (cache_ptr)->cLRU_list_size, (fail_val))  \
     }                                                                  \
} while (0) /* H5C__UPDATE_RP_FOR_UNPIN_CD_LRU() */

#define H5C__UPDATE_RP_FOR_UNPROTECT_CD_LRU(cache_ptr, entry_ptr, fail_val) \
do {                                                                      \
    /* Insert the entry at the head of either the clean or             \
     * dirty LRU list as appropriate.                                  \
     */                                                                \
    if((entry_ptr)->is_dirty) {                                        \
        H5C__AUX_DLL_PREPEND((entry_ptr), (cache_ptr)->dLRU_head_ptr,  \
                             (cache_ptr)->dLRU_tail_ptr,               \
                             (cache_ptr)->dLRU_list_len,               \
                             (cache_ptr)->dLRU_list_size, (fail_val))  \
    } else {                                                           \
        H5C__AUX_DLL_PREPEND((entry_ptr), (cache_ptr)->cLRU_head_ptr,  \
                             (cache_ptr)->cLRU_tail_ptr,               \
                             (cache_ptr)->cLRU_list_len,               \
                             (cache_ptr)->cLRU_list_size, (fail_val))  \
    }                                                                  \
} while (0) /* H5C__UPDATE_RP_FOR_UNPROTECT_CD_LRU() */

#else /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */

#define H5C__UPDATE_RP_FOR_EVICTION_CD_LRU(cache_ptr, entry_ptr, fail_val)
#define H5C__UPDATE_RP_FOR_FLUSH_CD_LRU(cache_ptr, entry_ptr, fail_val)
#define H5C__UPDATE_RP_FOR_INSERT_APPEND_CD_LRU(cache_ptr, entry_ptr, fail_val)
#define H5C__UPDATE_RP_FOR_INSERTION_CD_LRU(cache_ptr, entry_ptr, fail_val)
#define H5C__UPDATE_RP_FOR_PROTECT_CD_LRU(cache_ptr, entry_ptr, fail_val)
#define H5C__UPDATE_RP_FOR_MOVE_CD_LRU(cache_ptr, entry_ptr, was_dirty, fail_val)
#define H5C__UPDATE_RP_FOR_SIZE_CHANGE_CD_LRU(cache_ptr, entry_ptr, new_size, fail_val)
#define H5C__UPDATE_RP_FOR_UNPIN_CD_LRU(cache_ptr, entry_ptr, fail_val)
#define H5C__UPDATE_RP_FOR_UNPROTECT_CD_LRU(cache_ptr, entry_ptr, fail_val)

#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */

/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_EVICTION
 *
 * Purpose:     Update the replacement policy data structures for an
 *              eviction of the specified cache entry.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_EVICTION(cache_ptr, entry_ptr, fail_val)          \
do {                                                                            \
    assert(cache_ptr);                                                     \
    assert(entry_ptr);                                                     \
    assert(!(entry_ptr)->is_protected);                                    \
    assert(!(entry_ptr)->is_read_only);                                    \
    assert((entry_ptr)->ro_ref_count == 0);                                \
    assert(!(entry_ptr)->is_pinned);                                       \
    assert((entry_ptr)->size > 0);                                         \
                                                                             \
    /* Remove the entry from the LRU list */                                 \
    H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->LRU_head_ptr,                  \
                    (cache_ptr)->LRU_tail_ptr, (cache_ptr)->LRU_list_len,    \
                    (cache_ptr)->LRU_list_size, (fail_val))                  \
                                                                             \
    /* Remove the entry from the clean & dirty LRU lists, if enabled */      \
    H5C__UPDATE_RP_FOR_EVICTION_CD_LRU(cache_ptr, entry_ptr, fail_val);      \
} while (0) /* H5C__UPDATE_RP_FOR_EVICTION */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_FLUSH
 *
 * Purpose:     Update the replacement policy data structures for a flush
 *              of the specified cache entry.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_FLUSH(cache_ptr, entry_ptr, fail_val)            \
do {                                                                           \
    assert(cache_ptr);                                                    \
    assert(entry_ptr);                                                    \
    assert(!(entry_ptr)->is_protected);                                   \
    assert(!(entry_ptr)->is_read_only);                                   \
    assert((entry_ptr)->ro_ref_count == 0);                               \
    assert((entry_ptr)->size > 0);                                        \
                                                                            \
    if(!(entry_ptr)->is_pinned) {                                           \
        /* Remove the entry from its location in the LRU list               \
         * and re-insert it at the head of the list.                        \
         */                                                                 \
        H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->LRU_head_ptr,             \
                        (cache_ptr)->LRU_tail_ptr,                          \
                        (cache_ptr)->LRU_list_len,                          \
                        (cache_ptr)->LRU_list_size, (fail_val))             \
                                                                            \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->LRU_head_ptr,            \
                         (cache_ptr)->LRU_tail_ptr,                         \
                         (cache_ptr)->LRU_list_len,                         \
                         (cache_ptr)->LRU_list_size, (fail_val))            \
                                                                            \
        /* Maintain the clean & dirty LRU lists, if enabled */              \
        H5C__UPDATE_RP_FOR_FLUSH_CD_LRU(cache_ptr, entry_ptr, fail_val);    \
    }                                                                       \
} while (0) /* H5C__UPDATE_RP_FOR_FLUSH */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_INSERT_APPEND
 *
 * Purpose:     Update the replacement policy data structures for an
 *              insertion of the specified cache entry.
 *
 *              Unlike H5C__UPDATE_RP_FOR_INSERTION below, insert a non-pinned
 *              new entry as the LEAST recently used entry, not the
 *              most recently used.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_INSERT_APPEND(cache_ptr, entry_ptr, fail_val)   \
do {                                                                       \
    assert(cache_ptr);                                                   \
    assert(entry_ptr);                                                   \
    assert(!(entry_ptr)->is_protected);                                  \
    assert(!(entry_ptr)->is_read_only);                                  \
    assert((entry_ptr)->ro_ref_count == 0 );                             \
    assert((entry_ptr)->size > 0 );                                      \
                                                                           \
    if((entry_ptr)->is_pinned) {                                           \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->pel_head_ptr,           \
                         (cache_ptr)->pel_tail_ptr,                        \
                         (cache_ptr)->pel_len,                             \
                         (cache_ptr)->pel_size, (fail_val))                \
    } else {                                                               \
        /* Insert the entry at the tail of the LRU list. */                \
        H5C__DLL_APPEND((entry_ptr), (cache_ptr)->LRU_head_ptr,            \
                        (cache_ptr)->LRU_tail_ptr,                         \
                        (cache_ptr)->LRU_list_len,                         \
                        (cache_ptr)->LRU_list_size, (fail_val))            \
                                                                           \
        /* Maintain the clean & dirty LRU lists, if enabled */             \
        H5C__UPDATE_RP_FOR_INSERT_APPEND_CD_LRU(cache_ptr, entry_ptr, fail_val); \
    }                                                                      \
} while (0)


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_INSERTION
 *
 * Purpose:     Update the replacement policy data structures for an
 *              insertion of the specified cache entry.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_INSERTION(cache_ptr, entry_ptr, fail_val)         \
do {                                                                         \
    assert(cache_ptr);                                                       \
    assert(entry_ptr);                                                       \
    assert(!(entry_ptr)->is_protected);                                      \
    assert(!(entry_ptr)->is_read_only);                                      \
    assert((entry_ptr)->ro_ref_count == 0);                                  \
    assert((entry_ptr)->size > 0);                                           \
                                                                             \
    if((entry_ptr)->is_pinned) {                                             \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->pel_head_ptr,             \
                         (cache_ptr)->pel_tail_ptr,                          \
                         (cache_ptr)->pel_len,                               \
                         (cache_ptr)->pel_size, (fail_val))                  \
                                                                             \
    } else {                                                                 \
        /* Insert the entry at the head of the LRU list. */                  \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->LRU_head_ptr,             \
                         (cache_ptr)->LRU_tail_ptr,                          \
                         (cache_ptr)->LRU_list_len,                          \
                         (cache_ptr)->LRU_list_size, (fail_val))             \
                                                                             \
        /* Maintain the clean & dirty LRU lists, if enabled */               \
        H5C__UPDATE_RP_FOR_INSERTION_CD_LRU(cache_ptr, entry_ptr, fail_val); \
    }                                                                        \
} while (0)


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_PROTECT
 *
 * Purpose:     Update the replacement policy data structures for a
 *              protect of the specified cache entry.
 *
 *              To do this, unlink the specified entry from any data
 *              structures used by the replacement policy (or the pinned list,
 *              which is outside of the replacement policy), and add the
 *              entry to the protected list.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_PROTECT(cache_ptr, entry_ptr, fail_val)         \
do {                                                                       \
    assert(cache_ptr);                                                     \
    assert(entry_ptr);                                                     \
    assert(!(entry_ptr)->is_protected);                                    \
    assert(!(entry_ptr)->is_read_only);                                    \
    assert((entry_ptr)->ro_ref_count == 0);                                \
    assert((entry_ptr)->size > 0);                                         \
                                                                           \
    if((entry_ptr)->is_pinned) {                                           \
        H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->pel_head_ptr,            \
                        (cache_ptr)->pel_tail_ptr,                         \
                        (cache_ptr)->pel_len,                              \
                        (cache_ptr)->pel_size, (fail_val))                 \
    } else {                                                               \
        /* Remove the entry from the LRU list. */                          \
        H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->LRU_head_ptr,            \
                        (cache_ptr)->LRU_tail_ptr,                         \
                        (cache_ptr)->LRU_list_len,                         \
                        (cache_ptr)->LRU_list_size, (fail_val))            \
                                                                           \
        /* Maintain the clean & dirty LRU lists, if enabled */             \
        H5C__UPDATE_RP_FOR_PROTECT_CD_LRU(cache_ptr, entry_ptr, fail_val); \
    }                                                                      \
                                                                           \
    /* Regardless of whether the entry is pinned, add it to the protected  \
     * list.                                                               \
     */                                                                    \
    H5C__DLL_APPEND((entry_ptr), (cache_ptr)->pl_head_ptr,                 \
                    (cache_ptr)->pl_tail_ptr,                              \
                    (cache_ptr)->pl_len,                                   \
                    (cache_ptr)->pl_size, (fail_val))                      \
} while (0) /* H5C__UPDATE_RP_FOR_PROTECT */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_MOVE
 *
 * Purpose:     Update the replacement policy data structures for a
 *              move of the specified cache entry.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_MOVE(cache_ptr, entry_ptr, was_dirty, fail_val)   \
do {                                                                         \
    assert(cache_ptr);                                                     \
    assert(entry_ptr);                                                     \
    assert(!(entry_ptr)->is_read_only);                                    \
    assert((entry_ptr)->ro_ref_count == 0);                                \
    assert((entry_ptr)->size > 0);                                         \
                                                                             \
    if(!(entry_ptr)->is_pinned && !(entry_ptr)->is_protected) {              \
        /* Remove the entry from the LRU list, and re-insert it at the head. \
         */                                                                  \
        H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->LRU_head_ptr,              \
                         (cache_ptr)->LRU_tail_ptr,                          \
                         (cache_ptr)->LRU_list_len,                          \
                         (cache_ptr)->LRU_list_size, (fail_val))             \
                                                                             \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->LRU_head_ptr,             \
                         (cache_ptr)->LRU_tail_ptr,                          \
                         (cache_ptr)->LRU_list_len,                          \
                         (cache_ptr)->LRU_list_size, (fail_val))             \
                                                                             \
        /* Maintain the clean & dirty LRU lists, if enabled */               \
        H5C__UPDATE_RP_FOR_MOVE_CD_LRU(cache_ptr, entry_ptr, was_dirty, fail_val); \
    }                                                                        \
} while (0) /* H5C__UPDATE_RP_FOR_MOVE */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_SIZE_CHANGE
 *
 * Purpose:     Update the replacement policy data structures for a
 *              size change of the specified cache entry.
 *
 *              To do this, determine if the entry is pinned.  If it is,
 *              update the size of the pinned entry list.
 *
 *              If it isn't pinned, the entry must handled by the
 *              replacement policy.  Update the appropriate replacement
 *              policy data structures.
 *
 *              If the entry is accessed with collective operations for
 *              parallel I/O, update that list.
 *
 *-------------------------------------------------------------------------
 */

#ifdef H5_HAVE_PARALLEL

#define H5C__UPDATE_RP_FOR_SIZE_CHANGE_COLL(cache_ptr, entry_ptr, new_size, fail_val) \
do {                                                                      \
    if((entry_ptr)->coll_access) {                                     \
        H5C__DLL_UPDATE_FOR_SIZE_CHANGE((cache_ptr)->coll_list_len,    \
                                    (cache_ptr)->coll_list_size,       \
                                    (entry_ptr)->size,                 \
                                    (new_size), (fail_val));           \
                                                                       \
    }                                                                  \
} while (0) /* H5C__UPDATE_RP_FOR_SIZE_CHANGE_COLL() */

#else /* H5_HAVE_PARALLEL */

#define H5C__UPDATE_RP_FOR_SIZE_CHANGE_COLL(cache_ptr, entry_ptr, new_size, fail_val)

#endif /* H5_HAVE_PARALLEL */

#define H5C__UPDATE_RP_FOR_SIZE_CHANGE(cache_ptr, entry_ptr, new_size, fail_val)    \
do {                                                                         \
    assert(cache_ptr);                                                  \
    assert(entry_ptr);                                                  \
    assert(!(entry_ptr)->is_protected);                                 \
    assert(!(entry_ptr)->is_read_only);                                 \
    assert((entry_ptr)->ro_ref_count == 0);                             \
    assert((entry_ptr)->size > 0 );                                     \
    assert(new_size > 0 );                                              \
                                                                          \
    /* Maintain the collective access list, if enabled */                 \
    H5C__UPDATE_RP_FOR_SIZE_CHANGE_COLL(cache_ptr, entry_ptr, new_size, fail_val); \
                                                                          \
    if((entry_ptr)->is_pinned) {                                          \
        H5C__DLL_UPDATE_FOR_SIZE_CHANGE((cache_ptr)->pel_len,             \
                                    (cache_ptr)->pel_size,                \
                                    (entry_ptr)->size,                    \
                                    (new_size), (fail_val))               \
    } else {                                                              \
        /* Update the size of the LRU list */                             \
        H5C__DLL_UPDATE_FOR_SIZE_CHANGE((cache_ptr)->LRU_list_len,        \
                                        (cache_ptr)->LRU_list_size,       \
                                        (entry_ptr)->size,                \
                                        (new_size), (fail_val))           \
                                                                          \
        /* Maintain the clean & dirty LRU lists, if enabled */            \
        H5C__UPDATE_RP_FOR_SIZE_CHANGE_CD_LRU(cache_ptr, entry_ptr, new_size, fail_val); \
    }                                                                     \
} while (0) /* H5C__UPDATE_RP_FOR_SIZE_CHANGE */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_UNPIN
 *
 * Purpose:     Update the replacement policy data structures for an
 *              unpin of the specified cache entry.
 *
 *              To do this, unlink the specified entry from the pinned
 *              entry list, and re-insert it in the data structures used
 *              by the current replacement policy.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_UNPIN(cache_ptr, entry_ptr, fail_val)       \
do {                                                                   \
    assert(cache_ptr);                                               \
    assert(entry_ptr);                                               \
    assert(!(entry_ptr)->is_protected);                              \
    assert(!(entry_ptr)->is_read_only);                              \
    assert((entry_ptr)->ro_ref_count == 0 );                         \
    assert((entry_ptr)->is_pinned);                                  \
    assert((entry_ptr)->size > 0);                                   \
                                                                       \
    /* Regardless of the replacement policy, remove the entry from the \
     * pinned entry list.                                              \
     */                                                                \
    H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->pel_head_ptr,            \
                    (cache_ptr)->pel_tail_ptr, (cache_ptr)->pel_len,   \
                    (cache_ptr)->pel_size, (fail_val));                \
                                                                       \
    /* Insert the entry at the head of the LRU list. */                \
    H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->LRU_head_ptr,           \
                     (cache_ptr)->LRU_tail_ptr,                        \
                     (cache_ptr)->LRU_list_len,                        \
                     (cache_ptr)->LRU_list_size, (fail_val));          \
                                                                       \
    /* Maintain the clean & dirty LRU lists, if enabled */             \
    H5C__UPDATE_RP_FOR_UNPIN_CD_LRU(cache_ptr, entry_ptr, fail_val);   \
} while(0) /* H5C__UPDATE_RP_FOR_UNPIN */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__UPDATE_RP_FOR_UNPROTECT
 *
 * Purpose:     Update the replacement policy data structures for an
 *              unprotect of the specified cache entry.
 *
 *              To do this, unlink the specified entry from the protected
 *              list, and re-insert it in the data structures used by the
 *              current replacement policy.
 *
 *-------------------------------------------------------------------------
 */

#define H5C__UPDATE_RP_FOR_UNPROTECT(cache_ptr, entry_ptr, fail_val)       \
do {                                                                       \
    assert(cache_ptr);                                                   \
    assert(entry_ptr);                                                   \
    assert((entry_ptr)->is_protected);                                   \
    assert((entry_ptr)->size > 0);                                       \
                                                                           \
    /* Regardless of the replacement policy, remove the entry from the     \
     * protected list.                                                     \
     */                                                                    \
    H5C__DLL_REMOVE((entry_ptr), (cache_ptr)->pl_head_ptr,                 \
                    (cache_ptr)->pl_tail_ptr, (cache_ptr)->pl_len,         \
                    (cache_ptr)->pl_size, (fail_val))                      \
                                                                           \
    if((entry_ptr)->is_pinned) {                                           \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->pel_head_ptr,           \
                         (cache_ptr)->pel_tail_ptr,                        \
                         (cache_ptr)->pel_len,                             \
                         (cache_ptr)->pel_size, (fail_val))                \
    } else {                                                               \
        /* Insert the entry at the head of the LRU list. */                \
        H5C__DLL_PREPEND((entry_ptr), (cache_ptr)->LRU_head_ptr,           \
                         (cache_ptr)->LRU_tail_ptr,                        \
                         (cache_ptr)->LRU_list_len,                        \
                         (cache_ptr)->LRU_list_size, (fail_val))           \
                                                                           \
        /* Maintain the clean & dirty LRU lists, if enabled */             \
        H5C__UPDATE_RP_FOR_UNPROTECT_CD_LRU(cache_ptr, entry_ptr, fail_val); \
    }                                                                      \
} while (0) /* H5C__UPDATE_RP_FOR_UNPROTECT */


#ifdef H5_HAVE_PARALLEL

/* Macros that modify the "collective I/O" LRU list */
#define H5C__COLL_DLL_PREPEND(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val) \
    H5C__GEN_DLL_PREPEND(entry_ptr, coll_next, coll_prev, head_ptr, tail_ptr, len, list_size, fail_val)
#define H5C__COLL_DLL_REMOVE(entry_ptr, head_ptr, tail_ptr, len, list_size, fail_val)  \
    H5C__GEN_DLL_REMOVE(entry_ptr, coll_next, coll_prev, head_ptr, tail_ptr, len, list_size, fail_val)


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__INSERT_IN_COLL_LIST
 *
 * Purpose:     Insert entry into collective entries list
 *
 *-------------------------------------------------------------------------
 */

#define H5C__INSERT_IN_COLL_LIST(cache_ptr, entry_ptr, fail_val)        \
do {                                                                       \
    assert(cache_ptr);                                                \
    assert(entry_ptr);                                                \
                                                                        \
    /* Insert the entry at the head of the list. */                     \
    H5C__COLL_DLL_PREPEND((entry_ptr), (cache_ptr)->coll_head_ptr,      \
                          (cache_ptr)->coll_tail_ptr,                   \
                          (cache_ptr)->coll_list_len,                   \
                          (cache_ptr)->coll_list_size, (fail_val))      \
} while (0) /* H5C__INSERT_IN_COLL_LIST */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__REMOVE_FROM_COLL_LIST
 *
 * Purpose:     Remove entry from collective entries list
 *
 *-------------------------------------------------------------------------
 */

#define H5C__REMOVE_FROM_COLL_LIST(cache_ptr, entry_ptr, fail_val)      \
do {                                                                       \
    assert(cache_ptr);                                                \
    assert(entry_ptr);                                                \
                                                                        \
    /* Remove the entry from the list. */                               \
    H5C__COLL_DLL_REMOVE((entry_ptr), (cache_ptr)->coll_head_ptr,       \
                         (cache_ptr)->coll_tail_ptr,                    \
                         (cache_ptr)->coll_list_len,                    \
                         (cache_ptr)->coll_list_size, (fail_val))       \
} while (0) /* H5C__REMOVE_FROM_COLL_LIST */


/*-------------------------------------------------------------------------
 *
 * Macro:       H5C__MOVE_TO_TOP_IN_COLL_LIST
 *
 * Purpose:     Update entry position in collective entries list
 *
 *-------------------------------------------------------------------------
 */

#define H5C__MOVE_TO_TOP_IN_COLL_LIST(cache_ptr, entry_ptr, fail_val)   \
do {                                                                       \
    assert(cache_ptr);                                                \
    assert(entry_ptr);                                                \
                                                                        \
    /* Remove entry and insert at the head of the list. */              \
    H5C__COLL_DLL_REMOVE((entry_ptr), (cache_ptr)->coll_head_ptr,       \
                         (cache_ptr)->coll_tail_ptr,                    \
                         (cache_ptr)->coll_list_len,                    \
                         (cache_ptr)->coll_list_size, (fail_val))       \
                                                                        \
    H5C__COLL_DLL_PREPEND((entry_ptr), (cache_ptr)->coll_head_ptr,      \
                          (cache_ptr)->coll_tail_ptr,                   \
                          (cache_ptr)->coll_list_len,                   \
                          (cache_ptr)->coll_list_size, (fail_val))      \
                                                                        \
} while (0) /* H5C__MOVE_TO_TOP_IN_COLL_LIST */
#endif /* H5_HAVE_PARALLEL */


/****************************/
/* Package Private Typedefs */
/****************************/

/****************************************************************************
 *
 * structure H5C_tag_info_t
 *
 * Structure about each set of tagged entries for an object in the file.
 *
 * Each H5C_tag_info_t struct corresponds to a particular object in the file.
 *
 * Each H5C_cache_entry struct in the linked list of entries for this tag
 *      also contains a pointer back to the H5C_tag_info_t struct for the
 *      overall object.
 *
 *
 * The fields of this structure are discussed individually below:
 *
 * tag:         Address (i.e. "tag") of the object header for all the entries
 *              corresponding to parts of that object.
 *
 * head:        Head of doubly-linked list of all entries belonging to the tag.
 *
 * entry_cnt:   Number of entries on linked list of entries for this tag.
 *
 * corked:      Boolean flag indicating whether entries for this object can be
 *              evicted.
 *
 * hh:          uthash hash table handle (must be last)
 *
 ****************************************************************************/
typedef struct H5C_tag_info_t {
    haddr_t tag;                /* Tag (address) of the entries (must be first, for skiplist) */
    H5C_cache_entry_t *head;    /* Head of the list of entries for this tag */
    size_t entry_cnt;           /* Number of entries on list */
    bool corked;             /* Whether this object is corked */

    /* Hash table fields */
    UT_hash_handle hh;          /* Hash table handle (must be LAST) */
} H5C_tag_info_t;


/****************************************************************************
 *
 * structure H5C_t
 *
 * Structure for all information specific to an instance of the cache.
 *
 * While the cache was designed with multiple replacement policies in mind,
 * at present only a modified form of LRU is supported.
 *
 * The cache has a hash table in which all entries are stored.  Given the
 * advantages of flushing entries in increasing address order, a skip list
 * is used to track dirty entries.
 *
 * flush_in_progress: Boolean flag indicating whether a flush is in progress.
 *
 * log_info:    Information used by the cache logging functionality.
 *              Described in H5Clog.h.
 *
 * aux_ptr:    Pointer to void used to allow wrapper code to associate
 *        its data with an instance of H5C_t.  The H5C cache code
 *        sets this field to NULL, and otherwise leaves it alone.
 *
 * max_type_id:    Integer field containing the maximum type id number assigned
 *        to a type of entry in the cache.  All type ids from 0 to
 *        max_type_id inclusive must be defined.
 *
 * class_table_ptr: Pointer to an array of H5C_class_t of length
 *        max_type_id + 1.  Entry classes for the cache.
 *
 * max_cache_size:  Nominal maximum number of bytes that may be stored in the
 *        cache.  This value should be viewed as a soft limit, as the
 *        cache can exceed this value under the following circumstances:
 *
 *        a) All entries in the cache are protected, and the cache is
 *           asked to insert a new entry.  In this case the new entry
 *           will be created.  If this causes the cache to exceed
 *           max_cache_size, it will do so.  The cache will attempt
 *           to reduce its size as entries are unprotected.
 *
 *        b) When running in parallel mode, the cache may not be
 *           permitted to flush a dirty entry in response to a read.
 *           If there are no clean entries available to evict, the
 *           cache will exceed its maximum size.  Again the cache
 *           will attempt to reduce its size to the max_cache_size
 *           limit on the next cache write.
 *
 *        c) When an entry increases in size, the cache may exceed
 *           the max_cache_size limit until the next time the cache
 *           attempts to load or insert an entry.
 *
 *        d) When the evictions_enabled field is false (see below),
 *           the cache size will increase without limit until the
 *           field is set to true.
 *
 * min_clean_size: Nominal minimum number of clean bytes in the cache.
 *        The cache attempts to maintain this number of bytes of clean
 *        data so as to avoid case b) above.  Again, this is a soft limit.
 *
 * close_warning_received: Boolean flag indicating that a file closing
 *        warning has been received.
 *
 *
 * In addition to the callback functions required for each entry's class,
 * the cache requires the following callback functions for an instance of
 * the cache as a whole:
 *
 * check_write_permitted:  In certain applications, the cache may not
 *        be allowed to write to disk at certain time.  If specified,
 *        the check_write_permitted function is used to determine if
 *        a write is permissible at any given point in time.
 *
 *        If no such function is specified (i.e. this field is NULL),
 *        the cache uses the following write_permitted field to
 *        determine whether writes are permitted.
 *
 * write_permitted: If check_write_permitted is NULL, this boolean flag
 *        indicates whether writes are permitted.
 *
 * log_flush:    If provided, this function is called whenever a dirty
 *        entry is flushed to disk.
 *
 *
 * In cases where memory is plentiful, and performance is an issue, it may
 * be useful to disable all cache evictions, and thereby postpone metadata
 * writes.  The following field is used to implement this.
 *
 * evictions_enabled:  Boolean flag that is initialized to true.  When
 *         this flag is set to false, the metadata cache will not
 *         attempt to evict entries to make space for newly protected
 *         entries, and instead the will grow without limit.
 *
 *         Needless to say, this feature must be used with care.
 *
 *
 * The cache requires an index to facilitate searching for entries.  The
 * following fields support that index.
 *
 * We sometimes need to visit all entries in the cache, they are stored in
 * an index list.
 *
 * The index list is maintained by the same macros that maintain the
 * index, and must have the same length and size as the index proper.
 *
 * index_len:   Number of entries currently in the hash table used to index
 *        the cache.
 *
 * index_size:  Number of bytes of cache entries currently stored in the
 *        hash table used to index the cache.
 *
 *        This value should not be mistaken for footprint of the
 *        cache in memory.  The average cache entry is small, and
 *        the cache has a considerable overhead.  Multiplying the
 *        index_size by three should yield a conservative estimate
 *        of the cache's memory footprint.
 *
 * index_ring_len: Array of integer of length H5C_RING_NTYPES used to
 *        maintain a count of entries in the index by ring.  Note
 *        that the sum of all the cells in this array must equal
 *        the value stored in index_len above.
 *
 * index_ring_size: Array of size_t of length H5C_RING_NTYPES used to
 *        maintain the sum of the sizes of all entries in the index
 *        by ring.  Note that the sum of all cells in this array must
 *        equal the value stored in index_size above.
 *
 * clean_index_size: Number of bytes of clean entries currently stored in
 *        the hash table.  Note that the index_size field (above)
 *        is also the sum of the sizes of all entries in the cache.
 *        Thus we should have the invariant that clean_index_size +
 *        dirty_index_size == index_size.
 *
 *        WARNING:
 *           The value of the clean_index_size must not be mistaken for
 *           the current clean size of the cache.  Rather, the clean size
 *           of the cache is the current value of clean_index_size plus
 *           the amount of empty space (if any) in the cache.
 *
 * clean_index_ring_size: Array of size_t of length H5C_RING_NTYPES used to
 *        maintain the sum of the sizes of all clean entries in the
 *        index by ring.  Note that the sum of all cells in this array
 *        must equal the value stored in clean_index_size above.
 *
 * dirty_index_size: Number of bytes of dirty entries currently stored in
 *        the hash table.  Note that the index_size field (above)
 *        is also the sum of the sizes of all entries in the cache.
 *        Thus we should have the invariant that clean_index_size +
 *        dirty_index_size == index_size.
 *
 * dirty_index_ring_size: Array of size_t of length H5C_RING_NTYPES used to
 *        maintain the sum of the sizes of all dirty entries in the
 *        index by ring.  Note that the sum of all cells in this array
 *        must equal the value stored in dirty_index_size above.
 *
 * index: Array of pointers to H5C_cache_entry_t of size
 *        H5C__HASH_TABLE_LEN.  At present, this value is a power
 *        of two, not the usual prime number.
 *
 *        Hopefully the variable size of cache elements, the large
 *        hash table size, and the way in which HDF5 allocates space
 *        will combine to avoid problems with periodicity.  If so, we
 *        can use a trivial hash function (a bit-and and a 3 bit left
 *        shift) with some small savings.
 *
 *        If not, it will become evident in the statistics. Changing
 *        to the usual prime number length hash table will require
 *        changing the H5C__HASH_FCN macro and the deletion of the
 *        H5C__HASH_MASK #define.  No other changes should be required.
 *
 * il_len: Number of entries on the index list.
 *
 *        This must always be equal to index_len.  As such, this
 *        field is redundant.  However, the existing linked list
 *        management macros expect to maintain a length field, so
 *        this field exists primarily to avoid adding complexity to
 *        these macros.
 *
 * il_size: Number of bytes of cache entries currently stored in the
 *        index list.
 *
 *        This must always be equal to index_size.  As such, this
 *        field is redundant.  However, the existing linked list
 *        management macros expect to maintain a size field, so
 *        this field exists primarily to avoid adding complexity to
 *        these macros.
 *
 * il_head: Pointer to the head of the doubly linked list of entries in
 *        the index list.  Note that cache entries on this list are
 *        linked by their il_next and il_prev fields.
 *
 *        This field is NULL if the index is empty.
 *
 * il_tail: Pointer to the tail of the doubly linked list of entries in
 *        the index list.  Note that cache entries on this list are
 *        linked by their il_next and il_prev fields.
 *
 *        This field is NULL if the index is empty.
 *
 *
 * It is possible that an entry may be removed from the cache as the result
 * of the flush of a second entry.  In general, this causes little trouble,
 * but it is possible that the entry removed may be the next entry in the
 * scan of a list.  In this case, we must be able to detect the fact that the
 * entry has been removed, so that the scan doesn't attempt to proceed with
 * an entry that is no longer in the cache.
 *
 * The following fields are maintained to facilitate this.
 *
 * entries_removed_counter: Counter that is incremented each time an
 *        entry is removed from the cache by any means (eviction,
 *        expungement, or take ownership at this point in time).
 *        Functions that perform scans on lists may set this field
 *        to zero prior to calling H5C__flush_single_entry().
 *        Unexpected changes to the counter indicate that an entry
 *        was removed from the cache as a side effect of the flush.
 *
 * last_entry_removed_ptr: Pointer to the instance of H5C_cache_entry_t
 *        which contained the last entry to be removed from the cache,
 *        or NULL if there either is no such entry, or if a function
 *        performing a scan of a list has set this field to NULL prior
 *        to calling H5C__flush_single_entry().
 *
 *        WARNING!!! This field must NEVER be dereferenced.  It is
 *        maintained to allow functions that perform scans of lists
 *        to compare this pointer with their pointers to next, thus
 *        allowing them to avoid unnecessary restarts of scans if the
 *        pointers don't match, and if entries_removed_counter is one.
 *
 * entry_watched_for_removal:    Pointer to an instance of H5C_cache_entry_t
 *        which contains the 'next' entry for an iteration.  Removing
 *        this entry must trigger a rescan of the iteration, so each
 *        entry removed from the cache is compared against this pointer
 *        and the pointer is reset to NULL if the watched entry is
 *        removed.  (This functions similarly to a "dead man's switch")
 *
 *
 * When we flush the cache, we need to write entries out in increasing
 * address order.  An instance of a skip list is used to store dirty entries in
 * sorted order.
 *
 * The cost of maintaining the skip list is significant.  As it is only used
 * on flush and close, it is maintained only when needed.
 *
 * To do this, we add a flag to control maintenanace of the skip list.
 * This flag is initially set to false, which disables all operations
 * on the skip list.
 *
 * At the beginning of either flush or close, we scan the index list,
 * insert all dirtly entries in the skip list, and enable operations
 * on skip list by setting above control flag to true.
 *
 * At the end of a complete flush, we verify that the skip list is empty,
 * and set the control flag back to false, so as to avoid skip list
 * maintenance overhead until the next flush or close.
 *
 * In the case of a partial flush (i.e. flush marked entries), we remove
 * all remaining entries from the skip list, and then set the control flag
 * back to false -- again avoiding skip list maintenance overhead until
 * the next flush or close.
 *
 * slist_enabled: Boolean flag used to control operation of the skip
 *        list.  If this filed is false, operations on the slist are
 *        no-ops, and the slist must be empty.  If it is true,
 *        operations on the skip list proceed as usual, and all dirty
 *        entries in the metadata cache must be listed in the skip list.
 *
 * slist_changed: Boolean flag used to indicate whether the contents of
 *        the skip list has changed since the last time this flag was
 *        reset.  This is used in the cache flush code to detect
 *        conditions in which pre-serialize or serialize callbacks
 *        have modified the skip list -- which obliges us to restart
 *        the scan of the skip list from the beginning.
 *
 * slist_len: Number of entries currently in the skip list. Used to
 *        maintain a sorted list of dirty entries in the cache.
 *
 * slist_size: Number of bytes of cache entries currently stored in the
 *        skip list used to maintain a sorted list of dirty entries in
 *        the cache.
 *
 * slist_ring_len: Array of integer of length H5C_RING_NTYPES used to
 *        maintain a count of entries in the skip list by ring.  Note
 *        that the sum of all the cells in this array must equal
 *        the value stored in slist_len above.
 *
 * slist_ring_size: Array of size_t of length H5C_RING_NTYPES used to
 *        maintain the sum of the sizes of all entries in the skip list
 *        by ring.  Note that the sum of all cells in this array must
 *        equal the value stored in slist_size above.
 *
 * slist_ptr: Pointer to the instance of H5SL_t used maintain a sorted
 *        list of dirty entries in the cache.  This sorted list has
 *        two uses:
 *
 *        a) It allows us to flush dirty entries in increasing address
 *           order, which results in significant savings.
 *
 *        b) It facilitates checking for adjacent dirty entries when
 *           attempting to evict entries from the cache.
 *
 * num_last_entries: The number of entries in the cache that can only be
 *        flushed after all other entries in the cache have been flushed.
 *
 *        Note: At this time, the this field will only be applied to
 *              two types of entries: the superblock and the file driver info
 *              message.  The code utilizing these flags is protected with
 *              asserts to enforce this.
 *
 * The cache must deal with the case in which entries may be dirtied, moved,
 * or have their sizes changed during a flush.  To allow sanity checks in this
 * situation, the following two fields have been added.  They are only
 * compiled in when H5C_DO_SANITY_CHECKS is true.
 *
 * slist_len_increase: Number of entries that have been added to the
 *        skip list since the last time this field was set to zero.
 *        Note that this value can be negative.
 *
 * slist_size_increase: Total size of all entries that have been added
 *         to the skip list since the last time this field was set to
 *         zero.  Note that this value can be negative.
 *
 * Cache entries belonging to a particular object are "tagged" with that
 * object's base object header address.
 *
 * The following fields are maintained to facilitate this.
 *
 * tag_list: A collection to track entries that belong to an object.
 *         Each H5C_tag_info_t struct on the tag list corresponds to a
 *         particular object in the file.  Tagged entries can be flushed
 *         or evicted as a group, or corked to prevent entries from being
 *         evicted from the cache.
 *
 *         "Global" entries, like the superblock and the file's freelist,
 *         as well as shared entries like global heaps and shared object
 *         header messages, are not tagged.
 *
 * ignore_tags: Boolean flag to disable tag validation during entry insertion.
 *
 * num_objs_corked: Unsigned integer field containing the number of objects
 *         that are "corked".  The "corked" status of an object is found by
 *         searching the "tag_list".  This field is added for optimization
 *         so that the skip list search on "tag_list" can be skipped if this
 *         field is zero, i.e. no "corked" objects.
 *
 * When a cache entry is protected, it must be removed from the LRU
 * list(s), as it cannot be either flushed or evicted until it is unprotected.
 * The following fields are used to implement the protected list (pl).
 *
 * pl_len: Number of entries currently residing on the protected list.
 *
 * pl_size: Number of bytes of cache entries currently residing on the
 *         protected list.
 *
 * pl_head_ptr: Pointer to the head of the doubly linked list of protected
 *         entries.  Note that cache entries on this list are linked
 *         by their next and prev fields.
 *
 *         This field is NULL if the list is empty.
 *
 * pl_tail_ptr: Pointer to the tail of the doubly linked list of protected
 *         entries.  Note that cache entries on this list are linked
 *         by their next and prev fields.
 *
 *         This field is NULL if the list is empty.
 *
 *
 * For very frequently used entries, the protect/unprotect overhead can
 * become burdensome.  To avoid this overhead, the cache allows entries to
 * be "pinned".  A pinned entry is similar to a protected entry, in the
 * sense that it cannot be evicted, and that the entry can be modified at
 * any time.
 *
 * Pinning an entry has the following implications:
 *
 *      1) A pinned entry cannot be evicted.  Thus unprotected pinned
 *         entries reside in the pinned entry list, instead of the LRU
 *         list(s) or other lists maintained by the current replacement
 *         policy code.
 *
 *      2) A pinned entry can be accessed or modified at any time.
 *         This places an additional burden on the associated pre-serialize
 *         and serialize callbacks, which must ensure the entry is in
 *         a consistent state before creating an image of it.
 *
 *      3) A pinned entry can be marked as dirty (and possibly
 *         change size) while it is unprotected.
 *
 *      4) The flush-destroy code must allow pinned entries to
 *         be unpinned (and possibly unprotected) during the flush.
 *
 * Since pinned entries cannot be evicted, they must be kept on a pinned
 * entry list (pel), instead of being entrusted to the replacement policy
 * code.
 *
 * Maintaining the pinned entry list requires the following fields:
 *
 * pel_len: Number of entries currently residing on the pinned entry list.
 *
 * pel_size: Number of bytes of cache entries currently residing on
 *         the pinned entry list.
 *
 * pel_head_ptr: Pointer to the head of the doubly linked list of pinned
 *         but not protected entries.  Note that cache entries on
 *         this list are linked by their next and prev fields.
 *
 *         This field is NULL if the list is empty.
 *
 * pel_tail_ptr: Pointer to the tail of the doubly linked list of pinned
 *         but not protected entries.  Note that cache entries on
 *         this list are linked by their next and prev fields.
 *
 *         This field is NULL if the list is empty.
 *
 *
 * The cache must have a replacement policy, and the fields supporting this
 * policy must be accessible from this structure.
 *
 * Fields supporting the modified LRU policy:
 *
 * When operating in parallel mode, we must ensure that a read does not
 * cause a write.  If it does, the process will hang, as the write will
 * be collective and the other processes will not know to participate.
 *
 * To deal with this issue, the usual LRU policy has been modified by adding
 * clean and dirty LRU lists to the usual LRU list.  In general, these
 * lists only exist in parallel builds.
 *
 * The clean LRU list is simply the regular LRU list with all dirty cache
 * entries removed.
 *
 * Similarly, the dirty LRU list is the regular LRU list with all the clean
 * cache entries removed.
 *
 * When reading in parallel mode, we evict from the clean LRU list only.
 * This implies that we must try to ensure that the clean LRU list is
 * reasonably well stocked at all times.  We attempt to do this by trying
 * to flush enough entries on each write to keep the cLRU_list_size >=
 * min_clean_size.
 *
 * Even if we start with a completely clean cache, a sequence of protects
 * without unprotects can empty the clean LRU list.  In this case, the
 * cache must grow temporarily.  At the next sync point, we will attempt to
 * evict enough entries to reduce index_size to less than max_cache_size.
 * While this will usually be possible, all bets are off if enough entries
 * are protected.
 *
 * Discussions of the individual fields used by the modified LRU replacement
 * policy follow:
 *
 * LRU_list_len:  Number of cache entries currently on the LRU list.
 *
 *              The LRU_list_len + pl_len + pel_len must always
 *              equal index_len.
 *
 * LRU_list_size:  Number of bytes of cache entries currently residing on the
 *              LRU list.
 *
 *              The LRU_list_size + pl_size + pel_size must always
 *              equal index_size.
 *
 * LRU_head_ptr:  Pointer to the head of the doubly linked LRU list.  Cache
 *              entries on this list are linked by their next and prev fields.
 *
 *              This field is NULL if the list is empty.
 *
 * LRU_tail_ptr:  Pointer to the tail of the doubly linked LRU list.  Cache
 *              entries on this list are linked by their next and prev fields.
 *
 *              This field is NULL if the list is empty.
 *
 * cLRU_list_len: Number of cache entries currently on the clean LRU list.
 *
 *              The cLRU_list_len + dLRU_list_len must always
 *              equal LRU_list_len.
 *
 * cLRU_list_size:  Number of bytes of cache entries currently residing on
 *              the clean LRU list.
 *
 *              The cLRU_list_size + dLRU_list_size must always
 *              equal LRU_list_size.
 *
 * cLRU_head_ptr:  Pointer to the head of the doubly linked clean LRU list.
 *              Cache entries on this list are linked by their aux_next and
 *              aux_prev fields.
 *
 *              This field is NULL if the list is empty.
 *
 * cLRU_tail_ptr:  Pointer to the tail of the doubly linked clean LRU list.
 *              Cache entries on this list are linked by their aux_next and
 *              aux_prev fields.
 *
 *              This field is NULL if the list is empty.
 *
 * dLRU_list_len: Number of cache entries currently on the dirty LRU list.
 *
 *              The cLRU_list_len + dLRU_list_len must always
 *              equal LRU_list_len.
 *
 * dLRU_list_size:  Number of cache entries currently on the dirty LRU list.
 *
 *              The cLRU_list_len + dLRU_list_len must always
 *              equal LRU_list_len.
 *
 * dLRU_head_ptr:  Pointer to the head of the doubly linked dirty LRU list.
 *              Cache entries on this list are linked by their aux_next and
 *              aux_prev fields.
 *
 *              This field is NULL if the list is empty.
 *
 * dLRU_tail_ptr:  Pointer to the tail of the doubly linked dirty LRU list.
 *              Cache entries on this list are linked by their aux_next and
 *              aux_prev fields.
 *
 *              This field is NULL if the list is empty.
 *
 *
 * Automatic cache size adjustment:
 *
 * While the default cache size is adequate for many cases, there are
 * cases where the default is too small.  Ideally, the user should
 * adjust the cache size as required.  However, this is not possible in all
 * cases, so the cache has automatic cache size adjustment code.
 *
 * The configuration for the automatic cache size adjustment is stored in
 * the structure described below:
 *
 * size_increase_possible:  Depending on the configuration data given
 *        in the resize_ctl field, it may or may not be possible to
 *        increase the size of the cache.  Rather than test for all the
 *        ways this can happen, we simply set this flag when we receive
 *        a new configuration.
 *
 * flash_size_increase_possible: Depending on the configuration data given
 *        in the resize_ctl field, it may or may not be possible for a
 *        flash size increase to occur.  We set this flag whenever we
 *        receive a new configuration so as to avoid repeated calculations.
 *
 * flash_size_increase_threshold: If a flash cache size increase is possible,
 *        this field is used to store the minimum size of a new entry or size
 *        increase needed to trigger a flash cache size increase.  Note that
 *        this field must be updated whenever the size of the cache is changed.
 *
 * size_decrease_possible:  Depending on the configuration data given in the
 *        resize_ctl field, it may or may not be possible to decrease the
 *        size of the cache.  Rather than test for all the ways this can
 *        happen, we simply set this flag when we receive a new configuration.
 *
 * resize_enabled:  This is another convenience flag which is set whenever
 *        a new set of values for resize_ctl are provided.  Very simply:
 *
 *        resize_enabled = size_increase_possible || size_decrease_possible;
 *
 * cache_full: Boolean flag used to keep track of whether the cache is
 *        full, so we can refrain from increasing the size of a
 *        cache which hasn't used up the space allotted to it.
 *
 *        The field is initialized to false, and then set to true
 *        whenever we attempt to make space in the cache.
 *
 * size_decreased:  Boolean flag set to true whenever the maximum cache
 *        size is decreased.  The flag triggers a call to
 *        H5C__make_space_in_cache() on the next call to H5C_protect().
 *
 * resize_in_progress: As the metadata cache has become re-entrant, it is
 *        possible that a protect may trigger a call to
 *        H5C__auto_adjust_cache_size(), which may trigger a flush,
 *        which may trigger a protect, which will result in another
 *        call to H5C__auto_adjust_cache_size().
 *
 *        The resize_in_progress boolean flag is used to detect this,
 *        and to prevent the infinite recursion that would otherwise
 *        occur.
 *
 * msic_in_progress: As the metadata cache has become re-entrant, and as
 *        the free space manager code has become more tightly integrated
 *        with the metadata cache, it is possible that a call to
 *        H5C_insert_entry() may trigger a call to H5C_make_space_in_cache(),
 *        which, via H5C__flush_single_entry() and client callbacks, may
 *        trigger an infinite regression of calls to H5C_make_space_in_cache().
 *
 *        The msic_in_progress boolean flag is used to detect this,
 *        and prevent the infinite regression that would otherwise occur.
 *
 * resize_ctl: Instance of H5C_auto_size_ctl_t containing configuration
 *         data for automatic cache resizing.
 *
 * epoch_markers_active:  Integer field containing the number of epoch
 *        markers currently in use in the LRU list.  This value
 *        must be in the range [0, H5C__MAX_EPOCH_MARKERS - 1].
 *
 * epoch_marker_active:  Array of boolean of length H5C__MAX_EPOCH_MARKERS.
 *        This array is used to track which epoch markers are currently
 *        in use.
 *
 * epoch_marker_ringbuf:  Array of int of length H5C__MAX_EPOCH_MARKERS + 1.
 *
 *        To manage the epoch marker cache entries, it is necessary
 *        to track their order in the LRU list.  This is done with
 *        epoch_marker_ringbuf.  When markers are inserted at the
 *        head of the LRU list, the index of the marker in the
 *        epoch_markers array is inserted at the tail of the ring
 *        buffer.  When it becomes the epoch_marker_active'th marker
 *        in the LRU list, it will have worked its way to the head
 *        of the ring buffer as well.  This allows us to remove it
 *        without scanning the LRU list if such is required.
 *
 * epoch_marker_ringbuf_first: Integer field containing the index of the
 *        first entry in the ring buffer.
 *
 * epoch_marker_ringbuf_last: Integer field containing the index of the
 *        last entry in the ring buffer.
 *
 * epoch_marker_ringbuf_size: Integer field containing the number of entries
 *        in the ring buffer.
 *
 * epoch_markers:  Array of instances of H5C_cache_entry_t of length
 *        H5C__MAX_EPOCH_MARKERS.  The entries are used as markers in the
 *        LRU list to identify cache entries that haven't been accessed for
 *        some (small) specified number of epochs.  These entries (if any)
 *        can then be evicted and the cache size reduced -- ideally without
 *        evicting any of the current working set.  Needless to say, the epoch
 *        length and the number of epochs before an unused entry must be
 *        chosen so that all, or almost all, the working set will be accessed
 *        before the limit.
 *
 *        Epoch markers only appear in the LRU list, never in the index or
 *        skip list.  While they are of type H5C__EPOCH_MARKER_TYPE, and have
 *        associated class functions, these functions should never be called.
 *
 *        The addr fields of these instances of H5C_cache_entry_t are set to
 *        the index of the instance in the epoch_markers array, the size is
 *        set to 0, and the type field points to the constant structure
 *        epoch_marker_class defined in H5Cepoch.c.  The next and prev fields
 *        are used as usual to link the entry into the LRU list.
 *
 *        All other fields are unused.
 *
 *
 * Cache hit rate collection fields:
 *
 * We supply the current cache hit rate on request, so we must keep a
 * simple cache hit rate computation regardless of whether statistics
 * collection is enabled.  The following fields support this capability.
 *
 * cache_hits: Number of cache hits since the last time the cache hit rate
 *        statistics were reset.  Note that when automatic cache re-sizing
 *        is enabled, this field will be reset every automatic resize epoch.
 *
 * cache_accesses: Number of times the cache has been accessed while
 *        since the last since the last time the cache hit rate statistics
 *        were reset.  Note that when automatic cache re-sizing is enabled,
 *        this field will be reset every automatic resize epoch.
 *
 *
 * Metadata cache image management related fields.
 *
 * image_ctl: Instance of H5C_cache_image_ctl_t containing configuration
 *        data for generation of a cache image on file close.
 *
 * serialization_in_progress: Boolean field that is set to true iff
 *        the cache is in the process of being serialized.  This field is
 *        needed to support the H5C_serialization_in_progress() call, which
 *        is in turn required for sanity checks in some cache clients.
 *
 * load_image: Boolean flag indicating that the metadata cache image
 *        superblock extension message exists and should be read, and the
 *        image block read and decoded on the next call to H5C_protect().
 *
 * image_loaded: Boolean flag indicating that the metadata cache has
 *        loaded the metadata cache image as directed by the
 *        cache image superblock extension message.
 *
 * delete_image: Boolean flag indicating whether the metadata cache image
 *        superblock message should be deleted and the cache image
 *        file space freed after they have been read and decoded.
 *
 *        This flag should be set to true iff the file is opened
 *        R/W and there is a cache image to be read.
 *
 * image_addr: The base address of the on-disk metadata cache image, or
 *        HADDR_UNDEF if that value is undefined.  Note that this field
 *        is used both in the construction and write, and the read and
 *        decode of metadata cache image blocks.
 *
 * image_len: The size of the on disk metadata cache image, or zero if that
 *        value is undefined.  Note that this field is used both in the
 *        construction and write, and the read and decode of metadata cache
 *        image blocks.
 *
 * image_data_len:  The number of bytes of data in the on disk metadata
 *        cache image, or zero if that value is undefined.
 *
 *        In most cases, this value is the same as the image_len
 *        above.  It exists to allow for metadata cache image blocks
 *        that are larger than the actual image.  Thus in all
 *        cases image_data_len <= image_len.
 *
 * To create the metadata cache image, we must first serialize all the
 * entries in the metadata cache.  This is done by a scan of the index.
 * As entries must be serialized in increasing flush dependency height
 * order, we scan the index repeatedly, once for each flush dependency
 * height in increasing order.
 *
 * This operation is complicated by the fact that entries other than the
 * target may be inserted, loaded, relocated, or removed from the cache
 * (either by eviction or the take ownership flag) as the result of a
 * pre_serialize or serialize callback.  While entry removals are not
 * a problem for the scan of the index, insertions, loads, and relocations
 * are.  Hence the entries loaded, inserted, and relocated counters
 * listed below have been implemented to allow these conditions to be
 * detected and dealt with by restarting the scan.
 *
 * The serialization operation is further complicated by the fact that
 * the flush dependency height of a given entry may increase (as the
 * result of an entry load or insert) or decrease (as the result of an
 * entry removal -- via either eviction or the take ownership flag).  The
 * entry_fd_height_change_counter field is maintained to allow detection
 * of this condition, and a restart of the scan when it occurs.
 *
 * Note that all these new fields would work just as well as booleans.
 *
 * entries_loaded_counter: Number of entries loaded into the cache
 *        since the last time this field was reset.
 *
 * entries_inserted_counter: Number of entries inserted into the cache
 *        since the last time this field was reset.
 *
 * entries relocated_counter: Number of entries whose base address has
 *        been changed since the last time this field was reset.
 *
 * entry_fd_height_change_counter: Number of entries whose flush dependency
 *        height has changed since the last time this field was reset.
 *
 * The following fields are used assemble the cache image prior to
 * writing it to disk.
 *
 * num_entries_in_image: Unsigned integer field containing the number of
 *        entries to be copied into the metadata cache image.  Note that
 *        this value will be less than the number of entries in the cache,
 *        and the superblock and its related entries are not written to the
 *        metadata cache image.
 *
 * image_entries: Pointer to a dynamically allocated array of instance of
 *        H5C_image_entry_t of length num_entries_in_image, or NULL
 *        if that array does not exist.  This array is used to
 *        assemble entry data to be included in the image, and to
 *        sort them by flush dependency height and LRU rank.
 *
 * image_buffer: Pointer to the dynamically allocated buffer of length
 *        image_len in which the metadata cache image is assembled,
 *        or NULL if that buffer does not exist.
 *
 *
 * Free Space Manager Related fields:
 *
 * The free space managers for the file must be informed when we are about to
 * close or flush the file so that they order themselves accordingly.  This
 * used to be done much later in the close process, but with cache image and
 * page buffering, this is no longer viable, as we must finalize the on
 * disk image of all metadata much sooner.
 *
 * This is handled by the H5MF_settle_raw_data_fsm() and
 * H5MF_settle_meta_data_fsm() routines.  As these calls are expensive,
 * the following fields are used to track whether the target free space
 * managers are clean.
 *
 * They are also used in sanity checking, as once a free space manager is
 * settled, it should not become unsettled (i.e. be asked to allocate or
 * free file space) either ever (in the case of a file close) or until the
 * flush is complete.
 *
 * rdfsm_settled:  Boolean flag indicating whether the raw data free space
 *        manager is settled -- i.e. whether the correct space has
 *        been allocated for it in the file.
 *
 *        Note that the name of this field is deceptive.  In the
 *        multi file case, the flag applies to all free space
 *        managers that are not involved in allocating space for
 *        free space manager metadata.
 *
 * mdfsm_settled:  Boolean flag indicating whether the meta data free space
 *        manager is settled -- i.e. whether the correct space has
 *        been allocated for it in the file.
 *
 *        Note that the name of this field is deceptive.  In the
 *        multi-file case, the flag applies only to free space
 *        managers that are involved in allocating space for free
 *        space managers.
 *
 *
 * Statistics collection fields:
 *
 * When enabled, these fields are used to collect statistics as described
 * below.  The first set are collected only when H5C_COLLECT_CACHE_STATS
 * is true.
 *
 * hits: Array to record the number of times an entry with type id
 *        equal to the array index has been in cache when requested in
 *        the current epoch.
 *
 * misses: Array to record the number of times an entry with type id
 *        equal to the array index has not been in cache when
 *        requested in the current epoch.
 *
 * write_protects: Array to record the number of times an entry with
 *         type id equal to the array index has been write protected
 *         in the current epoch.
 *
 *         Observe that (hits + misses) = (write_protects + read_protects).
 *
 * read_protects: Array to record the number of times an entry with
 *         type id equal to the array index has been read protected in
 *         the current epoch.
 *
 *         Observe that (hits + misses) = (write_protects + read_protects).
 *
 * max_read_protects: Array to maximum number of simultaneous read
 *         protects on any entry with type id equal to the array index
 *         in the current epoch.
 *
 * insertions: Array to record the number of times an entry with type
 *        id equal to the array index has been inserted into the
 *        cache in the current epoch.
 *
 * pinned_insertions: Array to record the number of times an entry
 *         with type id equal to the array index has been inserted
 *         pinned into the cache in the current epoch.
 *
 * clears: Array to record the number of times a dirty entry with type
 *        id equal to the array index has been cleared in the current epoch.
 *
 * flushes: Array to record the number of times an entry with type id
 *        equal to the array index has been written to disk in the
 *        current epoch.
 *
 * evictions: Array to record the number of times an entry with type id
 *        equal to the array index has been evicted from the cache in
 *        the current epoch.
 *
 * take_ownerships: Array to record the number of times an entry with
 *        type id equal to the array index has been removed from the
 *        cache via the H5C__TAKE_OWNERSHIP_FLAG in the current epoch.
 *
 * moves: Array to record the number of times an entry with type
 *        id equal to the array index has been moved in the current epoch.
 *
 * entry_flush_moves: Array to record the number of times an entry
 *         with type id equal to the array index has been moved
 *         during its pre-serialize callback in the current epoch.
 *
 * cache_flush_moves: Array to record the number of times an entry
 *         with type id equal to the array index has been moved
 *         during a cache flush in the current epoch.
 *
 * pins: Array to record the number of times an entry with type
 *        id equal to the array index has been pinned in the current epoch.
 *
 * unpins: Array to record the number of times an entry with type
 *        id equal to the array index has been unpinned in the current epoch.
 *
 * dirty_pins: Array to record the number of times an entry with type
 *        id equal to the array index has been marked dirty while pinned
 *        in the current epoch.
 *
 * pinned_flushes: Array to record the number of times an entry
 *         with type id equal to the array index has been flushed while
 *         pinned in the current epoch.
 *
 * pinned_clears: Array to record the number of times an entry
 *         with type id equal to the array index has been cleared while
 *         pinned in the current epoch.
 *
 * size_increases: Array to record the number of times an entry
 *        with type id equal to the array index has increased in
 *        size in the current epoch.
 *
 * size_decreases: Array to record the number of times an entry
 *        with type id equal to the array index has decreased in
 *        size in the current epoch.
 *
 * entry_flush_size_changes: Array to record the number of times an entry
 *        with type id equal to the array index has changed size while in
 *        its pre-serialize callback.
 *
 * cache_flush_size_changes: Array to record the number of times an entry
 *        with type id equal to the array index has changed size during a
 *        cache flush
 *
 * total_ht_insertions: Number of times entries have been inserted into the
 *        hash table in the current epoch.
 *
 * total_ht_deletions: Number of times entries have been deleted from the
 *        hash table in the current epoch.
 *
 * successful_ht_searches: The total number of successful searches of the
 *        hash table in the current epoch.
 *
 * total_successful_ht_search_depth: The total number of entries other than
 *        the targets examined in successful searches of the hash table in
 *        the current epoch.
 *
 * failed_ht_searches: The total number of unsuccessful searches of the hash
 *        table in the current epoch.
 *
 * total_failed_ht_search_depth: The total number of entries examined in
 *        unsuccessful searches of the hash table in the current epoch.
 *
 * max_index_len: Largest value attained by the index_len field in the
 *        current epoch.
 *
 * max_index_size: Largest value attained by the index_size field in the
 *        current epoch.
 *
 * max_clean_index_size: Largest value attained by the clean_index_size field
 *         in the current epoch.
 *
 * max_dirty_index_size: Largest value attained by the dirty_index_size field
 *         in the current epoch.
 *
 * max_slist_len: Largest value attained by the slist_len field in the
 *        current epoch.
 *
 * max_slist_size: Largest value attained by the slist_size field in the
 *        current epoch.
 *
 * max_pl_len:  Largest value attained by the pl_len field in the
 *        current epoch.
 *
 * max_pl_size: Largest value attained by the pl_size field in the
 *        current epoch.
 *
 * max_pel_len: Largest value attained by the pel_len field in the
 *        current epoch.
 *
 * max_pel_size: Largest value attained by the pel_size field in the
 *        current epoch.
 *
 * calls_to_msic: Total number of calls to H5C__make_space_in_cache
 *
 * total_entries_skipped_in_msic: Number of clean entries skipped while
 *        enforcing the min_clean_fraction in H5C__make_space_in_cache().
 *
 * total_dirty_pf_entries_skipped_in_msic: Number of dirty prefetched entries
 *        skipped in H5C__make_space_in_cache().  Note that this can
 *        only occur when a file is opened R/O with a cache image
 *        containing dirty entries.
 *
 * total_entries_scanned_in_msic: Number of clean entries skipped while
 *        enforcing the min_clean_fraction in H5C__make_space_in_cache().
 *
 * max_entries_skipped_in_msic: Maximum number of clean entries skipped
 *        in any one call to H5C__make_space_in_cache().
 *
 * max_dirty_pf_entries_skipped_in_msic: Maximum number of dirty prefetched
 *        entries skipped in any one call to H5C__make_space_in_cache().
 *        Note that this can only occur when the file is opened
 *        R/O with a cache image containing dirty entries.
 *
 * max_entries_scanned_in_msic: Maximum number of entries scanned over
 *        in any one call to H5C__make_space_in_cache().
 *
 * entries_scanned_to_make_space: Number of entries scanned only when looking
 *        for entries to evict in order to make space in cache.
 *
 *
 * The following fields track statistics on cache images.
 *
 * images_created:  The number of cache images created since the last
 *        time statistics were reset.
 *
 *        At present, this field must always be either 0 or 1.
 *        Further, since cache images are only created at file
 *        close, this field should only be set at that time.
 *
 * images_read: The number of cache images read from file.  Note that
 *        reading an image is different from loading it -- reading the
 *        image means just that, while loading the image refers to decoding
 *        it and loading it into the metadata cache.
 *
 *        In the serial case, image_read should always equal images_loaded.
 *        However, in the parallel case, the image should only be read by
 *        process 0.  All other processes should receive the cache image via
 *        a broadcast from process 0.
 *
 * images_loaded:  The number of cache images loaded since the last time
 *        statistics were reset.
 *
 *        At present, this field must always be either 0 or 1.
 *        Further, since cache images are only loaded at the
 *        time of the first protect or on file close, this value
 *        should only change on those events.
 *
 * last_image_size:  Size of the most recently loaded metadata cache image
 *        loaded into the cache, or zero if no image has been loaded.
 *
 *        At present, at most one cache image can be loaded into
 *        the metadata cache for any given file, and this image
 *        will be loaded either on the first protect, or on file
 *        close if no entry is protected before then.
 *
 *
 * Fields for tracking prefetched entries.  Note that flushes and evictions
 * of prefetched entries are tracked in the flushes and evictions arrays
 * discussed above.
 *
 * prefetches: Number of prefetched entries that are loaded to the cache.
 *
 * dirty_prefetches: Number of dirty prefetched entries that are loaded
 *        into the cache.
 *
 * prefetch_hits: Number of prefetched entries that are actually used.
 *
 *
 * Entries may move, load, dirty, and delete other entries in their
 * pre_serialize and serialize callbacks, there is code to restart scans of
 * lists so as to avoid improper behavior if the next entry in the list is
 * the target of one on these operations.
 *
 * The following fields are use to count such occurrences.  They are used
 * both in tests (to verify that the scan has been restarted), and to
 * obtain estimates of how frequently these restarts occur.
 *
 * slist_scan_restarts: Number of times a scan of the skip list (that contains
 *        calls to H5C__flush_single_entry()) has been restarted to
 *        avoid potential issues with change of status of the next
 *        entry in the scan.
 *
 * LRU_scan_restarts: Number of times a scan of the LRU list (that contains
 *        calls to H5C__flush_single_entry()) has been restarted to
 *        avoid potential issues with change of status of the next
 *        entry in the scan.
 *
 * index_scan_restarts: Number of times a scan of the index has been
 *        restarted to avoid potential issues with load, insertion
 *        or change in flush dependency height of an entry other
 *        than the target entry as the result of call(s) to the
 *        pre_serialize or serialize callbacks.
 *
 *        Note that at present, this condition can only be triggered
 *        by a call to H5C_serialize_single_entry().
 *
 * The remaining stats are collected only when both H5C_COLLECT_CACHE_STATS
 * and H5C_COLLECT_CACHE_ENTRY_STATS are true.
 *
 * max_accesses: Array to record the maximum number of times any single
 *        entry with type id equal to the array index has been
 *        accessed in the current epoch.
 *
 * min_accesses: Array to record the minimum number of times any single
 *        entry with type id equal to the array index has been
 *        accessed in the current epoch.
 *
 * max_clears: Array to record the maximum number of times any single
 *        entry with type id equal to the array index has been cleared
 *        in the current epoch.
 *
 * max_flushes: Array to record the maximum number of times any single
 *        entry with type id equal to the array index has been
 *        flushed in the current epoch.
 *
 * max_size: Array to record the maximum size of any single entry
 *        with type id equal to the array index that has resided in
 *        the cache in the current epoch.
 *
 * max_pins: Array to record the maximum number of times that any single
 *        entry with type id equal to the array index that has been
 *        marked as pinned in the cache in the current epoch.
 *
 *
 * Fields supporting testing:
 *
 * prefix: Array of char used to prefix debugging output.  The field is
 *        intended to allow marking of output of with the processes mpi rank.
 *
 * get_entry_ptr_from_addr_counter: Counter used to track the number of
 *        times the H5C_get_entry_ptr_from_addr() function has been
 *        called successfully.  This field is only defined when
 *        NDEBUG is not #defined.
 *
 ****************************************************************************/
struct H5C_t {
    bool             flush_in_progress;
    H5C_log_info_t *    log_info;
    void *              aux_ptr;
    int32_t             max_type_id;
    const H5C_class_t * const *class_table_ptr;
    size_t              max_cache_size;
    size_t              min_clean_size;
    H5C_write_permitted_func_t check_write_permitted;
    bool             write_permitted;
    H5C_log_flush_func_t log_flush;
    bool             evictions_enabled;
    bool             close_warning_received;

    /* Fields for maintaining the [hash table] index of entries */
    uint32_t            index_len;
    size_t              index_size;
    uint32_t            index_ring_len[H5C_RING_NTYPES];
    size_t              index_ring_size[H5C_RING_NTYPES];
    size_t              clean_index_size;
    size_t              clean_index_ring_size[H5C_RING_NTYPES];
    size_t              dirty_index_size;
    size_t              dirty_index_ring_size[H5C_RING_NTYPES];
    H5C_cache_entry_t * index[H5C__HASH_TABLE_LEN];
    uint32_t            il_len;
    size_t              il_size;
    H5C_cache_entry_t * il_head;
    H5C_cache_entry_t * il_tail;

    /* Fields to detect entries removed during scans */
    int64_t             entries_removed_counter;
    H5C_cache_entry_t * last_entry_removed_ptr;
    H5C_cache_entry_t * entry_watched_for_removal;

    /* Fields for maintaining list of in-order entries, for flushing */
    bool             slist_enabled;
    bool             slist_changed;
    uint32_t            slist_len;
    size_t              slist_size;
    uint32_t            slist_ring_len[H5C_RING_NTYPES];
    size_t              slist_ring_size[H5C_RING_NTYPES];
    H5SL_t *            slist_ptr;
    uint32_t            num_last_entries;
#ifdef H5C_DO_SANITY_CHECKS
    int32_t             slist_len_increase;
    int64_t             slist_size_increase;
#endif /* H5C_DO_SANITY_CHECKS */

    /* Fields for maintaining list of tagged entries */
    H5C_tag_info_t *    tag_list;
    bool             ignore_tags;
    uint32_t            num_objs_corked;

    /* Fields for tracking protected entries */
    uint32_t            pl_len;
    size_t              pl_size;
    H5C_cache_entry_t * pl_head_ptr;
    H5C_cache_entry_t * pl_tail_ptr;

    /* Fields for tracking pinned entries */
    uint32_t            pel_len;
    size_t              pel_size;
    H5C_cache_entry_t * pel_head_ptr;
    H5C_cache_entry_t * pel_tail_ptr;

    /* Fields for complete LRU list of entries */
    uint32_t            LRU_list_len;
    size_t              LRU_list_size;
    H5C_cache_entry_t * LRU_head_ptr;
    H5C_cache_entry_t * LRU_tail_ptr;

#if H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS
    /* Fields for clean LRU list of entries */
    uint32_t            cLRU_list_len;
    size_t              cLRU_list_size;
    H5C_cache_entry_t * cLRU_head_ptr;
    H5C_cache_entry_t * cLRU_tail_ptr;

    /* Fields for dirty LRU list of entries */
    uint32_t            dLRU_list_len;
    size_t              dLRU_list_size;
    H5C_cache_entry_t * dLRU_head_ptr;
    H5C_cache_entry_t * dLRU_tail_ptr;
#endif /* H5C_MAINTAIN_CLEAN_AND_DIRTY_LRU_LISTS */

#ifdef H5_HAVE_PARALLEL
    /* Fields for collective metadata reads */
    uint32_t            coll_list_len;
    size_t              coll_list_size;
    H5C_cache_entry_t * coll_head_ptr;
    H5C_cache_entry_t * coll_tail_ptr;

    /* Fields for collective metadata writes */
    H5SL_t *            coll_write_list;
#endif /* H5_HAVE_PARALLEL */

    /* Fields for automatic cache size adjustment */
    bool             size_increase_possible;
    bool             flash_size_increase_possible;
    size_t              flash_size_increase_threshold;
    bool             size_decrease_possible;
    bool             resize_enabled;
    bool             cache_full;
    bool             size_decreased;
    bool             resize_in_progress;
    bool             msic_in_progress;
    H5C_auto_size_ctl_t resize_ctl;

    /* Fields for epoch markers used in automatic cache size adjustment */
    int32_t             epoch_markers_active;
    bool             epoch_marker_active[H5C__MAX_EPOCH_MARKERS];
    int32_t             epoch_marker_ringbuf[H5C__MAX_EPOCH_MARKERS+1];
    int32_t             epoch_marker_ringbuf_first;
    int32_t             epoch_marker_ringbuf_last;
    int32_t             epoch_marker_ringbuf_size;
    H5C_cache_entry_t   epoch_markers[H5C__MAX_EPOCH_MARKERS];

    /* Fields for cache hit rate collection */
    int64_t             cache_hits;
    int64_t             cache_accesses;

    /* fields supporting generation of a cache image on file close */
    H5C_cache_image_ctl_t image_ctl;
    bool             serialization_in_progress;
    bool             load_image;
    bool             image_loaded;
    bool             delete_image;
    haddr_t             image_addr;
    hsize_t             image_len;
    hsize_t             image_data_len;
    int64_t             entries_loaded_counter;
    int64_t             entries_inserted_counter;
    int64_t             entries_relocated_counter;
    int64_t             entry_fd_height_change_counter;
    uint32_t            num_entries_in_image;
    H5C_image_entry_t * image_entries;
    void *              image_buffer;

    /* Free Space Manager Related fields */
    bool             rdfsm_settled;
    bool             mdfsm_settled;

#if H5C_COLLECT_CACHE_STATS
    /* stats fields */
    int64_t             hits[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             misses[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             write_protects[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             read_protects[H5C__MAX_NUM_TYPE_IDS + 1];
    int32_t             max_read_protects[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             insertions[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             pinned_insertions[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             clears[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             flushes[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             evictions[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             take_ownerships[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             moves[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             entry_flush_moves[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             cache_flush_moves[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             pins[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             unpins[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             dirty_pins[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             pinned_flushes[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             pinned_clears[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             size_increases[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             size_decreases[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             entry_flush_size_changes[H5C__MAX_NUM_TYPE_IDS + 1];
    int64_t             cache_flush_size_changes[H5C__MAX_NUM_TYPE_IDS + 1];

    /* Fields for hash table operations */
    int64_t             total_ht_insertions;
    int64_t             total_ht_deletions;
    int64_t             successful_ht_searches;
    int64_t             total_successful_ht_search_depth;
    int64_t             failed_ht_searches;
    int64_t             total_failed_ht_search_depth;
    uint32_t            max_index_len;
    size_t              max_index_size;
    size_t              max_clean_index_size;
    size_t              max_dirty_index_size;

    /* Fields for in-order skip list */
    uint32_t            max_slist_len;
    size_t              max_slist_size;

    /* Fields for protected entry list */
    uint32_t            max_pl_len;
    size_t              max_pl_size;

    /* Fields for pinned entry list */
    uint32_t            max_pel_len;
    size_t              max_pel_size;

    /* Fields for tracking 'make space in cache' (msic) operations */
    int64_t             calls_to_msic;
    int64_t             total_entries_skipped_in_msic;
    int64_t             total_dirty_pf_entries_skipped_in_msic;
    int64_t             total_entries_scanned_in_msic;
    int32_t             max_entries_skipped_in_msic;
    int32_t             max_dirty_pf_entries_skipped_in_msic;
    int32_t             max_entries_scanned_in_msic;
    int64_t             entries_scanned_to_make_space;

    /* Fields for tracking skip list scan restarts */
    int64_t             slist_scan_restarts;
    int64_t             LRU_scan_restarts;
    int64_t             index_scan_restarts;

    /* Fields for tracking cache image operations */
    int32_t             images_created;
    int32_t             images_read;
    int32_t             images_loaded;
    hsize_t             last_image_size;

    /* Fields for tracking prefetched entries */
    int64_t             prefetches;
    int64_t             dirty_prefetches;
    int64_t             prefetch_hits;

#if H5C_COLLECT_CACHE_ENTRY_STATS
    int32_t             max_accesses[H5C__MAX_NUM_TYPE_IDS + 1];
    int32_t             min_accesses[H5C__MAX_NUM_TYPE_IDS + 1];
    int32_t             max_clears[H5C__MAX_NUM_TYPE_IDS + 1];
    int32_t             max_flushes[H5C__MAX_NUM_TYPE_IDS + 1];
    size_t              max_size[H5C__MAX_NUM_TYPE_IDS + 1];
    int32_t             max_pins[H5C__MAX_NUM_TYPE_IDS + 1];
#endif /* H5C_COLLECT_CACHE_ENTRY_STATS */
#endif /* H5C_COLLECT_CACHE_STATS */

    char                prefix[H5C__PREFIX_LEN];

#ifndef NDEBUG
    int64_t             get_entry_ptr_from_addr_counter;
#endif

}; /* H5C_t */

/* Define typedef for tagged cache entry iteration callbacks */
typedef int (*H5C_tag_iter_cb_t)(H5C_cache_entry_t *entry, void *ctx);


/*****************************/
/* Package Private Variables */
/*****************************/


/******************************/
/* Package Private Prototypes */
/******************************/
H5_DLL herr_t H5C__prep_image_for_file_close(H5F_t *f, bool *image_generated);

/* General routines */
H5_DLL herr_t H5C__auto_adjust_cache_size(H5F_t *f, bool write_permitted);
H5_DLL herr_t H5C__autoadjust__ageout__remove_all_markers(H5C_t *cache_ptr);
H5_DLL herr_t H5C__autoadjust__ageout__remove_excess_markers(H5C_t *cache_ptr);
H5_DLL herr_t H5C__flash_increase_cache_size(H5C_t *cache_ptr, size_t old_entry_size, size_t new_entry_size);
H5_DLL herr_t H5C__flush_invalidate_cache(H5F_t *f, unsigned flags);
H5_DLL herr_t H5C__flush_ring(H5F_t *f, H5C_ring_t ring, unsigned flags);
H5_DLL herr_t H5C__flush_single_entry(H5F_t *f, H5C_cache_entry_t *entry_ptr,
    unsigned flags);
H5_DLL herr_t H5C__generate_cache_image(H5F_t *f, H5C_t *cache_ptr);
H5_DLL herr_t H5C__load_cache_image(H5F_t *f);
H5_DLL herr_t H5C__make_space_in_cache(H5F_t * f, size_t space_needed,
    bool write_permitted);
H5_DLL herr_t H5C__serialize_cache(H5F_t *f);
H5_DLL herr_t H5C__serialize_single_entry(H5F_t *f, H5C_t *cache_ptr, H5C_cache_entry_t *entry_ptr);
H5_DLL herr_t H5C__iter_tagged_entries(H5C_t *cache, haddr_t tag, bool match_global,
    H5C_tag_iter_cb_t cb, void *cb_ctx);

/* Routines for operating on entry tags */
H5_DLL herr_t H5C__tag_entry(H5C_t * cache_ptr, H5C_cache_entry_t * entry_ptr);
H5_DLL herr_t H5C__untag_entry(H5C_t *cache, H5C_cache_entry_t *entry);

/* Routines for operating on cache images */
H5_DLL herr_t H5C__get_cache_image_config(const H5C_t *cache_ptr, H5C_cache_image_ctl_t *config_ptr);
H5_DLL herr_t H5C__image_stats(H5C_t *cache_ptr, bool print_header);

/* Debugging routines */
#ifdef H5C_DO_SLIST_SANITY_CHECKS
H5_DLL bool H5C__entry_in_skip_list(H5C_t *cache_ptr, H5C_cache_entry_t *target_ptr);
#endif
#ifdef H5C_DO_EXTREME_SANITY_CHECKS
H5_DLL herr_t H5C__validate_lru_list(H5C_t *cache_ptr);
H5_DLL herr_t H5C__validate_pinned_entry_list(H5C_t *cache_ptr);
H5_DLL herr_t H5C__validate_protected_entry_list(H5C_t *cache_ptr);
#endif /* H5C_DO_EXTREME_SANITY_CHECKS */

/* Testing functions */
#ifdef H5C_TESTING
H5_DLL herr_t H5C__verify_cork_tag_test(hid_t fid, H5O_token_t tag_token, bool status);
#endif /* H5C_TESTING */

#endif /* H5Cpkg_H */
/* clang-format on */

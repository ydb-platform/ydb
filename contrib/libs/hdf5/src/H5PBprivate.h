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

/*-------------------------------------------------------------------------
 *
 * Created:		H5PBprivate.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5PBprivate_H
#define H5PBprivate_H

/* Private headers needed by this header */
#include "H5private.h"   /* Generic Functions			*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5SLprivate.h" /* Skip List				*/

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Forward declaration for a page buffer entry */
struct H5PB_entry_t;

/* Typedef for the main structure for the page buffer */
typedef struct H5PB_t {
    size_t   max_size;       /* The total page buffer size */
    size_t   page_size;      /* Size of a single page */
    unsigned min_meta_perc;  /* Minimum ratio of metadata entries required before evicting meta entries */
    unsigned min_raw_perc;   /* Minimum ratio of raw data entries required before evicting raw entries */
    unsigned meta_count;     /* Number of entries for metadata */
    unsigned raw_count;      /* Number of entries for raw data */
    unsigned min_meta_count; /* Minimum # of entries for metadata */
    unsigned min_raw_count;  /* Minimum # of entries for raw data */

    H5SL_t *slist_ptr;    /* Skip list with all the active page entries */
    H5SL_t *mf_slist_ptr; /* Skip list containing newly allocated page entries inserted from the MF layer */

    size_t               LRU_list_len; /* Number of entries in the LRU (identical to slist_ptr count) */
    struct H5PB_entry_t *LRU_head_ptr; /* Head pointer of the LRU */
    struct H5PB_entry_t *LRU_tail_ptr; /* Tail pointer of the LRU */

    H5FL_fac_head_t *page_fac; /* Factory for allocating pages */

    /* Statistics */
    unsigned accesses[2];
    unsigned hits[2];
    unsigned misses[2];
    unsigned evictions[2];
    unsigned bypasses[2];
} H5PB_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* General routines */
H5_DLL herr_t H5PB_create(H5F_shared_t *f_sh, size_t page_buffer_size, unsigned page_buf_min_meta_perc,
                          unsigned page_buf_min_raw_perc);
H5_DLL herr_t H5PB_flush(H5F_shared_t *f_sh);
H5_DLL herr_t H5PB_dest(H5F_shared_t *f_sh);
H5_DLL herr_t H5PB_add_new_page(H5F_shared_t *f_sh, H5FD_mem_t type, haddr_t page_addr);
H5_DLL herr_t H5PB_update_entry(H5PB_t *page_buf, haddr_t addr, size_t size, const void *buf);
H5_DLL herr_t H5PB_remove_entry(const H5F_shared_t *f_sh, haddr_t addr);
H5_DLL herr_t H5PB_read(H5F_shared_t *f_sh, H5FD_mem_t type, haddr_t addr, size_t size, void *buf /*out*/);
H5_DLL herr_t H5PB_write(H5F_shared_t *f_sh, H5FD_mem_t type, haddr_t addr, size_t size, const void *buf);
H5_DLL herr_t H5PB_enabled(H5F_shared_t *f_sh, H5FD_mem_t type, bool *enabled);

/* Statistics routines */
H5_DLL herr_t H5PB_reset_stats(H5PB_t *page_buf);
H5_DLL herr_t H5PB_get_stats(const H5PB_t *page_buf, unsigned accesses[2], unsigned hits[2],
                             unsigned misses[2], unsigned evictions[2], unsigned bypasses[2]);
H5_DLL herr_t H5PB_print_stats(const H5PB_t *page_buf);

#endif /* H5PBprivate_H */

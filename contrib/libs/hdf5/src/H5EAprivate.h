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
 * Created:     H5EAprivate.h
 *
 * Purpose:     Private header for library accessible extensible
 *              array routines.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5EAprivate_H
#define H5EAprivate_H

/* Private headers needed by this file */
#include "H5ACprivate.h" /* Metadata cache               */
#include "H5Fprivate.h"  /* File access                  */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Extensible array class IDs */
typedef enum H5EA_cls_id_t {
    H5EA_CLS_CHUNK_ID = 0,  /* Extensible array is for indexing dataset chunks w/o filters */
    H5EA_CLS_FILT_CHUNK_ID, /* Extensible array is for indexing dataset chunks w/filters */

    /* Start real class IDs at 0 -QAK */
    /* (keep these last) */
    H5EA_CLS_TEST_ID, /* Extensible array is for testing (do not use for actual data) */
    H5EA_NUM_CLS_ID   /* Number of Extensible Array class IDs (must be last) */
} H5EA_cls_id_t;

/*
 * Each type of element that can be stored in an extesible array has a
 * variable of this type that contains class variables and methods.
 */
typedef struct H5EA_class_t {
    H5EA_cls_id_t id;            /* ID of Extensible Array class, as found in file */
    const char   *name;          /* Name of class (for debugging) */
    size_t        nat_elmt_size; /* Size of native (memory) element */

    /* Extensible array client callback methods */
    void *(*crt_context)(void *udata); /* Create context for other callbacks */
    herr_t (*dst_context)(void *ctx);  /* Destroy context */
    herr_t (*fill)(void  *nat_blk,
                   size_t nelmts); /* Fill array of elements with encoded form of "missing element" value */
    herr_t (*encode)(void *raw, const void *elmt, size_t nelmts,
                     void *ctx); /* Encode elements from native form to disk storage form */
    herr_t (*decode)(const void *raw, void *elmt, size_t nelmts,
                     void *ctx); /* Decode elements from disk storage form to native form */
    herr_t (*debug)(FILE *stream, int indent, int fwidth, hsize_t idx,
                    const void *elmt);                /* Print an element for debugging */
    void *(*crt_dbg_ctx)(H5F_t *f, haddr_t obj_addr); /* Create debugging context */
    herr_t (*dst_dbg_ctx)(void *dbg_ctx);             /* Destroy debugging context */
} H5EA_class_t;

/* Extensible array creation parameters */
typedef struct H5EA_create_t {
    const H5EA_class_t *cls;           /* Class of extensible array to create */
    uint8_t             raw_elmt_size; /* Element size in file (in bytes) */
    uint8_t max_nelmts_bits; /* Log2(Max. # of elements in array) - i.e. # of bits needed to store max. # of
                                elements */
    uint8_t idx_blk_elmts;   /* # of elements to store in index block */
    uint8_t data_blk_min_elmts;        /* Min. # of elements per data block */
    uint8_t sup_blk_min_data_ptrs;     /* Min. # of data block pointers for a super block */
    uint8_t max_dblk_page_nelmts_bits; /* Log2(Max. # of elements in data block page) - i.e. # of bits needed
                                          to store max. # of elements in data block page */
} H5EA_create_t;

/* Extensible array metadata statistics info */
/* (If these are ever exposed to applications, don't let the application see
 *      which fields are computed vs. which fields are stored. -QAK)
 */
typedef struct H5EA_stat_t {
    /* Non-stored (i.e. computed) fields */
    struct {
        hsize_t hdr_size;       /* Size of header */
        hsize_t nindex_blks;    /* # of index blocks (should be 0 or 1) */
        hsize_t index_blk_size; /* Size of index blocks allocated */
    } computed;

    /* Stored fields */
    struct {
        hsize_t nsuper_blks;    /* # of super blocks */
        hsize_t super_blk_size; /* Size of super blocks allocated */
        hsize_t ndata_blks;     /* # of data blocks */
        hsize_t data_blk_size;  /* Size of data blocks allocated */
        hsize_t max_idx_set; /* Highest element index stored (+1 - i.e. if element 0 has been set, this value
                                with be '1', if no elements have been stored, this value will be '0') */
        hsize_t nelmts;      /* # of elements "realized" */
    } stored;
} H5EA_stat_t;

/* Extensible array info (forward decl - defined in H5EApkg.h) */
typedef struct H5EA_t H5EA_t;

/* Define the operator callback function pointer for H5EA_iterate() */
typedef int (*H5EA_operator_t)(hsize_t idx, const void *_elmt, void *_udata);

/*****************************/
/* Library-private Variables */
/*****************************/

/* The Extensible Array class for dataset chunks w/o filters*/
H5_DLLVAR const H5EA_class_t H5EA_CLS_CHUNK[1];

/* The Extensible Array class for dataset chunks w/ filters*/
H5_DLLVAR const H5EA_class_t H5EA_CLS_FILT_CHUNK[1];

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* General routines */
H5_DLL H5EA_t *H5EA_create(H5F_t *f, const H5EA_create_t *cparam, void *ctx_udata);
H5_DLL H5EA_t *H5EA_open(H5F_t *f, haddr_t ea_addr, void *ctx_udata);
H5_DLL herr_t  H5EA_get_nelmts(const H5EA_t *ea, hsize_t *nelmts);
H5_DLL herr_t  H5EA_get_addr(const H5EA_t *ea, haddr_t *addr);
H5_DLL herr_t  H5EA_set(const H5EA_t *ea, hsize_t idx, const void *elmt);
H5_DLL herr_t  H5EA_get(const H5EA_t *ea, hsize_t idx, void *elmt);
H5_DLL herr_t  H5EA_depend(H5EA_t *ea, H5AC_proxy_entry_t *parent);
H5_DLL herr_t  H5EA_iterate(H5EA_t *fa, H5EA_operator_t op, void *udata);
H5_DLL herr_t  H5EA_close(H5EA_t *ea);
H5_DLL herr_t  H5EA_delete(H5F_t *f, haddr_t ea_addr, void *ctx_udata);
H5_DLL herr_t  H5EA_patch_file(H5EA_t *fa, H5F_t *f);

/* Statistics routines */
H5_DLL herr_t H5EA_get_stats(const H5EA_t *ea, H5EA_stat_t *stats);

#endif /* H5EAprivate_H */

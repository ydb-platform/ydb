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
 * Created:        H5FAprivate.h
 *
 * Purpose:        Private header for library accessible Fixed
 *                      Array routines.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5FAprivate_H
#define H5FAprivate_H

/* Private headers needed by this file */
#include "H5ACprivate.h" /* Metadata cache               */
#include "H5Fprivate.h"  /* File access                  */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Fixed Array class IDs */
typedef enum H5FA_cls_id_t {
    H5FA_CLS_CHUNK_ID = 0,  /* Fixed array is for indexing dataset chunks w/o filters   */
    H5FA_CLS_FILT_CHUNK_ID, /* Fixed array is for indexing dataset chunks w/filters     */

    /* Start real class IDs at 0 -QAK */
    /* (keep these last) */
    H5FA_CLS_TEST_ID, /* Fixed array is for testing (do not use for actual data)  */
    H5FA_NUM_CLS_ID   /* Number of Fixed Array class IDs (must be last)           */
} H5FA_cls_id_t;

/*
 * Each type of element that can be stored in a Fixed Array has a
 * variable of this type that contains class variables and methods.
 */
typedef struct H5FA_class_t {
    H5FA_cls_id_t id;            /* ID of Fixed Array class, as found in file    */
    const char   *name;          /* Name of class (for debugging)                */
    size_t        nat_elmt_size; /* Size of native (memory) element              */

    /* Fixed array client callback methods */
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
} H5FA_class_t;

/* Fixed array creation parameters */
typedef struct H5FA_create_t {
    const H5FA_class_t *cls;                       /* Class of Fixed Array to create   */
    uint8_t             raw_elmt_size;             /* Element size in file (in bytes)  */
    uint8_t             max_dblk_page_nelmts_bits; /* Log2(Max. # of elements in a data block page) -
                                                    * i.e. # of bits needed to store max. # of elements
                                                    * in a data block page
                                                    */
    hsize_t nelmts;                                /* # of elements in array */
} H5FA_create_t;

/* Fixed array metadata statistics info */
typedef struct H5FA_stat_t {
    /* Non-stored (i.e. computed) fields */
    hsize_t hdr_size;  /* Size of header       */
    hsize_t dblk_size; /* Size of data block   */

    /* Stored fields */
    hsize_t nelmts; /* # of elements        */
} H5FA_stat_t;

/* Fixed Array info (forward decl - defined in H5FApkg.h) */
typedef struct H5FA_t H5FA_t;

/* Define the operator callback function pointer for H5FA_iterate() */
typedef int (*H5FA_operator_t)(hsize_t idx, const void *_elmt, void *_udata);

/*****************************/
/* Library-private Variables */
/*****************************/

/* The Fixed Array class for dataset chunks w/o filters*/
H5_DLLVAR const H5FA_class_t H5FA_CLS_CHUNK[1];

/* The Fixed Array class for dataset chunks w/ filters*/
H5_DLLVAR const H5FA_class_t H5FA_CLS_FILT_CHUNK[1];

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* General routines */
H5_DLL H5FA_t *H5FA_create(H5F_t *f, const H5FA_create_t *cparam, void *ctx_udata);
H5_DLL H5FA_t *H5FA_open(H5F_t *f, haddr_t fa_addr, void *ctx_udata);
H5_DLL herr_t  H5FA_get_nelmts(const H5FA_t *fa, hsize_t *nelmts);
H5_DLL herr_t  H5FA_get_addr(const H5FA_t *fa, haddr_t *addr);
H5_DLL herr_t  H5FA_set(const H5FA_t *fa, hsize_t idx, const void *elmt);
H5_DLL herr_t  H5FA_get(const H5FA_t *fa, hsize_t idx, void *elmt);
H5_DLL herr_t  H5FA_depend(H5FA_t *fa, H5AC_proxy_entry_t *parent);
H5_DLL herr_t  H5FA_iterate(H5FA_t *fa, H5FA_operator_t op, void *udata);
H5_DLL herr_t  H5FA_close(H5FA_t *fa);
H5_DLL herr_t  H5FA_delete(H5F_t *f, haddr_t fa_addr, void *ctx_udata);
H5_DLL herr_t  H5FA_patch_file(H5FA_t *fa, H5F_t *f);

/* Statistics routines */
H5_DLL herr_t H5FA_get_stats(const H5FA_t *ea, H5FA_stat_t *stats);

/* Debugging routines */
#ifdef H5FA_DEBUG
#endif /* H5FA_DEBUG */

#endif /* H5FAprivate_H */

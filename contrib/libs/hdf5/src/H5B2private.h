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
 * Created:		H5B2private.h
 *
 * Purpose:		Private header for library accessible B-tree routines.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5B2private_H
#define H5B2private_H

/* Private headers needed by this file */
#include "H5ACprivate.h" /* Metadata cache                   */
#include "H5Fprivate.h"  /* File access                      */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* B-tree IDs for various internal things. */
typedef enum H5B2_subid_t {
    H5B2_TEST_ID = 0,         /* B-tree is for testing (do not use for actual data) */
    H5B2_FHEAP_HUGE_INDIR_ID, /* B-tree is for fractal heap indirectly accessed, non-filtered 'huge' objects
                               */
    H5B2_FHEAP_HUGE_FILT_INDIR_ID, /* B-tree is for fractal heap indirectly accessed, filtered 'huge' objects
                                    */
    H5B2_FHEAP_HUGE_DIR_ID, /* B-tree is for fractal heap directly accessed, non-filtered 'huge' objects */
    H5B2_FHEAP_HUGE_FILT_DIR_ID, /* B-tree is for fractal heap directly accessed, filtered 'huge' objects */
    H5B2_GRP_DENSE_NAME_ID,      /* B-tree is for indexing 'name' field for "dense" link storage in groups */
    H5B2_GRP_DENSE_CORDER_ID,    /* B-tree is for indexing 'creation order' field for "dense" link storage in
                                    groups */
    H5B2_SOHM_INDEX_ID,          /* B-tree is an index for shared object header messages */
    H5B2_ATTR_DENSE_NAME_ID,   /* B-tree is for indexing 'name' field for "dense" attribute storage on objects
                                */
    H5B2_ATTR_DENSE_CORDER_ID, /* B-tree is for indexing 'creation order' field for "dense" attribute storage
                                  on objects */
    H5B2_CDSET_ID,             /* B-tree is for non-filtered chunked dataset storage w/ >1 unlim dims */
    H5B2_CDSET_FILT_ID,        /* B-tree is for filtered chunked dataset storage w/ >1 unlim dims */
    H5B2_TEST2_ID,             /* Another B-tree is for testing (do not use for actual data) */
    H5B2_NUM_BTREE_ID          /* Number of B-tree IDs (must be last)  */
} H5B2_subid_t;

/* Define the operator callback function pointer for H5B2_iterate() */
typedef int (*H5B2_operator_t)(const void *record, void *op_data);

/* Define the 'found' callback function pointer for H5B2_find(), H5B2_neighbor() & H5B2_index() */
typedef herr_t (*H5B2_found_t)(const void *record, void *op_data);

/* Define the 'modify' callback function pointer for H5B2_modify() */
typedef herr_t (*H5B2_modify_t)(void *record, void *op_data, bool *changed);

/* Define the 'remove' callback function pointer for H5B2_remove() & H5B2_delete() */
typedef herr_t (*H5B2_remove_t)(const void *record, void *op_data);

/* Comparisons for H5B2_neighbor() call */
typedef enum H5B2_compare_t {
    H5B2_COMPARE_LESS,   /* Records with keys less than query value */
    H5B2_COMPARE_GREATER /* Records with keys greater than query value */
} H5B2_compare_t;

/*
 * Each class of object that can be pointed to by a B-tree has a
 * variable of this type that contains class variables and methods.
 */
typedef struct H5B2_class_t H5B2_class_t;
struct H5B2_class_t {
    H5B2_subid_t id;        /* ID of B-tree class, as found in file */
    const char  *name;      /* Name of B-tree class, for debugging */
    size_t       nrec_size; /* Size of native (memory) record */

    /* Extensible array client callback methods */
    void *(*crt_context)(void *udata);                 /* Create context for other client callbacks */
    herr_t (*dst_context)(void *ctx);                  /* Destroy client callback context */
    herr_t (*store)(void *nrecord, const void *udata); /* Store application record in native record table */
    herr_t (*compare)(const void *rec1, const void *rec2, int *result); /* Compare two native records */
    herr_t (*encode)(uint8_t *raw, const void *record,
                     void *ctx); /* Encode record from native form to disk storage form */
    herr_t (*decode)(const uint8_t *raw, void *record,
                     void *ctx); /* Decode record from disk storage form to native form */
    herr_t (*debug)(FILE *stream, int indent, int fwidth, /* Print a record for debugging */
                    const void *record, const void *ctx);
};

/* v2 B-tree creation parameters */
typedef struct H5B2_create_t {
    const H5B2_class_t *cls;           /* v2 B-tree client class */
    uint32_t            node_size;     /* Size of each node (in bytes) */
    uint32_t            rrec_size;     /* Size of raw record (in bytes) */
    uint8_t             split_percent; /* % full to split nodes */
    uint8_t             merge_percent; /* % full to merge nodes */
} H5B2_create_t;

/* v2 B-tree metadata statistics info */
typedef struct H5B2_stat_t {
    unsigned depth;    /* Depth of B-tree */
    hsize_t  nrecords; /* Number of records */
} H5B2_stat_t;

/* v2 B-tree info (forward decl - defined in H5B2pkg.h) */
typedef struct H5B2_t H5B2_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
H5_DLL H5B2_t *H5B2_create(H5F_t *f, const H5B2_create_t *cparam, void *ctx_udata);
H5_DLL H5B2_t *H5B2_open(H5F_t *f, haddr_t addr, void *ctx_udata);
H5_DLL herr_t  H5B2_get_addr(const H5B2_t *bt2, haddr_t *addr /*out*/);
H5_DLL herr_t  H5B2_insert(H5B2_t *bt2, void *udata);
H5_DLL herr_t  H5B2_iterate(H5B2_t *bt2, H5B2_operator_t op, void *op_data);
H5_DLL herr_t  H5B2_find(H5B2_t *bt2, void *udata, bool *found, H5B2_found_t op, void *op_data);
H5_DLL herr_t  H5B2_index(H5B2_t *bt2, H5_iter_order_t order, hsize_t idx, H5B2_found_t op, void *op_data);
H5_DLL herr_t  H5B2_neighbor(H5B2_t *bt2, H5B2_compare_t range, void *udata, H5B2_found_t op, void *op_data);
H5_DLL herr_t  H5B2_modify(H5B2_t *bt2, void *udata, H5B2_modify_t op, void *op_data);
H5_DLL herr_t  H5B2_update(H5B2_t *bt2, void *udata, H5B2_modify_t op, void *op_data);
H5_DLL herr_t  H5B2_remove(H5B2_t *b2, void *udata, H5B2_remove_t op, void *op_data);
H5_DLL herr_t  H5B2_remove_by_idx(H5B2_t *bt2, H5_iter_order_t order, hsize_t idx, H5B2_remove_t op,
                                  void *op_data);
H5_DLL herr_t  H5B2_get_nrec(const H5B2_t *bt2, hsize_t *nrec);
H5_DLL herr_t  H5B2_size(H5B2_t *bt2, hsize_t *btree_size);
H5_DLL herr_t  H5B2_close(H5B2_t *bt2);
H5_DLL herr_t  H5B2_delete(H5F_t *f, haddr_t addr, void *ctx_udata, H5B2_remove_t op, void *op_data);
H5_DLL herr_t  H5B2_depend(H5B2_t *bt2, H5AC_proxy_entry_t *parent);
H5_DLL herr_t  H5B2_patch_file(H5B2_t *fa, H5F_t *f);

/* Statistics routines */
H5_DLL herr_t H5B2_stat_info(H5B2_t *bt2, H5B2_stat_t *info);

#endif /* H5B2private_H */

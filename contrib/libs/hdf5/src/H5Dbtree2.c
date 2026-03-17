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
 *
 * Purpose: v2 B-tree indexing for chunked datasets with > 1 unlimited dimensions.
 *   Each dataset chunk in the b-tree is identified by its dimensional offset.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5Dpkg.h"      /* Datasets                             */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MFprivate.h" /* File space management                */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5VMprivate.h" /* Vector and array functions           */

/****************/
/* Local Macros */
/****************/

#define H5D_BT2_IDX_IS_OPEN(idx_info) (NULL != (idx_info)->storage->u.btree2.bt2)

/******************/
/* Local Typedefs */
/******************/
/* User data for creating callback context */
typedef struct H5D_bt2_ctx_ud_t {
    const H5F_t *f;          /* Pointer to file info */
    uint32_t     chunk_size; /* Size of chunk (bytes; for filtered object) */
    unsigned     ndims;      /* Number of dimensions */
    uint32_t    *dim;        /* Size of chunk in elements */
} H5D_bt2_ctx_ud_t;

/* The callback context */
typedef struct H5D_bt2_ctx_t {
    uint32_t  chunk_size;     /* Size of chunk (bytes; constant for unfiltered object) */
    size_t    sizeof_addr;    /* Size of file addresses in the file (bytes) */
    size_t    chunk_size_len; /* Size of chunk sizes in the file (bytes) */
    unsigned  ndims;          /* Number of dimensions in chunk */
    uint32_t *dim;            /* Size of chunk in elements */
} H5D_bt2_ctx_t;

/* Callback info for iteration over chunks in v2 B-tree */
typedef struct H5D_bt2_it_ud_t {
    H5D_chunk_cb_func_t cb;    /* Callback routine for the chunk */
    void               *udata; /* User data for the chunk's callback routine */
} H5D_bt2_it_ud_t;

/* User data for compare callback */
typedef struct H5D_bt2_ud_t {
    H5D_chunk_rec_t rec;   /* The record to search for */
    unsigned        ndims; /* Number of dimensions for the chunked dataset */
} H5D_bt2_ud_t;

/********************/
/* Local Prototypes */
/********************/

/* Shared v2 B-tree methods for indexing filtered and non-filtered chunked datasets */
static void  *H5D__bt2_crt_context(void *udata);
static herr_t H5D__bt2_dst_context(void *ctx);
static herr_t H5D__bt2_store(void *native, const void *udata);
static herr_t H5D__bt2_compare(const void *rec1, const void *rec2, int *result);

/* v2 B-tree class for indexing non-filtered chunked datasets */
static herr_t H5D__bt2_unfilt_encode(uint8_t *raw, const void *native, void *ctx);
static herr_t H5D__bt2_unfilt_decode(const uint8_t *raw, void *native, void *ctx);
static herr_t H5D__bt2_unfilt_debug(FILE *stream, int indent, int fwidth, const void *record,
                                    const void *u_ctx);

/* v2 B-tree class for indexing filtered chunked datasets */
static herr_t H5D__bt2_filt_encode(uint8_t *raw, const void *native, void *ctx);
static herr_t H5D__bt2_filt_decode(const uint8_t *raw, void *native, void *ctx);
static herr_t H5D__bt2_filt_debug(FILE *stream, int indent, int fwidth, const void *record,
                                  const void *u_ctx);

/* Helper routine */
static herr_t H5D__btree2_idx_depend(const H5D_chk_idx_info_t *idx_info);

/* Callback for H5B2_iterate() which is called in H5D__bt2_idx_iterate() */
static int H5D__bt2_idx_iterate_cb(const void *_record, void *_udata);

/* Callback for H5B2_find() which is called in H5D__bt2_idx_get_addr() */
static herr_t H5D__bt2_found_cb(const void *nrecord, void *op_data);

/*
 * Callback for H5B2_remove() and H5B2_delete() which is called
 * in H5D__bt2_idx_remove() and H5D__bt2_idx_delete().
 */
static herr_t H5D__bt2_remove_cb(const void *nrecord, void *_udata);

/* Callback for H5B2_update() which is called in H5D__bt2_idx_insert() */
static herr_t H5D__bt2_mod_cb(void *_record, void *_op_data, bool *changed);

/* Chunked layout indexing callbacks for v2 B-tree indexing */
static herr_t H5D__bt2_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t *space,
                                haddr_t dset_ohdr_addr);
static herr_t H5D__bt2_idx_create(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__bt2_idx_open(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__bt2_idx_close(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__bt2_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open);
static bool   H5D__bt2_idx_is_space_alloc(const H5O_storage_chunk_t *storage);
static herr_t H5D__bt2_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                                  const H5D_t *dset);
static herr_t H5D__bt2_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata);
static herr_t H5D__bt2_idx_load_metadata(const H5D_chk_idx_info_t *idx_info);
static int    H5D__bt2_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb,
                                   void *chunk_udata);
static herr_t H5D__bt2_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata);
static herr_t H5D__bt2_idx_delete(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__bt2_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src,
                                      const H5D_chk_idx_info_t *idx_info_dst);
static herr_t H5D__bt2_idx_copy_shutdown(H5O_storage_chunk_t *storage_src, H5O_storage_chunk_t *storage_dst);
static herr_t H5D__bt2_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *size);
static herr_t H5D__bt2_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr);
static herr_t H5D__bt2_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream);
static herr_t H5D__bt2_idx_dest(const H5D_chk_idx_info_t *idx_info);

/*********************/
/* Package Variables */
/*********************/

/* Chunked dataset I/O ops for v2 B-tree indexing */
const H5D_chunk_ops_t H5D_COPS_BT2[1] = {{
    true,                        /* Fixed array indices support SWMR access */
    H5D__bt2_idx_init,           /* init */
    H5D__bt2_idx_create,         /* create */
    H5D__bt2_idx_open,           /* open */
    H5D__bt2_idx_close,          /* close */
    H5D__bt2_idx_is_open,        /* is_open */
    H5D__bt2_idx_is_space_alloc, /* is_space_alloc */
    H5D__bt2_idx_insert,         /* insert */
    H5D__bt2_idx_get_addr,       /* get_addr */
    H5D__bt2_idx_load_metadata,  /* load_metadata */
    NULL,                        /* resize */
    H5D__bt2_idx_iterate,        /* iterate */
    H5D__bt2_idx_remove,         /* remove */
    H5D__bt2_idx_delete,         /* delete */
    H5D__bt2_idx_copy_setup,     /* copy_setup */
    H5D__bt2_idx_copy_shutdown,  /* copy_shutdown */
    H5D__bt2_idx_size,           /* size */
    H5D__bt2_idx_reset,          /* reset */
    H5D__bt2_idx_dump,           /* dump */
    H5D__bt2_idx_dest            /* destroy */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/* v2 B-tree class for indexing non-filtered chunked datasets */
const H5B2_class_t H5D_BT2[1] = {{
    /* B-tree class information */
    H5B2_CDSET_ID,           /* Type of B-tree */
    "H5B2_CDSET_ID",         /* Name of B-tree class */
    sizeof(H5D_chunk_rec_t), /* Size of native record */
    H5D__bt2_crt_context,    /* Create client callback context */
    H5D__bt2_dst_context,    /* Destroy client callback context */
    H5D__bt2_store,          /* Record storage callback */
    H5D__bt2_compare,        /* Record comparison callback */
    H5D__bt2_unfilt_encode,  /* Record encoding callback */
    H5D__bt2_unfilt_decode,  /* Record decoding callback */
    H5D__bt2_unfilt_debug    /* Record debugging callback */
}};

/* v2 B-tree class for indexing filtered chunked datasets */
const H5B2_class_t H5D_BT2_FILT[1] = {{
    /* B-tree class information */
    H5B2_CDSET_FILT_ID,      /* Type of B-tree */
    "H5B2_CDSET_FILT_ID",    /* Name of B-tree class */
    sizeof(H5D_chunk_rec_t), /* Size of native record */
    H5D__bt2_crt_context,    /* Create client callback context */
    H5D__bt2_dst_context,    /* Destroy client callback context */
    H5D__bt2_store,          /* Record storage callback */
    H5D__bt2_compare,        /* Record comparison callback */
    H5D__bt2_filt_encode,    /* Record encoding callback */
    H5D__bt2_filt_decode,    /* Record decoding callback */
    H5D__bt2_filt_debug      /* Record debugging callback */
}};

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5D_bt2_ctx_t struct */
H5FL_DEFINE_STATIC(H5D_bt2_ctx_t);

/* Declare a free list to manage the page elements */
H5FL_ARR_DEFINE_STATIC(uint32_t, H5O_LAYOUT_NDIMS);

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_crt_context
 *
 * Purpose:     Create client callback context
 *
 * Return:      Success:    non-NULL
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5D__bt2_crt_context(void *_udata)
{
    H5D_bt2_ctx_ud_t *udata = (H5D_bt2_ctx_ud_t *)_udata; /* User data for building callback context */
    H5D_bt2_ctx_t    *ctx;                                /* Callback context structure */
    uint32_t         *my_dim    = NULL;                   /* Pointer to copy of chunk dimension size */
    void             *ret_value = NULL;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(udata);
    assert(udata->f);
    assert(udata->ndims > 0 && udata->ndims < H5O_LAYOUT_NDIMS);

    /* Allocate callback context */
    if (NULL == (ctx = H5FL_MALLOC(H5D_bt2_ctx_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't allocate callback context");

    /* Determine the size of addresses and set the chunk size and # of dimensions for the dataset */
    ctx->sizeof_addr = H5F_SIZEOF_ADDR(udata->f);
    ctx->chunk_size  = udata->chunk_size;
    ctx->ndims       = udata->ndims;

    /* Set up the "local" information for this dataset's chunk dimension sizes */
    if (NULL == (my_dim = (uint32_t *)H5FL_ARR_MALLOC(uint32_t, H5O_LAYOUT_NDIMS)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL, "can't allocate chunk dims");
    H5MM_memcpy(my_dim, udata->dim, H5O_LAYOUT_NDIMS * sizeof(uint32_t));
    ctx->dim = my_dim;

    /*
     * Compute the size required for encoding the size of a chunk,
     * allowing for an extra byte, in case the filter makes the chunk larger.
     */
    ctx->chunk_size_len = 1 + ((H5VM_log2_gen((uint64_t)udata->chunk_size) + 8) / 8);
    if (ctx->chunk_size_len > 8)
        ctx->chunk_size_len = 8;

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_crt_context() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_dst_context
 *
 * Purpose:     Destroy client callback context
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_dst_context(void *_ctx)
{
    H5D_bt2_ctx_t *ctx = (H5D_bt2_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Free array for chunk dimension sizes */
    if (ctx->dim)
        H5FL_ARR_FREE(uint32_t, ctx->dim);
    /* Release callback context */
    ctx = H5FL_FREE(H5D_bt2_ctx_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_dst_context() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_store
 *
 * Purpose:     Store native information into record for v2 B-tree
 *              (non-filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_store(void *record, const void *_udata)
{
    const H5D_bt2_ud_t *udata = (const H5D_bt2_ud_t *)_udata; /* User data */

    FUNC_ENTER_PACKAGE_NOERR

    *(H5D_chunk_rec_t *)record = udata->rec;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_store() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_compare
 *
 * Purpose:     Compare two native information records, according to some
 *              key (non-filtered)
 *
 * Return:      <0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_compare(const void *_udata, const void *_rec2, int *result)
{
    const H5D_bt2_ud_t    *udata     = (const H5D_bt2_ud_t *)_udata;   /* User data */
    const H5D_chunk_rec_t *rec1      = &(udata->rec);                  /* The search record */
    const H5D_chunk_rec_t *rec2      = (const H5D_chunk_rec_t *)_rec2; /* The native record */
    herr_t                 ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(rec1);
    assert(rec2);

    /* Compare the offsets but ignore the other fields */
    *result = H5VM_vector_cmp_u(udata->ndims, rec1->scaled, rec2->scaled);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_compare() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_unfilt_encode
 *
 * Purpose:     Encode native information into raw form for storing on disk
 *              (non-filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_unfilt_encode(uint8_t *raw, const void *_record, void *_ctx)
{
    H5D_bt2_ctx_t         *ctx    = (H5D_bt2_ctx_t *)_ctx;            /* Callback context structure */
    const H5D_chunk_rec_t *record = (const H5D_chunk_rec_t *)_record; /* The native record */
    unsigned               u;                                         /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Encode the record's fields */
    H5F_addr_encode_len(ctx->sizeof_addr, &raw, record->chunk_addr);
    /* (Don't encode the chunk size & filter mask for non-filtered B-tree records) */
    for (u = 0; u < ctx->ndims; u++)
        UINT64ENCODE(raw, record->scaled[u]);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_unfilt_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_unfilt_decode
 *
 * Purpose:     Decode raw disk form of record into native form
 *              (non-filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_unfilt_decode(const uint8_t *raw, void *_record, void *_ctx)
{
    H5D_bt2_ctx_t   *ctx    = (H5D_bt2_ctx_t *)_ctx;      /* Callback context structure */
    H5D_chunk_rec_t *record = (H5D_chunk_rec_t *)_record; /* The native record */
    unsigned         u;                                   /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Decode the record's fields */
    H5F_addr_decode_len(ctx->sizeof_addr, &raw, &record->chunk_addr);
    record->nbytes      = ctx->chunk_size;
    record->filter_mask = 0;
    for (u = 0; u < ctx->ndims; u++)
        UINT64DECODE(raw, record->scaled[u]);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_unfilt_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_unfilt_debug
 *
 * Purpose:     Debug native form of record (non-filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_unfilt_debug(FILE *stream, int indent, int fwidth, const void *_record, const void *_ctx)
{
    const H5D_chunk_rec_t *record = (const H5D_chunk_rec_t *)_record; /* The native record */
    const H5D_bt2_ctx_t   *ctx    = (const H5D_bt2_ctx_t *)_ctx;      /* Callback context */
    unsigned               u;                                         /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(record);
    assert(ctx->chunk_size == record->nbytes);
    assert(0 == record->filter_mask);

    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Chunk address:", record->chunk_addr);

    fprintf(stream, "%*s%-*s {", indent, "", fwidth, "Logical offset:");
    for (u = 0; u < ctx->ndims; u++)
        fprintf(stream, "%s%" PRIuHSIZE, u ? ", " : "", record->scaled[u] * ctx->dim[u]);
    fputs("}\n", stream);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_unfilt_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_filt_encode
 *
 * Purpose:     Encode native information into raw form for storing on disk
 *              (filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_filt_encode(uint8_t *raw, const void *_record, void *_ctx)
{
    H5D_bt2_ctx_t         *ctx    = (H5D_bt2_ctx_t *)_ctx;            /* Callback context structure */
    const H5D_chunk_rec_t *record = (const H5D_chunk_rec_t *)_record; /* The native record */
    unsigned               u;                                         /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);
    assert(record);
    assert(H5_addr_defined(record->chunk_addr));
    assert(0 != record->nbytes);

    /* Encode the record's fields */
    H5F_addr_encode_len(ctx->sizeof_addr, &raw, record->chunk_addr);
    UINT64ENCODE_VAR(raw, record->nbytes, ctx->chunk_size_len);
    UINT32ENCODE(raw, record->filter_mask);
    for (u = 0; u < ctx->ndims; u++)
        UINT64ENCODE(raw, record->scaled[u]);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_filt_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_filt_decode
 *
 * Purpose:     Decode raw disk form of record into native form
 *              (filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_filt_decode(const uint8_t *raw, void *_record, void *_ctx)
{
    H5D_bt2_ctx_t   *ctx    = (H5D_bt2_ctx_t *)_ctx;      /* Callback context structure */
    H5D_chunk_rec_t *record = (H5D_chunk_rec_t *)_record; /* The native record */
    unsigned         u;                                   /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);
    assert(record);

    /* Decode the record's fields */
    H5F_addr_decode_len(ctx->sizeof_addr, &raw, &record->chunk_addr);
    UINT64DECODE_VAR(raw, record->nbytes, ctx->chunk_size_len);
    UINT32DECODE(raw, record->filter_mask);
    for (u = 0; u < ctx->ndims; u++)
        UINT64DECODE(raw, record->scaled[u]);

    /* Sanity checks */
    assert(H5_addr_defined(record->chunk_addr));
    assert(0 != record->nbytes);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_filt_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_filt_debug
 *
 * Purpose:     Debug native form of record (filtered)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_filt_debug(FILE *stream, int indent, int fwidth, const void *_record, const void *_ctx)
{
    const H5D_chunk_rec_t *record = (const H5D_chunk_rec_t *)_record; /* The native record */
    const H5D_bt2_ctx_t   *ctx    = (const H5D_bt2_ctx_t *)_ctx;      /* Callback context */
    unsigned               u;                                         /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(record);
    assert(H5_addr_defined(record->chunk_addr));
    assert(0 != record->nbytes);

    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Chunk address:", record->chunk_addr);
    fprintf(stream, "%*s%-*s %u bytes\n", indent, "", fwidth, "Chunk size:", (unsigned)record->nbytes);
    fprintf(stream, "%*s%-*s 0x%08x\n", indent, "", fwidth, "Filter mask:", record->filter_mask);

    fprintf(stream, "%*s%-*s {", indent, "", fwidth, "Logical offset:");
    for (u = 0; u < ctx->ndims; u++)
        fprintf(stream, "%s%" PRIuHSIZE, u ? ", " : "", record->scaled[u] * ctx->dim[u]);
    fputs("}\n", stream);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_filt_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_init
 *
 * Purpose:     Initialize the indexing information for a dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_init(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info, const H5S_t H5_ATTR_UNUSED *space,
                  haddr_t dset_ohdr_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(H5_addr_defined(dset_ohdr_addr));

    idx_info->storage->u.btree2.dset_ohdr_addr = dset_ohdr_addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__bt2_idx_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree2_idx_depend
 *
 * Purpose:     Create flush dependency between v2 B-tree and dataset's
 *              object header.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree2_idx_depend(const H5D_chk_idx_info_t *idx_info)
{
    H5O_t              *oh = NULL;           /* Object header */
    H5O_loc_t           oloc;                /* Temporary object header location for dataset */
    H5AC_proxy_entry_t *oh_proxy;            /* Dataset's object header proxy */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(H5D_CHUNK_IDX_BT2 == idx_info->layout->idx_type);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_BT2 == idx_info->storage->idx_type);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(idx_info->storage->u.btree2.bt2);

    /* Set up object header location for dataset */
    H5O_loc_reset(&oloc);
    oloc.file = idx_info->f;
    oloc.addr = idx_info->storage->u.btree.dset_ohdr_addr;

    /* Get header */
    if (NULL == (oh = H5O_protect(&oloc, H5AC__READ_ONLY_FLAG, true)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTPROTECT, FAIL, "unable to protect object header");

    /* Retrieve the dataset's object header proxy */
    if (NULL == (oh_proxy = H5O_get_proxy(oh)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get dataset object header proxy");

    /* Make the v2 B-tree a child flush dependency of the dataset's object header proxy */
    if (H5B2_depend(idx_info->storage->u.btree2.bt2, oh_proxy) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEPEND, FAIL,
                    "unable to create flush dependency on object header proxy");

done:
    /* Release the object header from the cache */
    if (oh && H5O_unprotect(&oloc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree2_idx_depend() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_create
 *
 * Purpose:     Create the v2 B-tree for tracking dataset chunks
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_create(const H5D_chk_idx_info_t *idx_info)
{
    H5B2_create_t    bt2_cparam;          /* v2 B-tree creation parameters */
    H5D_bt2_ctx_ud_t u_ctx;               /* data for context call */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(!H5_addr_defined(idx_info->storage->idx_addr));

    bt2_cparam.rrec_size = H5F_SIZEOF_ADDR(idx_info->f)         /* Address of chunk */
                           + (idx_info->layout->ndims - 1) * 8; /* # of dimensions x 64-bit chunk offsets */

    /* General parameters */
    if (idx_info->pline->nused > 0) {
        unsigned chunk_size_len; /* Size of encoded chunk size */

        /*
         * Compute the size required for encoding the size of a chunk,
         * allowing for an extra byte, in case the filter makes the chunk larger.
         */
        chunk_size_len = 1 + ((H5VM_log2_gen((uint64_t)idx_info->layout->size) + 8) / 8);
        if (chunk_size_len > 8)
            chunk_size_len = 8;

        bt2_cparam.rrec_size += chunk_size_len + 4; /* Size of encoded chunk size & filter mask */
        bt2_cparam.cls = H5D_BT2_FILT;
    } /* end if */
    else
        bt2_cparam.cls = H5D_BT2;

    bt2_cparam.node_size     = idx_info->layout->u.btree2.cparam.node_size;
    bt2_cparam.split_percent = idx_info->layout->u.btree2.cparam.split_percent;
    bt2_cparam.merge_percent = idx_info->layout->u.btree2.cparam.merge_percent;

    u_ctx.f          = idx_info->f;
    u_ctx.ndims      = idx_info->layout->ndims - 1;
    u_ctx.chunk_size = idx_info->layout->size;
    u_ctx.dim        = idx_info->layout->dim;

    /* Create the v2 B-tree for the chunked dataset */
    if (NULL == (idx_info->storage->u.btree2.bt2 = H5B2_create(idx_info->f, &bt2_cparam, &u_ctx)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "can't create v2 B-tree for tracking chunked dataset");

    /* Retrieve the v2 B-tree's address in the file */
    if (H5B2_get_addr(idx_info->storage->u.btree2.bt2, &(idx_info->storage->idx_addr)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "can't get v2 B-tree address for tracking chunked dataset");

    /* Check for SWMR writes to the file */
    if (H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)
        if (H5D__btree2_idx_depend(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDEPEND, FAIL,
                        "unable to create flush dependency on object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_open()
 *
 * Purpose:     Opens an existing v2 B-tree.
 *
 * Note:        This information is passively initialized from each index
 *              operation callback because those abstract chunk index
 *              operations are designed to work with the v2 B-tree chunk
 *              indices also, which don't require an 'open' for the data
 *              structure.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_open(const H5D_chk_idx_info_t *idx_info)
{
    H5D_bt2_ctx_ud_t u_ctx;               /* user data for creating context */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(H5D_CHUNK_IDX_BT2 == idx_info->layout->idx_type);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(NULL == idx_info->storage->u.btree2.bt2);

    /* Set up the user data */
    u_ctx.f          = idx_info->f;
    u_ctx.ndims      = idx_info->layout->ndims - 1;
    u_ctx.chunk_size = idx_info->layout->size;
    u_ctx.dim        = idx_info->layout->dim;

    /* Open v2 B-tree for the chunk index */
    if (NULL ==
        (idx_info->storage->u.btree2.bt2 = H5B2_open(idx_info->f, idx_info->storage->idx_addr, &u_ctx)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't open v2 B-tree for tracking chunked dataset");

    /* Check for SWMR writes to the file */
    if (H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)
        if (H5D__btree2_idx_depend(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDEPEND, FAIL,
                        "unable to create flush dependency on object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_close()
 *
 * Purpose:     Closes an existing v2 B-tree.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_close(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(idx_info);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_BT2 == idx_info->storage->idx_type);
    assert(idx_info->storage->u.btree2.bt2);

    if (H5B2_close(idx_info->storage->u.btree2.bt2) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close v2 B-tree");
    idx_info->storage->u.btree2.bt2 = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_close() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_is_open
 *
 * Purpose:     Query if the index is opened or not
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(idx_info);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_BT2 == idx_info->storage->idx_type);
    assert(is_open);

    *is_open = H5D_BT2_IDX_IS_OPEN(idx_info);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__bt2_idx_is_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_is_space_alloc
 *
 * Purpose:     Query if space is allocated for index method
 *
 * Return:      true/false
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__bt2_idx_is_space_alloc(const H5O_storage_chunk_t *storage)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);

    FUNC_LEAVE_NOAPI((bool)H5_addr_defined(storage->idx_addr))
} /* end H5D__bt2_idx_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_mod_cb
 *
 * Purpose:     Modify record for dataset chunk when it is found in a v2
 *              B-tree. This is the callback for H5B2_update() which is
 *              called in H5D__bt2_idx_insert().
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_mod_cb(void *_record, void *_op_data, bool *changed)
{
    H5D_bt2_ud_t    *op_data = (H5D_bt2_ud_t *)_op_data;   /* User data for v2 B-tree calls */
    H5D_chunk_rec_t *record  = (H5D_chunk_rec_t *)_record; /* Chunk record */

    FUNC_ENTER_PACKAGE_NOERR

/* Sanity check */
#ifndef NDEBUG
    {
        unsigned u; /* Local index variable */

        for (u = 0; u < op_data->ndims; u++)
            assert(record->scaled[u] == op_data->rec.scaled[u]);
    }
#endif /* NDEBUG */

    /* Modify record */
    *record = op_data->rec;

    /* Note that the record changed */
    *changed = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__bt2_mod_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_insert
 *
 * Purpose:     Insert chunk address into the indexing structure.
 *              A non-filtered chunk:
 *                Should not exist
 *                Allocate the chunk and pass chunk address back up
 *              A filtered chunk:
 *                If it was not found, create the chunk and pass chunk
 *                  address back up
 *                If it was found but its size changed, reallocate the chunk
 *                  and pass chunk address back up
 *                If it was found but its size was the same, pass chunk
 *                  address back up
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                    const H5D_t H5_ATTR_UNUSED *dset)
{
    H5B2_t      *bt2;                 /* v2 B-tree handle for indexing chunks */
    H5D_bt2_ud_t bt2_udata;           /* User data for v2 B-tree calls */
    unsigned     u;                   /* Local index variable */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);
    assert(H5_addr_defined(udata->chunk_block.offset));

    /* Check if the v2 B-tree is open yet */
    if (!H5D_BT2_IDX_IS_OPEN(idx_info)) {
        /* Open existing v2 B-tree */
        if (H5D__bt2_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open v2 B-tree");
    }    /* end if */
    else /* Patch the top level file pointer contained in bt2 if needed */
        if (H5B2_patch_file(idx_info->storage->u.btree2.bt2, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch v2 B-tree file pointer");

    /* Set convenience pointer to v2 B-tree structure */
    bt2 = idx_info->storage->u.btree2.bt2;

    /* Set up callback info */
    bt2_udata.ndims          = idx_info->layout->ndims - 1;
    bt2_udata.rec.chunk_addr = udata->chunk_block.offset;
    if (idx_info->pline->nused > 0) { /* filtered chunk */
        H5_CHECKED_ASSIGN(bt2_udata.rec.nbytes, uint32_t, udata->chunk_block.length, hsize_t);
        bt2_udata.rec.filter_mask = udata->filter_mask;
    }      /* end if */
    else { /* non-filtered chunk */
        bt2_udata.rec.nbytes      = idx_info->layout->size;
        bt2_udata.rec.filter_mask = 0;
    } /* end else */
    for (u = 0; u < (idx_info->layout->ndims - 1); u++)
        bt2_udata.rec.scaled[u] = udata->common.scaled[u];

    /* Update record for v2 B-tree (could be insert or modify) */
    if (H5B2_update(bt2, &bt2_udata, H5D__bt2_mod_cb, &bt2_udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTUPDATE, FAIL, "unable to update record in v2 B-tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_idx_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_found_cb
 *
 * Purpose:     Retrieve record for dataset chunk when it is found in a v2
 *              B-tree. This is the callback for H5B2_find() which is called
 *              in H5D__bt2_idx_get_addr() and H5D__bt2_idx_insert().
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_found_cb(const void *nrecord, void *op_data)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5D_chunk_rec_t *)op_data = *(const H5D_chunk_rec_t *)nrecord;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__bt2_found_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_get_addr
 *
 * Purpose:     Get the file address of a chunk if file space has been
 *              assigned.  Save the retrieved information in the udata
 *              supplied.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata)
{
    H5B2_t         *bt2;                 /* v2 B-tree handle for indexing chunks */
    H5D_bt2_ud_t    bt2_udata;           /* User data for v2 B-tree calls */
    H5D_chunk_rec_t found_rec;           /* Record found from searching for object */
    unsigned        u;                   /* Local index variable */
    bool            found;               /* Whether chunk was found */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->layout->ndims > 0);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Check if the v2 B-tree is open yet */
    if (!H5D_BT2_IDX_IS_OPEN(idx_info)) {
        /* Open existing v2 B-tree */
        if (H5D__bt2_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open v2 B-tree");
    }    /* end if */
    else /* Patch the top level file pointer contained in bt2 if needed */
        if (H5B2_patch_file(idx_info->storage->u.btree2.bt2, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch v2 B-tree file pointer");

    /* Set convenience pointer to v2 B-tree structure */
    bt2 = idx_info->storage->u.btree2.bt2;

    /* Clear the found record */
    found_rec.chunk_addr  = HADDR_UNDEF;
    found_rec.nbytes      = 0;
    found_rec.filter_mask = 0;

    /* Prepare user data for compare callback */
    bt2_udata.rec.chunk_addr = HADDR_UNDEF;
    bt2_udata.ndims          = idx_info->layout->ndims - 1;

    /* Set the chunk offset to be searched for */
    for (u = 0; u < (idx_info->layout->ndims - 1); u++)
        bt2_udata.rec.scaled[u] = udata->common.scaled[u];

    /* Go get chunk information from v2 B-tree */
    found = false;
    if (H5B2_find(bt2, &bt2_udata, &found, H5D__bt2_found_cb, &found_rec) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "can't check for chunk in v2 B-tree");

    /* Check if chunk was found */
    if (found) {
        /* Sanity check */
        assert(0 != found_rec.nbytes);

        /* Set common info for the chunk */
        udata->chunk_block.offset = found_rec.chunk_addr;

        /* Set other info for the chunk */
        if (idx_info->pline->nused > 0) { /* filtered chunk */
            udata->chunk_block.length = found_rec.nbytes;
            udata->filter_mask        = found_rec.filter_mask;
        }      /* end if */
        else { /* non-filtered chunk */
            udata->chunk_block.length = idx_info->layout->size;
            udata->filter_mask        = 0;
        } /* end else */
    }     /* end if */
    else {
        udata->chunk_block.offset = HADDR_UNDEF;
        udata->chunk_block.length = 0;
        udata->filter_mask        = 0;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_idx_get_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_load_metadata
 *
 * Purpose:     Load additional chunk index metadata beyond the chunk index
 *              itself.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_load_metadata(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    H5D_chunk_ud_t chunk_ud;
    hsize_t        scaled[H5O_LAYOUT_NDIMS] = {0};
    herr_t         ret_value                = SUCCEED;

    FUNC_ENTER_PACKAGE

    /*
     * After opening a dataset that uses a v2 Btree, the root
     * node will generally not be read in until an element is
     * looked up for the first time. Since there isn't currently
     * a good way of controlling that explicitly, perform a fake
     * lookup of a chunk to cause it to be read in.
     */
    chunk_ud.common.layout  = idx_info->layout;
    chunk_ud.common.storage = idx_info->storage;
    chunk_ud.common.scaled  = scaled;

    chunk_ud.chunk_block.offset = HADDR_UNDEF;
    chunk_ud.chunk_block.length = 0;
    chunk_ud.filter_mask        = 0;
    chunk_ud.new_unfilt_chunk   = false;
    chunk_ud.idx_hint           = UINT_MAX;

    if (H5D__bt2_idx_get_addr(idx_info, &chunk_ud) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't load v2 B-tree root node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_idx_load_metadata() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_iterate_cb
 *
 * Purpose:     Translate the B-tree specific chunk record into a generic
 *              form and make the callback to the generic chunk callback
 *              routine.
 *              This is the callback for H5B2_iterate() which is called in
 *              H5D__bt2_idx_iterate().
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__bt2_idx_iterate_cb(const void *_record, void *_udata)
{
    H5D_bt2_it_ud_t       *udata     = (H5D_bt2_it_ud_t *)_udata;        /* User data */
    const H5D_chunk_rec_t *record    = (const H5D_chunk_rec_t *)_record; /* Native record */
    int                    ret_value = -1;                               /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Make "generic chunk" callback */
    if ((ret_value = (udata->cb)(record, udata->udata)) < 0)
        HERROR(H5E_DATASET, H5E_CALLBACK, "failure in generic chunk iterator callback");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_idx_iterate_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_iterate
 *
 * Purpose:     Iterate over the chunks in an index, making a callback
 *              for each one.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__bt2_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb, void *chunk_udata)
{
    H5B2_t         *bt2;              /* v2 B-tree handle for indexing chunks */
    H5D_bt2_it_ud_t udata;            /* User data for B-tree iterator callback */
    int             ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(chunk_cb);
    assert(chunk_udata);

    /* Check if the v2 B-tree is open yet */
    if (!H5D_BT2_IDX_IS_OPEN(idx_info)) {
        /* Open existing v2 B-tree */
        if (H5D__bt2_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open v2 B-tree");
    }    /* end if */
    else /* Patch the top level file pointer contained in bt2 if needed */
        if (H5B2_patch_file(idx_info->storage->u.btree2.bt2, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch v2 B-tree file pointer");

    /* Set convenience pointer to v2 B-tree structure */
    bt2 = idx_info->storage->u.btree2.bt2;

    /* Prepare user data for iterate callback */
    udata.cb    = chunk_cb;
    udata.udata = chunk_udata;

    /* Iterate over the records in the v2 B-tree */
    if ((ret_value = H5B2_iterate(bt2, H5D__bt2_idx_iterate_cb, &udata)) < 0)
        HERROR(H5E_DATASET, H5E_BADITER, "unable to iterate over chunk v2 B-tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_remove_cb()
 *
 * Purpose:     Free space for 'dataset chunk' object as v2 B-tree
 *              is being deleted or v2 B-tree node is removed.
 *              This is the callback for H5B2_remove() and H5B2_delete()
 *              which are called in H5D__bt2_idx_remove() and
 *              H5D__bt2_idx_delete().
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_remove_cb(const void *_record, void *_udata)
{
    const H5D_chunk_rec_t *record    = (const H5D_chunk_rec_t *)_record; /* The native record */
    H5F_t                 *f         = (H5F_t *)_udata;                  /* User data for removal callback */
    herr_t                 ret_value = SUCCEED;                          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);

    /* Free the space in the file for the object being removed */
    H5_CHECK_OVERFLOW(record->nbytes, uint32_t, hsize_t);
    if (H5MF_xfree(f, H5FD_MEM_DRAW, record->chunk_addr, (hsize_t)record->nbytes) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to free chunk");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_remove_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_remove
 *
 * Purpose:     Remove chunk from index.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata)
{
    H5B2_t      *bt2;                 /* v2 B-tree handle for indexing chunks */
    H5D_bt2_ud_t bt2_udata;           /* User data for v2 B-tree find call */
    unsigned     u;                   /* Local index variable */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Check if the v2 B-tree is open yet */
    if (!H5D_BT2_IDX_IS_OPEN(idx_info)) {
        /* Open existing v2 B-tree */
        if (H5D__bt2_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open v2 B-tree");
    }    /* end if */
    else /* Patch the top level file pointer contained in bt2 if needed */
        if (H5B2_patch_file(idx_info->storage->u.btree2.bt2, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch v2 B-tree file pointer");

    /* Set convenience pointer to v2 B-tree structure */
    bt2 = idx_info->storage->u.btree2.bt2;

    /* Prepare user data for compare callback */
    bt2_udata.ndims = idx_info->layout->ndims - 1;

    /* Initialize the record to search for */
    for (u = 0; u < (idx_info->layout->ndims - 1); u++)
        bt2_udata.rec.scaled[u] = udata->scaled[u];

    /* Remove the record for the "dataset chunk" object from the v2 B-tree */
    /* (space in the file for the object is freed in the 'remove' callback) */
    if (H5B2_remove(bt2, &bt2_udata,
                    (H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE) ? NULL : H5D__bt2_remove_cb,
                    idx_info->f) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREMOVE, FAIL, "can't remove object from B-tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__bt2_idx_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_delete
 *
 * Purpose:     Delete index and raw data storage for entire dataset
 *              (i.e. all chunks)
 *
 * Return:      Success:    Non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_delete(const H5D_chk_idx_info_t *idx_info)
{
    H5B2_remove_t    remove_op;           /* The removal callback */
    H5D_bt2_ctx_ud_t u_ctx;               /* data for context call */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);

    /* Check if the index data structure has been allocated */
    if (H5_addr_defined(idx_info->storage->idx_addr)) {
        /* Set up user data for creating context */
        u_ctx.f          = idx_info->f;
        u_ctx.ndims      = idx_info->layout->ndims - 1;
        u_ctx.chunk_size = idx_info->layout->size;
        u_ctx.dim        = idx_info->layout->dim;

        /* Set remove operation.  Do not remove chunks in SWMR_WRITE mode */
        if (H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)
            remove_op = NULL;
        else
            remove_op = H5D__bt2_remove_cb;

        /* Delete the v2 B-tree */
        /*(space in the file for each object is freed in the 'remove' callback) */
        if (H5B2_delete(idx_info->f, idx_info->storage->idx_addr, &u_ctx, remove_op, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDELETE, FAIL, "can't delete v2 B-tree");

        idx_info->storage->idx_addr = HADDR_UNDEF;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_copy_setup
 *
 * Purpose:     Set up any necessary information for copying chunks
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src, const H5D_chk_idx_info_t *idx_info_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Source file */
    assert(idx_info_src);
    assert(idx_info_src->f);
    assert(idx_info_src->pline);
    assert(idx_info_src->layout);
    assert(idx_info_src->storage);

    /* Destination file */
    assert(idx_info_dst);
    assert(idx_info_dst->f);
    assert(idx_info_dst->pline);
    assert(idx_info_dst->layout);
    assert(idx_info_dst->storage);
    assert(!H5_addr_defined(idx_info_dst->storage->idx_addr));

    /* Check if the source v2 B-tree is open yet */
    if (!H5D_BT2_IDX_IS_OPEN(idx_info_src))
        if (H5D__bt2_idx_open(idx_info_src) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open v2 B-tree");

    /* Set copied metadata tag */
    H5_BEGIN_TAG(H5AC__COPIED_TAG)

    /* Create v2 B-tree that describes the chunked dataset in the destination file */
    if (H5D__bt2_idx_create(idx_info_dst) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize chunked storage");
    assert(H5_addr_defined(idx_info_dst->storage->idx_addr));

    /* Reset metadata tag */
    H5_END_TAG

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_copy_setup() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_copy_shutdown
 *
 * Purpose:     Shutdown any information from copying chunks
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_copy_shutdown(H5O_storage_chunk_t *storage_src, H5O_storage_chunk_t *storage_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(storage_src);
    assert(storage_src->u.btree2.bt2);
    assert(storage_dst);
    assert(storage_dst->u.btree2.bt2);

    /* Close v2 B-tree for source file */
    if (H5B2_close(storage_src->u.btree2.bt2) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close v2 B-tree");
    storage_src->u.btree2.bt2 = NULL;

    /* Close v2 B-tree for destination file */
    if (H5B2_close(storage_dst->u.btree2.bt2) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close v2 B-tree");
    storage_dst->u.btree2.bt2 = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_copy_shutdown() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_size
 *
 * Purpose:     Retrieve the amount of index storage for chunked dataset
 *
 * Return:      Success:    Non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *index_size)
{
    H5B2_t *bt2_cdset = NULL;    /* Pointer to v2 B-tree structure */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(index_size);

    /* Open v2 B-tree */
    if (H5D__bt2_idx_open(idx_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open v2 B-tree");

    /* Set convenience pointer to v2 B-tree structure */
    bt2_cdset = idx_info->storage->u.btree2.bt2;

    /* Get v2 B-tree size for indexing chunked dataset */
    if (H5B2_size(bt2_cdset, index_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                    "can't retrieve v2 B-tree storage info for chunked dataset");

done:
    /* Close v2 B-tree index */
    if (H5D__bt2_idx_close(idx_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close v2 B-tree for tracking chunked dataset");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_size() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_reset
 *
 * Purpose:     Reset indexing information.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(storage);

    /* Reset index info */
    if (reset_addr)
        storage->idx_addr = HADDR_UNDEF;
    storage->u.btree2.bt2 = NULL;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__bt2_idx_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_dump
 *
 * Purpose:     Dump indexing information to a stream.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(storage);
    assert(stream);

    fprintf(stream, "    Address: %" PRIuHADDR "\n", storage->idx_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__bt2_idx_dump() */

/*-------------------------------------------------------------------------
 * Function:    H5D__bt2_idx_dest
 *
 * Purpose:     Release indexing information in memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__bt2_idx_dest(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->storage);

    /* Check if the v2-btree is open */
    if (H5D_BT2_IDX_IS_OPEN(idx_info)) {
        /* Patch the top level file pointer contained in bt2 if needed */
        if (H5B2_patch_file(idx_info->storage->u.btree2.bt2, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch v2 B-tree file pointer");

        /* Close v2 B-tree */
        if (H5D__bt2_idx_close(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__bt2_idx_dest() */

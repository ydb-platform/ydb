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
 * Purpose:     Extensible array indexed (chunked) I/O functions.  The chunks
 *              are given a single-dimensional index which is used as the
 *              offset in an extensible array that maps a chunk coordinate to
 *              a disk address.
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
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5EAprivate.h" /* Extensible arrays                    */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MFprivate.h" /* File space management                */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5VMprivate.h" /* Vector functions                     */

/****************/
/* Local Macros */
/****************/

#define H5D_EARRAY_IDX_IS_OPEN(idx_info) (NULL != (idx_info)->storage->u.earray.ea)

/* Value to fill unset array elements with */
#define H5D_EARRAY_FILL HADDR_UNDEF
#define H5D_EARRAY_FILT_FILL                                                                                 \
    {                                                                                                        \
        HADDR_UNDEF, 0, 0                                                                                    \
    }

/******************/
/* Local Typedefs */
/******************/

/* Extensible array create/open user data */
typedef struct H5D_earray_ctx_ud_t {
    const H5F_t *f;          /* Pointer to file info */
    uint32_t     chunk_size; /* Size of chunk (bytes) */
} H5D_earray_ctx_ud_t;

/* Extensible array callback context */
typedef struct H5D_earray_ctx_t {
    size_t file_addr_len;  /* Size of addresses in the file (bytes) */
    size_t chunk_size_len; /* Size of chunk sizes in the file (bytes) */
} H5D_earray_ctx_t;

/* Extensible Array callback info for iteration over chunks */
typedef struct H5D_earray_it_ud_t {
    H5D_chunk_common_ud_t common;    /* Common info for Fixed Array user data (must be first) */
    H5D_chunk_rec_t       chunk_rec; /* Generic chunk record for callback */
    bool                  filtered;  /* Whether the chunks are filtered */
    H5D_chunk_cb_func_t   cb;        /* Chunk callback routine */
    void                 *udata;     /* User data for chunk callback routine */
} H5D_earray_it_ud_t;

/* Native extensible array element for chunks w/filters */
typedef struct H5D_earray_filt_elmt_t {
    haddr_t  addr;        /* Address of chunk */
    uint32_t nbytes;      /* Size of chunk (in file) */
    uint32_t filter_mask; /* Excluded filters for chunk */
} H5D_earray_filt_elmt_t;

/********************/
/* Local Prototypes */
/********************/
/* Extensible array iterator callbacks */
static int H5D__earray_idx_iterate_cb(hsize_t idx, const void *_elmt, void *_udata);
static int H5D__earray_idx_delete_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata);

/* Extensible array class callbacks for chunks w/o filters */
static void  *H5D__earray_crt_context(void *udata);
static herr_t H5D__earray_dst_context(void *ctx);
static herr_t H5D__earray_fill(void *nat_blk, size_t nelmts);
static herr_t H5D__earray_encode(void *raw, const void *elmt, size_t nelmts, void *ctx);
static herr_t H5D__earray_decode(const void *raw, void *elmt, size_t nelmts, void *ctx);
static herr_t H5D__earray_debug(FILE *stream, int indent, int fwidth, hsize_t idx, const void *elmt);
static void  *H5D__earray_crt_dbg_context(H5F_t *f, haddr_t obj_addr);
static herr_t H5D__earray_dst_dbg_context(void *dbg_ctx);

/* Extensible array class callbacks for chunks w/filters */
/* (some shared with callbacks for chunks w/o filters) */
static herr_t H5D__earray_filt_fill(void *nat_blk, size_t nelmts);
static herr_t H5D__earray_filt_encode(void *raw, const void *elmt, size_t nelmts, void *ctx);
static herr_t H5D__earray_filt_decode(const void *raw, void *elmt, size_t nelmts, void *ctx);
static herr_t H5D__earray_filt_debug(FILE *stream, int indent, int fwidth, hsize_t idx, const void *elmt);

/* Chunked layout indexing callbacks */
static herr_t H5D__earray_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t *space,
                                   haddr_t dset_ohdr_addr);
static herr_t H5D__earray_idx_create(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__earray_idx_open(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__earray_idx_close(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__earray_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open);
static bool   H5D__earray_idx_is_space_alloc(const H5O_storage_chunk_t *storage);
static herr_t H5D__earray_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                                     const H5D_t *dset);
static herr_t H5D__earray_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata);
static herr_t H5D__earray_idx_load_metadata(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__earray_idx_resize(H5O_layout_chunk_t *layout);
static int    H5D__earray_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb,
                                      void *chunk_udata);
static herr_t H5D__earray_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata);
static herr_t H5D__earray_idx_delete(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__earray_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src,
                                         const H5D_chk_idx_info_t *idx_info_dst);
static herr_t H5D__earray_idx_copy_shutdown(H5O_storage_chunk_t *storage_src,
                                            H5O_storage_chunk_t *storage_dst);
static herr_t H5D__earray_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *size);
static herr_t H5D__earray_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr);
static herr_t H5D__earray_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream);
static herr_t H5D__earray_idx_dest(const H5D_chk_idx_info_t *idx_info);

/* Generic extensible array routines */
static herr_t H5D__earray_idx_depend(const H5D_chk_idx_info_t *idx_info);

/*********************/
/* Package Variables */
/*********************/

/* Extensible array indexed chunk I/O ops */
const H5D_chunk_ops_t H5D_COPS_EARRAY[1] = {{
    true,                           /* Extensible array indices support SWMR access */
    H5D__earray_idx_init,           /* init */
    H5D__earray_idx_create,         /* create */
    H5D__earray_idx_open,           /* open */
    H5D__earray_idx_close,          /* close */
    H5D__earray_idx_is_open,        /* is_open */
    H5D__earray_idx_is_space_alloc, /* is_space_alloc */
    H5D__earray_idx_insert,         /* insert */
    H5D__earray_idx_get_addr,       /* get_addr */
    H5D__earray_idx_load_metadata,  /* load_metadata */
    H5D__earray_idx_resize,         /* resize */
    H5D__earray_idx_iterate,        /* iterate */
    H5D__earray_idx_remove,         /* remove */
    H5D__earray_idx_delete,         /* delete */
    H5D__earray_idx_copy_setup,     /* copy_setup */
    H5D__earray_idx_copy_shutdown,  /* copy_shutdown */
    H5D__earray_idx_size,           /* size */
    H5D__earray_idx_reset,          /* reset */
    H5D__earray_idx_dump,           /* dump */
    H5D__earray_idx_dest            /* destroy */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/* Extensible array class callbacks for dataset chunks w/o filters */
const H5EA_class_t H5EA_CLS_CHUNK[1] = {{
    H5EA_CLS_CHUNK_ID,           /* Type of extensible array */
    "Chunk w/o filters",         /* Name of extensible array class */
    sizeof(haddr_t),             /* Size of native element */
    H5D__earray_crt_context,     /* Create context */
    H5D__earray_dst_context,     /* Destroy context */
    H5D__earray_fill,            /* Fill block of missing elements callback */
    H5D__earray_encode,          /* Element encoding callback */
    H5D__earray_decode,          /* Element decoding callback */
    H5D__earray_debug,           /* Element debugging callback */
    H5D__earray_crt_dbg_context, /* Create debugging context */
    H5D__earray_dst_dbg_context  /* Destroy debugging context */
}};

/* Extensible array class callbacks for dataset chunks w/filters */
const H5EA_class_t H5EA_CLS_FILT_CHUNK[1] = {{
    H5EA_CLS_FILT_CHUNK_ID,         /* Type of extensible array */
    "Chunk w/filters",              /* Name of extensible array class */
    sizeof(H5D_earray_filt_elmt_t), /* Size of native element */
    H5D__earray_crt_context,        /* Create context */
    H5D__earray_dst_context,        /* Destroy context */
    H5D__earray_filt_fill,          /* Fill block of missing elements callback */
    H5D__earray_filt_encode,        /* Element encoding callback */
    H5D__earray_filt_decode,        /* Element decoding callback */
    H5D__earray_filt_debug,         /* Element debugging callback */
    H5D__earray_crt_dbg_context,    /* Create debugging context */
    H5D__earray_dst_dbg_context     /* Destroy debugging context */
}};

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5D_earray_ctx_t struct */
/* Declare a free list to manage the H5D_earray_ctx_ud_t struct */
H5FL_DEFINE_STATIC(H5D_earray_ctx_t);
H5FL_DEFINE_STATIC(H5D_earray_ctx_ud_t);

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_crt_context
 *
 * Purpose:     Create context for callbacks
 *
 * Return:      Success:    non-NULL
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5D__earray_crt_context(void *_udata)
{
    H5D_earray_ctx_t    *ctx;                                   /* Extensible array callback context */
    H5D_earray_ctx_ud_t *udata = (H5D_earray_ctx_ud_t *)_udata; /* User data for extensible array context */
    void                *ret_value = NULL;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(udata);
    assert(udata->f);
    assert(udata->chunk_size > 0);

    /* Allocate new context structure */
    if (NULL == (ctx = H5FL_MALLOC(H5D_earray_ctx_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL,
                    "can't allocate extensible array client callback context");

    /* Initialize the context */
    ctx->file_addr_len = H5F_SIZEOF_ADDR(udata->f);

    /* Compute the size required for encoding the size of a chunk, allowing
     *      for an extra byte, in case the filter makes the chunk larger.
     */
    ctx->chunk_size_len = 1 + ((H5VM_log2_gen((uint64_t)udata->chunk_size) + 8) / 8);
    if (ctx->chunk_size_len > 8)
        ctx->chunk_size_len = 8;

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_crt_context() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_dst_context
 *
 * Purpose:     Destroy context for callbacks
 *
 * Return:      Success:    non-NULL
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_dst_context(void *_ctx)
{
    H5D_earray_ctx_t *ctx = (H5D_earray_ctx_t *)_ctx; /* Extensible array callback context */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(ctx);

    /* Release context structure */
    ctx = H5FL_FREE(H5D_earray_ctx_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_dst_context() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_fill
 *
 * Purpose:     Fill "missing elements" in block of elements
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_fill(void *nat_blk, size_t nelmts)
{
    haddr_t fill_val = H5D_EARRAY_FILL; /* Value to fill elements with */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(nat_blk);
    assert(nelmts);

    H5VM_array_fill(nat_blk, &fill_val, H5EA_CLS_CHUNK->nat_elmt_size, nelmts);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_fill() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_encode
 *
 * Purpose:     Encode an element from "native" to "raw" form
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_encode(void *raw, const void *_elmt, size_t nelmts, void *_ctx)
{
    H5D_earray_ctx_t *ctx  = (H5D_earray_ctx_t *)_ctx; /* Extensible array callback context */
    const haddr_t    *elmt = (const haddr_t *)_elmt;   /* Convenience pointer to native elements */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(raw);
    assert(elmt);
    assert(nelmts);
    assert(ctx);

    /* Encode native elements into raw elements */
    while (nelmts) {
        /* Encode element */
        /* (advances 'raw' pointer) */
        H5F_addr_encode_len(ctx->file_addr_len, (uint8_t **)&raw, *elmt);

        /* Advance native element pointer */
        elmt++;

        /* Decrement # of elements to encode */
        nelmts--;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_decode
 *
 * Purpose:     Decode an element from "raw" to "native" form
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_decode(const void *_raw, void *_elmt, size_t nelmts, void *_ctx)
{
    H5D_earray_ctx_t *ctx  = (H5D_earray_ctx_t *)_ctx; /* Extensible array callback context */
    haddr_t          *elmt = (haddr_t *)_elmt;         /* Convenience pointer to native elements */
    const uint8_t    *raw  = (const uint8_t *)_raw;    /* Convenience pointer to raw elements */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(raw);
    assert(elmt);
    assert(nelmts);

    /* Decode raw elements into native elements */
    while (nelmts) {
        /* Decode element */
        /* (advances 'raw' pointer) */
        H5F_addr_decode_len(ctx->file_addr_len, &raw, elmt);

        /* Advance native element pointer */
        elmt++;

        /* Decrement # of elements to decode */
        nelmts--;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_debug
 *
 * Purpose:     Display an element for debugging
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_debug(FILE *stream, int indent, int fwidth, hsize_t idx, const void *elmt)
{
    char temp_str[128]; /* Temporary string, for formatting */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(stream);
    assert(elmt);

    /* Print element */
    snprintf(temp_str, sizeof(temp_str), "Element #%" PRIuHSIZE ":", idx);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, temp_str, *(const haddr_t *)elmt);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_filt_fill
 *
 * Purpose:     Fill "missing elements" in block of elements
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_filt_fill(void *nat_blk, size_t nelmts)
{
    H5D_earray_filt_elmt_t fill_val = H5D_EARRAY_FILT_FILL; /* Value to fill elements with */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(nat_blk);
    assert(nelmts);
    assert(sizeof(fill_val) == H5EA_CLS_FILT_CHUNK->nat_elmt_size);

    H5VM_array_fill(nat_blk, &fill_val, H5EA_CLS_FILT_CHUNK->nat_elmt_size, nelmts);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_filt_fill() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_filt_encode
 *
 * Purpose:     Encode an element from "native" to "raw" form
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_filt_encode(void *_raw, const void *_elmt, size_t nelmts, void *_ctx)
{
    H5D_earray_ctx_t             *ctx = (H5D_earray_ctx_t *)_ctx; /* Extensible array callback context */
    uint8_t                      *raw = (uint8_t *)_raw;          /* Convenience pointer to raw elements */
    const H5D_earray_filt_elmt_t *elmt =
        (const H5D_earray_filt_elmt_t *)_elmt; /* Convenience pointer to native elements */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(raw);
    assert(elmt);
    assert(nelmts);
    assert(ctx);

    /* Encode native elements into raw elements */
    while (nelmts) {
        /* Encode element */
        /* (advances 'raw' pointer) */
        H5F_addr_encode_len(ctx->file_addr_len, &raw, elmt->addr);
        UINT64ENCODE_VAR(raw, elmt->nbytes, ctx->chunk_size_len);
        UINT32ENCODE(raw, elmt->filter_mask);

        /* Advance native element pointer */
        elmt++;

        /* Decrement # of elements to encode */
        nelmts--;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_filt_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_filt_decode
 *
 * Purpose:     Decode an element from "raw" to "native" form
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_filt_decode(const void *_raw, void *_elmt, size_t nelmts, void *_ctx)
{
    H5D_earray_ctx_t       *ctx = (H5D_earray_ctx_t *)_ctx; /* Extensible array callback context */
    H5D_earray_filt_elmt_t *elmt =
        (H5D_earray_filt_elmt_t *)_elmt;        /* Convenience pointer to native elements */
    const uint8_t *raw = (const uint8_t *)_raw; /* Convenience pointer to raw elements */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(raw);
    assert(elmt);
    assert(nelmts);

    /* Decode raw elements into native elements */
    while (nelmts) {
        /* Decode element */
        /* (advances 'raw' pointer) */
        H5F_addr_decode_len(ctx->file_addr_len, &raw, &elmt->addr);
        UINT64DECODE_VAR(raw, elmt->nbytes, ctx->chunk_size_len);
        UINT32DECODE(raw, elmt->filter_mask);

        /* Advance native element pointer */
        elmt++;

        /* Decrement # of elements to decode */
        nelmts--;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_filt_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_filt_debug
 *
 * Purpose:     Display an element for debugging
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_filt_debug(FILE *stream, int indent, int fwidth, hsize_t idx, const void *_elmt)
{
    const H5D_earray_filt_elmt_t *elmt =
        (const H5D_earray_filt_elmt_t *)_elmt; /* Convenience pointer to native elements */
    char temp_str[128];                        /* Temporary string, for formatting */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(stream);
    assert(elmt);

    /* Print element */
    snprintf(temp_str, sizeof(temp_str), "Element #%" PRIuHSIZE ":", idx);
    fprintf(stream, "%*s%-*s {%" PRIuHADDR ", %u, %0x}\n", indent, "", fwidth, temp_str, elmt->addr,
            elmt->nbytes, elmt->filter_mask);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_filt_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_crt_dbg_context
 *
 * Purpose:     Create context for debugging callback
 *              (get the layout message in the specified object header)
 *
 * Return:      Success:    non-NULL
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5D__earray_crt_dbg_context(H5F_t *f, haddr_t obj_addr)
{
    H5D_earray_ctx_ud_t *dbg_ctx = NULL;     /* Context for fixed array callback */
    H5O_loc_t            obj_loc;            /* Pointer to an object's location */
    bool                 obj_opened = false; /* Flag to indicate that the object header was opened */
    H5O_layout_t         layout;             /* Layout message */
    void                *ret_value = NULL;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(H5_addr_defined(obj_addr));

    /* Allocate context for debugging callback */
    if (NULL == (dbg_ctx = H5FL_MALLOC(H5D_earray_ctx_ud_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, NULL,
                    "can't allocate extensible array client callback context");

    /* Set up the object header location info */
    H5O_loc_reset(&obj_loc);
    obj_loc.file = f;
    obj_loc.addr = obj_addr;

    /* Open the object header where the layout message resides */
    if (H5O_open(&obj_loc) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, NULL, "can't open object header");
    obj_opened = true;

    /* Read the layout message */
    if (NULL == H5O_msg_read(&obj_loc, H5O_LAYOUT_ID, &layout))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, NULL, "can't get layout info");

    /* close the object header */
    if (H5O_close(&obj_loc, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, NULL, "can't close object header");

    /* Create user data */
    dbg_ctx->f          = f;
    dbg_ctx->chunk_size = layout.u.chunk.size;

    /* Set return value */
    ret_value = dbg_ctx;

done:
    /* Cleanup on error */
    if (ret_value == NULL) {
        /* Release context structure */
        if (dbg_ctx)
            dbg_ctx = H5FL_FREE(H5D_earray_ctx_ud_t, dbg_ctx);

        /* Close object header */
        if (obj_opened)
            if (H5O_close(&obj_loc, NULL) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, NULL, "can't close object header");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_crt_dbg_context() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_dst_dbg_context
 *
 * Purpose:     Destroy context for debugging callback
 *              (free the layout message from the specified object header)
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_dst_dbg_context(void *_dbg_ctx)
{
    H5D_earray_ctx_ud_t *dbg_ctx =
        (H5D_earray_ctx_ud_t *)_dbg_ctx; /* Context for extensible array callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(dbg_ctx);

    /* Release context structure */
    dbg_ctx = H5FL_FREE(H5D_earray_ctx_ud_t, dbg_ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_dst_dbg_context() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_depend
 *
 * Purpose:     Create flush dependency between extensible array and dataset's
 *              object header.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_depend(const H5D_chk_idx_info_t *idx_info)
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
    assert(H5D_CHUNK_IDX_EARRAY == idx_info->layout->idx_type);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_EARRAY == idx_info->storage->idx_type);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(idx_info->storage->u.earray.ea);

    /* Set up object header location for dataset */
    H5O_loc_reset(&oloc);
    oloc.file = idx_info->f;
    oloc.addr = idx_info->storage->u.earray.dset_ohdr_addr;

    /* Get header */
    if (NULL == (oh = H5O_protect(&oloc, H5AC__READ_ONLY_FLAG, true)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTPROTECT, FAIL, "unable to protect object header");

    /* Retrieve the dataset's object header proxy */
    if (NULL == (oh_proxy = H5O_get_proxy(oh)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get dataset object header proxy");

    /* Make the extensible array a child flush dependency of the dataset's object header */
    if (H5EA_depend(idx_info->storage->u.earray.ea, oh_proxy) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEPEND, FAIL,
                    "unable to create flush dependency on object header proxy");

done:
    /* Release the object header from the cache */
    if (oh && H5O_unprotect(&oloc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_depend() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_init
 *
 * Purpose:     Initialize the indexing information for a dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t *space, haddr_t dset_ohdr_addr)
{
    hsize_t  max_dims[H5O_LAYOUT_NDIMS]; /* Max. size of dataset dimensions */
    int      unlim_dim;                  /* Rank of the dataset's unlimited dimension */
    int      sndims;                     /* Rank of dataspace */
    unsigned ndims;                      /* Rank of dataspace */
    unsigned u;                          /* Local index variable */
    herr_t   ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(space);
    assert(H5_addr_defined(dset_ohdr_addr));

    /* Get the dim info for dataset */
    if ((sndims = H5S_get_simple_extent_dims(space, NULL, max_dims)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataspace dimensions");
    H5_CHECKED_ASSIGN(ndims, unsigned, sndims, int);

    /* Find the rank of the unlimited dimension */
    unlim_dim = (-1);
    for (u = 0; u < ndims; u++) {
        /* Check for unlimited dimension */
        if (H5S_UNLIMITED == max_dims[u]) {
            /* Check if we've already found an unlimited dimension */
            if (unlim_dim >= 0)
                HGOTO_ERROR(H5E_DATASET, H5E_ALREADYINIT, FAIL, "already found unlimited dimension");

            /* Set the unlimited dimension */
            unlim_dim = (int)u;
        } /* end if */
    }     /* end for */

    /* Check if we didn't find an unlimited dimension */
    if (unlim_dim < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_UNINITIALIZED, FAIL, "didn't find unlimited dimension");

    /* Set the unlimited dimension for the layout's future use */
    idx_info->layout->u.earray.unlim_dim = (unsigned)unlim_dim;

    /* Store the dataset's object header address for later */
    idx_info->storage->u.earray.dset_ohdr_addr = dset_ohdr_addr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_create
 *
 * Purpose:     Creates a new indexed-storage extensible array and initializes
 *              the layout struct with information about the storage.  The
 *              struct should be immediately written to the object header.
 *
 *              This function must be called before passing LAYOUT to any of
 *              the other indexed storage functions!
 *
 * Return:      Non-negative on success (with the LAYOUT argument initialized
 *              and ready to write to an object header). Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_create(const H5D_chk_idx_info_t *idx_info)
{
    H5EA_create_t       cparam;              /* Extensible array creation parameters */
    H5D_earray_ctx_ud_t udata;               /* User data for extensible array create call */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(!H5_addr_defined(idx_info->storage->idx_addr));
    assert(NULL == idx_info->storage->u.earray.ea);

    /* General parameters */
    if (idx_info->pline->nused > 0) {
        unsigned chunk_size_len; /* Size of encoded chunk size */

        /* Compute the size required for encoding the size of a chunk, allowing
         *      for an extra byte, in case the filter makes the chunk larger.
         */
        chunk_size_len = 1 + ((H5VM_log2_gen((uint64_t)idx_info->layout->size) + 8) / 8);
        if (chunk_size_len > 8)
            chunk_size_len = 8;

        cparam.cls           = H5EA_CLS_FILT_CHUNK;
        cparam.raw_elmt_size = (uint8_t)(H5F_SIZEOF_ADDR(idx_info->f) + chunk_size_len + 4);
    } /* end if */
    else {
        cparam.cls           = H5EA_CLS_CHUNK;
        cparam.raw_elmt_size = (uint8_t)H5F_SIZEOF_ADDR(idx_info->f);
    } /* end else */
    cparam.max_nelmts_bits = idx_info->layout->u.earray.cparam.max_nelmts_bits;
    assert(cparam.max_nelmts_bits > 0);
    cparam.idx_blk_elmts = idx_info->layout->u.earray.cparam.idx_blk_elmts;
    assert(cparam.idx_blk_elmts > 0);
    cparam.sup_blk_min_data_ptrs = idx_info->layout->u.earray.cparam.sup_blk_min_data_ptrs;
    assert(cparam.sup_blk_min_data_ptrs > 0);
    cparam.data_blk_min_elmts = idx_info->layout->u.earray.cparam.data_blk_min_elmts;
    assert(cparam.data_blk_min_elmts > 0);
    cparam.max_dblk_page_nelmts_bits = idx_info->layout->u.earray.cparam.max_dblk_page_nelmts_bits;
    assert(cparam.max_dblk_page_nelmts_bits > 0);

    /* Set up the user data */
    udata.f          = idx_info->f;
    udata.chunk_size = idx_info->layout->size;

    /* Create the extensible array for the chunk index */
    if (NULL == (idx_info->storage->u.earray.ea = H5EA_create(idx_info->f, &cparam, &udata)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create extensible array");

    /* Get the address of the extensible array in file */
    if (H5EA_get_addr(idx_info->storage->u.earray.ea, &(idx_info->storage->idx_addr)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't query extensible array address");

    /* Check for SWMR writes to the file */
    if (H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)
        if (H5D__earray_idx_depend(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDEPEND, FAIL,
                        "unable to create flush dependency on object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_open
 *
 * Purpose:     Opens an existing extensible array.
 *
 * Note:        This information is passively initialized from each index
 *              operation callback because those abstract chunk index
 *              operations are designed to work with the v1 B-tree chunk
 *              indices also, which don't require an 'open' for the data
 *              structure.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_open(const H5D_chk_idx_info_t *idx_info)
{
    H5D_earray_ctx_ud_t udata;               /* User data for extensible array open call */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(H5D_CHUNK_IDX_EARRAY == idx_info->layout->idx_type);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_EARRAY == idx_info->storage->idx_type);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(NULL == idx_info->storage->u.earray.ea);

    /* Set up the user data */
    udata.f          = idx_info->f;
    udata.chunk_size = idx_info->layout->size;

    /* Open the extensible array for the chunk index */
    if (NULL ==
        (idx_info->storage->u.earray.ea = H5EA_open(idx_info->f, idx_info->storage->idx_addr, &udata)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't open extensible array");

    /* Check for SWMR writes to the file */
    if (H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)
        if (H5D__earray_idx_depend(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDEPEND, FAIL,
                        "unable to create flush dependency on object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_close
 *
 * Purpose:     Closes an existing extensible array.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_close(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(idx_info);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_EARRAY == idx_info->storage->idx_type);
    assert(idx_info->storage->u.earray.ea);

    if (H5EA_close(idx_info->storage->u.earray.ea) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close extensible array");
    idx_info->storage->u.earray.ea = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_close() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_is_open
 *
 * Purpose:     Query if the index is opened or not
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(idx_info);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_EARRAY == idx_info->storage->idx_type);
    assert(is_open);

    *is_open = H5D_EARRAY_IDX_IS_OPEN(idx_info);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_idx_is_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_is_space_alloc
 *
 * Purpose:     Query if space is allocated for index method
 *
 * Return:      true/false
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__earray_idx_is_space_alloc(const H5O_storage_chunk_t *storage)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);

    FUNC_LEAVE_NOAPI((bool)H5_addr_defined(storage->idx_addr))
} /* end H5D__earray_idx_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_insert
 *
 * Purpose:     Insert chunk address into the indexing structure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                       const H5D_t H5_ATTR_UNUSED *dset)
{
    H5EA_t *ea;                  /* Pointer to extensible array structure */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Check if the extensible array is open yet */
    if (!H5D_EARRAY_IDX_IS_OPEN(idx_info)) {
        /* Open the extensible array in file */
        if (H5D__earray_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open extensible array");
    }
    else /* Patch the top level file pointer contained in ea if needed */
        H5EA_patch_file(idx_info->storage->u.earray.ea, idx_info->f);

    /* Set convenience pointer to extensible array structure */
    ea = idx_info->storage->u.earray.ea;

    if (!H5_addr_defined(udata->chunk_block.offset))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "The chunk should have allocated already");
    if (udata->chunk_idx != (udata->chunk_idx & 0xffffffff)) /* negative value */
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "chunk index must be less than 2^32");

    /* Check for filters on chunks */
    if (idx_info->pline->nused > 0) {
        H5D_earray_filt_elmt_t elmt; /* Extensible array element */

        elmt.addr = udata->chunk_block.offset;
        H5_CHECKED_ASSIGN(elmt.nbytes, uint32_t, udata->chunk_block.length, hsize_t);
        elmt.filter_mask = udata->filter_mask;

        /* Set the info for the chunk */
        if (H5EA_set(ea, udata->chunk_idx, &elmt) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set chunk info");
    } /* end if */
    else {
        /* Set the address for the chunk */
        if (H5EA_set(ea, udata->chunk_idx, &udata->chunk_block.offset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set chunk address");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__earray_idx_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_get_addr
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
H5D__earray_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata)
{
    H5EA_t *ea;                  /* Pointer to extensible array structure */
    hsize_t idx;                 /* Array index of chunk */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Check if the extensible array is open yet */
    if (!H5D_EARRAY_IDX_IS_OPEN(idx_info)) {
        /* Open the extensible array in file */
        if (H5D__earray_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open extensible array");
    }
    else /* Patch the top level file pointer contained in ea if needed */
        H5EA_patch_file(idx_info->storage->u.earray.ea, idx_info->f);

    /* Set convenience pointer to extensible array structure */
    ea = idx_info->storage->u.earray.ea;

    /* Check for unlimited dim. not being the slowest-changing dim. */
    if (idx_info->layout->u.earray.unlim_dim > 0) {
        hsize_t  swizzled_coords[H5O_LAYOUT_NDIMS];     /* swizzled chunk coordinates */
        unsigned ndims = (idx_info->layout->ndims - 1); /* Number of dimensions */
        unsigned u;

        /* Compute coordinate offset from scaled offset */
        for (u = 0; u < ndims; u++)
            swizzled_coords[u] = udata->common.scaled[u] * idx_info->layout->dim[u];

        H5VM_swizzle_coords(hsize_t, swizzled_coords, idx_info->layout->u.earray.unlim_dim);

        /* Calculate the index of this chunk */
        idx = H5VM_chunk_index(ndims, swizzled_coords, idx_info->layout->u.earray.swizzled_dim,
                               idx_info->layout->u.earray.swizzled_max_down_chunks);
    } /* end if */
    else {
        /* Calculate the index of this chunk */
        idx = H5VM_array_offset_pre((idx_info->layout->ndims - 1), idx_info->layout->max_down_chunks,
                                    udata->common.scaled);
    } /* end else */

    udata->chunk_idx = idx;

    /* Check for filters on chunks */
    if (idx_info->pline->nused > 0) {
        H5D_earray_filt_elmt_t elmt; /* Extensible array element */

        /* Get the information for the chunk */
        if (H5EA_get(ea, idx, &elmt) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk info");

        /* Set the info for the chunk */
        udata->chunk_block.offset = elmt.addr;
        udata->chunk_block.length = elmt.nbytes;
        udata->filter_mask        = elmt.filter_mask;
    } /* end if */
    else {
        /* Get the address for the chunk */
        if (H5EA_get(ea, idx, &udata->chunk_block.offset) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk address");

        /* Update the other (constant) information for the chunk */
        udata->chunk_block.length = idx_info->layout->size;
        udata->filter_mask        = 0;
    } /* end else */

    if (!H5_addr_defined(udata->chunk_block.offset))
        udata->chunk_block.length = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__earray_idx_get_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_load_metadata
 *
 * Purpose:     Load additional chunk index metadata beyond the chunk index
 *              itself.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_load_metadata(const H5D_chk_idx_info_t *idx_info)
{
    H5D_chunk_ud_t chunk_ud;
    hsize_t        scaled[H5O_LAYOUT_NDIMS] = {0};
    herr_t         ret_value                = SUCCEED;

    FUNC_ENTER_PACKAGE

    /*
     * After opening a dataset that uses an extensible array,
     * the extensible array header index block will generally
     * not be read in until an element is looked up for the
     * first time. Since there isn't currently a good way of
     * controlling that explicitly, perform a fake lookup of
     * a chunk to cause it to be read in or created if it
     * doesn't exist yet.
     */
    chunk_ud.common.layout  = idx_info->layout;
    chunk_ud.common.storage = idx_info->storage;
    chunk_ud.common.scaled  = scaled;

    chunk_ud.chunk_block.offset = HADDR_UNDEF;
    chunk_ud.chunk_block.length = 0;
    chunk_ud.filter_mask        = 0;
    chunk_ud.new_unfilt_chunk   = false;
    chunk_ud.idx_hint           = UINT_MAX;

    if (H5D__earray_idx_get_addr(idx_info, &chunk_ud) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't load extensible array header index block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__earray_idx_load_metadata() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_resize
 *
 * Purpose:     Calculate/setup the swizzled down chunk array, used for chunk
 *              index calculations.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_resize(H5O_layout_chunk_t *layout)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(layout);

    /* "Swizzle" constant dimensions for this dataset */
    if (layout->u.earray.unlim_dim > 0) {
        hsize_t swizzled_chunks[H5O_LAYOUT_NDIMS]; /* Swizzled form of # of chunks in each dimension */
        hsize_t
            swizzled_max_chunks[H5O_LAYOUT_NDIMS]; /* Swizzled form of max # of chunks in each dimension */

        /* Get the swizzled chunk dimensions */
        H5MM_memcpy(layout->u.earray.swizzled_dim, layout->dim, (layout->ndims - 1) * sizeof(layout->dim[0]));
        H5VM_swizzle_coords(uint32_t, layout->u.earray.swizzled_dim, layout->u.earray.unlim_dim);

        /* Get the swizzled number of chunks in each dimension */
        H5MM_memcpy(swizzled_chunks, layout->chunks, (layout->ndims - 1) * sizeof(swizzled_chunks[0]));
        H5VM_swizzle_coords(hsize_t, swizzled_chunks, layout->u.earray.unlim_dim);

        /* Get the swizzled "down" sizes for each dimension */
        H5VM_array_down((layout->ndims - 1), swizzled_chunks, layout->u.earray.swizzled_down_chunks);

        /* Get the swizzled max number of chunks in each dimension */
        H5MM_memcpy(swizzled_max_chunks, layout->max_chunks,
                    (layout->ndims - 1) * sizeof(swizzled_max_chunks[0]));
        H5VM_swizzle_coords(hsize_t, swizzled_max_chunks, layout->u.earray.unlim_dim);

        /* Get the swizzled max "down" sizes for each dimension */
        H5VM_array_down((layout->ndims - 1), swizzled_max_chunks, layout->u.earray.swizzled_max_down_chunks);
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_idx_resize() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_iterate_cb
 *
 * Purpose:     Callback routine for extensible array element iteration.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__earray_idx_iterate_cb(hsize_t H5_ATTR_UNUSED idx, const void *_elmt, void *_udata)
{
    H5D_earray_it_ud_t *udata = (H5D_earray_it_ud_t *)_udata; /* User data */
    unsigned            ndims;                                /* Rank of chunk */
    int                 curr_dim;                             /* Current dimension */
    int                 ret_value = H5_ITER_CONT;             /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Compose generic chunk record for callback */
    if (udata->filtered) {
        const H5D_earray_filt_elmt_t *filt_elmt = (const H5D_earray_filt_elmt_t *)_elmt;

        udata->chunk_rec.chunk_addr  = filt_elmt->addr;
        udata->chunk_rec.nbytes      = filt_elmt->nbytes;
        udata->chunk_rec.filter_mask = filt_elmt->filter_mask;
    } /* end if */
    else
        udata->chunk_rec.chunk_addr = *(const haddr_t *)_elmt;

    /* Make "generic chunk" callback */
    if (H5_addr_defined(udata->chunk_rec.chunk_addr))
        if ((ret_value = (udata->cb)(&udata->chunk_rec, udata->udata)) < 0)
            HERROR(H5E_DATASET, H5E_CALLBACK, "failure in generic chunk iterator callback");

    /* Update coordinates of chunk in dataset */
    ndims = udata->common.layout->ndims - 1;
    assert(ndims > 0);
    curr_dim = (int)(ndims - 1);
    while (curr_dim >= 0) {
        /* Increment coordinate in current dimension */
        udata->chunk_rec.scaled[curr_dim]++;

        /* Check if we went off the end of the current dimension */
        if (udata->chunk_rec.scaled[curr_dim] >= udata->common.layout->max_chunks[curr_dim]) {
            /* Reset coordinate & move to next faster dimension */
            udata->chunk_rec.scaled[curr_dim] = 0;
            curr_dim--;
        } /* end if */
        else
            break;
    } /* end while */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__earray_idx_iterate_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_iterate
 *
 * Purpose:     Iterate over the chunks in an index, making a callback
 *              for each one.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__earray_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb, void *chunk_udata)
{
    H5EA_t     *ea;                       /* Pointer to extensible array structure */
    H5EA_stat_t ea_stat;                  /* Extensible array statistics */
    int         ret_value = H5_ITER_CONT; /* Return value */

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

    /* Check if the extensible array is open yet */
    if (!H5D_EARRAY_IDX_IS_OPEN(idx_info)) {
        /* Open the extensible array in file */
        if (H5D__earray_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, H5_ITER_ERROR, "can't open extensible array");
    }
    else /* Patch the top level file pointer contained in ea if needed */
        H5EA_patch_file(idx_info->storage->u.earray.ea, idx_info->f);

    /* Set convenience pointer to extensible array structure */
    ea = idx_info->storage->u.earray.ea;

    /* Get the extensible array statistics */
    if (H5EA_get_stats(ea, &ea_stat) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5_ITER_ERROR, "can't query extensible array statistics");

    if (ea_stat.stored.max_idx_set > 0) {
        H5D_earray_it_ud_t udata; /* User data for iteration callback */

        /* Initialize userdata */
        memset(&udata, 0, sizeof udata);
        udata.common.layout  = idx_info->layout;
        udata.common.storage = idx_info->storage;
        memset(&udata.chunk_rec, 0, sizeof(udata.chunk_rec));
        udata.filtered = (idx_info->pline->nused > 0);
        if (!udata.filtered) {
            udata.chunk_rec.nbytes      = idx_info->layout->size;
            udata.chunk_rec.filter_mask = 0;
        } /* end if */
        udata.cb    = chunk_cb;
        udata.udata = chunk_udata;

        /* Iterate over the extensible array elements */
        if ((ret_value = H5EA_iterate(ea, H5D__earray_idx_iterate_cb, &udata)) < 0)
            HERROR(H5E_DATASET, H5E_BADITER, "unable to iterate over fixed array chunk index");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_remove
 *
 * Purpose:     Remove chunk from index.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata)
{
    H5EA_t *ea;                  /* Pointer to extensible array structure */
    hsize_t idx;                 /* Array index of chunk */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Check if the extensible array is open yet */
    if (!H5D_EARRAY_IDX_IS_OPEN(idx_info)) {
        /* Open the extensible array in file */
        if (H5D__earray_idx_open(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open extensible array");
    }
    else /* Patch the top level file pointer contained in ea if needed */
        if (H5EA_patch_file(idx_info->storage->u.earray.ea, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch earray file pointer");

    /* Set convenience pointer to extensible array structure */
    ea = idx_info->storage->u.earray.ea;

    /* Check for unlimited dim. not being the slowest-changing dim. */
    if (idx_info->layout->u.earray.unlim_dim > 0) {
        hsize_t  swizzled_coords[H5O_LAYOUT_NDIMS];     /* swizzled chunk coordinates */
        unsigned ndims = (idx_info->layout->ndims - 1); /* Number of dimensions */
        unsigned u;

        /* Compute coordinate offset from scaled offset */
        for (u = 0; u < ndims; u++)
            swizzled_coords[u] = udata->scaled[u] * idx_info->layout->dim[u];

        H5VM_swizzle_coords(hsize_t, swizzled_coords, idx_info->layout->u.earray.unlim_dim);

        /* Calculate the index of this chunk */
        idx = H5VM_chunk_index(ndims, swizzled_coords, idx_info->layout->u.earray.swizzled_dim,
                               idx_info->layout->u.earray.swizzled_max_down_chunks);
    } /* end if */
    else {
        /* Calculate the index of this chunk */
        idx = H5VM_array_offset_pre((idx_info->layout->ndims - 1), idx_info->layout->max_down_chunks,
                                    udata->scaled);
    } /* end else */

    /* Check for filters on chunks */
    if (idx_info->pline->nused > 0) {
        H5D_earray_filt_elmt_t elmt; /* Extensible array element */

        /* Get the info about the chunk for the index */
        if (H5EA_get(ea, idx, &elmt) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk info");

        /* Remove raw data chunk from file if not doing SWMR writes */
        assert(H5_addr_defined(elmt.addr));
        if (!(H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)) {
            H5_CHECK_OVERFLOW(elmt.nbytes, /*From: */ uint32_t, /*To: */ hsize_t);
            if (H5MF_xfree(idx_info->f, H5FD_MEM_DRAW, elmt.addr, (hsize_t)elmt.nbytes) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to free chunk");
        } /* end if */

        /* Reset the info about the chunk for the index */
        elmt.addr        = HADDR_UNDEF;
        elmt.nbytes      = 0;
        elmt.filter_mask = 0;
        if (H5EA_set(ea, idx, &elmt) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to reset chunk info");
    } /* end if */
    else {
        haddr_t addr = HADDR_UNDEF; /* Chunk address */

        /* Get the address of the chunk for the index */
        if (H5EA_get(ea, idx, &addr) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk address");

        /* Remove raw data chunk from file if not doing SWMR writes */
        assert(H5_addr_defined(addr));
        if (!(H5F_INTENT(idx_info->f) & H5F_ACC_SWMR_WRITE)) {
            H5_CHECK_OVERFLOW(idx_info->layout->size, /*From: */ uint32_t, /*To: */ hsize_t);
            if (H5MF_xfree(idx_info->f, H5FD_MEM_DRAW, addr, (hsize_t)idx_info->layout->size) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to free chunk");
        } /* end if */

        /* Reset the address of the chunk for the index */
        addr = HADDR_UNDEF;
        if (H5EA_set(ea, idx, &addr) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to reset chunk address");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__earray_idx_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_delete_cb
 *
 * Purpose:     Delete space for chunk in file
 *
 * Return:      Success:    Non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__earray_idx_delete_cb(const H5D_chunk_rec_t *chunk_rec, void *_udata)
{
    H5F_t *f         = (H5F_t *)_udata; /* User data for callback */
    int    ret_value = H5_ITER_CONT;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(chunk_rec);
    assert(H5_addr_defined(chunk_rec->chunk_addr));
    assert(chunk_rec->nbytes > 0);
    assert(f);

    /* Remove raw data chunk from file */
    H5_CHECK_OVERFLOW(chunk_rec->nbytes, /*From: */ uint32_t, /*To: */ hsize_t);
    if (H5MF_xfree(f, H5FD_MEM_DRAW, chunk_rec->chunk_addr, (hsize_t)chunk_rec->nbytes) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, H5_ITER_ERROR, "unable to free chunk");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_delete_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_delete
 *
 * Purpose:     Delete index and raw data storage for entire dataset
 *              (i.e. all chunks)
 *
 * Note:        This implementation is slow, particularly for sparse
 *              extensible arrays, replace it with call to H5EA_iterate()
 *              when that's available.
 *
 * Return:      Success:    Non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_delete(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);

    /* Check if the index data structure has been allocated */
    if (H5_addr_defined(idx_info->storage->idx_addr)) {
        H5D_earray_ctx_ud_t ctx_udata; /* User data for extensible array open call */

        /* Iterate over the chunk addresses in the extensible array, deleting each chunk */
        if (H5D__earray_idx_iterate(idx_info, H5D__earray_idx_delete_cb, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "unable to iterate over chunk addresses");

        /* Close extensible array */
        if (H5D__earray_idx_close(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close extensible array");

        /* Set up the context user data */
        ctx_udata.f          = idx_info->f;
        ctx_udata.chunk_size = idx_info->layout->size;

        /* Delete extensible array */
        if (H5EA_delete(idx_info->f, idx_info->storage->idx_addr, &ctx_udata) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDELETE, FAIL, "unable to delete chunk extensible array");
        idx_info->storage->idx_addr = HADDR_UNDEF;
    } /* end if */
    else
        assert(NULL == idx_info->storage->u.earray.ea);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_copy_setup
 *
 * Purpose:     Set up any necessary information for copying chunks
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src, const H5D_chk_idx_info_t *idx_info_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info_src);
    assert(idx_info_src->f);
    assert(idx_info_src->pline);
    assert(idx_info_src->layout);
    assert(idx_info_src->storage);
    assert(idx_info_dst);
    assert(idx_info_dst->f);
    assert(idx_info_dst->pline);
    assert(idx_info_dst->layout);
    assert(idx_info_dst->storage);
    assert(!H5_addr_defined(idx_info_dst->storage->idx_addr));

    /* Check if the source extensible array is open yet */
    if (!H5D_EARRAY_IDX_IS_OPEN(idx_info_src))
        /* Open the extensible array in file */
        if (H5D__earray_idx_open(idx_info_src) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open extensible array");

    /* Set copied metadata tag */
    H5_BEGIN_TAG(H5AC__COPIED_TAG)

    /* Create the extensible array that describes chunked storage in the dest. file */
    if (H5D__earray_idx_create(idx_info_dst) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize chunked storage");
    assert(H5_addr_defined(idx_info_dst->storage->idx_addr));

    /* Reset metadata tag */
    H5_END_TAG

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_copy_setup() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_copy_shutdown
 *
 * Purpose:     Shutdown any information from copying chunks
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_copy_shutdown(H5O_storage_chunk_t *storage_src, H5O_storage_chunk_t *storage_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(storage_src);
    assert(storage_src->u.earray.ea);
    assert(storage_dst);
    assert(storage_dst->u.earray.ea);

    /* Close extensible arrays */
    if (H5EA_close(storage_src->u.earray.ea) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close extensible array");
    storage_src->u.earray.ea = NULL;
    if (H5EA_close(storage_dst->u.earray.ea) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close extensible array");
    storage_dst->u.earray.ea = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_copy_shutdown() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_size
 *
 * Purpose:     Retrieve the amount of index storage for chunked dataset
 *
 * Return:      Success:        Non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *index_size)
{
    H5EA_t     *ea;                  /* Pointer to extensible array structure */
    H5EA_stat_t ea_stat;             /* Extensible array statistics */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(index_size);

    /* Open the extensible array in file */
    if (H5D__earray_idx_open(idx_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't open extensible array");

    /* Set convenience pointer to extensible array structure */
    ea = idx_info->storage->u.earray.ea;

    /* Get the extensible array statistics */
    if (H5EA_get_stats(ea, &ea_stat) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't query extensible array statistics");

    /* Set the size of the extensible array */
    *index_size = ea_stat.computed.hdr_size + ea_stat.computed.index_blk_size +
                  ea_stat.stored.super_blk_size + ea_stat.stored.data_blk_size;

done:
    if (idx_info->storage->u.earray.ea) {
        if (H5D__earray_idx_close(idx_info) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close extensible array");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_size() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_reset
 *
 * Purpose:     Reset indexing information.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);

    /* Reset index info */
    if (reset_addr) {
        storage->idx_addr                = HADDR_UNDEF;
        storage->u.earray.dset_ohdr_addr = HADDR_UNDEF;
    } /* end if */
    storage->u.earray.ea = NULL;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_idx_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_dump
 *
 * Purpose:     Dump indexing information to a stream.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);
    assert(stream);

    fprintf(stream, "    Address: %" PRIuHADDR "\n", storage->idx_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__earray_idx_dump() */

/*-------------------------------------------------------------------------
 * Function:    H5D__earray_idx_dest
 *
 * Purpose:     Release indexing information in memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__earray_idx_dest(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->storage);

    /* Check if the extensible array is open */
    if (H5D_EARRAY_IDX_IS_OPEN(idx_info)) {
        /* Patch the top level file pointer contained in ea if needed */
        if (H5EA_patch_file(idx_info->storage->u.earray.ea, idx_info->f) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch earray file pointer");

        /* Close extensible array */
        if (H5D__earray_idx_close(idx_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to close extensible array");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__earray_idx_dest() */

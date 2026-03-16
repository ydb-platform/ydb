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
 * Created:		H5HFbtree2.c
 *
 * Purpose:		v2 B-tree callbacks for "huge" object tracker
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5B2private.h" /* B-Trees (Version 2)                      */
#include "H5Eprivate.h"  /* Error Handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5HFpkg.h"     /* Fractal Heaps                            */
#include "H5MFprivate.h" /* File Memory Management                   */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* v2 B-tree client callback context */
typedef struct H5HF_huge_bt2_ctx_t {
    uint8_t sizeof_size; /* Size of file sizes */
    uint8_t sizeof_addr; /* Size of file addresses */
} H5HF_huge_bt2_ctx_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* v2 B-tree driver callbacks */

/* Common callbacks */
static void  *H5HF__huge_bt2_crt_context(void *udata);
static herr_t H5HF__huge_bt2_dst_context(void *ctx);

/* Callbacks for indirect objects */
static herr_t H5HF__huge_bt2_indir_store(void *native, const void *udata);
static herr_t H5HF__huge_bt2_indir_compare(const void *rec1, const void *rec2, int *result);
static herr_t H5HF__huge_bt2_indir_encode(uint8_t *raw, const void *native, void *ctx);
static herr_t H5HF__huge_bt2_indir_decode(const uint8_t *raw, void *native, void *ctx);
static herr_t H5HF__huge_bt2_indir_debug(FILE *stream, int indent, int fwidth, const void *record,
                                         const void *_udata);

/* Callbacks for filtered indirect objects */
static herr_t H5HF__huge_bt2_filt_indir_store(void *native, const void *udata);
static herr_t H5HF__huge_bt2_filt_indir_compare(const void *rec1, const void *rec2, int *result);
static herr_t H5HF__huge_bt2_filt_indir_encode(uint8_t *raw, const void *native, void *ctx);
static herr_t H5HF__huge_bt2_filt_indir_decode(const uint8_t *raw, void *native, void *ctx);
static herr_t H5HF__huge_bt2_filt_indir_debug(FILE *stream, int indent, int fwidth, const void *record,
                                              const void *_udata);

/* Callbacks for direct objects */
static herr_t H5HF__huge_bt2_dir_store(void *native, const void *udata);
static herr_t H5HF__huge_bt2_dir_compare(const void *rec1, const void *rec2, int *result);
static herr_t H5HF__huge_bt2_dir_encode(uint8_t *raw, const void *native, void *ctx);
static herr_t H5HF__huge_bt2_dir_decode(const uint8_t *raw, void *native, void *ctx);
static herr_t H5HF__huge_bt2_dir_debug(FILE *stream, int indent, int fwidth, const void *record,
                                       const void *_udata);

/* Callbacks for filtered direct objects */
static herr_t H5HF__huge_bt2_filt_dir_store(void *native, const void *udata);
static herr_t H5HF__huge_bt2_filt_dir_compare(const void *rec1, const void *rec2, int *result);
static herr_t H5HF__huge_bt2_filt_dir_encode(uint8_t *raw, const void *native, void *ctx);
static herr_t H5HF__huge_bt2_filt_dir_decode(const uint8_t *raw, void *native, void *ctx);
static herr_t H5HF__huge_bt2_filt_dir_debug(FILE *stream, int indent, int fwidth, const void *record,
                                            const void *_udata);

/*********************/
/* Package Variables */
/*********************/
/* v2 B-tree class for indirectly accessed 'huge' objects */
const H5B2_class_t H5HF_HUGE_BT2_INDIR[1] = {{
    /* B-tree class information */
    H5B2_FHEAP_HUGE_INDIR_ID,          /* Type of B-tree */
    "H5B2_FHEAP_HUGE_INDIR_ID",        /* Name of B-tree class */
    sizeof(H5HF_huge_bt2_indir_rec_t), /* Size of native record */
    H5HF__huge_bt2_crt_context,        /* Create client callback context */
    H5HF__huge_bt2_dst_context,        /* Destroy client callback context */
    H5HF__huge_bt2_indir_store,        /* Record storage callback */
    H5HF__huge_bt2_indir_compare,      /* Record comparison callback */
    H5HF__huge_bt2_indir_encode,       /* Record encoding callback */
    H5HF__huge_bt2_indir_decode,       /* Record decoding callback */
    H5HF__huge_bt2_indir_debug         /* Record debugging callback */
}};

/* v2 B-tree class for indirectly accessed, filtered 'huge' objects */
const H5B2_class_t H5HF_HUGE_BT2_FILT_INDIR[1] = {{
    /* B-tree class information */
    H5B2_FHEAP_HUGE_FILT_INDIR_ID,          /* Type of B-tree */
    "H5B2_FHEAP_HUGE_FILT_INDIR_ID",        /* Name of B-tree class */
    sizeof(H5HF_huge_bt2_filt_indir_rec_t), /* Size of native record */
    H5HF__huge_bt2_crt_context,             /* Create client callback context */
    H5HF__huge_bt2_dst_context,             /* Destroy client callback context */
    H5HF__huge_bt2_filt_indir_store,        /* Record storage callback */
    H5HF__huge_bt2_filt_indir_compare,      /* Record comparison callback */
    H5HF__huge_bt2_filt_indir_encode,       /* Record encoding callback */
    H5HF__huge_bt2_filt_indir_decode,       /* Record decoding callback */
    H5HF__huge_bt2_filt_indir_debug         /* Record debugging callback */
}};

/* v2 B-tree class for directly accessed 'huge' objects */
const H5B2_class_t H5HF_HUGE_BT2_DIR[1] = {{
    /* B-tree class information */
    H5B2_FHEAP_HUGE_DIR_ID,          /* Type of B-tree */
    "H5B2_FHEAP_HUGE_DIR_ID",        /* Name of B-tree class */
    sizeof(H5HF_huge_bt2_dir_rec_t), /* Size of native record */
    H5HF__huge_bt2_crt_context,      /* Create client callback context */
    H5HF__huge_bt2_dst_context,      /* Destroy client callback context */
    H5HF__huge_bt2_dir_store,        /* Record storage callback */
    H5HF__huge_bt2_dir_compare,      /* Record comparison callback */
    H5HF__huge_bt2_dir_encode,       /* Record encoding callback */
    H5HF__huge_bt2_dir_decode,       /* Record decoding callback */
    H5HF__huge_bt2_dir_debug         /* Record debugging callback */
}};

/* v2 B-tree class for directly accessed, filtered 'huge' objects */
const H5B2_class_t H5HF_HUGE_BT2_FILT_DIR[1] = {{
    /* B-tree class information */
    H5B2_FHEAP_HUGE_FILT_DIR_ID,          /* Type of B-tree */
    "H5B2_FHEAP_HUGE_FILT_DIR_ID",        /* Name of B-tree class */
    sizeof(H5HF_huge_bt2_filt_dir_rec_t), /* Size of native record */
    H5HF__huge_bt2_crt_context,           /* Create client callback context */
    H5HF__huge_bt2_dst_context,           /* Destroy client callback context */
    H5HF__huge_bt2_filt_dir_store,        /* Record storage callback */
    H5HF__huge_bt2_filt_dir_compare,      /* Record comparison callback */
    H5HF__huge_bt2_filt_dir_encode,       /* Record encoding callback */
    H5HF__huge_bt2_filt_dir_decode,       /* Record decoding callback */
    H5HF__huge_bt2_filt_dir_debug         /* Record debugging callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5HF_huge_bt2_ctx_t struct */
H5FL_DEFINE_STATIC(H5HF_huge_bt2_ctx_t);

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_crt_context
 *
 * Purpose:	Create client callback context
 *
 * Note:	Common to all 'huge' v2 B-tree clients
 *
 * Return:	Success:	non-NULL
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5HF__huge_bt2_crt_context(void *_f)
{
    H5F_t               *f = (H5F_t *)_f;  /* User data for building callback context */
    H5HF_huge_bt2_ctx_t *ctx;              /* Callback context structure */
    void                *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);

    /* Allocate callback context */
    if (NULL == (ctx = H5FL_MALLOC(H5HF_huge_bt2_ctx_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "can't allocate callback context");

    /* Determine the size of addresses & lengths in the file */
    ctx->sizeof_addr = H5F_SIZEOF_ADDR(f);
    ctx->sizeof_size = H5F_SIZEOF_SIZE(f);

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__huge_bt2_crt_context() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dst_context
 *
 * Purpose:	Destroy client callback context
 *
 * Note:	Common to all 'huge' v2 B-tree clients
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_dst_context(void *_ctx)
{
    H5HF_huge_bt2_ctx_t *ctx = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Release callback context */
    ctx = H5FL_FREE(H5HF_huge_bt2_ctx_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_dst_context() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_found
 *
 * Purpose:	Retrieve record for indirectly accessed 'huge' object, when
 *              it's found in the v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_indir_found(const void *nrecord, void *op_data)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_indir_rec_t *)op_data = *(const H5HF_huge_bt2_indir_rec_t *)nrecord;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_indir_found() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_remove
 *
 * Purpose:	Free space for indirectly accessed 'huge' object, as v2 B-tree
 *              is being deleted or v2 B-tree node is removed
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_indir_remove(const void *nrecord, void *_udata)
{
    H5HF_huge_remove_ud_t *udata     = (H5HF_huge_remove_ud_t *)_udata; /* User callback data */
    herr_t                 ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the space in the file for the object being removed */
    if (H5MF_xfree(udata->hdr->f, H5FD_MEM_FHEAP_HUGE_OBJ, ((const H5HF_huge_bt2_indir_rec_t *)nrecord)->addr,
                   ((const H5HF_huge_bt2_indir_rec_t *)nrecord)->len) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free space for huge object on disk");

    /* Set the length of the object removed */
    udata->obj_len = ((const H5HF_huge_bt2_indir_rec_t *)nrecord)->len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__huge_bt2_indir_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_store
 *
 * Purpose:	Store native information into record for v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_indir_store(void *nrecord, const void *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_indir_rec_t *)nrecord = *(const H5HF_huge_bt2_indir_rec_t *)udata;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_indir_store() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_compare
 *
 * Purpose:	Compare two native information records, according to some key
 *
 * Return:	<0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_indir_compare(const void *_rec1, const void *_rec2, int *result)
{
    FUNC_ENTER_PACKAGE_NOERR

    *result = (int)(((const H5HF_huge_bt2_indir_rec_t *)_rec1)->id -
                    ((const H5HF_huge_bt2_indir_rec_t *)_rec2)->id);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_indir_compare() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_encode
 *
 * Purpose:	Encode native information into raw form for storing on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_indir_encode(uint8_t *raw, const void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t             *ctx     = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    const H5HF_huge_bt2_indir_rec_t *nrecord = (const H5HF_huge_bt2_indir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Encode the record's fields */
    H5F_addr_encode_len(ctx->sizeof_addr, &raw, nrecord->addr);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->id, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_indir_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_decode
 *
 * Purpose:	Decode raw disk form of record into native form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_indir_decode(const uint8_t *raw, void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t       *ctx     = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    H5HF_huge_bt2_indir_rec_t *nrecord = (H5HF_huge_bt2_indir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Decode the record's fields */
    H5F_addr_decode_len(ctx->sizeof_addr, &raw, &nrecord->addr);
    H5_DECODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);
    H5_DECODE_LENGTH_LEN(raw, nrecord->id, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_indir_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_indir_debug
 *
 * Purpose:	Debug native form of record
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_indir_debug(FILE *stream, int indent, int fwidth, const void *_nrecord,
                           const void H5_ATTR_UNUSED *_udata)
{
    const H5HF_huge_bt2_indir_rec_t *nrecord = (const H5HF_huge_bt2_indir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    fprintf(stream, "%*s%-*s {%" PRIuHADDR ", %" PRIuHSIZE ", %" PRIuHSIZE "}\n", indent, "", fwidth,
            "Record:", nrecord->addr, nrecord->len, nrecord->id);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_indir_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_found
 *
 * Purpose:	Retrieve record for indirectly accessed, filtered 'huge' object,
 *              when it's found in the v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_filt_indir_found(const void *nrecord, void *op_data)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_filt_indir_rec_t *)op_data = *(const H5HF_huge_bt2_filt_indir_rec_t *)nrecord;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_indir_found() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_remove
 *
 * Purpose:	Free space for indirectly accessed, filtered 'huge' object, as
 *              v2 B-tree is being deleted or v2 B-tree node is removed
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_filt_indir_remove(const void *nrecord, void *_udata)
{
    H5HF_huge_remove_ud_t *udata     = (H5HF_huge_remove_ud_t *)_udata; /* User callback data */
    herr_t                 ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the space in the file for the object being removed */
    if (H5MF_xfree(udata->hdr->f, H5FD_MEM_FHEAP_HUGE_OBJ,
                   ((const H5HF_huge_bt2_filt_indir_rec_t *)nrecord)->addr,
                   ((const H5HF_huge_bt2_filt_indir_rec_t *)nrecord)->len) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free space for huge object on disk");

    /* Set the length of the object removed */
    udata->obj_len = ((const H5HF_huge_bt2_filt_indir_rec_t *)nrecord)->obj_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__huge_bt2_filt_indir_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_store
 *
 * Purpose:	Store native information into record for v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_indir_store(void *nrecord, const void *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_filt_indir_rec_t *)nrecord = *(const H5HF_huge_bt2_filt_indir_rec_t *)udata;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_indir_store() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_compare
 *
 * Purpose:	Compare two native information records, according to some key
 *
 * Return:	<0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_indir_compare(const void *_rec1, const void *_rec2, int *result)
{
    FUNC_ENTER_PACKAGE_NOERR

    *result = (int)(((const H5HF_huge_bt2_filt_indir_rec_t *)_rec1)->id -
                    ((const H5HF_huge_bt2_filt_indir_rec_t *)_rec2)->id);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_indir_compare() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_encode
 *
 * Purpose:	Encode native information into raw form for storing on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_indir_encode(uint8_t *raw, const void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t                  *ctx = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    const H5HF_huge_bt2_filt_indir_rec_t *nrecord = (const H5HF_huge_bt2_filt_indir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Encode the record's fields */
    H5F_addr_encode_len(ctx->sizeof_addr, &raw, nrecord->addr);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);
    UINT32ENCODE(raw, nrecord->filter_mask);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->obj_size, ctx->sizeof_size);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->id, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_indir_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_decode
 *
 * Purpose:	Decode raw disk form of record into native form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_indir_decode(const uint8_t *raw, void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t            *ctx     = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    H5HF_huge_bt2_filt_indir_rec_t *nrecord = (H5HF_huge_bt2_filt_indir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Decode the record's fields */
    H5F_addr_decode_len(ctx->sizeof_addr, &raw, &nrecord->addr);
    H5_DECODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);
    UINT32DECODE(raw, nrecord->filter_mask);
    H5_DECODE_LENGTH_LEN(raw, nrecord->obj_size, ctx->sizeof_size);
    H5_DECODE_LENGTH_LEN(raw, nrecord->id, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_indir_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_indir_debug
 *
 * Purpose:	Debug native form of record
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_indir_debug(FILE *stream, int indent, int fwidth, const void *_nrecord,
                                const void H5_ATTR_UNUSED *_udata)
{
    const H5HF_huge_bt2_filt_indir_rec_t *nrecord = (const H5HF_huge_bt2_filt_indir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    fprintf(stream, "%*s%-*s {%" PRIuHADDR ", %" PRIuHSIZE ", %x, %" PRIuHSIZE ", %" PRIuHSIZE "}\n", indent,
            "", fwidth, "Record:", nrecord->addr, nrecord->len, nrecord->filter_mask, nrecord->obj_size,
            nrecord->id);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_indir_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dir_remove
 *
 * Purpose:	Free space for directly accessed 'huge' object, as v2 B-tree
 *              is being deleted or v2 B-tree node is being removed
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_dir_remove(const void *nrecord, void *_udata)
{
    H5HF_huge_remove_ud_t *udata     = (H5HF_huge_remove_ud_t *)_udata; /* User callback data */
    herr_t                 ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the space in the file for the object being removed */
    if (H5MF_xfree(udata->hdr->f, H5FD_MEM_FHEAP_HUGE_OBJ, ((const H5HF_huge_bt2_indir_rec_t *)nrecord)->addr,
                   ((const H5HF_huge_bt2_indir_rec_t *)nrecord)->len) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free space for huge object on disk");

    /* Set the length of the object removed */
    udata->obj_len = ((const H5HF_huge_bt2_indir_rec_t *)nrecord)->len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__huge_bt2_dir_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dir_store
 *
 * Purpose:	Store native information into record for v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_dir_store(void *nrecord, const void *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_dir_rec_t *)nrecord = *(const H5HF_huge_bt2_dir_rec_t *)udata;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_dir_store() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dir_compare
 *
 * Purpose:	Compare two native information records, according to some key
 *
 * Return:	<0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_dir_compare(const void *_rec1, const void *_rec2, int *result)
{
    const H5HF_huge_bt2_dir_rec_t *rec1 = (const H5HF_huge_bt2_dir_rec_t *)_rec1;
    const H5HF_huge_bt2_dir_rec_t *rec2 = (const H5HF_huge_bt2_dir_rec_t *)_rec2;

    FUNC_ENTER_PACKAGE_NOERR

    if (rec1->addr < rec2->addr)
        *result = -1;
    else if (rec1->addr > rec2->addr)
        *result = 1;
    else if (rec1->len < rec2->len)
        *result = -1;
    else if (rec1->len > rec2->len)
        *result = 1;
    else
        *result = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_dir_compare() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dir_encode
 *
 * Purpose:	Encode native information into raw form for storing on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_dir_encode(uint8_t *raw, const void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t           *ctx     = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    const H5HF_huge_bt2_dir_rec_t *nrecord = (const H5HF_huge_bt2_dir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Encode the record's fields */
    H5F_addr_encode_len(ctx->sizeof_addr, &raw, nrecord->addr);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_dir_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dir_decode
 *
 * Purpose:	Decode raw disk form of record into native form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_dir_decode(const uint8_t *raw, void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t     *ctx     = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    H5HF_huge_bt2_dir_rec_t *nrecord = (H5HF_huge_bt2_dir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Decode the record's fields */
    H5F_addr_decode_len(ctx->sizeof_addr, &raw, &nrecord->addr);
    H5_DECODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_dir_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_dir_debug
 *
 * Purpose:	Debug native form of record
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_dir_debug(FILE *stream, int indent, int fwidth, const void *_nrecord,
                         const void H5_ATTR_UNUSED *_udata)
{
    const H5HF_huge_bt2_dir_rec_t *nrecord = (const H5HF_huge_bt2_dir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    fprintf(stream, "%*s%-*s {%" PRIuHADDR ", %" PRIuHSIZE "}\n", indent, "", fwidth,
            "Record:", nrecord->addr, nrecord->len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_dir_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_found
 *
 * Purpose:	Retrieve record for directly accessed, filtered 'huge' object,
 *              when it's found in the v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_filt_dir_found(const void *nrecord, void *op_data)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_filt_dir_rec_t *)op_data = *(const H5HF_huge_bt2_filt_dir_rec_t *)nrecord;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_dir_found() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_remove
 *
 * Purpose:	Free space for directly accessed, filtered 'huge' object, as
 *              v2 B-tree is being deleted or v2 B-tree node is removed
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__huge_bt2_filt_dir_remove(const void *nrecord, void *_udata)
{
    H5HF_huge_remove_ud_t *udata     = (H5HF_huge_remove_ud_t *)_udata; /* User callback data */
    herr_t                 ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the space in the file for the object being removed */
    if (H5MF_xfree(udata->hdr->f, H5FD_MEM_FHEAP_HUGE_OBJ,
                   ((const H5HF_huge_bt2_filt_dir_rec_t *)nrecord)->addr,
                   ((const H5HF_huge_bt2_filt_dir_rec_t *)nrecord)->len) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free space for huge object on disk");

    /* Set the length of the object removed */
    udata->obj_len = ((const H5HF_huge_bt2_filt_dir_rec_t *)nrecord)->obj_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__huge_bt2_filt_dir_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_store
 *
 * Purpose:	Store native information into record for v2 B-tree
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_dir_store(void *nrecord, const void *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    *(H5HF_huge_bt2_filt_dir_rec_t *)nrecord = *(const H5HF_huge_bt2_filt_dir_rec_t *)udata;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_dir_store() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_compare
 *
 * Purpose:	Compare two native information records, according to some key
 *
 * Return:	<0 if rec1 < rec2
 *              =0 if rec1 == rec2
 *              >0 if rec1 > rec2
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_dir_compare(const void *_rec1, const void *_rec2, int *result)
{
    const H5HF_huge_bt2_filt_dir_rec_t *rec1 = (const H5HF_huge_bt2_filt_dir_rec_t *)_rec1;
    const H5HF_huge_bt2_filt_dir_rec_t *rec2 = (const H5HF_huge_bt2_filt_dir_rec_t *)_rec2;

    FUNC_ENTER_PACKAGE_NOERR

    if (rec1->addr < rec2->addr)
        *result = -1;
    else if (rec1->addr > rec2->addr)
        *result = 1;
    else if (rec1->len < rec2->len)
        *result = -1;
    else if (rec1->len > rec2->len)
        *result = 1;
    else
        *result = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_dir_compare() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_encode
 *
 * Purpose:	Encode native information into raw form for storing on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_dir_encode(uint8_t *raw, const void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t                *ctx = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    const H5HF_huge_bt2_filt_dir_rec_t *nrecord = (const H5HF_huge_bt2_filt_dir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Encode the record's fields */
    H5F_addr_encode_len(ctx->sizeof_addr, &raw, nrecord->addr);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);
    UINT32ENCODE(raw, nrecord->filter_mask);
    H5_ENCODE_LENGTH_LEN(raw, nrecord->obj_size, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_dir_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_decode
 *
 * Purpose:	Decode raw disk form of record into native form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_dir_decode(const uint8_t *raw, void *_nrecord, void *_ctx)
{
    H5HF_huge_bt2_ctx_t          *ctx     = (H5HF_huge_bt2_ctx_t *)_ctx; /* Callback context structure */
    H5HF_huge_bt2_filt_dir_rec_t *nrecord = (H5HF_huge_bt2_filt_dir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Decode the record's fields */
    H5F_addr_decode_len(ctx->sizeof_addr, &raw, &nrecord->addr);
    H5_DECODE_LENGTH_LEN(raw, nrecord->len, ctx->sizeof_size);
    UINT32DECODE(raw, nrecord->filter_mask);
    H5_DECODE_LENGTH_LEN(raw, nrecord->obj_size, ctx->sizeof_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_dir_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__huge_bt2_filt_dir_debug
 *
 * Purpose:	Debug native form of record
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__huge_bt2_filt_dir_debug(FILE *stream, int indent, int fwidth, const void *_nrecord,
                              const void H5_ATTR_UNUSED *_udata)
{
    const H5HF_huge_bt2_filt_dir_rec_t *nrecord = (const H5HF_huge_bt2_filt_dir_rec_t *)_nrecord;

    FUNC_ENTER_PACKAGE_NOERR

    fprintf(stream, "%*s%-*s {%" PRIuHADDR ", %" PRIuHSIZE ", %x, %" PRIuHSIZE "}\n", indent, "", fwidth,
            "Record:", nrecord->addr, nrecord->len, nrecord->filter_mask, nrecord->obj_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF__huge_bt2_filt_dir_debug() */

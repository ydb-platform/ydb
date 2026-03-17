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
 * Purpose:	Extensible array testing functions.
 *
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5EAmodule.h" /* This source code file is part of the H5EA module */
#define H5EA_TESTING

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5EApkg.h"     /* Extensible Arrays			*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5VMprivate.h" /* Vector functions			*/

/****************/
/* Local Macros */
/****************/

/* Sanity checking value for callback contexts */
#define H5EA__TEST_BOGUS_VAL 42

/******************/
/* Local Typedefs */
/******************/

/* Callback context */
typedef struct H5EA__test_ctx_t {
    uint32_t        bogus; /* Placeholder field to verify that context is working */
    H5EA__ctx_cb_t *cb;    /* Pointer to context's callback action */
} H5EA__test_ctx_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Extensible array class callbacks */
static void  *H5EA__test_crt_context(void *udata);
static herr_t H5EA__test_dst_context(void *ctx);
static herr_t H5EA__test_fill(void *nat_blk, size_t nelmts);
static herr_t H5EA__test_encode(void *raw, const void *elmt, size_t nelmts, void *ctx);
static herr_t H5EA__test_decode(const void *raw, void *elmt, size_t nelmts, void *ctx);
static herr_t H5EA__test_debug(FILE *stream, int indent, int fwidth, hsize_t idx, const void *elmt);
static void  *H5EA__test_crt_dbg_context(H5F_t H5_ATTR_UNUSED *f, haddr_t H5_ATTR_UNUSED obj_addr);
static herr_t H5EA__test_dst_dbg_context(void *_ctx);

/*********************/
/* Package Variables */
/*********************/

/* Extensible array testing class information */
const H5EA_class_t H5EA_CLS_TEST[1] = {{
    H5EA_CLS_TEST_ID,           /* Type of Extensible array */
    "Testing",                  /* Name of Extensible Array class */
    sizeof(uint64_t),           /* Size of native element */
    H5EA__test_crt_context,     /* Create context */
    H5EA__test_dst_context,     /* Destroy context */
    H5EA__test_fill,            /* Fill block of missing elements callback */
    H5EA__test_encode,          /* Element encoding callback */
    H5EA__test_decode,          /* Element decoding callback */
    H5EA__test_debug,           /* Element debugging callback */
    H5EA__test_crt_dbg_context, /* Create debugging context */
    H5EA__test_dst_dbg_context  /* Destroy debugging context */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5EA__test_ctx_t struct */
H5FL_DEFINE_STATIC(H5EA__test_ctx_t);

/* Declare a free list to manage the H5EA__ctx_cb_t struct */
H5FL_DEFINE_STATIC(H5EA__ctx_cb_t);

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_crt_context
 *
 * Purpose:	Create context for callbacks
 *
 * Return:	Success:	non-NULL
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__test_crt_context(void *_udata)
{
    H5EA__test_ctx_t *ctx;                                  /* Context for callbacks */
    H5EA__ctx_cb_t   *udata     = (H5EA__ctx_cb_t *)_udata; /* User data for context */
    void             *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Allocate new context structure */
    if (NULL == (ctx = H5FL_MALLOC(H5EA__test_ctx_t)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "can't allocate extensible array client callback context");

    /* Initialize the context */
    ctx->bogus = H5EA__TEST_BOGUS_VAL;
    ctx->cb    = udata;

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__test_crt_context() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_dst_context
 *
 * Purpose:	Destroy context for callbacks
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__test_dst_context(void *_ctx)
{
    H5EA__test_ctx_t *ctx = (H5EA__test_ctx_t *)_ctx; /* Callback context to destroy */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(H5EA__TEST_BOGUS_VAL == ctx->bogus);

    /* Release context structure */
    ctx = H5FL_FREE(H5EA__test_ctx_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__test_dst_context() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_fill
 *
 * Purpose:	Fill "missing elements" in block of elements
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__test_fill(void *nat_blk, size_t nelmts)
{
    uint64_t fill_val = H5EA_TEST_FILL; /* Value to fill elements with */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(nat_blk);
    assert(nelmts);

    H5VM_array_fill(nat_blk, &fill_val, sizeof(uint64_t), nelmts);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__test_fill() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_encode
 *
 * Purpose:	Encode an element from "native" to "raw" form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__test_encode(void *raw, const void *_elmt, size_t nelmts, void *_ctx)
{
    H5EA__test_ctx_t *ctx       = (H5EA__test_ctx_t *)_ctx; /* Callback context to destroy */
    const uint64_t   *elmt      = (const uint64_t *)_elmt;  /* Convenience pointer to native elements */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(raw);
    assert(elmt);
    assert(nelmts);
    assert(H5EA__TEST_BOGUS_VAL == ctx->bogus);

    /* Check for callback action */
    if (ctx->cb) {
        if ((*ctx->cb->encode)(elmt, nelmts, ctx->cb->udata) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, FAIL, "extensible array testing callback action failed");
    }

    /* Encode native elements into raw elements */
    while (nelmts) {
        /* Encode element - advances 'raw' pointer */
        UINT64ENCODE(raw, *elmt);

        /* Advance native element pointer */
        elmt++;

        /* Decrement # of elements to encode */
        nelmts--;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__test_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_decode
 *
 * Purpose:	Decode an element from "raw" to "native" form
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__test_decode(const void *_raw, void *_elmt, size_t nelmts, void H5_ATTR_NDEBUG_UNUSED *_ctx)
{
#ifndef NDEBUG
    H5EA__test_ctx_t *ctx = (H5EA__test_ctx_t *)_ctx; /* Callback context to destroy */
#endif                                                /* NDEBUG */
    uint64_t      *elmt = (uint64_t *)_elmt;          /* Convenience pointer to native elements */
    const uint8_t *raw  = (const uint8_t *)_raw;      /* Convenience pointer to raw elements */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(raw);
    assert(elmt);
    assert(nelmts);
    assert(H5EA__TEST_BOGUS_VAL == ctx->bogus);

    /* Decode raw elements into native elements */
    while (nelmts) {
        /* Decode element - advances 'raw' pointer */
        UINT64DECODE(raw, *elmt);

        /* Advance native element pointer */
        elmt++;

        /* Decrement # of elements to decode */
        nelmts--;
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__test_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_debug
 *
 * Purpose:	Display an element for debugging
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__test_debug(FILE *stream, int indent, int fwidth, hsize_t idx, const void *elmt)
{
    char temp_str[128]; /* Temporary string, for formatting */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(stream);
    assert(elmt);

    /* Print element */
    snprintf(temp_str, sizeof(temp_str), "Element #%llu:", (unsigned long long)idx);
    fprintf(stream, "%*s%-*s %llu\n", indent, "", fwidth, temp_str,
            (unsigned long long)*(const uint64_t *)elmt);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__test_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__test_crt_dbg_context
 *
 * Purpose:     Create context for debugging callback
 *
 * Return:      Success:        non-NULL
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__test_crt_dbg_context(H5F_t H5_ATTR_UNUSED *f, haddr_t H5_ATTR_UNUSED obj_addr)
{
    H5EA__ctx_cb_t *ctx; /* Context for callbacks */
    void           *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Allocate new context structure */
    if (NULL == (ctx = H5FL_MALLOC(H5EA__ctx_cb_t)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "can't allocate extensible array client callback context");

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__test_crt_dbg_context() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__test_dst_dbg_context
 *
 * Purpose:	Destroy context for callbacks
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__test_dst_dbg_context(void *_ctx)
{
    H5EA__ctx_cb_t *ctx = (H5EA__ctx_cb_t *)_ctx; /* Callback context to destroy */

    FUNC_ENTER_PACKAGE_NOERR

    assert(_ctx);

    /* Release context structure */
    ctx = H5FL_FREE(H5EA__ctx_cb_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__test_dst_dbg_context() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__get_cparam_test
 *
 * Purpose:	Retrieve the parameters used to create the extensible array
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__get_cparam_test(const H5EA_t *ea, H5EA_create_t *cparam)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(ea);
    assert(cparam);

    /* Get extensible array creation parameters */
    cparam->raw_elmt_size             = ea->hdr->cparam.raw_elmt_size;
    cparam->max_nelmts_bits           = ea->hdr->cparam.max_nelmts_bits;
    cparam->idx_blk_elmts             = ea->hdr->cparam.idx_blk_elmts;
    cparam->sup_blk_min_data_ptrs     = ea->hdr->cparam.sup_blk_min_data_ptrs;
    cparam->data_blk_min_elmts        = ea->hdr->cparam.data_blk_min_elmts;
    cparam->max_dblk_page_nelmts_bits = ea->hdr->cparam.max_dblk_page_nelmts_bits;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__get_cparam_test() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cmp_cparam_test
 *
 * Purpose:	Compare the parameters used to create the extensible array
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
int
H5EA__cmp_cparam_test(const H5EA_create_t *cparam1, const H5EA_create_t *cparam2)
{
    int ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(cparam1);
    assert(cparam2);

    /* Compare creation parameters for array */
    if (cparam1->raw_elmt_size < cparam2->raw_elmt_size)
        HGOTO_DONE(-1);
    else if (cparam1->raw_elmt_size > cparam2->raw_elmt_size)
        HGOTO_DONE(1);

    if (cparam1->max_nelmts_bits < cparam2->max_nelmts_bits)
        HGOTO_DONE(-1);
    else if (cparam1->max_nelmts_bits > cparam2->max_nelmts_bits)
        HGOTO_DONE(1);

    if (cparam1->idx_blk_elmts < cparam2->idx_blk_elmts)
        HGOTO_DONE(-1);
    else if (cparam1->idx_blk_elmts > cparam2->idx_blk_elmts)
        HGOTO_DONE(1);

    if (cparam1->sup_blk_min_data_ptrs < cparam2->sup_blk_min_data_ptrs)
        HGOTO_DONE(-1);
    else if (cparam1->sup_blk_min_data_ptrs > cparam2->sup_blk_min_data_ptrs)
        HGOTO_DONE(1);

    if (cparam1->data_blk_min_elmts < cparam2->data_blk_min_elmts)
        HGOTO_DONE(-1);
    else if (cparam1->data_blk_min_elmts > cparam2->data_blk_min_elmts)
        HGOTO_DONE(1);

    if (cparam1->max_dblk_page_nelmts_bits < cparam2->max_dblk_page_nelmts_bits)
        HGOTO_DONE(-1);
    else if (cparam1->max_dblk_page_nelmts_bits > cparam2->max_dblk_page_nelmts_bits)
        HGOTO_DONE(1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cmp_cparam_test() */

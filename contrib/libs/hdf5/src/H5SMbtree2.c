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

/****************/
/* Module Setup */
/****************/

#define H5O_FRIEND      /*suppress error about including H5Opkg	  */
#include "H5SMmodule.h" /* This source code file is part of the H5SM module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object Headers                       */
#include "H5SMpkg.h"     /* Shared object header messages        */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* v2 B-tree callbacks */
static void  *H5SM__bt2_crt_context(void *udata);
static herr_t H5SM__bt2_dst_context(void *ctx);
static herr_t H5SM__bt2_store(void *native, const void *udata);
static herr_t H5SM__bt2_debug(FILE *stream, int indent, int fwidth, const void *record, const void *_udata);

/*****************************/
/* Library Private Variables */
/*****************************/
/* v2 B-tree class for SOHM indexes*/
const H5B2_class_t H5SM_INDEX[1] = {{
    /* B-tree class information */
    H5B2_SOHM_INDEX_ID,    /* Type of B-tree */
    "H5B2_SOHM_INDEX_ID",  /* Name of B-tree class */
    sizeof(H5SM_sohm_t),   /* Size of native record */
    H5SM__bt2_crt_context, /* Create client callback context */
    H5SM__bt2_dst_context, /* Destroy client callback context */
    H5SM__bt2_store,       /* Record storage callback */
    H5SM__message_compare, /* Record comparison callback */
    H5SM__message_encode,  /* Record encoding callback */
    H5SM__message_decode,  /* Record decoding callback */
    H5SM__bt2_debug        /* Record debugging callback */
}};

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5SM_bt2_ctx_t struct */
H5FL_DEFINE_STATIC(H5SM_bt2_ctx_t);

/*-------------------------------------------------------------------------
 * Function:	H5SM__bt2_crt_context
 *
 * Purpose:	Create client callback context
 *
 * Return:	Success:	non-NULL
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5SM__bt2_crt_context(void *_f)
{
    H5F_t          *f = (H5F_t *)_f;  /* User data for building callback context */
    H5SM_bt2_ctx_t *ctx;              /* Callback context structure */
    void           *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);

    /* Allocate callback context */
    if (NULL == (ctx = H5FL_MALLOC(H5SM_bt2_ctx_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "can't allocate callback context");

    /* Determine the size of addresses & lengths in the file */
    ctx->sizeof_addr = H5F_SIZEOF_ADDR(f);

    /* Set return value */
    ret_value = ctx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5SM__bt2_crt_context() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__bt2_dst_context
 *
 * Purpose:	Destroy client callback context
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__bt2_dst_context(void *_ctx)
{
    H5SM_bt2_ctx_t *ctx = (H5SM_bt2_ctx_t *)_ctx; /* Callback context structure */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ctx);

    /* Release callback context */
    ctx = H5FL_FREE(H5SM_bt2_ctx_t, ctx);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5SM__bt2_dst_context() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__bt2_store
 *
 * Purpose:	Store a H5SM_sohm_t SOHM message in the B-tree.  The message
 *              comes in UDATA as a H5SM_mesg_key_t* and is copied to
 *              NATIVE as a H5SM_sohm_t.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__bt2_store(void *native, const void *udata)
{
    const H5SM_mesg_key_t *key = (const H5SM_mesg_key_t *)udata;

    FUNC_ENTER_PACKAGE_NOERR

    /* Copy the source message to the B-tree */
    *(H5SM_sohm_t *)native = key->message;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__bt2_store */

/*-------------------------------------------------------------------------
 * Function:	H5SM__bt2_debug
 *
 * Purpose:	Print debugging information for a H5SM_sohm_t.
 *
 * Return:	Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__bt2_debug(FILE *stream, int indent, int fwidth, const void *record, const void H5_ATTR_UNUSED *_udata)
{
    const H5SM_sohm_t *sohm = (const H5SM_sohm_t *)record;

    FUNC_ENTER_PACKAGE_NOERR

    if (sohm->location == H5SM_IN_HEAP)
        fprintf(stream, "%*s%-*s {%" PRIu64 ", %" PRIo32 ", %" PRIxHSIZE "}\n", indent, "", fwidth,
                "Shared Message in heap:", sohm->u.heap_loc.fheap_id.val, sohm->hash,
                sohm->u.heap_loc.ref_count);
    else {
        assert(sohm->location == H5SM_IN_OH);
        fprintf(stream, "%*s%-*s {%" PRIuHADDR ", %" PRIo32 ", %x, %" PRIx32 "}\n", indent, "", fwidth,
                "Shared Message in OH:", sohm->u.mesg_loc.oh_addr, sohm->hash, sohm->msg_type_id,
                sohm->u.mesg_loc.index);
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__bt2_debug */

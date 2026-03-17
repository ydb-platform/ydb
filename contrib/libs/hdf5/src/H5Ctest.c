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
 * Created:     H5Ctest.c
 *
 * Purpose:     Functions in this file support the metadata cache regression
 *              tests
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Cmodule.h" /* This source code file is part of the H5C module */
#define H5C_TESTING    /*suppress warning about H5C testing funcs*/
#define H5F_FRIEND     /*suppress error about including H5Fpkg	  */

/***********/
/* Headers */
/***********/
#include "H5private.h"          /* Generic Functions			    */
#include "H5Cpkg.h"             /* Cache				    */
#include "H5Eprivate.h"         /* Error handling		  	    */
#include "H5Fpkg.h"             /* Files				    */
#include "H5Iprivate.h"         /* IDs			  		    */
#include "H5VLprivate.h"        /* Virtual Object Layer                     */
#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Typedef for tagged entry iterator callback context - verify cork tag */
typedef struct {
    bool status; /* Corked status */
} H5C_tag_iter_vct_ctx_t;

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5C__verify_cork_tag_test_cb
 *
 * Purpose:     Verify the cork status for an entry
 *
 * Return:      SUCCEED on success, FAIL on error
 *
 *-------------------------------------------------------------------------
 */
static int
H5C__verify_cork_tag_test_cb(H5C_cache_entry_t *entry, void *_ctx)
{
    H5C_tag_iter_vct_ctx_t *ctx = (H5C_tag_iter_vct_ctx_t *)_ctx; /* Get pointer to iterator context */
    bool                    is_corked;                            /* Corked status for entry */
    int                     ret_value = H5_ITER_CONT;             /* Return value */

    /* Function enter macro */
    FUNC_ENTER_PACKAGE

    /* Santify checks */
    assert(entry);
    assert(ctx);

    /* Retrieve corked status for entry */
    is_corked = entry->tag_info ? entry->tag_info->corked : false;

    /* Verify corked status for entry */
    if (is_corked != ctx->status)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, H5_ITER_ERROR, "bad cork status");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__verify_cork_tag_test_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5C__verify_cork_tag_test
 *
 * Purpose:     This routine verifies that all cache entries associated with
 *      the object tag are marked with the desired "cork" status.
 *
 * Return:      SUCCEED on success, FAIL on error
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__verify_cork_tag_test(hid_t fid, H5O_token_t tag_token, bool status)
{
    H5F_t                 *f;                   /* File Pointer */
    H5C_t                 *cache;               /* Cache Pointer */
    H5C_tag_iter_vct_ctx_t ctx;                 /* Context for iterator callback */
    haddr_t                tag;                 /* Tagged address */
    herr_t                 ret_value = SUCCEED; /* Return value */

    /* Function enter macro */
    FUNC_ENTER_PACKAGE

    /* Get file pointer */
    if (NULL == (f = (H5F_t *)H5VL_object_verify(fid, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file");

    /* Convert token to address */
    tag = HADDR_UNDEF;
    if (H5VL_native_token_to_addr(f, H5I_FILE, tag_token, &tag) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "can't get address for token");

    /* Get cache pointer */
    cache = f->shared->cache;

    /* Construct context for iterator callbacks */
    ctx.status = status;

    /* Iterate through tagged entries in the cache */
    if (H5C__iter_tagged_entries(cache, tag, false, H5C__verify_cork_tag_test_cb, &ctx) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADITER, FAIL, "iteration of tagged entries failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__verify_cork_tag_test() */

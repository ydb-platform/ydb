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
 * Created:     H5HLprfx.c
 *
 * Purpose:     Prefix routines for local heaps.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HLmodule.h" /* This source code file is part of the H5HL module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions            */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5FLprivate.h" /* Free lists                   */
#include "H5HLpkg.h"     /* Local Heaps                  */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

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

/* Declare a free list to manage the H5HL_prfx_t struct */
H5FL_DEFINE_STATIC(H5HL_prfx_t);

/*-------------------------------------------------------------------------
 * Function:    H5HL__prfx_new
 *
 * Purpose:     Create a new local heap prefix object
 *
 * Return:      Success:    non-NULL pointer to new local heap prefix
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5HL_prfx_t *
H5HL__prfx_new(H5HL_t *heap)
{
    H5HL_prfx_t *prfx      = NULL; /* New local heap prefix */
    H5HL_prfx_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(heap);

    /* Allocate new local heap prefix */
    if (NULL == (prfx = H5FL_CALLOC(H5HL_prfx_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "memory allocation failed for local heap prefix");

    /* Increment ref. count on heap data structure */
    if (FAIL == H5HL__inc_rc(heap))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL, "can't increment heap ref. count");

    /* Link the heap & the prefix */
    prfx->heap = heap;
    heap->prfx = prfx;

    /* Set the return value */
    ret_value = prfx;

done:
    /* Ensure that the prefix memory is deallocated on errors */
    if (!ret_value && prfx != NULL)
        /* H5FL_FREE always returns NULL so we can't check for errors */
        prfx = H5FL_FREE(H5HL_prfx_t, prfx);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__prfx_new() */

/*-------------------------------------------------------------------------
 * Function:    H5HL__prfx_dest
 *
 * Purpose:     Destroy a local heap prefix object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL__prfx_dest(H5HL_prfx_t *prfx)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(prfx);

    /* Check if prefix was initialized */
    if (prfx->heap) {
        /* Unlink prefix from heap */
        prfx->heap->prfx = NULL;

        /* Decrement ref. count on heap data structure */
        if (FAIL == H5HL__dec_rc(prfx->heap))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement heap ref. count");

        /* Unlink heap from prefix */
        prfx->heap = NULL;
    }

done:
    /* Free prefix memory */
    /* H5FL_FREE always returns NULL so we can't check for errors */
    prfx = H5FL_FREE(H5HL_prfx_t, prfx);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__prfx_dest() */

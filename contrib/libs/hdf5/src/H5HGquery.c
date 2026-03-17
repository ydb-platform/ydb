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
 * Purpose:	Query routines for global heaps.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5HGmodule.h" /* This source code file is part of the H5HG module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5HGpkg.h"    /* Global heaps				*/

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

/*-------------------------------------------------------------------------
 * Function:	H5HG_get_addr
 *
 * Purpose:	Query the address of a global heap object.
 *
 * Return:	Address of heap on success/abort on failure (shouldn't fail)
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5HG_get_addr(const H5HG_heap_t *heap)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(heap);

    FUNC_LEAVE_NOAPI(heap->addr)
} /* H5HG_get_addr() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_get_size
 *
 * Purpose:	Query the size of a global heap object.
 *
 * Return:	Size of heap on success/abort on failure (shouldn't fail)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5HG_get_size(const H5HG_heap_t *heap)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(heap);

    FUNC_LEAVE_NOAPI(heap->size)
} /* H5HG_get_size() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_get_free_size
 *
 * Purpose:	Query the free size of a global heap object.
 *
 * Return:	Free size of heap on success/abort on failure (shouldn't fail)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5HG_get_free_size(const H5HG_heap_t *heap)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(heap);

    FUNC_LEAVE_NOAPI(heap->obj[0].size)
} /* H5HG_get_free_size() */

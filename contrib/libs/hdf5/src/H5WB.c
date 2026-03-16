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
 * Created:		H5WB.c
 *
 * Purpose:		Implements the "wrapped buffer" code for wrapping
 *                      an existing [statically sized] buffer, in order to
 *                      avoid lots of memory allocation calls.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/* Typedef for buffer wrapper */
struct H5WB_t {
    void  *wrapped_buf;  /* Pointer to wrapped buffer */
    size_t wrapped_size; /* Size of wrapped buffer */
    void  *actual_buf;   /* Pointer to actual buffer */
    size_t actual_size;  /* Size of actual buffer used */
    size_t alloc_size;   /* Size of actual buffer allocated */
};

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

/* Declare a free list to manage the H5WB_t struct */
H5FL_DEFINE_STATIC(H5WB_t);

/* Declare a free list to manage the extra buffer information */
H5FL_BLK_DEFINE_STATIC(extra_buf);

/*-------------------------------------------------------------------------
 * Function:	H5WB_wrap
 *
 * Purpose:	Wraps an existing [possibly static] buffer
 *
 * Return:	Pointer to buffer wrapper info on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5WB_t *
H5WB_wrap(void *buf, size_t buf_size)
{
    H5WB_t *wb = NULL; /* Wrapped buffer info */
    H5WB_t *ret_value; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /*
     * Check arguments.
     */
    assert(buf);
    assert(buf_size);

    /* Create wrapped buffer info */
    if (NULL == (wb = H5FL_MALLOC(H5WB_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for wrapped buffer info");

    /* Wrap buffer given */
    wb->wrapped_buf  = buf;
    wb->wrapped_size = buf_size;

    /* No actual buffer yet */
    wb->actual_buf  = NULL;
    wb->actual_size = 0;
    wb->alloc_size  = 0;

    /* Set the return value */
    ret_value = wb;

done:
    /* Release resources on error */
    if (!ret_value && wb)
        wb = H5FL_FREE(H5WB_t, wb);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5WB_wrap() */

/*-------------------------------------------------------------------------
 * Function:	H5WB_actual
 *
 * Purpose:	Get the pointer to an "actual" buffer, of at least a certain
 *              size.
 *
 * Return:	Pointer to buffer pointer on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
void *
H5WB_actual(H5WB_t *wb, size_t need)
{
    void *ret_value; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /*
     * Check arguments.
     */
    assert(wb);
    assert(wb->wrapped_buf);

    /* Check for previously allocated buffer */
    if (wb->actual_buf && wb->actual_buf != wb->wrapped_buf) {
        /* Sanity check */
        assert(wb->actual_size > wb->wrapped_size);

        /* Check if we can reuse existing buffer */
        if (need <= wb->alloc_size)
            HGOTO_DONE(wb->actual_buf);
        /* Can't reuse existing buffer, free it and proceed */
        else
            wb->actual_buf = H5FL_BLK_FREE(extra_buf, wb->actual_buf);
    } /* end if */

    /* Check if size needed can be fulfilled with wrapped buffer */
    if (need > wb->wrapped_size) {
        /* Need to allocate new buffer */
        if (NULL == (wb->actual_buf = H5FL_BLK_MALLOC(extra_buf, need)))
            HGOTO_ERROR(H5E_ATTR, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Remember size of buffer allocated */
        wb->alloc_size = need;
    } /* end if */
    else {
        /* Don't have to allocate a new buffer, use the wrapped one */
        wb->actual_buf = wb->wrapped_buf;
        wb->alloc_size = 0;
    } /* end else */

    /* Set the return value */
    ret_value = wb->actual_buf;

done:
    /* Remember size of buffer used, if we were successful */
    if (ret_value)
        wb->actual_size = need;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5WB_actual() */

/*-------------------------------------------------------------------------
 * Function:	H5WB_actual_clear
 *
 * Purpose:	Get the pointer to an "actual" buffer, of at least a certain
 *              size.  Also, clear actual buffer to zeros.
 *
 * Return:	Pointer to buffer pointer on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
void *
H5WB_actual_clear(H5WB_t *wb, size_t need)
{
    void *ret_value; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /*
     * Check arguments.
     */
    assert(wb);
    assert(wb->wrapped_buf);

    /* Get a pointer to an actual buffer */
    if (NULL == (ret_value = H5WB_actual(wb, need)))
        HGOTO_ERROR(H5E_ATTR, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Clear the buffer */
    memset(ret_value, 0, need);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5WB_actual_clear() */

/*-------------------------------------------------------------------------
 * Function:	H5WB_unwrap
 *
 * Purpose:	"unwrap" a wrapped buffer, releasing all resources used
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5WB_unwrap(H5WB_t *wb)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(wb);
    assert(wb->wrapped_buf);

    /* Release any extra buffers allocated */
    if (wb->actual_buf && wb->actual_buf != wb->wrapped_buf) {
        /* Sanity check */
        assert(wb->actual_size > wb->wrapped_size);

        wb->actual_buf = H5FL_BLK_FREE(extra_buf, wb->actual_buf);
    } /* end if */

    /* Release the buffer wrapper info */
    wb = H5FL_FREE(H5WB_t, wb);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5WB_unwrap() */

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
 * Created:     H5ESevent.c
 *
 * Purpose:     Operations on "events" for managing asynchronous
 *                      operations.
 *
 *                      Please see the asynchronous I/O RFC document
 *                      for a full description of how they work, etc.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5ESmodule.h" /* This source code file is part of the H5ES module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			 */
#include "H5Eprivate.h"  /* Error handling		  	 */
#include "H5ESpkg.h"     /* Event Sets                           */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MMprivate.h" /* Memory management                    */

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

/* Declare a static free list to manage H5ES_event_t structs */
H5FL_DEFINE_STATIC(H5ES_event_t);

/*-------------------------------------------------------------------------
 * Function:    H5ES__event_new
 *
 * Purpose:     Allocate and initialize a new event
 *
 * Return:      Non-NULL pointer to new event on success, NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5ES_event_t *
H5ES__event_new(H5VL_t *connector, void *token)
{
    H5ES_event_t  *ev        = NULL; /* New event */
    H5VL_object_t *request   = NULL; /* Async request token VOL object */
    H5ES_event_t  *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(connector);
    assert(token);

    /* Create vol object for token */
    if (NULL == (request = H5VL_create_object(token, connector))) {
        if (H5VL_request_free(token) < 0)
            HDONE_ERROR(H5E_EVENTSET, H5E_CANTFREE, NULL, "can't free request");
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTINIT, NULL, "can't create vol object for request token");
    } /* end if */

    /* Allocate space for new event */
    if (NULL == (ev = H5FL_CALLOC(H5ES_event_t)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, NULL, "can't allocate event object");

    /* Set request for event */
    ev->request = request;

    /* Set return value */
    ret_value = ev;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__event_new() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__event_free
 *
 * Purpose:     Free an event
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__event_free(H5ES_event_t *ev)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ev);

    /* The 'app_func_name', 'app_file_name', and 'api_name' strings are statically allocated (by the compiler)
     * and are not allocated, so there's no need to free them.
     */
    ev->op_info.api_name = NULL;
    if (ev->op_info.api_args)
        H5MM_xfree_const(ev->op_info.api_args);
    ev->op_info.app_file_name = NULL;
    ev->op_info.app_func_name = NULL;
    if (ev->request) {
        /* Free the request */
        if (H5VL_request_free(ev->request) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTFREE, FAIL, "unable to free request");

        /* Free the VOL object for the request */
        if (H5VL_free_object(ev->request) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, FAIL, "can't free VOL request object");
    } /* end if */

    H5FL_FREE(H5ES_event_t, ev);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__event_free() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__event_completed
 *
 * Purpose:     Handle a completed event
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__event_completed(H5ES_event_t *ev, H5ES_event_list_t *el)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ev);

    /* Remove the event from the event list */
    H5ES__list_remove(el, ev);

    /* Free the event */
    if (H5ES__event_free(ev) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTFREE, FAIL, "unable to free event");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__event_completed() */

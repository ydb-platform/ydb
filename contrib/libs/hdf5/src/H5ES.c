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
 * Created:     H5ES.c
 *
 * Purpose:     Implements an "event set" for managing asynchronous
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
#include "H5Iprivate.h"  /* IDs                                  */
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

/*-------------------------------------------------------------------------
 * Function:    H5EScreate
 *
 * Purpose:     Creates an event set.
 *
 * Return:      Success:    An ID for the event set
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5EScreate(void)
{
    H5ES_t *es;                          /* Pointer to event set object */
    hid_t   ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE0("i", "");

    /* Create the new event set object */
    if (NULL == (es = H5ES__create()))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTCREATE, H5I_INVALID_HID, "can't create event set");

    /* Register the new event set to get an ID for it */
    if ((ret_value = H5I_register(H5I_EVENTSET, es, true)) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTREGISTER, H5I_INVALID_HID, "can't register event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5EScreate() */

/*-------------------------------------------------------------------------
 * Function:    H5ESinsert_request
 *
 * Purpose:     Insert a request from a VOL connector into an event set.
 *
 * Note:        This function is primarily targeted at VOL connector
 *              authors and is _not_ designed for general-purpose application use.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESinsert_request(hid_t es_id, hid_t connector_id, void *request)
{
    H5ES_t *es;                  /* Event set */
    H5VL_t *connector = NULL;    /* VOL connector */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ii*x", es_id, connector_id, request);

    /* Check arguments */
    if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
    if (NULL == request)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL request pointer");

    /* Create new VOL connector object, using the connector ID */
    if (NULL == (connector = H5VL_new_connector(connector_id)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTCREATE, FAIL, "can't create VOL connector object");

    /* Insert request into event set */
    if (H5ES__insert_request(es, connector, request) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTINSERT, FAIL, "can't insert request into event set");

done:
    /* Clean up on error */
    if (ret_value < 0)
        /* Release newly created connector */
        if (connector && H5VL_conn_dec_rc(connector) < 0)
            HDONE_ERROR(H5E_EVENTSET, H5E_CANTDEC, FAIL, "unable to decrement ref count on VOL connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5ESinsert_request() */

/*-------------------------------------------------------------------------
 * Function:    H5ESget_count
 *
 * Purpose:     Retrieve the # of events in an event set
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESget_count(hid_t es_id, size_t *count /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", es_id, count);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");

        /* Retrieve the count, if non-NULL */
        if (count)
            *count = H5ES__list_count(&es->active);
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESget_count() */

/*-------------------------------------------------------------------------
 * Function:    H5ESget_op_counter
 *
 * Purpose:     Retrieve the counter that will be assigned to the next operation
 *              inserted into the event set.
 *
 * Note:        This is designed for wrapper libraries mainly, to use as a
 *              mechanism for matching operations inserted into the event
 *              set with [possible] errors that occur.
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESget_op_counter(hid_t es_id, uint64_t *op_counter /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", es_id, op_counter);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");

        /* Retrieve the operation counter, if non-NULL */
        if (op_counter)
            *op_counter = es->op_counter;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESget_op_counter() */

/*-------------------------------------------------------------------------
 * Function:    H5ESget_requests
 *
 * Purpose:     Retrieve the requests in an event set.  Up to *count
 *              requests are stored in the provided requests array, and
 *              the connector ids corresponding to these requests are
 *              stored in the provided connector_ids array.  Either or
 *              both of these arrays may be NULL, in which case this
 *              information is not returned.  If these arrays are
 *              non-NULL, they must be large enough to contain *count
 *              entries.  On exit, *count is set to the total number of
 *              events in the event set.
 *
 *              Events are returned in the order they were added to the
 *              event set.  If order is H5_ITER_INC or H5_ITER_NATIVE,
 *              events will be returned starting from the oldest. If order
 *              is H5_ITER_DEC, events will be returned starting with the
 *              newest/most recent.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESget_requests(hid_t es_id, H5_iter_order_t order, hid_t *connector_ids, void **requests, size_t array_len,
                 size_t *count /*out*/)
{
    H5ES_t *es;                  /* Event set */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iIo*i**xzx", es_id, order, connector_ids, requests, array_len, count);

    /* Check arguments */
    if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Call internal routine */
    if (array_len > 0 && (requests || connector_ids))
        if (H5ES__get_requests(es, order, connector_ids, requests, array_len) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTGET, FAIL, "can't get requests");

    /* Retrieve the count, if non-NULL */
    if (count)
        *count = H5ES__list_count(&es->active);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESget_requests() */

/*-------------------------------------------------------------------------
 * Function:    H5ESwait
 *
 * Purpose:     Wait (with timeout) for operations in event set to complete
 *
 * Note:        Timeout value is in ns, and is for the H5ESwait call, not each
 *              individual operation.   For example: if '10' is passed as
 *              a timeout value and the event set waited 4ns for the first
 *              operation to complete, the remaining operations would be
 *              allowed to wait for at most 6ns more.  i.e. the timeout value
 *              is "used up" across all operations, until it reaches 0, then
 *              any remaining operations are only checked for completion, not
 *              waited on.
 *
 * Note:        This call will stop waiting on operations and will return
 *              immediately if an operation fails.  If a failure occurs, the
 *              value returned for the # of operations in progress may be
 *              inaccurate.
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESwait(hid_t es_id, uint64_t timeout, size_t *num_in_progress /*out*/, hbool_t *op_failed /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iULxx", es_id, timeout, num_in_progress, op_failed);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
        if (NULL == num_in_progress)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL num_in_progress pointer");
        if (NULL == op_failed)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL op_failed pointer");

        /* Wait for operations */
        if (H5ES__wait(es, timeout, num_in_progress, op_failed) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTWAIT, FAIL, "can't wait on operations");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESwait() */

/*-------------------------------------------------------------------------
 * Function:    H5EScancel
 *
 * Purpose:     Attempt to cancel operations in an event set
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EScancel(hid_t es_id, size_t *num_not_canceled /*out*/, hbool_t *op_failed /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", es_id, num_not_canceled, op_failed);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
        if (NULL == num_not_canceled)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL num_not_canceled pointer");
        if (NULL == op_failed)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL op_failed pointer");

        /* Cancel operations */
        if (H5ES__cancel(es, num_not_canceled, op_failed) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTCANCEL, FAIL, "can't cancel operations");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5EScancel() */

/*-------------------------------------------------------------------------
 * Function:    H5ESget_err_status
 *
 * Purpose:     Check if event set has failed operations
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESget_err_status(hid_t es_id, hbool_t *err_status /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", es_id, err_status);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");

        /* Retrieve the error flag, if non-NULL */
        if (err_status)
            *err_status = es->err_occurred;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESget_err_status() */

/*-------------------------------------------------------------------------
 * Function:    H5ESget_err_count
 *
 * Purpose:     Retrieve # of failed operations
 *
 * Note:        Does not wait for active operations to complete, so count may
 *              not include all failures.
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESget_err_count(hid_t es_id, size_t *num_errs /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", es_id, num_errs);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");

        /* Retrieve the error flag, if non-NULL */
        if (num_errs) {
            if (es->err_occurred)
                *num_errs = H5ES__list_count(&es->failed);
            else
                *num_errs = 0;
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESget_err_count() */

/*-------------------------------------------------------------------------
 * Function:    H5ESget_err_info
 *
 * Purpose:     Retrieve information about failed operations
 *
 * Note:        The strings retrieved for each error info must be released
 *              by calling H5free_memory().
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESget_err_info(hid_t es_id, size_t num_err_info, H5ES_err_info_t err_info[] /*out*/,
                 size_t *num_cleared /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "izxx", es_id, num_err_info, err_info, num_cleared);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
        if (0 == num_err_info)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "err_info array size is 0");
        if (NULL == err_info)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL err_info array pointer");
        if (NULL == num_cleared)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL errors cleared pointer");

        /* Retrieve the error information */
        if (H5ES__get_err_info(es, num_err_info, err_info, num_cleared) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTGET, FAIL, "can't retrieve error info for failed operation(s)");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESget_err_info() */

/*-------------------------------------------------------------------------
 * Function:    H5ESfree_err_info
 *
 * Purpose:     Convenience routine to free 1+ H5ES_err_info_t structs.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESfree_err_info(size_t num_err_info, H5ES_err_info_t err_info[])
{
    size_t u;                   /* Local index variable */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "z*#", num_err_info, err_info);

    /* Check arguments */
    if (0 == num_err_info)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "err_info array size is 0");
    if (NULL == err_info)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL err_info array pointer");

    /* Iterate over array, releasing error information */
    for (u = 0; u < num_err_info; u++) {
        H5MM_xfree(err_info[u].api_name);
        H5MM_xfree(err_info[u].api_args);
        H5MM_xfree(err_info[u].app_file_name);
        H5MM_xfree(err_info[u].app_func_name);
        if (H5I_dec_app_ref(err_info[u].err_stack_id) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTDEC, FAIL, "can't close error stack for err_info #%zu", u);
    } /* end for */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESfree_err_info() */

/*-------------------------------------------------------------------------
 * Function:    H5ESregister_insert_func
 *
 * Purpose:     Registers a callback to invoke when a new operation is inserted
 *              into an event set.
 *
 * Note:        Only one insert callback can be registered for each event set.
 *              Registering a new callback will replace the existing one.
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESregister_insert_func(hid_t es_id, H5ES_event_insert_func_t func, void *ctx)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iEI*x", es_id, func, ctx);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
        if (NULL == func)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL function callback pointer");

        /* Set the event set's insert callback */
        es->ins_func = func;
        es->ins_ctx  = ctx;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESregister_insert_func() */

/*-------------------------------------------------------------------------
 * Function:    H5ESregister_complete_func
 *
 * Purpose:     Registers a callback to invoke when an operation completes
 *              within an event set.
 *
 * Note:        Only one complete callback can be registered for each event set.
 *              Registering a new callback will replace the existing one.
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESregister_complete_func(hid_t es_id, H5ES_event_complete_func_t func, void *ctx)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iEC*x", es_id, func, ctx);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        H5ES_t *es; /* Event set */

        /* Check arguments */
        if (NULL == (es = H5I_object_verify(es_id, H5I_EVENTSET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid event set identifier");
        if (NULL == func)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL function callback pointer");

        /* Set the event set's completion callback */
        es->comp_func = func;
        es->comp_ctx  = ctx;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESregister_complete_func() */

/*-------------------------------------------------------------------------
 * Function:    H5ESclose
 *
 * Purpose:     Closes an event set.
 *
 * Note:        Fails if active operations are present.
 *
 * Note:        H5ES_NONE is a valid value for 'es_id', but functions as a no-op
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ESclose(hid_t es_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", es_id);

    /* Passing H5ES_NONE is valid, but a no-op */
    if (H5ES_NONE != es_id) {
        /* Check arguments */
        if (H5I_EVENTSET != H5I_get_type(es_id))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an event set");

        /*
         * Decrement the counter on the object.  It will be freed if the count
         * reaches zero.
         */
        if (H5I_dec_app_ref(es_id) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTDEC, FAIL, "unable to decrement ref count on event set");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5ESclose() */

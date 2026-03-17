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
 * Created:     H5ESint.c
 *
 * Purpose:     Internal "event set" routines for managing asynchronous
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
#include "H5RSprivate.h" /* Reference-counted strings            */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Callback context for get events operations */
typedef struct H5ES_get_requests_ctx_t {
    hid_t *connector_ids; /* Output buffer for list of connector IDs that match the above requests */
    void **requests;      /* Output buffer for list of requests in event set */
    size_t array_len;     /* Length of the above output buffers */
    size_t i;             /* Number of elements filled in output buffers */
} H5ES_get_requests_ctx_t;

/* Callback context for wait operations */
typedef struct H5ES_wait_ctx_t {
    H5ES_t  *es;              /* Event set being operated on */
    uint64_t timeout;         /* Timeout for wait operation (in ns) */
    size_t  *num_in_progress; /* Count of # of operations that have not completed */
    bool    *op_failed;       /* Flag to indicate an operation failed */
} H5ES_wait_ctx_t;

/* Callback context for cancel operations */
typedef struct H5ES_cancel_ctx_t {
    H5ES_t *es;               /* Event set being operated on */
    size_t *num_not_canceled; /* Count of # of operations were not canceled */
    bool   *op_failed;        /* Flag to indicate an operation failed */
} H5ES_cancel_ctx_t;

/* Callback context for get error info (gei) operations */
typedef struct H5ES_gei_ctx_t {
    H5ES_t          *es;            /* Event set being operated on */
    size_t           num_err_info;  /* # of elements in err_info[] array */
    size_t           curr_err;      /* Index of current error in array */
    H5ES_err_info_t *curr_err_info; /* Pointer to current element in err_info[] array */
} H5ES_gei_ctx_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5ES__close(H5ES_t *es);
static herr_t H5ES__close_cb(void *es, void **request_token);
static herr_t H5ES__insert(H5ES_t *es, H5VL_t *connector, void *request_token, const char *app_file,
                           const char *app_func, unsigned app_line, const char *caller, const char *api_args);
static int    H5ES__get_requests_cb(H5ES_event_t *ev, void *_ctx);
static herr_t H5ES__handle_fail(H5ES_t *es, H5ES_event_t *ev);
static herr_t H5ES__op_complete(H5ES_t *es, H5ES_event_t *ev, H5VL_request_status_t ev_status);
static int    H5ES__wait_cb(H5ES_event_t *ev, void *_ctx);
static int    H5ES__cancel_cb(H5ES_event_t *ev, void *_ctx);
static int    H5ES__get_err_info_cb(H5ES_event_t *ev, void *_ctx);
static int    H5ES__close_failed_cb(H5ES_event_t *ev, void *_ctx);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Event Set ID class */
static const H5I_class_t H5I_EVENTSET_CLS[1] = {{
    H5I_EVENTSET,              /* ID class value */
    0,                         /* Class flags */
    0,                         /* # of reserved IDs for class */
    (H5I_free_t)H5ES__close_cb /* Callback routine for closing objects of this class */
}};

/* Declare a static free list to manage H5ES_t structs */
H5FL_DEFINE_STATIC(H5ES_t);

/*-------------------------------------------------------------------------
 * Function:    H5ES_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5ES_init(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Initialize the ID group for the event set IDs */
    if (H5I_register_type(H5I_EVENTSET_CLS) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTINIT, FAIL, "unable to initialize interface");

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5ES_term_package
 *
 * Purpose:     Terminate this interface.
 *
 * Return:      Success:    Positive if anything is done that might
 *                          affect other interfaces; zero otherwise.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5ES_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Destroy the event set ID group */
    n += (H5I_dec_type_ref(H5I_EVENTSET) > 0);

    FUNC_LEAVE_NOAPI(n)
} /* end H5ES_term_package() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__close_cb
 *
 * Purpose:     Called when the ref count reaches zero on an event set's ID
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5ES__close_cb(void *_es, void H5_ATTR_UNUSED **rt)
{
    H5ES_t *es        = (H5ES_t *)_es; /* The event set to close */
    herr_t  ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);

    /* Close the event set object */
    if (H5ES__close(es) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CLOSEERROR, FAIL, "unable to close event set");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__close_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__create
 *
 * Purpose:     Private function to create an event set object
 *
 * Return:      Success:    Pointer to an event set struct
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5ES_t *
H5ES__create(void)
{
    H5ES_t *es        = NULL; /* Pointer to event set */
    H5ES_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate space for new event set */
    if (NULL == (es = H5FL_CALLOC(H5ES_t)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, NULL, "can't allocate event set object");

    /* Set the return value */
    ret_value = es;

done:
    if (!ret_value)
        if (es && H5ES__close(es) < 0)
            HDONE_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, NULL, "unable to free event set");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__create() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__insert
 *
 * Purpose:     Insert a request token into an event set
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5ES__insert(H5ES_t *es, H5VL_t *connector, void *request_token, const char *app_file, const char *app_func,
             unsigned app_line, const char *caller, const char *api_args)
{
    H5ES_event_t *ev          = NULL;    /* Event for request */
    bool          ev_inserted = false;   /* Flag to indicate that event is in active list */
    herr_t        ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);

    /* Create new event */
    if (NULL == (ev = H5ES__event_new(connector, request_token)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTCREATE, FAIL, "can't create event object");

    /* Copy the app source information */
    /* The 'app_func' & 'app_file' strings are statically allocated (by the compiler)
     * there's no need to duplicate them.
     */
    ev->op_info.app_file_name = app_file;
    ev->op_info.app_func_name = app_func;
    ev->op_info.app_line_num  = app_line;

    /* Set the event's operation counter */
    ev->op_info.op_ins_count = es->op_counter++;

    /* Set the event's timestamp & execution time */
    ev->op_info.op_ins_ts    = H5_now_usec();
    ev->op_info.op_exec_ts   = UINT64_MAX;
    ev->op_info.op_exec_time = UINT64_MAX;

    /* Copy the API routine's name & arguments */
    /* The 'caller' string is also statically allocated (by the compiler)
     * there's no need to duplicate it.
     */
    ev->op_info.api_name = caller;
    assert(ev->op_info.api_args == NULL);
    if (api_args && NULL == (ev->op_info.api_args = H5MM_xstrdup(api_args)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, FAIL, "can't copy API routine arguments");

    /* Append fully initialized event onto the event set's 'active' list */
    H5ES__list_append(&es->active, ev);
    ev_inserted = true;

    /* Invoke the event set's 'insert' callback, if present */
    if (es->ins_func)
        if ((es->ins_func)(&ev->op_info, es->ins_ctx) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CALLBACK, FAIL, "'insert' callback for event set failed");

done:
    /* Release resources on error */
    if (ret_value < 0)
        if (ev) {
            if (ev_inserted)
                H5ES__list_remove(&es->active, ev);
            if (H5ES__event_free(ev) < 0)
                HDONE_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, FAIL, "unable to release event");
        }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__insert() */

/*-------------------------------------------------------------------------
 * Function:    H5ES_insert
 *
 * Purpose:     Insert a request token into an event set
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES_insert(hid_t es_id, H5VL_t *connector, void *token, const char *caller, const char *caller_args, ...)
{
    H5ES_t     *es = NULL;             /* Event set for the operation */
    const char *app_file;              /* Application source file name */
    const char *app_func;              /* Application source function name */
    unsigned    app_line;              /* Application source line number */
    H5RS_str_t *rs = NULL;             /* Ref-counted string to compose formatted argument string in */
    const char *api_args;              /* Pointer to api_args string from ref-counted string */
    va_list     ap;                    /* Varargs for caller */
    bool        arg_started = false;   /* Whether the va_list has been started */
    herr_t      ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(connector);
    assert(token);
    assert(caller);
    assert(caller_args);

    /* Get event set */
    if (NULL == (es = (H5ES_t *)H5I_object_verify(es_id, H5I_EVENTSET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an event set");

    /* Check for errors in event set */
    if (es->err_occurred)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTINSERT, FAIL, "event set has failed operations");

    /* Start working on the API routines arguments */
    va_start(ap, caller_args);
    arg_started = true;

    /* Copy the app source information */
    (void)va_arg(ap, char *); /* Toss the 'app_file' parameter name */
    app_file = va_arg(ap, char *);
    (void)va_arg(ap, char *); /* Toss the 'app_func' parameter name */
    app_func = va_arg(ap, char *);
    (void)va_arg(ap, char *); /* Toss the 'app_line' parameter name */
    app_line = va_arg(ap, unsigned);

    /* Create the string for the API routine's arguments */
    if (NULL == (rs = H5RS_create(NULL)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, FAIL, "can't allocate ref-counted string");

    /* Copy the string for the API routine's arguments */
    /* (skip the six characters from the app's file, function and line # arguments) */
    assert(0 == strncmp(caller_args, "*s*sIu", 6));
    if (H5_trace_args(rs, caller_args + 6, ap) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTSET, FAIL, "can't create formatted API arguments");
    if (NULL == (api_args = H5RS_get_str(rs)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTGET, FAIL, "can't get pointer to formatted API arguments");

    /* Insert the operation into the event set */
    if (H5ES__insert(es, connector, token, app_file, app_func, app_line, caller, api_args) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTINSERT, FAIL, "event set has failed operations");

done:
    /* Clean up */
    if (arg_started)
        va_end(ap);
    if (rs)
        H5RS_decr(rs);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__insert_request
 *
 * Purpose:     Directly insert a request token into an event set
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__insert_request(H5ES_t *es, H5VL_t *connector, void *token)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);
    assert(connector);
    assert(token);

    /* Insert an 'anonymous' operation into the event set */
    if (H5ES__insert(es, connector, token, NULL, NULL, 0, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTINSERT, FAIL, "event set has failed operations");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__insert_request() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__get_requests_cb
 *
 * Purpose:     Iterator callback for H5ES__get_events - adds the event to
 *              the list.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static int
H5ES__get_requests_cb(H5ES_event_t *ev, void *_ctx)
{
    H5ES_get_requests_ctx_t *ctx       = (H5ES_get_requests_ctx_t *)_ctx; /* Callback context */
    int                      ret_value = H5_ITER_CONT;                    /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(ev);
    assert(ctx);
    assert(ctx->i < ctx->array_len);

    /* Get the connector ID for the event */
    if (ctx->connector_ids)
        ctx->connector_ids[ctx->i] = ev->request->connector->id;

    /* Get the request for the event */
    if (ctx->requests)
        ctx->requests[ctx->i] = ev->request->data;

    /* Check if we've run out of room in the arrays */
    if (++ctx->i == ctx->array_len)
        ret_value = H5_ITER_STOP;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__get_requests_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__get_requests
 *
 * Purpose:     Get all requests in an event set.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__get_requests(H5ES_t *es, H5_iter_order_t order, hid_t *connector_ids, void **requests, size_t array_len)
{
    H5ES_get_requests_ctx_t ctx;                 /* Callback context */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);
    assert(array_len > 0);
    assert(requests || connector_ids);

    /* Set up context for iterator callbacks */
    ctx.connector_ids = connector_ids;
    ctx.requests      = requests;
    ctx.array_len     = array_len;
    ctx.i             = 0;

    /* Iterate over the events in the set */
    if (H5ES__list_iterate(&es->active, order, H5ES__get_requests_cb, &ctx) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADITER, FAIL, "iteration failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__get_requests() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__handle_fail
 *
 * Purpose:     Handle a failed event
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5ES__handle_fail(H5ES_t *es, H5ES_event_t *ev)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(es);
    assert(es->active.head);
    assert(ev);

    /* Set error flag for event set */
    es->err_occurred = true;

    /* Remove event from normal list */
    H5ES__list_remove(&es->active, ev);

    /* Append event onto the event set's error list */
    H5ES__list_append(&es->failed, ev);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5ES__handle_fail() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__op_complete
 *
 * Purpose:     Handle an operation completing
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5ES__op_complete(H5ES_t *es, H5ES_event_t *ev, H5VL_request_status_t ev_status)
{
    H5VL_request_specific_args_t vol_cb_args;                    /* Arguments to VOL callback */
    hid_t                        err_stack_id = H5I_INVALID_HID; /* Error stack for failed operation */
    herr_t                       ret_value    = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);
    assert(ev);
    assert(H5VL_REQUEST_STATUS_SUCCEED == ev_status || H5VL_REQUEST_STATUS_FAIL == ev_status ||
           H5VL_REQUEST_STATUS_CANCELED == ev_status);

    /* Handle each form of event completion */
    if (H5VL_REQUEST_STATUS_SUCCEED == ev_status || H5VL_REQUEST_STATUS_CANCELED == ev_status) {
        /* Invoke the event set's 'complete' callback, if present */
        if (es->comp_func) {
            H5ES_status_t op_status; /* Status for complete callback */

            /* Set appropriate info for callback */
            if (H5VL_REQUEST_STATUS_SUCCEED == ev_status) {
                /* Translate status */
                op_status = H5ES_STATUS_SUCCEED;

                /* Set up VOL callback arguments */
                vol_cb_args.op_type                      = H5VL_REQUEST_GET_EXEC_TIME;
                vol_cb_args.args.get_exec_time.exec_ts   = &ev->op_info.op_exec_ts;
                vol_cb_args.args.get_exec_time.exec_time = &ev->op_info.op_exec_time;

                /* Retrieve the execution time info */
                if (H5VL_request_specific(ev->request, &vol_cb_args) < 0)
                    HGOTO_ERROR(H5E_EVENTSET, H5E_CANTGET, FAIL,
                                "unable to retrieve execution time info for operation");
            }
            else
                /* Translate status */
                op_status = H5ES_STATUS_CANCELED;

            if ((es->comp_func)(&ev->op_info, op_status, H5I_INVALID_HID, es->comp_ctx) < 0)
                HGOTO_ERROR(H5E_EVENTSET, H5E_CALLBACK, FAIL, "'complete' callback for event set failed");
        } /* end if */

        /* Event success or cancellation */
        if (H5ES__event_completed(ev, &es->active) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, FAIL, "unable to release completed event");
    } /* end if */
    else if (H5VL_REQUEST_STATUS_FAIL == ev_status) {
        /* Invoke the event set's 'complete' callback, if present */
        if (es->comp_func) {
            /* Set up VOL callback arguments */
            vol_cb_args.op_type                         = H5VL_REQUEST_GET_ERR_STACK;
            vol_cb_args.args.get_err_stack.err_stack_id = H5I_INVALID_HID;

            /* Retrieve the error stack for the operation */
            if (H5VL_request_specific(ev->request, &vol_cb_args) < 0)
                HGOTO_ERROR(H5E_EVENTSET, H5E_CANTGET, FAIL, "unable to retrieve error stack for operation");

            /* Set values */
            err_stack_id = vol_cb_args.args.get_err_stack.err_stack_id;

            if ((es->comp_func)(&ev->op_info, H5ES_STATUS_FAIL, err_stack_id, es->comp_ctx) < 0)
                HGOTO_ERROR(H5E_EVENTSET, H5E_CALLBACK, FAIL, "'complete' callback for event set failed");
        } /* end if */

        /* Handle failure */
        if (H5ES__handle_fail(es, ev) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTSET, FAIL, "unable to handle failed event");
    } /* end else-if */
    else
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADVALUE, FAIL, "unknown event status?!?");

done:
    /* Clean up */
    if (H5I_INVALID_HID != err_stack_id)
        if (H5I_dec_ref(err_stack_id) < 0)
            HDONE_ERROR(H5E_EVENTSET, H5E_CANTDEC, FAIL,
                        "unable to decrement ref count on error stack for failed operation")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__op_complete() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__wait_cb
 *
 * Purpose:     Common routine for testing / waiting on an operation
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static int
H5ES__wait_cb(H5ES_event_t *ev, void *_ctx)
{
    H5ES_wait_ctx_t      *ctx       = (H5ES_wait_ctx_t *)_ctx;     /* Callback context */
    H5VL_request_status_t ev_status = H5VL_REQUEST_STATUS_SUCCEED; /* Status from event's operation */
    uint64_t start_time = 0, elapsed_time = 0; /* Start and elapsed times for waiting on an operation */
    int      ret_value = H5_ITER_CONT;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ev);
    assert(ctx);

    /* Wait on the request */
    if (ctx->timeout != H5ES_WAIT_NONE && ctx->timeout != H5ES_WAIT_FOREVER)
        start_time = H5_now_usec();
    if (H5VL_request_wait(ev->request, ctx->timeout, &ev_status) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTWAIT, H5_ITER_ERROR, "unable to test operation");
    if (ctx->timeout != H5ES_WAIT_NONE && ctx->timeout != H5ES_WAIT_FOREVER)
        elapsed_time = H5_now_usec() - start_time;

    /* Check for status values that indicate we should break out of the loop */
    if (ev_status == H5VL_REQUEST_STATUS_FAIL) {
        /* Handle event completion */
        if (H5ES__op_complete(ctx->es, ev, ev_status) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, H5_ITER_ERROR, "unable to release completed event");

        /* Record the error */
        *ctx->op_failed = true;

        /* Exit from the iteration */
        ret_value = H5_ITER_STOP;
    } /* end if */
    else if (ev_status == H5VL_REQUEST_STATUS_SUCCEED || ev_status == H5VL_REQUEST_STATUS_CANCELED) {
        /* Handle event completion */
        if (H5ES__op_complete(ctx->es, ev, ev_status) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, H5_ITER_ERROR, "unable to release completed event");
    } /* end else-if */
    else if (ev_status == H5VL_REQUEST_STATUS_CANT_CANCEL)
        /* Should never get a status of 'can't cancel' back from test / wait operation */
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADVALUE, H5_ITER_ERROR,
                    "received \"can't cancel\" status for operation");
    else {
        /* Sanity check */
        assert(ev_status == H5VL_REQUEST_STATUS_IN_PROGRESS);

        /* Increment "in progress operation" counter */
        (*ctx->num_in_progress)++;
    } /* end if */

    /* Check for updateable timeout */
    if (ctx->timeout != H5ES_WAIT_NONE && ctx->timeout != H5ES_WAIT_FOREVER) {
        /* Update timeout for next operation */
        if ((elapsed_time * 1000) > ctx->timeout)
            ctx->timeout = H5ES_WAIT_NONE;
        else
            ctx->timeout -= (elapsed_time * 1000); /* Convert us to ns */
    }                                              /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__wait_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__wait
 *
 * Purpose:     Wait for operations in event set to complete
 *
 * Note:        Timeout value is in ns, and is for H5ES__wait itself.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__wait(H5ES_t *es, uint64_t timeout, size_t *num_in_progress, bool *op_failed)
{
    H5ES_wait_ctx_t ctx;                 /* Iterator callback context info */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);
    assert(num_in_progress);
    assert(op_failed);

    /* Set user's parameters to known values */
    *num_in_progress = 0;
    *op_failed       = false;

    /* Set up context for iterator callbacks */
    ctx.es              = es;
    ctx.timeout         = timeout;
    ctx.num_in_progress = num_in_progress;
    ctx.op_failed       = op_failed;

    /* Iterate over the events in the set, waiting for them to complete */
    if (H5ES__list_iterate(&es->active, H5_ITER_NATIVE, H5ES__wait_cb, &ctx) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADITER, FAIL, "iteration failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__wait() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__cancel_cb
 *
 * Purpose:     Callback for canceling operations
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static int
H5ES__cancel_cb(H5ES_event_t *ev, void *_ctx)
{
    H5ES_cancel_ctx_t    *ctx       = (H5ES_cancel_ctx_t *)_ctx;   /* Callback context */
    H5VL_request_status_t ev_status = H5VL_REQUEST_STATUS_SUCCEED; /* Status from event's operation */
    int                   ret_value = H5_ITER_CONT;                /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ev);
    assert(ctx);

    /* Attempt to cancel the request */
    if (H5VL_request_cancel(ev->request, &ev_status) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTCANCEL, H5_ITER_ERROR, "unable to cancel operation");

    /* Check for status values that indicate we should break out of the loop */
    if (ev_status == H5VL_REQUEST_STATUS_FAIL) {
        /* Handle event completion */
        if (H5ES__op_complete(ctx->es, ev, ev_status) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTSET, H5_ITER_ERROR, "unable to handle failed event");

        /* Record the error */
        *ctx->op_failed = true;

        /* Exit from the iteration */
        ret_value = H5_ITER_STOP;
    } /* end if */
    else if (ev_status == H5VL_REQUEST_STATUS_SUCCEED) {
        /* Increment "not canceled" counter */
        (*ctx->num_not_canceled)++;

        /* Handle event completion */
        if (H5ES__op_complete(ctx->es, ev, ev_status) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, H5_ITER_ERROR, "unable to release completed event");
    } /* end else-if */
    else if (ev_status == H5VL_REQUEST_STATUS_CANT_CANCEL || ev_status == H5VL_REQUEST_STATUS_IN_PROGRESS) {
        /* Increment "not canceled" counter */
        (*ctx->num_not_canceled)++;
    } /* end else-if */
    else {
        /* Sanity check */
        assert(ev_status == H5VL_REQUEST_STATUS_CANCELED);

        /* Handle event completion */
        if (H5ES__op_complete(ctx->es, ev, ev_status) < 0)
            HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, H5_ITER_ERROR, "unable to release completed event");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__cancel_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__cancel
 *
 * Purpose:     Cancel operations in event set
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__cancel(H5ES_t *es, size_t *num_not_canceled, bool *op_failed)
{
    H5ES_cancel_ctx_t ctx;                 /* Iterator callback context info */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);
    assert(num_not_canceled);
    assert(op_failed);

    /* Set user's parameters to known values */
    *num_not_canceled = 0;
    *op_failed        = false;

    /* Set up context for iterator callbacks */
    ctx.es               = es;
    ctx.num_not_canceled = num_not_canceled;
    ctx.op_failed        = op_failed;

    /* Iterate over the events in the set, attempting to cancel them */
    if (H5ES__list_iterate(&es->active, H5_ITER_NATIVE, H5ES__cancel_cb, &ctx) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADITER, FAIL, "iteration failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__cancel() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__get_err_info_cb
 *
 * Purpose:     Retrieve information about a failed operation
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static int
H5ES__get_err_info_cb(H5ES_event_t *ev, void *_ctx)
{
    H5VL_request_specific_args_t vol_cb_args;                        /* Arguments to VOL callback */
    H5ES_gei_ctx_t              *ctx       = (H5ES_gei_ctx_t *)_ctx; /* Callback context */
    int                          ret_value = H5_ITER_CONT;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ev);
    assert(ctx);

    /* Copy operation info for event */
    /* The 'app_func_name', 'app_file_name', and 'api_name' strings are statically allocated (by the compiler)
     * so there's no need to duplicate them internally, but they are duplicated
     * here, when they are given back to the user.
     */
    if (NULL == (ctx->curr_err_info->api_name = H5MM_xstrdup(ev->op_info.api_name)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, H5_ITER_ERROR, "can't copy HDF5 API routine name");
    if (NULL == (ctx->curr_err_info->api_args = H5MM_xstrdup(ev->op_info.api_args)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, H5_ITER_ERROR, "can't copy HDF5 API routine arguments");
    if (NULL == (ctx->curr_err_info->app_file_name = H5MM_xstrdup(ev->op_info.app_file_name)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, H5_ITER_ERROR, "can't copy HDF5 application file name");
    if (NULL == (ctx->curr_err_info->app_func_name = H5MM_xstrdup(ev->op_info.app_func_name)))
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTALLOC, H5_ITER_ERROR, "can't copy HDF5 application function name");
    ctx->curr_err_info->app_line_num = ev->op_info.app_line_num;
    ctx->curr_err_info->op_ins_count = ev->op_info.op_ins_count;
    ctx->curr_err_info->op_ins_ts    = ev->op_info.op_ins_ts;
    ctx->curr_err_info->op_exec_ts   = ev->op_info.op_exec_ts;
    ctx->curr_err_info->op_exec_time = ev->op_info.op_exec_time;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                         = H5VL_REQUEST_GET_ERR_STACK;
    vol_cb_args.args.get_err_stack.err_stack_id = H5I_INVALID_HID;

    /* Get error stack for event */
    if (H5VL_request_specific(ev->request, &vol_cb_args) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTGET, H5_ITER_ERROR, "unable to retrieve error stack for operation");

    /* Set value */
    ctx->curr_err_info->err_stack_id = vol_cb_args.args.get_err_stack.err_stack_id;

    /* Remove event from event set's failed list */
    H5ES__list_remove(&ctx->es->failed, ev);

    /* Free event node */
    if (H5ES__event_free(ev) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, H5_ITER_ERROR, "unable to release failed event");

    /* Advance to next element of err_info[] array */
    ctx->curr_err++;
    ctx->curr_err_info++;

    /* Stop iteration if err_info[] array is full */
    if (ctx->curr_err == ctx->num_err_info)
        ret_value = H5_ITER_STOP;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__get_err_info_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__get_err_info
 *
 * Purpose:     Retrieve information about failed operations
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__get_err_info(H5ES_t *es, size_t num_err_info, H5ES_err_info_t err_info[], size_t *num_cleared)
{
    H5ES_gei_ctx_t ctx;                 /* Iterator callback context info */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);
    assert(num_err_info);
    assert(err_info);
    assert(num_cleared);

    /* Set up context for iterator callbacks */
    ctx.es            = es;
    ctx.num_err_info  = num_err_info;
    ctx.curr_err      = 0;
    ctx.curr_err_info = &err_info[0];

    /* Iterate over the failed events in the set, copying their error info */
    if (H5ES__list_iterate(&es->failed, H5_ITER_NATIVE, H5ES__get_err_info_cb, &ctx) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADITER, FAIL, "iteration failed");

    /* Set # of failed events cleared from event set's failed list */
    *num_cleared = ctx.curr_err;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__get_err_info() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__close_failed_cb
 *
 * Purpose:     Release a failed event
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static int
H5ES__close_failed_cb(H5ES_event_t *ev, void *_ctx)
{
    H5ES_t *es        = (H5ES_t *)_ctx; /* Callback context */
    int     ret_value = H5_ITER_CONT;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ev);
    assert(es);

    /* Remove event from event set's failed list */
    H5ES__list_remove(&es->failed, ev);

    /* Free event node */
    if (H5ES__event_free(ev) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_CANTRELEASE, H5_ITER_ERROR, "unable to release failed event");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__close_failed_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5ES__close
 *
 * Purpose:     Destroy an event set object
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5ES__close(H5ES_t *es)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(es);

    /* Fail if active operations still present */
    if (H5ES__list_count(&es->active) > 0)
        HGOTO_ERROR(
            H5E_EVENTSET, H5E_CANTCLOSEOBJ, FAIL,
            "can't close event set while unfinished operations are present (i.e. wait on event set first)");

    /* Iterate over the failed events in the set, releasing them */
    if (H5ES__list_iterate(&es->failed, H5_ITER_NATIVE, H5ES__close_failed_cb, (void *)es) < 0)
        HGOTO_ERROR(H5E_EVENTSET, H5E_BADITER, FAIL, "iteration failed");

    /* Release the event set */
    es = H5FL_FREE(H5ES_t, es);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5ES__close() */

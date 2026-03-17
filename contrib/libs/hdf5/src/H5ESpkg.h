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
 * Purpose: This file contains declarations which are visible only within
 *          the H5ES package.  Source files outside the H5ES package should
 *          include H5ESprivate.h instead.
 */
#if !(defined H5ES_FRIEND || defined H5ES_MODULE)
#error "Do not include this file outside the H5ES package!"
#endif

#ifndef H5ESpkg_H
#define H5ESpkg_H

/* Get package's private header */
#include "H5ESprivate.h"

/* Other private headers needed by this file */

/**************************/
/* Package Private Macros */
/**************************/

/****************************/
/* Package Private Typedefs */
/****************************/

/* Typedef for event nodes */
typedef struct H5ES_event_t {
    H5VL_object_t       *request;     /* Request token for event */
    struct H5ES_event_t *prev, *next; /* Previous and next event nodes */

    H5ES_op_info_t op_info; /* Useful info about operation */
} H5ES_event_t;

/* Typedef for lists of event set operations */
typedef struct H5ES_event_list_t {
    size_t        count;       /* # of events in list */
    H5ES_event_t *head, *tail; /* Head & tail of events in list */
} H5ES_event_list_t;

/* Typedef for event set objects */
struct H5ES_t {
    uint64_t                   op_counter; /* Count of operations inserted into this set */
    H5ES_event_insert_func_t   ins_func;   /* Callback to invoke for operation inserts */
    void                      *ins_ctx;    /* Context for callback to invoke for operation inserts */
    H5ES_event_complete_func_t comp_func;  /* Callback to invoke for operation completions */
    void                      *comp_ctx;   /* Context for callback to invoke for operation inserts */

    /* Active events */
    H5ES_event_list_t active; /* List of active events in set */

    /* Failed events */
    bool              err_occurred; /* Flag for error from an operation */
    H5ES_event_list_t failed;       /* List of failed events in set */
};

/* Event list iterator callback function */
typedef int (*H5ES_list_iter_func_t)(H5ES_event_t *ev, void *ctx);

/*****************************/
/* Package Private Variables */
/*****************************/

/******************************/
/* Package Private Prototypes */
/******************************/
H5_DLL H5ES_t *H5ES__create(void);
H5_DLL herr_t  H5ES__insert_request(H5ES_t *es, H5VL_t *connector, void *token);
H5_DLL herr_t  H5ES__wait(H5ES_t *es, uint64_t timeout, size_t *num_in_progress, bool *op_failed);
H5_DLL herr_t  H5ES__get_requests(H5ES_t *es, H5_iter_order_t order, hid_t *connector_ids, void **requests,
                                  size_t array_len);
H5_DLL herr_t  H5ES__cancel(H5ES_t *es, size_t *num_not_canceled, bool *op_failed);
H5_DLL herr_t  H5ES__get_err_info(H5ES_t *es, size_t num_err_info, H5ES_err_info_t err_info[],
                                  size_t *num_cleared);

/* Event list operations */
H5_DLL void   H5ES__list_append(H5ES_event_list_t *el, H5ES_event_t *ev);
H5_DLL size_t H5ES__list_count(const H5ES_event_list_t *el);
H5_DLL int    H5ES__list_iterate(H5ES_event_list_t *el, H5_iter_order_t order, H5ES_list_iter_func_t cb,
                                 void *ctx);
H5_DLL void   H5ES__list_remove(H5ES_event_list_t *el, const H5ES_event_t *ev);

/* Event operations */
H5_DLL H5ES_event_t *H5ES__event_new(H5VL_t *connector, void *token);
H5_DLL herr_t        H5ES__event_free(H5ES_event_t *ev);
H5_DLL herr_t        H5ES__event_completed(H5ES_event_t *ev, H5ES_event_list_t *el);

#endif /* H5ESpkg_H */

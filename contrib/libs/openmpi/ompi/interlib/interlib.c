/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "opal/mca/pmix/pmix.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/interlib/interlib.h"

#include "mpi.h"

typedef struct {
    int status;
    volatile bool active;
} myreg_t;

/*
 * errhandler id
 */
static size_t interlibhandler_id = SIZE_MAX;


static void model_registration_callback(int status,
                                        size_t errhandler_ref,
                                        void *cbdata)
{
    myreg_t *trk = (myreg_t*)cbdata;

    trk->status = status;
    interlibhandler_id = errhandler_ref;
    trk->active = false;
}
static void model_callback(int status,
                           const opal_process_name_t *source,
                           opal_list_t *info, opal_list_t *results,
                           opal_pmix_notification_complete_fn_t cbfunc,
                           void *cbdata)
{
    opal_value_t *val;

    if (NULL != getenv("OMPI_SHOW_MODEL_CALLBACK")) {
        /* we can ignore our own callback as we obviously
         * know that we are MPI */
        if (NULL != info) {
            OPAL_LIST_FOREACH(val, info, opal_value_t) {
                if (0 == strcmp(val->key, OPAL_PMIX_PROGRAMMING_MODEL) &&
                    0 == strcmp(val->data.string, "MPI")) {
                    goto cback;
                }
                if (OPAL_STRING == val->type) {
                        opal_output(0, "OMPI Model Callback Key: %s Val %s", val->key, val->data.string);
                }
            }
        }
    }
    /* otherwise, do something clever here */

  cback:
    /* we must NOT tell the event handler state machine that we
     * are the last step as that will prevent it from notifying
     * anyone else that might be listening for declarations */
    if (NULL != cbfunc) {
        cbfunc(OMPI_SUCCESS, NULL, NULL, NULL, cbdata);
    }
}

int ompi_interlib_declare(int threadlevel, char *version)
{
    opal_list_t info, directives;
    opal_value_t *kv;
    myreg_t trk;
    int ret;

    /* Register an event handler for library model declarations  */
    trk.status = OPAL_ERROR;
    trk.active = true;
    /* give it a name so we can distinguish it */
    OBJ_CONSTRUCT(&directives, opal_list_t);
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_EVENT_HDLR_NAME);
    kv->type = OPAL_STRING;
    kv->data.string = strdup("MPI-Model-Declarations");
    opal_list_append(&directives, &kv->super);
    /* specify the event code */
    OBJ_CONSTRUCT(&info, opal_list_t);
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup("status");   // the key here is irrelevant
    kv->type = OPAL_INT;
    kv->data.integer = OPAL_ERR_MODEL_DECLARED;
    opal_list_append(&info, &kv->super);
    /* we could constrain the range to proc_local - technically, this
     * isn't required so long as the code that generates
     * the event stipulates its range as proc_local. We rely
     * on that here */
    opal_pmix.register_evhandler(&info, &directives, model_callback,
                                 model_registration_callback,
                                 (void*)&trk);
    OMPI_LAZY_WAIT_FOR_COMPLETION(trk.active);

    OPAL_LIST_DESTRUCT(&directives);
    OPAL_LIST_DESTRUCT(&info);
    if (OPAL_SUCCESS != trk.status) {
        return trk.status;
    }

    /* declare that we are present and active */
    OBJ_CONSTRUCT(&info, opal_list_t);
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_PROGRAMMING_MODEL);
    kv->type = OPAL_STRING;
    kv->data.string = strdup("MPI");
    opal_list_append(&info, &kv->super);
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_MODEL_LIBRARY_NAME);
    kv->type = OPAL_STRING;
    kv->data.string = strdup("OpenMPI");
    opal_list_append(&info, &kv->super);
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_MODEL_LIBRARY_VERSION);
    kv->type = OPAL_STRING;
    kv->data.string = strdup(version);
    opal_list_append(&info, &kv->super);
    kv = OBJ_NEW(opal_value_t);
    kv->key = strdup(OPAL_PMIX_THREADING_MODEL);
    kv->type = OPAL_STRING;
    if (MPI_THREAD_SINGLE == threadlevel) {
        kv->data.string = strdup("NONE");
    } else {
        kv->data.string = strdup("PTHREAD");
    }
    opal_list_append(&info, &kv->super);
    /* call pmix to initialize these values */
    ret = opal_pmix.init(&info);
    OPAL_LIST_DESTRUCT(&info);
    /* account for our refcount on pmix_init */
    opal_pmix.finalize();
    return ret;
}

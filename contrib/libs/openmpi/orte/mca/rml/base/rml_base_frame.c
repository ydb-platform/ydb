/*
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>

#include "opal/dss/dss.h"
#include "orte/mca/mca.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/util/output.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_wait.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"

#include "orte/mca/rml/base/base.h"

/* The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct. */
#include "orte/mca/rml/base/static-components.h"


/* Initialising stub fns in the global var used by other modules */
orte_rml_base_API_t orte_rml = {
    .ping                   = orte_rml_API_ping,
    .send_nb                = orte_rml_API_send_nb,
    .send_buffer_nb         = orte_rml_API_send_buffer_nb,
    .recv_nb                = orte_rml_API_recv_nb,
    .recv_buffer_nb         = orte_rml_API_recv_buffer_nb,
    .recv_cancel            = orte_rml_API_recv_cancel,
    .purge                  = orte_rml_API_purge,
    .query_transports       = orte_rml_API_query_transports,
    .open_conduit           = orte_rml_API_open_conduit,
    .close_conduit          = orte_rml_API_close_conduit,
    .get_routed             = orte_rml_API_get_routed
};

orte_rml_base_t orte_rml_base = {{{0}}};

orte_rml_component_t *orte_rml_component = NULL;

static bool selected = false;

static int orte_rml_base_register(mca_base_register_flag_t flags)
{
    orte_rml_base.max_retries = 3;
    mca_base_var_register("orte", "rml", "base", "max_retries",
                           "Max #times to retry sending a message",
                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                           OPAL_INFO_LVL_9,
                           MCA_BASE_VAR_SCOPE_READONLY,
                           &orte_rml_base.max_retries);

#if OPAL_ENABLE_TIMING
    orte_rml_base.timing = false;
    (void) mca_base_var_register ("orte", "rml", "base", "timing",
                                  "Enable RML timings",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_rml_base.timing);
#endif

    return ORTE_SUCCESS;
}

static void cleanup(int sd, short args, void *cbdata)
{
    orte_lock_t *lk = (orte_lock_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(active);
    OPAL_LIST_DESTRUCT(&orte_rml_base.posted_recvs);
    if (NULL != lk) {
        ORTE_POST_OBJECT(lk);
        ORTE_WAKEUP_THREAD(lk);
    }
}

static int orte_rml_base_close(void)
{
    orte_lock_t lock;
    int idx, total_conduits = opal_pointer_array_get_size(&orte_rml_base.conduits);
    orte_rml_base_module_t *mod;
    orte_rml_component_t *comp;

     /* cycle thru the conduits opened and call each module's finalize */
     /* The components finalise/close() will be responsible for freeing the module pointers   */
    for (idx = 0; idx < total_conduits ; idx++)
    {
        if( NULL != (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits,idx))) {
            /* close the conduit */
            comp = (orte_rml_component_t*)mod->component;
            if (NULL != comp && NULL != comp->close_conduit) {
                comp->close_conduit(mod);
            }
            free(mod);
        }

    }
    OBJ_DESTRUCT(&orte_rml_base.conduits);

    OPAL_LIST_DESTRUCT(&orte_rml_base.actives)

    /* because the RML posted recvs list is in a separate
     * async thread for apps, we can't just destruct it here.
     * Instead, we push it into that event thread and destruct
     * it there */
     if (ORTE_PROC_IS_APP) {
        opal_event_t ev;
        ORTE_CONSTRUCT_LOCK(&lock);
        opal_event_set(orte_event_base, &ev, -1,
                       OPAL_EV_WRITE, cleanup, (void*)&lock);
        opal_event_set_priority(&ev, ORTE_ERROR_PRI);
        ORTE_POST_OBJECT(ev);
        opal_event_active(&ev, OPAL_EV_WRITE, 1);
        ORTE_WAIT_THREAD(&lock);
        ORTE_DESTRUCT_LOCK(&lock);
     } else {
        /* we can call the destruct directly */
        cleanup(0, 0, NULL);
     }

    return mca_base_framework_components_close(&orte_rml_base_framework, NULL);
}

static int orte_rml_base_open(mca_base_open_flag_t flags)
{
    /* Initialize globals */
    /* construct object for holding the active plugin modules */
    OBJ_CONSTRUCT(&orte_rml_base.actives, opal_list_t);
    OBJ_CONSTRUCT(&orte_rml_base.posted_recvs, opal_list_t);
    OBJ_CONSTRUCT(&orte_rml_base.unmatched_msgs, opal_list_t);
    OBJ_CONSTRUCT(&orte_rml_base.conduits, opal_pointer_array_t);
    opal_pointer_array_init(&orte_rml_base.conduits,1,INT16_MAX,1);

    /* Open up all available components */
    return mca_base_framework_components_open(&orte_rml_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, rml, "ORTE Run-Time Messaging Layer",
                           orte_rml_base_register, orte_rml_base_open, orte_rml_base_close,
                           mca_rml_base_static_components, 0);

OBJ_CLASS_INSTANCE(orte_rml_base_active_t,
                   opal_list_item_t,
                   NULL, NULL);

/**
 * Function for ordering the component(plugin) by priority
 */
int orte_rml_base_select(void)
{
   mca_base_component_list_item_t *cli=NULL;
   orte_rml_component_t *component=NULL;
   orte_rml_base_active_t *newmodule, *mod;
   bool inserted;

   if (selected) {
      return ORTE_SUCCESS;
   }
   selected = true;

   OPAL_LIST_FOREACH(cli, &orte_rml_base_framework.framework_components, mca_base_component_list_item_t ) {
       component = (orte_rml_component_t*) cli->cli_component;

       opal_output_verbose(10, orte_rml_base_framework.framework_output,
                           "orte_rml_base_select: Initializing %s component %s",
                            component->base.mca_type_name,
                            component->base.mca_component_name);

       /* add to the list of available components */
       newmodule = OBJ_NEW(orte_rml_base_active_t);
       newmodule->pri = component->priority;
       newmodule->component = component;

       /* maintain priority order */
       inserted = false;
       OPAL_LIST_FOREACH(mod, &orte_rml_base.actives, orte_rml_base_active_t) {
           if (newmodule->pri > mod->pri) {
               opal_list_insert_pos(&orte_rml_base.actives,
                            (opal_list_item_t*)mod, &newmodule->super);
               inserted = true;
               break;
           }
       }
       if (!inserted) {
           /* must be lowest priority - add to end */
           opal_list_append(&orte_rml_base.actives, &newmodule->super);
       }
    }

    if (4 < opal_output_get_verbosity(orte_rml_base_framework.framework_output)) {
        opal_output(0, "%s: Final rml priorities", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        /* show the prioritized list */
        OPAL_LIST_FOREACH(mod, &orte_rml_base.actives, orte_rml_base_active_t) {
            opal_output(0, "\tComponent: %s Priority: %d", mod->component->base.mca_component_name, mod->pri);
        }
    }

    return ORTE_SUCCESS;
}

void orte_rml_send_callback(int status, orte_process_name_t *peer,
                            opal_buffer_t* buffer, orte_rml_tag_t tag,
                            void* cbdata)

{
    OBJ_RELEASE(buffer);
    if (ORTE_SUCCESS != status) {
        opal_output_verbose(2, orte_rml_base_framework.framework_output,
                            "%s UNABLE TO SEND MESSAGE TO %s TAG %d: %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(peer), tag,
                            ORTE_ERROR_NAME(status));
        if (ORTE_ERR_NO_PATH_TO_TARGET == status) {
            ORTE_ACTIVATE_PROC_STATE(peer, ORTE_PROC_STATE_NO_PATH_TO_TARGET);
        } else if (ORTE_ERR_ADDRESSEE_UNKNOWN == status) {
            ORTE_ACTIVATE_PROC_STATE(peer, ORTE_PROC_STATE_PEER_UNKNOWN);
        } else {
            ORTE_ACTIVATE_PROC_STATE(peer, ORTE_PROC_STATE_UNABLE_TO_SEND_MSG);
        }
    }
}

void orte_rml_recv_callback(int status, orte_process_name_t* sender,
                            opal_buffer_t *buffer,
                            orte_rml_tag_t tag, void *cbdata)
{
    orte_rml_recv_cb_t *blob = (orte_rml_recv_cb_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(blob);
    /* transfer the sender */
    blob->name.jobid = sender->jobid;
    blob->name.vpid = sender->vpid;
    /* just copy the payload to the buf */
    opal_dss.copy_payload(&blob->data, buffer);
    /* flag as complete */
    ORTE_POST_OBJECT(blob);
    blob->active = false;
}


/***   RML CLASS INSTANCES   ***/
static void xfer_cons(orte_self_send_xfer_t *xfer)
{
    xfer->iov = NULL;
    xfer->cbfunc.iov = NULL;
    xfer->buffer = NULL;
    xfer->cbfunc.buffer = NULL;
    xfer->cbdata = NULL;
}
OBJ_CLASS_INSTANCE(orte_self_send_xfer_t,
                   opal_object_t,
                   xfer_cons, NULL);

static void send_cons(orte_rml_send_t *ptr)
{
    ptr->retries = 0;
    ptr->cbdata = NULL;
    ptr->iov = NULL;
    ptr->buffer = NULL;
    ptr->data = NULL;
    ptr->seq_num = 0xFFFFFFFF;
    ptr->routed = NULL;
}
static void send_des(orte_rml_send_t *ptr)
{
    if (NULL != ptr->routed) {
        free(ptr->routed);
    }
}
OBJ_CLASS_INSTANCE(orte_rml_send_t,
                   opal_list_item_t,
                   send_cons, send_des);


static void send_req_cons(orte_rml_send_request_t *ptr)
{
    OBJ_CONSTRUCT(&ptr->send, orte_rml_send_t);
}
static void send_req_des(orte_rml_send_request_t *ptr)
{
    OBJ_DESTRUCT(&ptr->send);
}
OBJ_CLASS_INSTANCE(orte_rml_send_request_t,
                   opal_object_t,
                   send_req_cons, send_req_des);

static void recv_cons(orte_rml_recv_t *ptr)
{
    ptr->iov.iov_base = NULL;
    ptr->iov.iov_len = 0;
}
static void recv_des(orte_rml_recv_t *ptr)
{
    if (NULL != ptr->iov.iov_base) {
        free(ptr->iov.iov_base);
    }
}
OBJ_CLASS_INSTANCE(orte_rml_recv_t,
                   opal_list_item_t,
                   recv_cons, recv_des);

static void rcv_cons(orte_rml_recv_cb_t *ptr)
{
    OBJ_CONSTRUCT(&ptr->data, opal_buffer_t);
    ptr->active = false;
}
static void rcv_des(orte_rml_recv_cb_t *ptr)
{
    OBJ_DESTRUCT(&ptr->data);
}
OBJ_CLASS_INSTANCE(orte_rml_recv_cb_t, opal_object_t,
                   rcv_cons, rcv_des);

static void prcv_cons(orte_rml_posted_recv_t *ptr)
{
    ptr->cbdata = NULL;
}
OBJ_CLASS_INSTANCE(orte_rml_posted_recv_t,
                   opal_list_item_t,
                   prcv_cons, NULL);

static void prq_cons(orte_rml_recv_request_t *ptr)
{
    ptr->cancel = false;
    ptr->post = OBJ_NEW(orte_rml_posted_recv_t);
}
static void prq_des(orte_rml_recv_request_t *ptr)
{
    if (NULL != ptr->post) {
        OBJ_RELEASE(ptr->post);
    }
}
OBJ_CLASS_INSTANCE(orte_rml_recv_request_t,
                   opal_object_t,
                   prq_cons, prq_des);

static void pthcons(orte_rml_pathway_t *p)
{
    p->component = NULL;
    OBJ_CONSTRUCT(&p->attributes, opal_list_t);
    OBJ_CONSTRUCT(&p->transports, opal_list_t);
}
static void pthdes(orte_rml_pathway_t *p)
{
    if (NULL != p->component) {
        free(p->component);
    }
    OPAL_LIST_DESTRUCT(&p->attributes);
    OPAL_LIST_DESTRUCT(&p->transports);
}
OBJ_CLASS_INSTANCE(orte_rml_pathway_t,
                   opal_list_item_t,
                   pthcons, pthdes);

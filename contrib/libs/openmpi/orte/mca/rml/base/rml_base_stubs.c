/*
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
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

#include "opal/class/opal_list.h"
#include "opal/dss/dss.h"
#include "orte/mca/mca.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_wait.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"

#include "orte/mca/rml/base/base.h"

/*
 * The stub API interface functions
 */

/** Open a conduit  - check if the ORTE_RML_INCLUDE_COMP attribute is provided, this is     */
/*  a comma seperated list of components, try to open the conduit in this order.            */
/*  if the ORTE_RML_INCLUDE_COMP is not provided or this list was not able to open conduit  */
/*  call the open_conduit() of the component in priority order to see if they can use the   */
/*  attribute to open a conduit.                                                            */
/*  Note:  The component takes care of checking for duplicate and returning the previously  */
/*  opened module* in case of duplicates. Currently we are saving it in a new conduit_id    */
/*  even if it is duplicate. [ToDo] compare the module* received from component to see if   */
/*  already present in array and return the prev conduit_id instead of adding it again to array */
/* @param[in]   attributes  The attributes is a list of opal_value_t of type OPAL_STRING    */
orte_rml_conduit_t orte_rml_API_open_conduit(opal_list_t *attributes)
{
    orte_rml_base_active_t *active;
    orte_rml_component_t *comp;
    orte_rml_base_module_t *mod, *ourmod=NULL;
    int rc;

    opal_output_verbose(10,orte_rml_base_framework.framework_output,
                         "%s rml:base:open_conduit",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* bozo check - you cannot specify both include and exclude */
    if (orte_get_attribute(attributes, ORTE_RML_INCLUDE_COMP_ATTRIB, NULL, OPAL_STRING) &&
        orte_get_attribute(attributes, ORTE_RML_EXCLUDE_COMP_ATTRIB, NULL, OPAL_STRING)) {
       // orte_show_help();
        return ORTE_ERR_NOT_SUPPORTED;
    }

    /* cycle thru the actives in priority order and let each one see if they can support this request */
    OPAL_LIST_FOREACH(active, &orte_rml_base.actives, orte_rml_base_active_t) {
        comp = (orte_rml_component_t *)active->component;
        if (NULL != comp->open_conduit) {
            if (NULL != (mod = comp->open_conduit(attributes))) {
                opal_output_verbose(2, orte_rml_base_framework.framework_output,
                                    "%s rml:base:open_conduit Component %s provided a conduit",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    active->component->base.mca_component_name);
                ourmod = mod;
                break;
            }
        }
    }
    if (NULL != ourmod) {
        /* we got an answer - store this conduit in our array */
        rc = opal_pointer_array_add(&orte_rml_base.conduits, ourmod);
        if (rc < 0) {
            return ORTE_RML_CONDUIT_INVALID;
        }
        return rc;
    }
    /* we get here if nobody could support it */
    ORTE_ERROR_LOG(ORTE_ERR_NOT_SUPPORTED);
    return ORTE_RML_CONDUIT_INVALID;
}



/** Shutdown the communication system and clean up resources */
void orte_rml_API_close_conduit(orte_rml_conduit_t id)
{
    orte_rml_base_module_t *mod;
    orte_rml_component_t *comp;

    opal_output_verbose(10,orte_rml_base_framework.framework_output,
                         "%s rml:base:close_conduit(%d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (int)id);

    if( NULL != (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits, id))) {
        comp = (orte_rml_component_t*)mod->component;
        if (NULL != comp && NULL != comp->close_conduit) {
            comp->close_conduit(mod);
        }
        opal_pointer_array_set_item(&orte_rml_base.conduits, id, NULL);
        free(mod);
    }
}



/** Ping process for connectivity check */
int orte_rml_API_ping(orte_rml_conduit_t conduit_id,
                      const char* contact_info,
                      const struct timeval* tv)
{
    int rc = ORTE_ERR_UNREACH;
    orte_rml_base_module_t *mod;

    opal_output_verbose(10,orte_rml_base_framework.framework_output,
                         "%s rml:base:ping(conduit-%d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),conduit_id);

    /* get the module */
    if (NULL == (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits, conduit_id))) {
        return rc;
    }
    if (NULL == mod->ping) {
        return rc;
    }
    rc = mod->ping((struct orte_rml_base_module_t*)mod, contact_info, tv);
    return rc;
}


/** Send non-blocking iovec message through a specific conduit*/
int orte_rml_API_send_nb(orte_rml_conduit_t conduit_id,
                         orte_process_name_t* peer,
                         struct iovec* msg,
                         int count,
                         orte_rml_tag_t tag,
                         orte_rml_callback_fn_t cbfunc,
                         void* cbdata)
{
    int rc = ORTE_ERR_UNREACH;
    orte_rml_base_module_t *mod;

    opal_output_verbose(10,orte_rml_base_framework.framework_output,
                         "%s rml:base:send_nb() to peer %s through conduit %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(peer),conduit_id);
    /* get the module */
    if (NULL == (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits, conduit_id))) {
        return rc;
    }
    if (NULL == mod->send_nb) {
        return rc;
    }
    rc = mod->send_nb((struct orte_rml_base_module_t*)mod, peer, msg, count, tag, cbfunc, cbdata);
    return rc;
}

/** Send non-blocking buffer message */
int orte_rml_API_send_buffer_nb(orte_rml_conduit_t conduit_id,
                                orte_process_name_t* peer,
                                struct opal_buffer_t* buffer,
                                orte_rml_tag_t tag,
                                orte_rml_buffer_callback_fn_t cbfunc,
                                void* cbdata)
{
    int rc = ORTE_ERR_UNREACH;
    orte_rml_base_module_t *mod;

    opal_output_verbose(10,orte_rml_base_framework.framework_output,
                         "%s rml:base:send_buffer_nb() to peer %s through conduit %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(peer),conduit_id);

    /* get the module */
    if (NULL == (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits, conduit_id))) {
        return rc;
    }
    if (NULL == mod->send_buffer_nb) {
        return rc;
    }
    rc = mod->send_buffer_nb((struct orte_rml_base_module_t*)mod, peer, buffer, tag, cbfunc, cbdata);
    return rc;
}

/** post a receive for an IOV message - this is done
 * strictly in the base, and so it does not go to a module */
void orte_rml_API_recv_nb(orte_process_name_t* peer,
                          orte_rml_tag_t tag,
                          bool persistent,
                          orte_rml_callback_fn_t cbfunc,
                          void* cbdata)
{
    orte_rml_recv_request_t *req;

    opal_output_verbose(10, orte_rml_base_framework.framework_output,
                         "%s rml_recv_nb for peer %s tag %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(peer), tag);

    /* push the request into the event base so we can add
     * the receive to our list of posted recvs */
    req = OBJ_NEW(orte_rml_recv_request_t);
    req->post->buffer_data = false;
    req->post->peer.jobid = peer->jobid;
    req->post->peer.vpid = peer->vpid;
    req->post->tag = tag;
    req->post->persistent = persistent;
    req->post->cbfunc.iov = cbfunc;
    req->post->cbdata = cbdata;
    ORTE_THREADSHIFT(req, orte_event_base, orte_rml_base_post_recv, ORTE_MSG_PRI);
}

/** Receive non-blocking buffer message */
void orte_rml_API_recv_buffer_nb(orte_process_name_t* peer,
                                 orte_rml_tag_t tag,
                                 bool persistent,
                                 orte_rml_buffer_callback_fn_t cbfunc,
                                 void* cbdata)
{
    orte_rml_recv_request_t *req;

    opal_output_verbose(10, orte_rml_base_framework.framework_output,
                         "%s rml_recv_buffer_nb for peer %s tag %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(peer), tag);

    /* push the request into the event base so we can add
     * the receive to our list of posted recvs */
    req = OBJ_NEW(orte_rml_recv_request_t);
    req->post->buffer_data = true;
    req->post->peer.jobid = peer->jobid;
    req->post->peer.vpid = peer->vpid;
    req->post->tag = tag;
    req->post->persistent = persistent;
    req->post->cbfunc.buffer = cbfunc;
    req->post->cbdata = cbdata;
    ORTE_THREADSHIFT(req, orte_event_base, orte_rml_base_post_recv, ORTE_MSG_PRI);
}

/** Cancel posted non-blocking receive */
void orte_rml_API_recv_cancel(orte_process_name_t* peer, orte_rml_tag_t tag)
{
    orte_rml_recv_request_t *req;

    opal_output_verbose(10, orte_rml_base_framework.framework_output,
                         "%s rml_recv_cancel for peer %s tag %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(peer), tag);

    ORTE_ACQUIRE_OBJECT(orte_event_base_active);
    if (!orte_event_base_active) {
        /* no event will be processed any more, so simply return. */
        return;
    }

    /* push the request into the event base so we can remove
     * the receive from our list of posted recvs */
    req = OBJ_NEW(orte_rml_recv_request_t);
    req->cancel = true;
    req->post->peer.jobid = peer->jobid;
    req->post->peer.vpid = peer->vpid;
    req->post->tag = tag;
    ORTE_THREADSHIFT(req, orte_event_base, orte_rml_base_post_recv, ORTE_MSG_PRI);
}

/** Purge information */
void orte_rml_API_purge(orte_process_name_t *peer)
{
    orte_rml_base_module_t *mod;
    int i;

    for (i=0; i < orte_rml_base.conduits.size; i++) {
        /* get the module */
        if (NULL != (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits, i))) {
            if (NULL != mod->purge) {
                mod->purge(peer);
            }
        }
    }
}


int orte_rml_API_query_transports(opal_list_t *providers)
{

    orte_rml_base_active_t *active;
    orte_rml_pathway_t *p;

    opal_output_verbose(10,orte_rml_base_framework.framework_output,
                         "%s rml:base:orte_rml_API_query_transports()",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* cycle thru the actives */
    OPAL_LIST_FOREACH(active, &orte_rml_base.actives, orte_rml_base_active_t) {
        if (NULL != active->component->query_transports) {
            opal_output_verbose(10,orte_rml_base_framework.framework_output,
                                "\n calling  module: %s->query_transports() \n",
                                active->component->base.mca_component_name);
            if (NULL != (p = active->component->query_transports())) {
                /* pass the results across */
                OBJ_RETAIN(p);
                opal_list_append(providers, &p->super);
            }
        }
    }
    return ORTE_SUCCESS;

}

char* orte_rml_API_get_routed(orte_rml_conduit_t id)
{
    orte_rml_base_module_t *mod;

    /* get the module */
    if (NULL != (mod = (orte_rml_base_module_t*)opal_pointer_array_get_item(&orte_rml_base.conduits, id))) {
        return mod->routed;
    }

    return NULL;
}

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2014 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include "opal/util/output.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/argv.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/state/state.h"
#include "orte/mca/rml/rml.h"
#include "orte/util/threads.h"
#include "orte/mca/oob/base/base.h"
#if OPAL_ENABLE_FT_CR == 1
#include "orte/mca/state/base/base.h"
#endif

static void process_uri(char *uri);

void orte_oob_base_send_nb(int fd, short args, void *cbdata)
{
    orte_oob_send_t *cd = (orte_oob_send_t*)cbdata;
    orte_rml_send_t *msg;
    mca_base_component_list_item_t *cli;
    orte_oob_base_peer_t *pr;
    int rc;
    uint64_t ui64;
    bool msg_sent;
    mca_oob_base_component_t *component;
    bool reachable;
    char *uri;

    ORTE_ACQUIRE_OBJECT(cd);

    /* done with this. release it now */
    msg = cd->msg;
    OBJ_RELEASE(cd);

    opal_output_verbose(5, orte_oob_base_framework.framework_output,
                        "%s oob:base:send to target %s - attempt %u",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&msg->dst), msg->retries);

    /* don't try forever - if we have exceeded the number of retries,
     * then report this message as undeliverable even if someone continues
     * to think they could reach it */
    if (orte_rml_base.max_retries <= msg->retries) {
        msg->status = ORTE_ERR_NO_PATH_TO_TARGET;
        ORTE_RML_SEND_COMPLETE(msg);
        return;
    }

    /* check if we have this peer in our hash table */
    memcpy(&ui64, (char*)&msg->dst, sizeof(uint64_t));
    if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&pr) ||
        NULL == pr) {
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s oob:base:send unknown peer %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&msg->dst));
        /* for direct launched procs, the URI might be in the database,
         * so check there next - if it is, the peer object will be added
         * to our hash table. However, we don't want to chase up to the
         * server after it, so indicate it is optional
         */
        OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_PROC_URI, &msg->dst,
                                      (char**)&uri, OPAL_STRING);
        if (OPAL_SUCCESS == rc ) {
            if (NULL != uri) {
                process_uri(uri);
                if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                                     ui64, (void**)&pr) ||
                    NULL == pr) {
                    /* that is just plain wrong */
                    ORTE_ERROR_LOG(ORTE_ERR_ADDRESSEE_UNKNOWN);
                    msg->status = ORTE_ERR_ADDRESSEE_UNKNOWN;
                    ORTE_RML_SEND_COMPLETE(msg);
                    return;
                }
            } else {
                ORTE_ERROR_LOG(ORTE_ERR_ADDRESSEE_UNKNOWN);
                msg->status = ORTE_ERR_ADDRESSEE_UNKNOWN;
                ORTE_RML_SEND_COMPLETE(msg);
                return;
            }
        } else {
            /* even though we don't know about this peer yet, we still might
             * be able to get to it via routing, so ask each component if
             * it can reach it
             */
            reachable = false;
            pr = NULL;
            OPAL_LIST_FOREACH(cli, &orte_oob_base.actives, mca_base_component_list_item_t) {
                component = (mca_oob_base_component_t*)cli->cli_component;
                if (NULL != component->is_reachable) {
                    if (component->is_reachable(msg->routed, &msg->dst)) {
                        /* there is a way to reach this peer - record it
                         * so we don't waste this time again
                         */
                        if (NULL == pr) {
                            pr = OBJ_NEW(orte_oob_base_peer_t);
                            if (OPAL_SUCCESS != (rc = opal_hash_table_set_value_uint64(&orte_oob_base.peers, ui64, (void*)pr))) {
                                ORTE_ERROR_LOG(rc);
                                msg->status = ORTE_ERR_ADDRESSEE_UNKNOWN;
                                ORTE_RML_SEND_COMPLETE(msg);
                                return;
                            }
                        }
                        /* mark that this component can reach the peer */
                        opal_bitmap_set_bit(&pr->addressable, component->idx);
                        /* flag that at least one component can reach this peer */
                        reachable = true;
                    }
                }
            }
            /* if nobody could reach it, then that's an error */
            if (!reachable) {
                /* if we are a daemon or HNP, then it could be that
                 * this is a local proc we just haven't heard from
                 * yet due to a race condition. Check that situation */
                if (ORTE_PROC_IS_DAEMON || ORTE_PROC_IS_HNP) {
                    ++msg->retries;
                    if (msg->retries < orte_rml_base.max_retries) {
                        ORTE_OOB_SEND(msg);
                        return;
                    }
                }
                msg->status = ORTE_ERR_ADDRESSEE_UNKNOWN;
                ORTE_RML_SEND_COMPLETE(msg);
                return;
            }
        }
    }


    /* if we already have a connection to this peer, use it */
    if (NULL != pr->component) {
        /* post this msg for send by this transport - the component
         * runs on our event base, so we can just call their function
         */
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s oob:base:send known transport for peer %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&msg->dst));
        if (ORTE_SUCCESS == (rc = pr->component->send_nb(msg))) {
            return;
        }
    }

    /* if we haven't identified a transport to this peer,
     * loop across all available components in priority order until
     * one replies that it has a module that can reach this peer.
     * Let it try to make the connection
     */
    msg_sent = false;
    OPAL_LIST_FOREACH(cli, &orte_oob_base.actives, mca_base_component_list_item_t) {
        component = (mca_oob_base_component_t*)cli->cli_component;
        /* is this peer reachable via this component? */
        if (!component->is_reachable(msg->routed, &msg->dst)) {
            continue;
        }
        /* it is addressable, so attempt to send via that transport */
        if (ORTE_SUCCESS == (rc = component->send_nb(msg))) {
            /* the msg status will be set upon send completion/failure */
            msg_sent = true;
            /* point to this transport for any future messages */
            pr->component = component;
            break;
        } else if (ORTE_ERR_TAKE_NEXT_OPTION != rc) {
            /* components return "next option" if they can't connect
             * to this peer. anything else is a true error.
             */
            ORTE_ERROR_LOG(rc);
            msg->status = rc;
            ORTE_RML_SEND_COMPLETE(msg);
            return;
        }
    }

    /* if no component can reach this peer, that's an error - post
     * it back to the RML for handling
     */
    if (!msg_sent) {
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s oob:base:send no path to target %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&msg->dst));
        msg->status = ORTE_ERR_NO_PATH_TO_TARGET;
        ORTE_RML_SEND_COMPLETE(msg);
    }
}

/**
 * Obtain a uri for initial connection purposes
 *
 * During initial wireup, we can only transfer contact info on the daemon
 * command line. This limits what we can send to a string representation of
 * the actual contact info, which gets sent in a uri-like form. Not every
 * oob module can support this transaction, so this function will loop
 * across all oob components/modules, letting each add to the uri string if
 * it supports bootstrap operations. An error will be returned in the cbfunc
 * if NO component can successfully provide a contact.
 *
 * Note: since there is a limit to what an OS will allow on a cmd line, we
 * impose a limit on the length of the resulting uri via an MCA param. The
 * default value of -1 implies unlimited - however, users with large numbers
 * of interfaces on their nodes may wish to restrict the size.
 */
void orte_oob_base_get_addr(char **uri)
{
    char *turi, *final=NULL, *tmp;
    size_t len = 0;
    int rc=ORTE_SUCCESS;
    bool one_added = false;
    mca_base_component_list_item_t *cli;
    mca_oob_base_component_t *component;
    opal_value_t val;

    /* start with our process name */
    if (ORTE_SUCCESS != (rc = orte_util_convert_process_name_to_string(&final, ORTE_PROC_MY_NAME))) {
        ORTE_ERROR_LOG(rc);
        *uri = NULL;
        return;
    }
    len = strlen(final);

    /* loop across all available modules to get their input
     * up to the max length
     */
    OPAL_LIST_FOREACH(cli, &orte_oob_base.actives, mca_base_component_list_item_t) {
        component = (mca_oob_base_component_t*)cli->cli_component;
        /* ask the component for its input, obtained when it
         * opened its modules
         */
        if (NULL == component->get_addr) {
            /* doesn't support this ability */
            continue;
        }
        /* the components operate within our event base, so we
         * can directly call their get_uri function to get the
         * pointer to the uri - this is not a copy, so
         * do NOT free it!
         */
        turi = component->get_addr();
        if (NULL != turi) {
            /* check overall length for limits */
            if (0 < orte_oob_base.max_uri_length &&
                orte_oob_base.max_uri_length < (int)(len + strlen(turi))) {
                /* cannot accept the payload */
                continue;
            }
            /* add new value to final one */
            asprintf(&tmp, "%s;%s", final, turi);
            free(turi);
            free(final);
            final = tmp;
            len = strlen(final);
            /* flag that at least one contributed */
            one_added = true;
        }
    }

    if (!one_added) {
        /* nobody could contribute */
        if (NULL != final) {
            free(final);
            final = NULL;
        }
    }

    *uri = final;
    /* push this into our modex storage */
    OBJ_CONSTRUCT(&val, opal_value_t);
    val.key = OPAL_PMIX_PROC_URI;
    val.type = OPAL_STRING;
    val.data.string = final;
    if (OPAL_SUCCESS != (rc = opal_pmix.store_local(ORTE_PROC_MY_NAME, &val))) {
        ORTE_ERROR_LOG(rc);
    }
    val.key = NULL;
    val.data.string = NULL;
    OBJ_DESTRUCT(&val);
}

static void process_uri(char *uri)
{
    orte_process_name_t peer;
    char *cptr;
    mca_base_component_list_item_t *cli;
    mca_oob_base_component_t *component;
    char **uris=NULL;
    int rc;
    uint64_t ui64;
    orte_oob_base_peer_t *pr;

    /* find the first semi-colon in the string */
    cptr = strchr(uri, ';');
    if (NULL == cptr) {
        /* got a problem - there must be at least two fields,
         * the first containing the process name of our peer
         * and all others containing the OOB contact info
         */
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return;
    }
    *cptr = '\0';
    cptr++;

    /* the first field is the process name, so convert it */
    orte_util_convert_string_to_process_name(&peer, uri);

    /* if the peer is us, no need to go further as we already
     * know our own contact info
     */
    if (peer.jobid == ORTE_PROC_MY_NAME->jobid &&
        peer.vpid == ORTE_PROC_MY_NAME->vpid) {
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s:set_addr peer %s is me",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&peer));
        return;
    }

    /* split the rest of the uri into component parts */
    uris = opal_argv_split(cptr, ';');

    /* get the peer object for this process */
    memcpy(&ui64, (char*)&peer, sizeof(uint64_t));
    if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&pr) ||
        NULL == pr) {
        pr = OBJ_NEW(orte_oob_base_peer_t);
        if (OPAL_SUCCESS != (rc = opal_hash_table_set_value_uint64(&orte_oob_base.peers, ui64, (void*)pr))) {
            ORTE_ERROR_LOG(rc);
            opal_argv_free(uris);
            return;
        }
    }

    /* loop across all available components and let them extract
     * whatever piece(s) of the uri they find relevant - they
     * are all operating on our event base, so we can just
     * directly call their functions
     */
    rc = ORTE_ERR_UNREACH;
    OPAL_LIST_FOREACH(cli, &orte_oob_base.actives, mca_base_component_list_item_t) {
        component = (mca_oob_base_component_t*)cli->cli_component;
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s:set_addr checking if peer %s is reachable via component %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(&peer), component->oob_base.mca_component_name);
        if (NULL != component->set_addr) {
            if (ORTE_SUCCESS == component->set_addr(&peer, uris)) {
                /* this component found reachable addresses
                 * in the uris
                 */
                opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                    "%s: peer %s is reachable via component %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    ORTE_NAME_PRINT(&peer), component->oob_base.mca_component_name);
                opal_bitmap_set_bit(&pr->addressable, component->idx);
            } else {
                opal_output_verbose(5, orte_oob_base_framework.framework_output,
                                    "%s: peer %s is NOT reachable via component %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    ORTE_NAME_PRINT(&peer), component->oob_base.mca_component_name);
            }
        }
    }
    opal_argv_free(uris);
}

void orte_oob_base_get_transports(opal_list_t *transports)
{
    mca_base_component_list_item_t *cli;
    mca_oob_base_component_t *component;
    orte_rml_pathway_t *p;

    opal_output_verbose(5, orte_oob_base_framework.framework_output,
                        "%s: get transports",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    OPAL_LIST_FOREACH(cli, &orte_oob_base.actives, mca_base_component_list_item_t) {
        component = (mca_oob_base_component_t*)cli->cli_component;
        opal_output_verbose(5, orte_oob_base_framework.framework_output,
                            "%s:get transports for component %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            component->oob_base.mca_component_name);
        if (NULL != component->query_transports) {
            if (NULL != (p = component->query_transports())) {
                opal_list_append(transports, &p->super);
            }
        }
    }
}

#if OPAL_ENABLE_FT_CR == 1
void orte_oob_base_ft_event(int sd, short argc, void *cbdata)
{
    int rc;
    mca_base_component_list_item_t *cli;
    mca_oob_base_component_t *component;
    orte_state_caddy_t *state = (orte_state_caddy_t*)cbdata;

    opal_output_verbose(5, orte_oob_base_framework.framework_output,
                        "%s oob:base:ft_event %s(%d)",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        orte_job_state_to_str(state->job_state),
                        state->job_state);

    /* loop across all available modules in priority order
     * and call each one's ft_event handler
     */
    OPAL_LIST_FOREACH(cli, &orte_oob_base.actives, mca_base_component_list_item_t) {
        component = (mca_oob_base_component_t*)cli->cli_component;
        if (NULL == component->ft_event) {
            /* doesn't support this ability */
            continue;
        }

        if (ORTE_SUCCESS != (rc = component->ft_event(state->job_state))) {
            ORTE_ERROR_LOG(rc);
        }
    }
    OBJ_RELEASE(state);
}

#endif

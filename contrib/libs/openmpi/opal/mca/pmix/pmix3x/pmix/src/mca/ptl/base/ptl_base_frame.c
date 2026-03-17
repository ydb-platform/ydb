/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 */
#include <src/include/pmix_config.h>

#include <pmix_common.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/class/pmix_list.h"
#include "src/client/pmix_client_ops.h"
#include "src/mca/ptl/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "src/mca/ptl/base/static-components.h"

#define PMIX_MAX_MSG_SIZE   16

/* Instantiate the global vars */
pmix_ptl_globals_t pmix_ptl_globals = {{{0}}};
int pmix_ptl_base_output = -1;

static size_t max_msg_size = PMIX_MAX_MSG_SIZE;

static int pmix_ptl_register(pmix_mca_base_register_flag_t flags)
{
    pmix_mca_base_var_register("pmix", "ptl", "base", "max_msg_size",
                               "Max size (in Mbytes) of a client/server msg",
                               PMIX_MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                               PMIX_INFO_LVL_2,
                               PMIX_MCA_BASE_VAR_SCOPE_READONLY,
                               &max_msg_size);
    pmix_ptl_globals.max_msg_size = max_msg_size * 1024 * 1024;
    return PMIX_SUCCESS;
}

static pmix_status_t pmix_ptl_close(void)
{
    if (!pmix_ptl_globals.initialized) {
        return PMIX_SUCCESS;
    }
    pmix_ptl_globals.initialized = false;

    /* ensure the listen thread has been shut down */
    pmix_ptl_base_stop_listening();

    if (NULL != pmix_client_globals.myserver) {
        if (0 <= pmix_client_globals.myserver->sd) {
            CLOSE_THE_SOCKET(pmix_client_globals.myserver->sd);
            pmix_client_globals.myserver->sd = -1;
        }
    }

    /* the components will cleanup when closed */
    PMIX_LIST_DESTRUCT(&pmix_ptl_globals.actives);
    PMIX_LIST_DESTRUCT(&pmix_ptl_globals.posted_recvs);
    PMIX_LIST_DESTRUCT(&pmix_ptl_globals.unexpected_msgs);
    PMIX_LIST_DESTRUCT(&pmix_ptl_globals.listeners);

    return pmix_mca_base_framework_components_close(&pmix_ptl_base_framework, NULL);
}

static pmix_status_t pmix_ptl_open(pmix_mca_base_open_flag_t flags)
{
    pmix_status_t rc;

    /* initialize globals */
    pmix_ptl_globals.initialized = true;
    PMIX_CONSTRUCT(&pmix_ptl_globals.actives, pmix_list_t);
    PMIX_CONSTRUCT(&pmix_ptl_globals.posted_recvs, pmix_list_t);
    PMIX_CONSTRUCT(&pmix_ptl_globals.unexpected_msgs, pmix_list_t);
    pmix_ptl_globals.listen_thread_active = false;
    PMIX_CONSTRUCT(&pmix_ptl_globals.listeners, pmix_list_t);
    pmix_ptl_globals.current_tag = PMIX_PTL_TAG_DYNAMIC;

    /* Open up all available components */
    rc = pmix_mca_base_framework_components_open(&pmix_ptl_base_framework, flags);
    pmix_ptl_base_output = pmix_ptl_base_framework.framework_output;
    return rc;
}

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, ptl, "PMIx Transfer Layer",
                                pmix_ptl_register, pmix_ptl_open, pmix_ptl_close,
                                mca_ptl_base_static_components, 0);

/***   INSTANTIATE INTERNAL CLASSES   ***/
PMIX_CLASS_INSTANCE(pmix_ptl_base_active_t,
                    pmix_list_item_t,
                    NULL, NULL);

static void scon(pmix_ptl_send_t *p)
{
    memset(&p->hdr, 0, sizeof(pmix_ptl_hdr_t));
    p->hdr.tag = UINT32_MAX;
    p->hdr.nbytes = 0;
    p->data = NULL;
    p->hdr_sent = false;
    p->sdptr = NULL;
    p->sdbytes = 0;
}
static void sdes(pmix_ptl_send_t *p)
{
    if (NULL != p->data) {
        PMIX_RELEASE(p->data);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_ptl_send_t,
                                pmix_list_item_t,
                                scon, sdes);

static void rcon(pmix_ptl_recv_t *p)
{
    p->peer = NULL;
    memset(&p->hdr, 0, sizeof(pmix_ptl_hdr_t));
    p->hdr.tag = UINT32_MAX;
    p->hdr.nbytes = 0;
    p->data = NULL;
    p->hdr_recvd = false;
    p->rdptr = NULL;
    p->rdbytes = 0;
}
static void rdes(pmix_ptl_recv_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_ptl_recv_t,
                                pmix_list_item_t,
                                rcon, rdes);

static void prcon(pmix_ptl_posted_recv_t *p)
{
    p->tag = UINT32_MAX;
    p->cbfunc = NULL;
    p->cbdata = NULL;
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_ptl_posted_recv_t,
                                pmix_list_item_t,
                                prcon, NULL);


static void srcon(pmix_ptl_sr_t *p)
{
    p->peer = NULL;
    p->bfr = NULL;
    p->cbfunc = NULL;
    p->cbdata = NULL;
}
static void srdes(pmix_ptl_sr_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_ptl_sr_t,
                                pmix_object_t,
                                srcon, srdes);

static void pccon(pmix_pending_connection_t *p)
{
    p->need_id = false;
    memset(p->nspace, 0, PMIX_MAX_NSLEN+1);
    p->info = NULL;
    p->ninfo = 0;
    p->peer = NULL;
    p->bfrops = NULL;
    p->psec = NULL;
    p->gds = NULL;
    p->ptl = NULL;
    p->cred = NULL;
    p->proc_type = PMIX_PROC_UNDEF;
}
static void pcdes(pmix_pending_connection_t *p)
{
    if (NULL != p->info) {
        PMIX_INFO_FREE(p->info, p->ninfo);
    }
    if (NULL != p->bfrops) {
        free(p->bfrops);
    }
    if (NULL != p->psec) {
        free(p->psec);
    }
    if (NULL != p->gds) {
        free(p->gds);
    }
    if (NULL != p->cred) {
        free(p->cred);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_pending_connection_t,
                                pmix_object_t,
                                pccon, pcdes);

static void lcon(pmix_listener_t *p)
{
    p->socket = -1;
    p->varname = NULL;
    p->uri = NULL;
    p->owner_given = false;
    p->group_given = false;
    p->mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
}
static void ldes(pmix_listener_t *p)
{
    if (0 <= p->socket) {
        CLOSE_THE_SOCKET(p->socket);
    }
    if (NULL != p->varname) {
        free(p->varname);
    }
    if (NULL != p->uri) {
        free(p->uri);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_listener_t,
                                pmix_list_item_t,
                                lcon, ldes);

static void qcon(pmix_ptl_queue_t *p)
{
    p->peer = NULL;
    p->buf = NULL;
    p->tag = UINT32_MAX;
}
static void qdes(pmix_ptl_queue_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
}
PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_ptl_queue_t,
                                pmix_object_t,
                                qcon, qdes);

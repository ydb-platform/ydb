/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2006-2010 University of Houston.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2016 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      Mellanox Technologies. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/dss/dss.h"
#include "opal/mca/pmix/pmix.h"

#include "ompi/proc/proc.h"
#include "ompi/communicator/communicator.h"
#include "ompi/op/op.h"
#include "ompi/constants.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_list.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/request/request.h"
#include "ompi/runtime/mpiruntime.h"

struct ompi_comm_cid_context_t;

typedef int (*ompi_comm_allreduce_impl_fn_t) (int *inbuf, int *outbuf, int count, struct ompi_op_t *op,
                                              struct ompi_comm_cid_context_t *cid_context,
                                              ompi_request_t **req);


struct ompi_comm_cid_context_t {
    opal_object_t super;

    ompi_communicator_t *newcomm;
    ompi_communicator_t **newcommp;
    ompi_communicator_t *comm;
    ompi_communicator_t *bridgecomm;

    ompi_comm_allreduce_impl_fn_t allreduce_fn;

    int nextcid;
    int nextlocal_cid;
    int start;
    int flag, rflag;
    int local_leader;
    int remote_leader;
    int iter;
    /** storage for activate barrier */
    int ok;
    char *port_string;
    bool send_first;
    int pml_tag;
    char *pmix_tag;
};

typedef struct ompi_comm_cid_context_t ompi_comm_cid_context_t;

static void mca_comm_cid_context_construct (ompi_comm_cid_context_t *context)
{
    memset ((void *) ((intptr_t) context + sizeof (context->super)), 0, sizeof (*context) - sizeof (context->super));
}

static void mca_comm_cid_context_destruct (ompi_comm_cid_context_t *context)
{
    free (context->port_string);
    free (context->pmix_tag);
}

OBJ_CLASS_INSTANCE (ompi_comm_cid_context_t, opal_object_t,
                    mca_comm_cid_context_construct,
                    mca_comm_cid_context_destruct);

struct ompi_comm_allreduce_context_t {
    opal_object_t super;

    int *inbuf;
    int *outbuf;
    int count;
    struct ompi_op_t *op;
    ompi_comm_cid_context_t *cid_context;
    int *tmpbuf;

    /* for group allreduce */
    int peers_comm[3];
};

typedef struct ompi_comm_allreduce_context_t ompi_comm_allreduce_context_t;

static void ompi_comm_allreduce_context_construct (ompi_comm_allreduce_context_t *context)
{
    memset ((void *) ((intptr_t) context + sizeof (context->super)), 0, sizeof (*context) - sizeof (context->super));
}

static void ompi_comm_allreduce_context_destruct (ompi_comm_allreduce_context_t *context)
{
    free (context->tmpbuf);
}

OBJ_CLASS_INSTANCE (ompi_comm_allreduce_context_t, opal_object_t,
                    ompi_comm_allreduce_context_construct,
                    ompi_comm_allreduce_context_destruct);

/**
 * These functions make sure, that we determine the global result over
 * an intra communicators (simple), an inter-communicator and a
 * pseudo inter-communicator described by two separate intra-comms
 * and a bridge-comm (intercomm-create scenario).
 */

/* non-blocking intracommunicator allreduce */
static int ompi_comm_allreduce_intra_nb (int *inbuf, int *outbuf, int count,
                                         struct ompi_op_t *op, ompi_comm_cid_context_t *cid_context,
                                         ompi_request_t **req);

/* non-blocking intercommunicator allreduce */
static int ompi_comm_allreduce_inter_nb (int *inbuf, int *outbuf, int count,
                                         struct ompi_op_t *op, ompi_comm_cid_context_t *cid_context,
                                         ompi_request_t **req);

static int ompi_comm_allreduce_group_nb (int *inbuf, int *outbuf, int count,
                                         struct ompi_op_t *op, ompi_comm_cid_context_t *cid_context,
                                         ompi_request_t **req);

static int ompi_comm_allreduce_intra_pmix_nb (int *inbuf, int *outbuf, int count,
                                              struct ompi_op_t *op, ompi_comm_cid_context_t *cid_context,
                                              ompi_request_t **req);

static int ompi_comm_allreduce_intra_bridge_nb (int *inbuf, int *outbuf, int count,
                                                struct ompi_op_t *op, ompi_comm_cid_context_t *cid_context,
                                                ompi_request_t **req);

static opal_mutex_t ompi_cid_lock = OPAL_MUTEX_STATIC_INIT;


int ompi_comm_cid_init (void)
{
    return OMPI_SUCCESS;
}

static ompi_comm_cid_context_t *mca_comm_cid_context_alloc (ompi_communicator_t *newcomm, ompi_communicator_t *comm,
                                                            ompi_communicator_t *bridgecomm, const void *arg0,
                                                            const void *arg1, const char *pmix_tag, bool send_first,
                                                            int mode)
{
    ompi_comm_cid_context_t *context;

    context = OBJ_NEW(ompi_comm_cid_context_t);
    if (OPAL_UNLIKELY(NULL == context)) {
        return NULL;
    }

    context->newcomm       = newcomm;
    context->comm          = comm;
    context->bridgecomm    = bridgecomm;
    context->pml_tag       = 0;

    /* Determine which implementation of allreduce we have to use
     * for the current mode. */
    switch (mode) {
    case OMPI_COMM_CID_INTRA:
        context->allreduce_fn = ompi_comm_allreduce_intra_nb;
        break;
    case OMPI_COMM_CID_INTER:
        context->allreduce_fn = ompi_comm_allreduce_inter_nb;
        break;
    case OMPI_COMM_CID_GROUP:
        context->allreduce_fn = ompi_comm_allreduce_group_nb;
        context->pml_tag = ((int *) arg0)[0];
        break;
    case OMPI_COMM_CID_INTRA_PMIX:
        context->allreduce_fn = ompi_comm_allreduce_intra_pmix_nb;
        context->local_leader = ((int *) arg0)[0];
        if (arg1) {
            context->port_string = strdup ((char *) arg1);
        }
        context->pmix_tag = strdup ((char *) pmix_tag);
        break;
    case OMPI_COMM_CID_INTRA_BRIDGE:
        context->allreduce_fn = ompi_comm_allreduce_intra_bridge_nb;
        context->local_leader = ((int *) arg0)[0];
        context->remote_leader = ((int *) arg1)[0];
        break;
    default:
        OBJ_RELEASE(context);
        return NULL;
    }

    context->send_first = send_first;
    context->iter = 0;
    context->ok = 1;

    return context;
}

static ompi_comm_allreduce_context_t *ompi_comm_allreduce_context_alloc (int *inbuf, int *outbuf,
                                                                         int count, struct ompi_op_t *op,
                                                                         ompi_comm_cid_context_t *cid_context)
{
    ompi_comm_allreduce_context_t *context;

    context = OBJ_NEW(ompi_comm_allreduce_context_t);
    if (OPAL_UNLIKELY(NULL == context)) {
        return NULL;
    }

    context->inbuf = inbuf;
    context->outbuf = outbuf;
    context->count = count;
    context->op = op;
    context->cid_context = cid_context;

    return context;
}

/* find the next available local cid and start an allreduce */
static int ompi_comm_allreduce_getnextcid (ompi_comm_request_t *request);
/* verify that the maximum cid is locally available and start an allreduce */
static int ompi_comm_checkcid (ompi_comm_request_t *request);
/* verify that the cid was available globally */
static int ompi_comm_nextcid_check_flag (ompi_comm_request_t *request);

static volatile int64_t ompi_comm_cid_lowest_id = INT64_MAX;

int ompi_comm_nextcid_nb (ompi_communicator_t *newcomm, ompi_communicator_t *comm,
                          ompi_communicator_t *bridgecomm, const void *arg0, const void *arg1,
                          bool send_first, int mode, ompi_request_t **req)
{
    ompi_comm_cid_context_t *context;
    ompi_comm_request_t *request;

    context = mca_comm_cid_context_alloc (newcomm, comm, bridgecomm, arg0, arg1,
                                          "nextcid", send_first, mode);
    if (NULL == context) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    context->start = ompi_mpi_communicators.lowest_free;

    request = ompi_comm_request_get ();
    if (NULL == request) {
        OBJ_RELEASE(context);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request->context = &context->super;

    ompi_comm_request_schedule_append (request, ompi_comm_allreduce_getnextcid, NULL, 0);
    ompi_comm_request_start (request);

    *req = &request->super;


    return OMPI_SUCCESS;
}

int ompi_comm_nextcid (ompi_communicator_t *newcomm, ompi_communicator_t *comm,
                       ompi_communicator_t *bridgecomm, const void *arg0, const void *arg1,
                       bool send_first, int mode)
{
    ompi_request_t *req;
    int rc;

    rc = ompi_comm_nextcid_nb (newcomm, comm, bridgecomm, arg0, arg1, send_first, mode, &req);
    if (OMPI_SUCCESS != rc) {
        return rc;
    }

    ompi_request_wait_completion (req);
    rc = req->req_status.MPI_ERROR;
    ompi_comm_request_return ((ompi_comm_request_t *) req);

    return rc;
}

static int ompi_comm_allreduce_getnextcid (ompi_comm_request_t *request)
{
    ompi_comm_cid_context_t *context = (ompi_comm_cid_context_t *) request->context;
    int64_t my_id = ((int64_t) ompi_comm_get_cid (context->comm) << 32 | context->pml_tag);
    ompi_request_t *subreq;
    bool flag;
    int ret;
    int participate = (context->newcomm->c_local_group->grp_my_rank != MPI_UNDEFINED);

    if (OPAL_THREAD_TRYLOCK(&ompi_cid_lock)) {
        return ompi_comm_request_schedule_append (request, ompi_comm_allreduce_getnextcid, NULL, 0);
    }

    if (ompi_comm_cid_lowest_id < my_id) {
        OPAL_THREAD_UNLOCK(&ompi_cid_lock);
        return ompi_comm_request_schedule_append (request, ompi_comm_allreduce_getnextcid, NULL, 0);
    }

    ompi_comm_cid_lowest_id = my_id;

    /**
     * This is the real algorithm described in the doc
     */
    if( participate ){
        flag = false;
        context->nextlocal_cid = mca_pml.pml_max_contextid;
        for (unsigned int i = context->start ; i < mca_pml.pml_max_contextid ; ++i) {
            flag = opal_pointer_array_test_and_set_item (&ompi_mpi_communicators, i,
                                                         context->comm);
            if (true == flag) {
                context->nextlocal_cid = i;
                break;
            }
        }
    } else {
        context->nextlocal_cid = 0;
    }

    ret = context->allreduce_fn (&context->nextlocal_cid, &context->nextcid, 1, MPI_MAX,
                                 context, &subreq);
    /* there was a failure during non-blocking collective
     * all we can do is abort
     */
    if (OMPI_SUCCESS != ret) {
        goto err_exit;
    }

    if ( ((unsigned int) context->nextlocal_cid == mca_pml.pml_max_contextid) ) {
        /* Our local CID space is out, others already aware (allreduce above) */
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto err_exit;
    }
    OPAL_THREAD_UNLOCK(&ompi_cid_lock);

    /* next we want to verify that the resulting commid is ok */
    return ompi_comm_request_schedule_append (request, ompi_comm_checkcid, &subreq, 1);
err_exit:
    if (participate && flag) {
        opal_pointer_array_test_and_set_item(&ompi_mpi_communicators, context->nextlocal_cid, NULL);
    }
    ompi_comm_cid_lowest_id = INT64_MAX;
    OPAL_THREAD_UNLOCK(&ompi_cid_lock);
    return ret;

}

static int ompi_comm_checkcid (ompi_comm_request_t *request)
{
    ompi_comm_cid_context_t *context = (ompi_comm_cid_context_t *) request->context;
    ompi_request_t *subreq;
    int ret;
    int participate = (context->newcomm->c_local_group->grp_my_rank != MPI_UNDEFINED);

    if (OPAL_THREAD_TRYLOCK(&ompi_cid_lock)) {
        return ompi_comm_request_schedule_append (request, ompi_comm_checkcid, NULL, 0);
    }

    if( !participate ){
        context->flag = 1;
    } else {
        context->flag = (context->nextcid == context->nextlocal_cid);
        if ( participate && !context->flag) {
            opal_pointer_array_set_item(&ompi_mpi_communicators, context->nextlocal_cid, NULL);

            context->flag = opal_pointer_array_test_and_set_item (&ompi_mpi_communicators,
                                                                  context->nextcid, context->comm);
        }
    }

    ++context->iter;

    ret = context->allreduce_fn (&context->flag, &context->rflag, 1, MPI_MIN, context, &subreq);
    if (OMPI_SUCCESS == ret) {
        ompi_comm_request_schedule_append (request, ompi_comm_nextcid_check_flag, &subreq, 1);
    } else {
        if (participate && context->flag ) {
            opal_pointer_array_test_and_set_item(&ompi_mpi_communicators, context->nextlocal_cid, NULL);
        }
        ompi_comm_cid_lowest_id = INT64_MAX;
    }

    OPAL_THREAD_UNLOCK(&ompi_cid_lock);
    return ret;
}

static int ompi_comm_nextcid_check_flag (ompi_comm_request_t *request)
{
    ompi_comm_cid_context_t *context = (ompi_comm_cid_context_t *) request->context;
    int participate = (context->newcomm->c_local_group->grp_my_rank != MPI_UNDEFINED);

    if (OPAL_THREAD_TRYLOCK(&ompi_cid_lock)) {
        return ompi_comm_request_schedule_append (request, ompi_comm_nextcid_check_flag, NULL, 0);
    }

    if (1 == context->rflag) {
        if( !participate ) {
            /* we need to provide something sane here
             * but we cannot use `nextcid` as we may have it
             * in-use, go ahead with next locally-available CID
             */
            context->nextlocal_cid = mca_pml.pml_max_contextid;
            for (unsigned int i = context->start ; i < mca_pml.pml_max_contextid ; ++i) {
                bool flag;
                flag = opal_pointer_array_test_and_set_item (&ompi_mpi_communicators, i,
                                                                context->comm);
                if (true == flag) {
                    context->nextlocal_cid = i;
                    break;
                }
            }
            context->nextcid = context->nextlocal_cid;
        }

        /* set the according values to the newcomm */
        context->newcomm->c_contextid = context->nextcid;
        opal_pointer_array_set_item (&ompi_mpi_communicators, context->nextcid, context->newcomm);

        /* unlock the cid generator */
        ompi_comm_cid_lowest_id = INT64_MAX;
        OPAL_THREAD_UNLOCK(&ompi_cid_lock);

        /* done! */
        return OMPI_SUCCESS;
    }

    if (participate && (1 == context->flag)) {
        /* we could use this cid, but other don't agree */
        opal_pointer_array_set_item (&ompi_mpi_communicators, context->nextcid, NULL);
        context->start = context->nextcid + 1; /* that's where we can start the next round */
    }

    ++context->iter;

    OPAL_THREAD_UNLOCK(&ompi_cid_lock);

    /* try again */
    return ompi_comm_allreduce_getnextcid (request);
}

/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
/* This routine serves two purposes:
 * - the allreduce acts as a kind of Barrier,
 *   which avoids, that we have incoming fragments
 *   on the new communicator before everybody has set
 *   up the comm structure.
 * - some components (e.g. the collective MagPIe component
 *   might want to generate new communicators and communicate
 *   using the new comm. Thus, it can just be called after
 *   the 'barrier'.
 *
 * The reason that this routine is in comm_cid and not in
 * comm.c is, that this file contains the allreduce implementations
 * which are required, and thus we avoid having duplicate code...
 */

/* Non-blocking version of ompi_comm_activate */
static int ompi_comm_activate_nb_complete (ompi_comm_request_t *request);

int ompi_comm_activate_nb (ompi_communicator_t **newcomm, ompi_communicator_t *comm,
                           ompi_communicator_t *bridgecomm, const void *arg0,
                           const void *arg1, bool send_first, int mode, ompi_request_t **req)
{
    ompi_comm_cid_context_t *context;
    ompi_comm_request_t *request;
    ompi_request_t *subreq;
    int ret = 0;

    context = mca_comm_cid_context_alloc (*newcomm, comm, bridgecomm, arg0, arg1, "activate",
                                          send_first, mode);
    if (NULL == context) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* keep track of the pointer so it can be set to MPI_COMM_NULL on failure */
    context->newcommp = newcomm;

    request = ompi_comm_request_get ();
    if (NULL == request) {
        OBJ_RELEASE(context);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request->context = &context->super;

    if (MPI_UNDEFINED != (*newcomm)->c_local_group->grp_my_rank) {
        /* Initialize the PML stuff in the newcomm  */
        if ( OMPI_SUCCESS != (ret = MCA_PML_CALL(add_comm(*newcomm))) ) {
            OBJ_RELEASE(*newcomm);
            OBJ_RELEASE(context);
            *newcomm = MPI_COMM_NULL;
            return ret;
        }
        OMPI_COMM_SET_PML_ADDED(*newcomm);
    }

    /* Step 1: the barrier, after which it is allowed to
     * send messages over the new communicator
     */
    ret = context->allreduce_fn (&context->ok, &context->ok, 1, MPI_MIN, context,
                                 &subreq);
    if (OMPI_SUCCESS != ret) {
        ompi_comm_request_return (request);
        return ret;
    }

    ompi_comm_request_schedule_append (request, ompi_comm_activate_nb_complete, &subreq, 1);
    ompi_comm_request_start (request);

    *req = &request->super;

    return OMPI_SUCCESS;
}

int ompi_comm_activate (ompi_communicator_t **newcomm, ompi_communicator_t *comm,
                        ompi_communicator_t *bridgecomm, const void *arg0,
                        const void *arg1, bool send_first, int mode)
{
    ompi_request_t *req;
    int rc;

    rc = ompi_comm_activate_nb (newcomm, comm, bridgecomm, arg0, arg1, send_first, mode, &req);
    if (OMPI_SUCCESS != rc) {
        return rc;
    }

    ompi_request_wait_completion (req);
    rc = req->req_status.MPI_ERROR;
    ompi_comm_request_return ((ompi_comm_request_t *) req);

    return rc;
}

static int ompi_comm_activate_nb_complete (ompi_comm_request_t *request)
{
    ompi_comm_cid_context_t *context = (ompi_comm_cid_context_t *) request->context;
    int ret;

    /**
     * Check to see if this process is in the new communicator.
     *
     * Specifically, this function is invoked by all proceses in the
     * old communicator, regardless of whether they are in the new
     * communicator or not.  This is because it is far simpler to use
     * MPI collective functions on the old communicator to determine
     * some data for the new communicator (e.g., remote_leader) than
     * to kludge up our own pseudo-collective routines over just the
     * processes in the new communicator.  Hence, *all* processes in
     * the old communicator need to invoke this function.
     *
     * That being said, only processes in the new communicator need to
     * select a coll module for the new communicator.  More
     * specifically, proceses who are not in the new communicator
     * should *not* select a coll module -- for example,
     * ompi_comm_rank(newcomm) returns MPI_UNDEFINED for processes who
     * are not in the new communicator.  This can cause errors in the
     * selection / initialization of a coll module.  Plus, it's
     * wasteful -- processes in the new communicator will end up
     * freeing the new communicator anyway, so we might as well leave
     * the coll selection as NULL (the coll base comm unselect code
     * handles that case properly).
     */
    if (MPI_UNDEFINED == (context->newcomm)->c_local_group->grp_my_rank) {
        return OMPI_SUCCESS;
    }

    /* Let the collectives components fight over who will do
       collective on this new comm.  */
    if (OMPI_SUCCESS != (ret = mca_coll_base_comm_select(context->newcomm))) {
        OBJ_RELEASE(context->newcomm);
        *context->newcommp = MPI_COMM_NULL;
        return ret;
    }

    /* For an inter communicator, we have to deal with the potential
     * problem of what is happening if the local_comm that we created
     * has a lower CID than the parent comm. This is not a problem
     * as long as the user calls MPI_Comm_free on the inter communicator.
     * However, if the communicators are not freed by the user but released
     * by Open MPI in MPI_Finalize, we walk through the list of still available
     * communicators and free them one by one. Thus, local_comm is freed before
     * the actual inter-communicator. However, the local_comm pointer in the
     * inter communicator will still contain the 'previous' address of the local_comm
     * and thus this will lead to a segmentation violation. In order to prevent
     * that from happening, we increase the reference counter local_comm
     * by one if its CID is lower than the parent. We cannot increase however
     *  its reference counter if the CID of local_comm is larger than
     * the CID of the inter communicators, since a regular MPI_Comm_free would
     * leave in that the case the local_comm hanging around and thus we would not
     * recycle CID's properly, which was the reason and the cause for this trouble.
     */
    if (OMPI_COMM_IS_INTER(context->newcomm)) {
        if (OMPI_COMM_CID_IS_LOWER(context->newcomm, context->comm)) {
            OMPI_COMM_SET_EXTRA_RETAIN (context->newcomm);
            OBJ_RETAIN (context->newcomm);
        }
    }

    /* done */
    return OMPI_SUCCESS;
}

/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
static int ompi_comm_allreduce_intra_nb (int *inbuf, int *outbuf, int count, struct ompi_op_t *op,
                                         ompi_comm_cid_context_t *context, ompi_request_t **req)
{
    ompi_communicator_t *comm = context->comm;

    return comm->c_coll->coll_iallreduce (inbuf, outbuf, count, MPI_INT, op, comm,
                                         req, comm->c_coll->coll_iallreduce_module);
}

/* Non-blocking version of ompi_comm_allreduce_inter */
static int ompi_comm_allreduce_inter_leader_exchange (ompi_comm_request_t *request);
static int ompi_comm_allreduce_inter_leader_reduce (ompi_comm_request_t *request);
static int ompi_comm_allreduce_inter_bcast (ompi_comm_request_t *request);

static int ompi_comm_allreduce_inter_nb (int *inbuf, int *outbuf,
                                         int count, struct ompi_op_t *op,
                                         ompi_comm_cid_context_t *cid_context,
                                         ompi_request_t **req)
{
    ompi_communicator_t *intercomm = cid_context->comm;
    ompi_comm_allreduce_context_t *context;
    ompi_comm_request_t *request;
    ompi_request_t *subreq;
    int local_rank, rc;

    if (!OMPI_COMM_IS_INTER (cid_context->comm)) {
        return MPI_ERR_COMM;
    }

    request = ompi_comm_request_get ();
    if (OPAL_UNLIKELY(NULL == request)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    context = ompi_comm_allreduce_context_alloc (inbuf, outbuf, count, op, cid_context);
    if (OPAL_UNLIKELY(NULL == context)) {
        ompi_comm_request_return (request);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request->context = &context->super;

    /* Allocate temporary arrays */
    local_rank = ompi_comm_rank (intercomm);

    if (0 == local_rank) {
        context->tmpbuf  = (int *) calloc (count, sizeof(int));
        if (OPAL_UNLIKELY (NULL == context->tmpbuf)) {
            ompi_comm_request_return (request);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
    }

    /* Execute the inter-allreduce: the result from the local will be in the buffer of the remote group
     * and vise-versa. */
    rc = intercomm->c_local_comm->c_coll->coll_ireduce (inbuf, context->tmpbuf, count, MPI_INT, op, 0,
                                                       intercomm->c_local_comm, &subreq,
                                                       intercomm->c_local_comm->c_coll->coll_ireduce_module);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        ompi_comm_request_return (request);
        return rc;
    }

    if (0 == local_rank) {
        ompi_comm_request_schedule_append (request, ompi_comm_allreduce_inter_leader_exchange, &subreq, 1);
    } else {
        ompi_comm_request_schedule_append (request, ompi_comm_allreduce_inter_bcast, &subreq, 1);
    }

    ompi_comm_request_start (request);
    *req = &request->super;

    return OMPI_SUCCESS;
}


static int ompi_comm_allreduce_inter_leader_exchange (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_communicator_t *intercomm = context->cid_context->comm;
    ompi_request_t *subreqs[2];
    int rc;

    /* local leader exchange their data and determine the overall result
       for both groups */
    rc = MCA_PML_CALL(irecv (context->outbuf, context->count, MPI_INT, 0, OMPI_COMM_ALLREDUCE_TAG,
                             intercomm, subreqs));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        return rc;
    }

    rc = MCA_PML_CALL(isend (context->tmpbuf, context->count, MPI_INT, 0, OMPI_COMM_ALLREDUCE_TAG,
                             MCA_PML_BASE_SEND_STANDARD, intercomm, subreqs + 1));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        return rc;
    }

    return ompi_comm_request_schedule_append (request, ompi_comm_allreduce_inter_leader_reduce, subreqs, 2);
}

static int ompi_comm_allreduce_inter_leader_reduce (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;

    ompi_op_reduce (context->op, context->tmpbuf, context->outbuf, context->count, MPI_INT);

    return ompi_comm_allreduce_inter_bcast (request);
}


static int ompi_comm_allreduce_inter_bcast (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_communicator_t *comm = context->cid_context->comm->c_local_comm;
    ompi_request_t *subreq;
    int rc;

    /* both roots have the same result. broadcast to the local group */
    rc = comm->c_coll->coll_ibcast (context->outbuf, context->count, MPI_INT, 0, comm,
                                   &subreq, comm->c_coll->coll_ibcast_module);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        return rc;
    }

    return ompi_comm_request_schedule_append (request, NULL, &subreq, 1);
}

static int ompi_comm_allreduce_bridged_schedule_bcast (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_communicator_t *comm = context->cid_context->comm;
    ompi_request_t *subreq;
    int rc;

    rc = comm->c_coll->coll_ibcast (context->outbuf, context->count, MPI_INT,
                                   context->cid_context->local_leader, comm,
                                   &subreq, comm->c_coll->coll_ibcast_module);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        return rc;
    }

    return ompi_comm_request_schedule_append (request, NULL, &subreq, 1);
}

static int ompi_comm_allreduce_bridged_xchng_complete (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;

    /* step 3: reduce leader data */
    ompi_op_reduce (context->op, context->tmpbuf, context->outbuf, context->count, MPI_INT);

    /* schedule the broadcast to local peers */
    return ompi_comm_allreduce_bridged_schedule_bcast (request);
}

static int ompi_comm_allreduce_bridged_reduce_complete (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_communicator_t *bridgecomm = context->cid_context->bridgecomm;
    ompi_request_t *subreq[2];
    int rc;

    /* step 2: leader exchange */
    rc = MCA_PML_CALL(irecv (context->outbuf, context->count, MPI_INT, context->cid_context->remote_leader,
                             OMPI_COMM_ALLREDUCE_TAG, bridgecomm, subreq + 1));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        return rc;
    }

    rc = MCA_PML_CALL(isend (context->tmpbuf, context->count, MPI_INT, context->cid_context->remote_leader,
                             OMPI_COMM_ALLREDUCE_TAG, MCA_PML_BASE_SEND_STANDARD, bridgecomm,
                             subreq));
    if (OPAL_UNLIKELY(OMPI_SUCCESS != rc)) {
        return rc;
    }

    return ompi_comm_request_schedule_append (request, ompi_comm_allreduce_bridged_xchng_complete, subreq, 2);
}

static int ompi_comm_allreduce_intra_bridge_nb (int *inbuf, int *outbuf,
                                                int count, struct ompi_op_t *op,
                                                ompi_comm_cid_context_t *cid_context,
                                                ompi_request_t **req)
{
    ompi_communicator_t *comm = cid_context->comm;
    ompi_comm_allreduce_context_t *context;
    int local_rank = ompi_comm_rank (comm);
    ompi_comm_request_t *request;
    ompi_request_t *subreq;
    int rc;

    context = ompi_comm_allreduce_context_alloc (inbuf, outbuf, count, op, cid_context);
    if (OPAL_UNLIKELY(NULL == context)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    if (local_rank == cid_context->local_leader) {
        context->tmpbuf = (int *) calloc (count, sizeof (int));
        if (OPAL_UNLIKELY(NULL == context->tmpbuf)) {
            OBJ_RELEASE(context);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
    }

    request = ompi_comm_request_get ();
    if (OPAL_UNLIKELY(NULL == request)) {
        OBJ_RELEASE(context);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request->context = &context->super;

    if (cid_context->local_leader == local_rank) {
        memcpy (context->tmpbuf, inbuf, count * sizeof (int));
    }

    /* step 1: reduce to the local leader */
    rc = comm->c_coll->coll_ireduce (inbuf, context->tmpbuf, count, MPI_INT, op,
                                    cid_context->local_leader, comm, &subreq,
                                    comm->c_coll->coll_ireduce_module);
    if ( OMPI_SUCCESS != rc ) {
        ompi_comm_request_return (request);
        return rc;
    }

    if (cid_context->local_leader == local_rank) {
        rc = ompi_comm_request_schedule_append (request, ompi_comm_allreduce_bridged_reduce_complete,
                                                &subreq, 1);
    } else {
        /* go ahead and schedule the broadcast */
        ompi_comm_request_schedule_append (request, NULL, &subreq, 1);

        rc = ompi_comm_allreduce_bridged_schedule_bcast (request);
    }

    if (OMPI_SUCCESS != rc) {
        ompi_comm_request_return (request);
        return rc;
    }

    ompi_comm_request_start (request);

    *req = &request->super;

    return OMPI_SUCCESS;
}

static int ompi_comm_allreduce_pmix_reduce_complete (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_comm_cid_context_t *cid_context = context->cid_context;
    int32_t size_count = context->count;
    opal_value_t info;
    opal_pmix_pdata_t pdat;
    opal_buffer_t sbuf;
    int rc;
    int bytes_written;
    const int output_id = 0;
    const int verbosity_level = 1;

    OBJ_CONSTRUCT(&sbuf, opal_buffer_t);

    if (OPAL_SUCCESS != (rc = opal_dss.pack(&sbuf, context->tmpbuf, (int32_t)context->count, OPAL_INT))) {
        OBJ_DESTRUCT(&sbuf);
        opal_output_verbose (verbosity_level, output_id, "pack failed. rc  %d\n", rc);
        return rc;
    }

    OBJ_CONSTRUCT(&info, opal_value_t);
    OBJ_CONSTRUCT(&pdat, opal_pmix_pdata_t);

    info.type = OPAL_BYTE_OBJECT;
    pdat.value.type = OPAL_BYTE_OBJECT;

    opal_dss.unload(&sbuf, (void**)&info.data.bo.bytes, &info.data.bo.size);
    OBJ_DESTRUCT(&sbuf);

    bytes_written = asprintf(&info.key,
                             cid_context->send_first ? "%s:%s:send:%d"
                                                     : "%s:%s:recv:%d",
                             cid_context->port_string,
                             cid_context->pmix_tag,
                             cid_context->iter);

    if (bytes_written == -1) {
        opal_output_verbose (verbosity_level, output_id, "writing info.key failed\n");
    } else {
        bytes_written = asprintf(&pdat.value.key,
                                 cid_context->send_first ? "%s:%s:recv:%d"
                                                         : "%s:%s:send:%d",
                                 cid_context->port_string,
                                 cid_context->pmix_tag,
                                 cid_context->iter);

        if (bytes_written == -1) {
            opal_output_verbose (verbosity_level, output_id, "writing pdat.value.key failed\n");
        }
    }

    if (bytes_written == -1) {
        // write with separate calls,
        // just in case the args are the cause of failure
        opal_output_verbose (verbosity_level, output_id, "send first: %d\n", cid_context->send_first);
        opal_output_verbose (verbosity_level, output_id, "port string: %s\n", cid_context->port_string);
        opal_output_verbose (verbosity_level, output_id, "pmix tag: %s\n", cid_context->pmix_tag);
        opal_output_verbose (verbosity_level, output_id, "iter: %d\n", cid_context->iter);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* this macro is not actually non-blocking. if a non-blocking version becomes available this function
     * needs to be reworked to take advantage of it. */
    OPAL_PMIX_EXCHANGE(rc, &info, &pdat, 600);  // give them 10 minutes
    OBJ_DESTRUCT(&info);
    if (OPAL_SUCCESS != rc) {
        OBJ_DESTRUCT(&pdat);
        return rc;
    }

    OBJ_CONSTRUCT(&sbuf, opal_buffer_t);
    opal_dss.load(&sbuf, pdat.value.data.bo.bytes, pdat.value.data.bo.size);
    pdat.value.data.bo.bytes = NULL;
    pdat.value.data.bo.size = 0;
    OBJ_DESTRUCT(&pdat);

    rc = opal_dss.unpack (&sbuf, context->outbuf, &size_count, OPAL_INT);
    OBJ_DESTRUCT(&sbuf);
    if (OPAL_UNLIKELY(OPAL_SUCCESS != rc)) {
        return rc;
    }

    ompi_op_reduce (context->op, context->tmpbuf, context->outbuf, size_count, MPI_INT);

    return ompi_comm_allreduce_bridged_schedule_bcast (request);
}

static int ompi_comm_allreduce_intra_pmix_nb (int *inbuf, int *outbuf,
                                              int count, struct ompi_op_t *op,
                                              ompi_comm_cid_context_t *cid_context,
                                              ompi_request_t **req)
{
    ompi_communicator_t *comm = cid_context->comm;
    ompi_comm_allreduce_context_t *context;
    int local_rank = ompi_comm_rank (comm);
    ompi_comm_request_t *request;
    ompi_request_t *subreq;
    int rc;

    context = ompi_comm_allreduce_context_alloc (inbuf, outbuf, count, op, cid_context);
    if (OPAL_UNLIKELY(NULL == context)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    if (cid_context->local_leader == local_rank) {
        context->tmpbuf = (int *) calloc (count, sizeof(int));
        if (OPAL_UNLIKELY(NULL == context->tmpbuf)) {
            OBJ_RELEASE(context);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
    }

    request = ompi_comm_request_get ();
    if (NULL == request) {
        OBJ_RELEASE(context);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request->context = &context->super;

    /* comm is an intra-communicator */
    rc = comm->c_coll->coll_ireduce (inbuf, context->tmpbuf, count, MPI_INT, op,
                                    cid_context->local_leader, comm,
                                    &subreq, comm->c_coll->coll_ireduce_module);
    if ( OMPI_SUCCESS != rc ) {
        ompi_comm_request_return (request);
        return rc;
    }

    if (cid_context->local_leader == local_rank) {
        rc = ompi_comm_request_schedule_append (request, ompi_comm_allreduce_pmix_reduce_complete,
                                                &subreq, 1);
    } else {
        /* go ahead and schedule the broadcast */
        rc = ompi_comm_request_schedule_append (request, NULL, &subreq, 1);

        rc = ompi_comm_allreduce_bridged_schedule_bcast (request);
    }

    if (OMPI_SUCCESS != rc) {
        ompi_comm_request_return (request);
        return rc;
    }

    ompi_comm_request_start (request);
    *req = (ompi_request_t *) request;

    /* use the same function as bridged to schedule the broadcast */
    return OMPI_SUCCESS;
}

static int ompi_comm_allreduce_group_broadcast (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_comm_cid_context_t *cid_context = context->cid_context;
    ompi_request_t *subreq[2];
    int subreq_count = 0;
    int rc;

    for (int i = 0 ; i < 2 ; ++i) {
        if (MPI_PROC_NULL != context->peers_comm[i + 1]) {
            rc = MCA_PML_CALL(isend(context->outbuf, context->count, MPI_INT, context->peers_comm[i+1],
                                    cid_context->pml_tag, MCA_PML_BASE_SEND_STANDARD,
                                    cid_context->comm, subreq + subreq_count++));
            if (OMPI_SUCCESS != rc) {
                return rc;
            }
        }
    }

    return ompi_comm_request_schedule_append (request, NULL, subreq, subreq_count);
}

static int ompi_comm_allreduce_group_recv_complete (ompi_comm_request_t *request)
{
    ompi_comm_allreduce_context_t *context = (ompi_comm_allreduce_context_t *) request->context;
    ompi_comm_cid_context_t *cid_context = context->cid_context;
    int *tmp = context->tmpbuf;
    ompi_request_t *subreq[2];
    int rc;

    for (int i = 0 ; i < 2 ; ++i) {
        if (MPI_PROC_NULL != context->peers_comm[i + 1]) {
            ompi_op_reduce (context->op, tmp, context->outbuf, context->count, MPI_INT);
            tmp += context->count;
        }
    }

    if (MPI_PROC_NULL != context->peers_comm[0]) {
        /* interior node */
        rc = MCA_PML_CALL(isend(context->outbuf, context->count, MPI_INT, context->peers_comm[0],
                                cid_context->pml_tag, MCA_PML_BASE_SEND_STANDARD,
                                cid_context->comm, subreq));
        if (OMPI_SUCCESS != rc) {
            return rc;
        }

        rc = MCA_PML_CALL(irecv(context->outbuf, context->count, MPI_INT, context->peers_comm[0],
                                cid_context->pml_tag, cid_context->comm, subreq + 1));
        if (OMPI_SUCCESS != rc) {
            return rc;
        }

        return ompi_comm_request_schedule_append (request, ompi_comm_allreduce_group_broadcast, subreq, 2);
    }

    /* root */
    return ompi_comm_allreduce_group_broadcast (request);
}

static int ompi_comm_allreduce_group_nb (int *inbuf, int *outbuf, int count,
                                         struct ompi_op_t *op, ompi_comm_cid_context_t *cid_context,
                                         ompi_request_t **req)
{
    ompi_group_t *group = cid_context->newcomm->c_local_group;
    const int group_size = ompi_group_size (group);
    const int group_rank = ompi_group_rank (group);
    ompi_communicator_t *comm = cid_context->comm;
    int peers_group[3], *tmp, subreq_count = 0;
    ompi_comm_allreduce_context_t *context;
    ompi_comm_request_t *request;
    ompi_request_t *subreq[3];

    context = ompi_comm_allreduce_context_alloc (inbuf, outbuf, count, op, cid_context);
    if (NULL == context) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    tmp = context->tmpbuf = calloc (sizeof (int), count * 3);
    if (NULL == context->tmpbuf) {
        OBJ_RELEASE(context);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request = ompi_comm_request_get ();
    if (NULL == request) {
        OBJ_RELEASE(context);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    request->context = &context->super;

    /* basic recursive doubling allreduce on the group */
    peers_group[0] = group_rank ? ((group_rank - 1) >> 1) : MPI_PROC_NULL;
    peers_group[1] = (group_rank * 2 + 1) < group_size ? group_rank * 2 + 1: MPI_PROC_NULL;
    peers_group[2] = (group_rank * 2 + 2) < group_size ? group_rank * 2 + 2 : MPI_PROC_NULL;

    /* translate the ranks into the ranks of the parent communicator */
    ompi_group_translate_ranks (group, 3, peers_group, comm->c_local_group, context->peers_comm);

    /* reduce */
    memmove (outbuf, inbuf, sizeof (int) * count);

    for (int i = 0 ; i < 2 ; ++i) {
        if (MPI_PROC_NULL != context->peers_comm[i + 1]) {
            int rc = MCA_PML_CALL(irecv(tmp, count, MPI_INT, context->peers_comm[i + 1],
                                        cid_context->pml_tag, comm, subreq + subreq_count++));
            if (OMPI_SUCCESS != rc) {
                ompi_comm_request_return (request);
                return rc;
            }

            tmp += count;
        }
    }

    ompi_comm_request_schedule_append (request, ompi_comm_allreduce_group_recv_complete, subreq, subreq_count);

    ompi_comm_request_start (request);
    *req = &request->super;

    return OMPI_SUCCESS;
}

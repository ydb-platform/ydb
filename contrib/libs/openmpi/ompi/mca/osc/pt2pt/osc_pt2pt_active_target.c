/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010-2016 IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "osc_pt2pt.h"
#include "osc_pt2pt_header.h"
#include "osc_pt2pt_data_move.h"
#include "osc_pt2pt_frag.h"

#include "mpi.h"
#include "opal/runtime/opal_progress.h"
#include "opal/threads/mutex.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/osc/base/base.h"

/**
 * compare_ranks:
 *
 * @param[in] ptra    Pointer to integer item
 * @param[in] ptrb    Pointer to integer item
 *
 * @returns 0 if *ptra == *ptrb
 * @returns -1 if *ptra < *ptrb
 * @returns 1 otherwise
 *
 * This function is used to sort the rank list. It can be removed if
 * groups are always in order.
 */
static int compare_ranks (const void *ptra, const void *ptrb)
{
    int a = *((int *) ptra);
    int b = *((int *) ptrb);

    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    }

    return 0;
}

/**
 * ompi_osc_pt2pt_get_comm_ranks:
 *
 * @param[in] module    - OSC PT2PT module
 * @param[in] sub_group - Group with ranks to translate
 *
 * @returns an array of translated ranks on success or NULL on failure
 *
 * Translate the ranks given in {sub_group} into ranks in the
 * communicator used to create {module}.
 */
static ompi_osc_pt2pt_peer_t **ompi_osc_pt2pt_get_peers (ompi_osc_pt2pt_module_t *module, ompi_group_t *sub_group)
{
    int size = ompi_group_size(sub_group);
    ompi_osc_pt2pt_peer_t **peers;
    int *ranks1, *ranks2;
    int ret;

    ranks1 = calloc (size, sizeof(int));
    ranks2 = calloc (size, sizeof(int));
    peers = calloc (size, sizeof (ompi_osc_pt2pt_peer_t *));
    if (NULL == ranks1 || NULL == ranks2 || NULL == peers) {
        free (ranks1);
        free (ranks2);
        free (peers);
        return NULL;
    }

    for (int i = 0 ; i < size ; ++i) {
        ranks1[i] = i;
    }

    ret = ompi_group_translate_ranks (sub_group, size, ranks1, module->comm->c_local_group,
                                      ranks2);
    free (ranks1);
    if (OMPI_SUCCESS != ret) {
        free (ranks2);
        free (peers);
        return NULL;
    }

    qsort (ranks2, size, sizeof (int), compare_ranks);
    for (int i = 0 ; i < size ; ++i) {
        peers[i] = ompi_osc_pt2pt_peer_lookup (module, ranks2[i]);
        OBJ_RETAIN(peers[i]);
    }
    free (ranks2);

    return peers;
}

static void ompi_osc_pt2pt_release_peers (ompi_osc_pt2pt_peer_t **peers, int npeers)
{
    if (peers) {
        for (int i = 0 ; i < npeers ; ++i) {
            OBJ_RELEASE(peers[i]);
        }

        free (peers);
    }
}

int ompi_osc_pt2pt_fence(int assert, ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    uint32_t incoming_reqs;
    int ret = OMPI_SUCCESS;

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: fence start"));

    /* can't enter an active target epoch when in a passive target epoch */
    if (ompi_osc_pt2pt_in_passive_epoch (module)) {
        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                             "osc pt2pt: could not enter fence. already in an access epoch"));
        return OMPI_ERR_RMA_SYNC;
    }

    /* active sends are now active (we will close the epoch if NOSUCCEED is specified) */
    if (0 == (assert & MPI_MODE_NOSUCCEED)) {
        module->all_sync.type = OMPI_OSC_PT2PT_SYNC_TYPE_FENCE;
        module->all_sync.eager_send_active = true;
    }

    /* short-circuit the noprecede case */
    if (0 != (assert & MPI_MODE_NOPRECEDE)) {
        module->comm->c_coll->coll_barrier (module->comm,  module->comm->c_coll->coll_barrier_module);
        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "osc pt2pt: fence end (short circuit)"));
        return ret;
    }

    /* try to start all requests.  */
    ret = ompi_osc_pt2pt_frag_flush_all(module);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: fence done sending"));

    /* find out how much data everyone is going to send us.  */
    ret = module->comm->c_coll->coll_reduce_scatter_block (module->epoch_outgoing_frag_count,
                                                          &incoming_reqs, 1, MPI_UINT32_T,
                                                          MPI_SUM, module->comm,
                                                          module->comm->c_coll->coll_reduce_scatter_block_module);
    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    OPAL_THREAD_LOCK(&module->lock);
    bzero(module->epoch_outgoing_frag_count,
          sizeof(uint32_t) * ompi_comm_size(module->comm));

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: fence expects %d requests",
                         incoming_reqs));

    /* set our complete condition for incoming requests */
    OPAL_THREAD_ADD_FETCH32(&module->active_incoming_frag_count, -incoming_reqs);

    /* wait for completion */
    while (module->outgoing_frag_count < 0 || module->active_incoming_frag_count < 0) {
        opal_condition_wait(&module->cond, &module->lock);
    }

    if (assert & MPI_MODE_NOSUCCEED) {
        /* as specified in MPI-3 p 438 3-5 the fence can end an epoch. it isn't explicitly
         * stated that MPI_MODE_NOSUCCEED ends the epoch but it is a safe assumption. */
        ompi_osc_pt2pt_sync_reset (&module->all_sync);
    }

    module->all_sync.epoch_active = false;
    OPAL_THREAD_UNLOCK(&module->lock);

    module->comm->c_coll->coll_barrier (module->comm, module->comm->c_coll->coll_barrier_module);

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "osc pt2pt: fence end: %d", ret));

    return OMPI_SUCCESS;
}


int ompi_osc_pt2pt_start (ompi_group_t *group, int assert, ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_sync_t *sync = &module->all_sync;

    OPAL_THREAD_LOCK(&sync->lock);

    /* check if we are already in an access epoch */
    if (ompi_osc_pt2pt_access_epoch_active (module)) {
        OPAL_THREAD_UNLOCK(&sync->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    /* mark all procs in this group as being in an access epoch */
    sync->num_peers = ompi_group_size (group);
    sync->sync.pscw.group = group;

    /* haven't processed any post messages yet */
    sync->sync_expected = sync->num_peers;

    /* If the previous epoch was from Fence, then eager_send_active is still
     * set to true at this time, but it shoulnd't be true until we get our
     * incoming Posts. So reset to 'false' for this new epoch.
     */
    sync->eager_send_active = false;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_start entering with group size %d...",
                         sync->num_peers));

    sync->type = OMPI_OSC_PT2PT_SYNC_TYPE_PSCW;

    /* prevent us from entering a passive-target, fence, or another pscw access epoch until
     * the matching complete is called */
    sync->epoch_active = true;

    /* save the group */
    OBJ_RETAIN(group);

    if (0 == ompi_group_size (group)) {
        /* nothing more to do. this is an empty start epoch */
        sync->eager_send_active = true;
        OPAL_THREAD_UNLOCK(&sync->lock);
        return OMPI_SUCCESS;
    }

    opal_atomic_wmb ();

    /* translate the group ranks into the communicator */
    sync->peer_list.peers = ompi_osc_pt2pt_get_peers (module, group);
    if (NULL == sync->peer_list.peers) {
        OPAL_THREAD_UNLOCK(&sync->lock);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    if (!(assert & MPI_MODE_NOCHECK)) {
        for (int i = 0 ; i < sync->num_peers ; ++i) {
            ompi_osc_pt2pt_peer_t *peer = sync->peer_list.peers[i];

            if (ompi_osc_pt2pt_peer_unex (peer)) {
                /* the peer already sent a post message for this pscw access epoch */
                OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                                     "found unexpected post from %d",
                                     peer->rank));
                OPAL_THREAD_ADD_FETCH32 (&sync->sync_expected, -1);
                ompi_osc_pt2pt_peer_set_unex (peer, false);
            }
        }
    } else {
        sync->sync_expected = 0;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "post messages still needed: %d", sync->sync_expected));

    /* if we've already received all the post messages, we can eager
       send.  Otherwise, eager send will be enabled when
       numb_post_messages reaches 0 */
    if (0 == sync->sync_expected) {
        sync->eager_send_active = true;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_start complete. eager sends active: %d",
                         sync->eager_send_active));

    OPAL_THREAD_UNLOCK(&sync->lock);
    return OMPI_SUCCESS;
}


int ompi_osc_pt2pt_complete (ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_sync_t *sync = &module->all_sync;
    int my_rank = ompi_comm_rank (module->comm);
    ompi_osc_pt2pt_peer_t **peers;
    int ret = OMPI_SUCCESS;
    ompi_group_t *group;
    size_t group_size;

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_complete entering..."));

    OPAL_THREAD_LOCK(&sync->lock);
    if (OMPI_OSC_PT2PT_SYNC_TYPE_PSCW != sync->type) {
        OPAL_THREAD_UNLOCK(&sync->lock);
        return OMPI_ERR_RMA_SYNC;
    }

    /* wait for all the post messages */
    ompi_osc_pt2pt_sync_wait_nolock (sync);

    /* phase 1 cleanup sync object */
    group = sync->sync.pscw.group;
    group_size = sync->num_peers;

    peers = sync->peer_list.peers;

    /* need to reset the sync here to avoid processing incorrect post messages */
    ompi_osc_pt2pt_sync_reset (sync);
    OPAL_THREAD_UNLOCK(&sync->lock);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_complete all posts received. sending complete messages..."));

    /* for each process in group, send a control message with number
       of updates coming, then start all the requests.  Note that the
       control send is processed as another message in a fragment, so
       this might get queued until the flush_all (which is fine).

       At the same time, clean out the outgoing count for the next
       round. */
    for (size_t i = 0 ; i < group_size ; ++i) {
        ompi_osc_pt2pt_header_complete_t complete_req;
        int rank = peers[i]->rank;

        if (my_rank == rank) {
            /* shortcut for self */
            osc_pt2pt_incoming_complete (module, rank, 0);
            continue;
        }

        complete_req.base.type = OMPI_OSC_PT2PT_HDR_TYPE_COMPLETE;
        complete_req.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID;
        complete_req.frag_count = module->epoch_outgoing_frag_count[rank];
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#if OPAL_ENABLE_DEBUG
        complete_req.padding[0] = 0;
        complete_req.padding[1] = 0;
#endif
        osc_pt2pt_hton(&complete_req, ompi_comm_peer_lookup (module->comm, rank));
#endif

        ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, rank);

        /* XXX -- TODO -- since fragment are always delivered in order we do not need to count anything but long
         * requests. once that is done this can be removed. */
        if (peer->active_frag && (peer->active_frag->remain_len < sizeof (complete_req))) {
            ++complete_req.frag_count;
        }

        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "ompi_osc_pt2pt_complete sending complete message to %d. frag_count: %u",
                             rank, complete_req.frag_count));

        ret = ompi_osc_pt2pt_control_send (module, rank, &complete_req,
                                           sizeof(ompi_osc_pt2pt_header_complete_t));
        if (OMPI_SUCCESS != ret) {
            break;
        }

        ret = ompi_osc_pt2pt_frag_flush_target (module, rank);
        if (OMPI_SUCCESS != ret) {
            break;
        }

        /* zero the fragment counts here to ensure they are zerod */
        module->epoch_outgoing_frag_count[rank] = 0;
    }

    if (peers) {
        /* release our reference to peers in this group */
        ompi_osc_pt2pt_release_peers (peers, group_size);
    }

    if (OMPI_SUCCESS != ret) {
        return ret;
    }

    OPAL_THREAD_LOCK(&module->lock);
    /* wait for outgoing requests to complete.  Don't wait for incoming, as
       we're only completing the access epoch, not the exposure epoch */
    while (module->outgoing_frag_count < 0) {
        opal_condition_wait(&module->cond, &module->lock);
    }

    /* unlock here, as group cleanup can take a while... */
    OPAL_THREAD_UNLOCK(&module->lock);

    /* phase 2 cleanup group */
    OBJ_RELEASE(group);

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_complete complete"));

    return OMPI_SUCCESS;
}


int ompi_osc_pt2pt_post (ompi_group_t *group, int assert, ompi_win_t *win)
{
    int ret = OMPI_SUCCESS;
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_osc_pt2pt_header_post_t post_req;
    ompi_osc_pt2pt_peer_t **peers;

    /* can't check for all access epoch here due to fence */
    if (module->pw_group) {
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_post entering with group size %d...",
                         ompi_group_size (group)));

    OPAL_THREAD_LOCK(&module->lock);

    /* ensure we're not already in a post */
    if (NULL != module->pw_group) {
        OPAL_THREAD_UNLOCK(&(module->lock));
        return OMPI_ERR_RMA_SYNC;
    }

    /* save the group */
    OBJ_RETAIN(group);

    module->pw_group = group;

    /* Update completion counter.  Can't have received any completion
       messages yet; complete won't send a completion header until
       we've sent a post header. */
    module->num_complete_msgs = -ompi_group_size(module->pw_group);

    OPAL_THREAD_UNLOCK(&(module->lock));

    if ((assert & MPI_MODE_NOCHECK) || 0 == ompi_group_size (group)) {
        return OMPI_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "sending post messages"));

    /* translate group ranks into the communicator */
    peers = ompi_osc_pt2pt_get_peers (module, module->pw_group);
    if (OPAL_UNLIKELY(NULL == peers)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* send a hello counter to everyone in group */
    for (int i = 0 ; i < ompi_group_size(module->pw_group) ; ++i) {
        ompi_osc_pt2pt_peer_t *peer = peers[i];
        int rank = peer->rank;

        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output, "Sending post message to rank %d", rank));
        ompi_proc_t *proc = ompi_comm_peer_lookup (module->comm, rank);

        /* shortcut for self */
        if (ompi_proc_local() == proc) {
            OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output, "ompi_osc_pt2pt_complete self post"));
            osc_pt2pt_incoming_post (module, ompi_comm_rank(module->comm));
            continue;
        }

        post_req.base.type = OMPI_OSC_PT2PT_HDR_TYPE_POST;
        post_req.base.flags = OMPI_OSC_PT2PT_HDR_FLAG_VALID;
        osc_pt2pt_hton(&post_req, proc);

        /* we don't want to send any data, since we're the exposure
           epoch only, so use an unbuffered send */
        ret = ompi_osc_pt2pt_control_send_unbuffered(module, rank, &post_req,
                                                     sizeof(ompi_osc_pt2pt_header_post_t));
        if (OMPI_SUCCESS != ret) {
            break;
        }
    }

    ompi_osc_pt2pt_release_peers (peers, ompi_group_size(module->pw_group));

    return ret;
}


int ompi_osc_pt2pt_wait (ompi_win_t *win)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_group_t *group;

    if (NULL == module->pw_group) {
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_wait entering... module %p", (void *) module));

    OPAL_THREAD_LOCK(&module->lock);
    while (0 != module->num_complete_msgs || module->active_incoming_frag_count < 0) {
        OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output, "module %p, num_complete_msgs = %d, "
                             "active_incoming_frag_count = %d", (void *) module, module->num_complete_msgs,
                             module->active_incoming_frag_count));
        opal_condition_wait(&module->cond, &module->lock);
    }

    group = module->pw_group;
    module->pw_group = NULL;
    OPAL_THREAD_UNLOCK(&module->lock);

    OBJ_RELEASE(group);

    OPAL_OUTPUT_VERBOSE((25, ompi_osc_base_framework.framework_output,
                         "ompi_osc_pt2pt_wait complete"));

    return OMPI_SUCCESS;
}


int ompi_osc_pt2pt_test (ompi_win_t *win, int *flag)
{
    ompi_osc_pt2pt_module_t *module = GET_MODULE(win);
    ompi_group_t *group;
    int ret = OMPI_SUCCESS;

#if !OPAL_ENABLE_PROGRESS_THREADS
    opal_progress();
#endif

    if (NULL == module->pw_group) {
        return OMPI_ERR_RMA_SYNC;
    }

    OPAL_THREAD_LOCK(&(module->lock));

    if (0 != module->num_complete_msgs || module->active_incoming_frag_count < 0) {
        *flag = 0;
    } else {
        *flag = 1;

        group = module->pw_group;
        module->pw_group = NULL;

        OBJ_RELEASE(group);
    }

    OPAL_THREAD_UNLOCK(&(module->lock));

    return ret;
}

void osc_pt2pt_incoming_complete (ompi_osc_pt2pt_module_t *module, int source, int frag_count)
{
    OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                         "osc pt2pt:  process_complete got complete message from %d. expected fragment count %d. "
                         "current incomming count: %d. expected complete msgs: %d", source,
                         frag_count, module->active_incoming_frag_count, module->num_complete_msgs));

    /* the current fragment is not part of the frag_count so we need to add it here */
    OPAL_THREAD_ADD_FETCH32(&module->active_incoming_frag_count, -frag_count);

    /* make sure the signal count is written before changing the complete message count */
    opal_atomic_wmb ();

    if (0 == OPAL_THREAD_ADD_FETCH32(&module->num_complete_msgs, 1)) {
        OPAL_THREAD_LOCK(&module->lock);
        opal_condition_broadcast (&module->cond);
        OPAL_THREAD_UNLOCK(&module->lock);
    }
}

void osc_pt2pt_incoming_post (ompi_osc_pt2pt_module_t *module, int source)
{
    ompi_osc_pt2pt_sync_t *sync = &module->all_sync;

    OPAL_THREAD_LOCK(&sync->lock);

    /* verify that this proc is part of the current start group */
    if (!ompi_osc_pt2pt_sync_pscw_peer (module, source, NULL)) {
        ompi_osc_pt2pt_peer_t *peer = ompi_osc_pt2pt_peer_lookup (module, source);

        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "received unexpected post message from %d for future PSCW synchronization",
                             source));

        ompi_osc_pt2pt_peer_set_unex (peer, true);
        OPAL_THREAD_UNLOCK(&sync->lock);
    } else {
        OPAL_THREAD_UNLOCK(&sync->lock);

        ompi_osc_pt2pt_sync_expected (sync);

        OPAL_OUTPUT_VERBOSE((50, ompi_osc_base_framework.framework_output,
                             "received post message for PSCW synchronization. post messages still needed: %d",
                             sync->sync_expected));
    }
}

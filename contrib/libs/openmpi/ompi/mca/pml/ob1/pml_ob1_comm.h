/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_PML_OB1_COMM_H
#define MCA_PML_OB1_COMM_H

#include "opal/threads/mutex.h"
#include "opal/class/opal_list.h"
#include "ompi/proc/proc.h"
#include "ompi/communicator/communicator.h"
BEGIN_C_DECLS


struct mca_pml_ob1_comm_proc_t {
    opal_object_t super;
    struct ompi_proc_t* ompi_proc;
    uint16_t expected_sequence;    /**< send message sequence number - receiver side */
#if OPAL_ENABLE_MULTI_THREADS
    volatile int32_t send_sequence; /**< send side sequence number */
#else
    int32_t send_sequence; /**< send side sequence number */
#endif
    struct mca_pml_ob1_recv_frag_t* frags_cant_match;  /**< out-of-order fragment queues */
    opal_list_t specific_receives; /**< queues of unmatched specific receives */
    opal_list_t unexpected_frags;  /**< unexpected fragment queues */
};
typedef struct mca_pml_ob1_comm_proc_t mca_pml_ob1_comm_proc_t;

OBJ_CLASS_DECLARATION(mca_pml_ob1_comm_proc_t);

/**
 *  Cached on ompi_communicator_t to hold queues/state
 *  used by the PML<->PTL interface for matching logic.
 */
struct mca_pml_comm_t {
    opal_object_t super;
    volatile uint32_t recv_sequence;  /**< recv request sequence number - receiver side */
    opal_mutex_t matching_lock;   /**< matching lock */
    opal_list_t wild_receives;    /**< queue of unmatched wild (source process not specified) receives */
    opal_mutex_t proc_lock;
    mca_pml_ob1_comm_proc_t **procs;
    size_t num_procs;
    size_t last_probed;
};
typedef struct mca_pml_comm_t mca_pml_ob1_comm_t;

OBJ_CLASS_DECLARATION(mca_pml_ob1_comm_t);

static inline mca_pml_ob1_comm_proc_t *mca_pml_ob1_peer_lookup (struct ompi_communicator_t *comm, int rank)
{
    mca_pml_ob1_comm_t *pml_comm = (mca_pml_ob1_comm_t *)comm->c_pml_comm;

    if (OPAL_UNLIKELY(NULL == pml_comm->procs[rank])) {
        OPAL_THREAD_LOCK(&pml_comm->proc_lock);
        if (NULL == pml_comm->procs[rank]) {
            mca_pml_ob1_comm_proc_t* proc = OBJ_NEW(mca_pml_ob1_comm_proc_t);
            proc->ompi_proc = ompi_comm_peer_lookup (comm, rank);
            OBJ_RETAIN(proc->ompi_proc);
            opal_atomic_wmb ();
            pml_comm->procs[rank] = proc;
        }
        OPAL_THREAD_UNLOCK(&pml_comm->proc_lock);
    }

    return pml_comm->procs[rank];
}

/**
 * Initialize an instance of mca_pml_ob1_comm_t based on the communicator size.
 *
 * @param  comm   Instance of mca_pml_ob1_comm_t
 * @param  size   Size of communicator
 * @return        OMPI_SUCCESS or error status on failure.
 */

extern int mca_pml_ob1_comm_init_size(mca_pml_ob1_comm_t* comm, size_t size);

END_C_DECLS
#endif


/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#include "ompi_config.h"

#include "ompi/constants.h"
#include "ompi/op/op.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "opal/include/opal/sys/atomic.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/patterns/net/netpatterns.h"
#include "coll_ops.h"

/**
 * Bcast - subgroup in communicator
 *  This is a very simple algorithm - binary tree, transmitting the full
 *  message at each step.
 */
OMPI_DECLSPEC int ompi_comm_bcast_pml(void *buffer, int root, int count,
        ompi_datatype_t *dtype, int my_rank_in_group,
        int n_peers, int *ranks_in_comm,ompi_communicator_t *comm)
{
    /* local variables */
    int rc=OMPI_SUCCESS,msg_cnt,i;
    ompi_request_t *requests[2];
    int node_rank, peer_rank;
    netpatterns_tree_node_t node_data;

    /*
     * shift rank to root==0 tree
     */
    node_rank=(my_rank_in_group-root+n_peers)%n_peers;

    /*
     * compute my communication pattern - binary tree
     */
    rc=ompi_netpatterns_setup_narray_tree(2, node_rank, n_peers,
            &node_data);
    if( OMPI_SUCCESS != rc ) {
        goto Error;
    }

    /* 1 process special case */
    if(1 == n_peers) {
        return OMPI_SUCCESS;
    }

    /* if I have parents - wait on the data to arrive */
    if(node_data.n_parents) {
        /* I will have only 1 parent */
        peer_rank=node_data.parent_rank;
        peer_rank=(peer_rank+root)%n_peers;
        /* translate back to actual rank */
        rc=MCA_PML_CALL(recv(buffer, count,dtype,peer_rank,
                    -OMPI_COMMON_TAG_BCAST, comm, MPI_STATUSES_IGNORE));
        if( 0 > rc ) {
            goto Error;
        }
    }

    /* send the data to my children */
    msg_cnt=0;
    for(i=0 ; i < node_data.n_children ; i++ ) {
        peer_rank=node_data.children_ranks[i];
        peer_rank=(peer_rank+root)%n_peers;
        rc=MCA_PML_CALL(isend(buffer,
                    count,dtype,peer_rank,
                    -OMPI_COMMON_TAG_BCAST,MCA_PML_BASE_SEND_STANDARD,
                    comm,&(requests[msg_cnt])));
        if( 0 > rc ) {
            goto Error;
        }
        msg_cnt++;
    }
    /* wait for send completion */
    if(msg_cnt) {
        /* wait on send and receive completion */
        ompi_request_wait_all(msg_cnt,requests,MPI_STATUSES_IGNORE);
    }

    if (node_data.children_ranks) {
        free(node_data.children_ranks);
    }

    /* return */
    return OMPI_SUCCESS;

Error:
    return rc;
}

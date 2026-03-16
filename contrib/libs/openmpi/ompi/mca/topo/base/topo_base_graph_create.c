/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/mca/topo/topo.h"

/*
 *
 * function - makes a new communicator to which topology information
 *            has been attached
 *
 * @param comm_old input communicator without topology (handle)
 * @param nnodes number of nodes in graph (integer)
 * @param index array of integers describing node degrees (see below)
 * @param edges array of integers describing graph edges (see below)
 * @param reorder ranking may be reordered (true) or not (false) (logical)
 * @param comm_graph communicator with graph topology added (handle)
 *
 * @retval MPI_SUCCESS
 * @retval MPI_ERR_OUT_OF_RESOURCE
 */

int mca_topo_base_graph_create(mca_topo_base_module_t *topo,
                               ompi_communicator_t* old_comm,
                               int nnodes,
                               const int *index,
                               const int *edges,
                               bool reorder,
                               ompi_communicator_t** comm_topo)
{
    ompi_communicator_t *new_comm;
    int new_rank, num_procs, ret, i;
    ompi_proc_t **topo_procs = NULL;
    mca_topo_base_comm_graph_2_2_0_t* graph;

    num_procs = old_comm->c_local_group->grp_proc_count;
    new_rank = old_comm->c_local_group->grp_my_rank;
    assert(topo->type == OMPI_COMM_GRAPH);

    if( num_procs < nnodes ) {
        return MPI_ERR_DIMS;
    }
    if( num_procs > nnodes ) {
        num_procs = nnodes;
    }
    if( new_rank > (nnodes - 1) ) {
        new_rank = MPI_UNDEFINED;
        num_procs = 0;
        nnodes = 0;
    }

    graph = OBJ_NEW(mca_topo_base_comm_graph_2_2_0_t);
    if( NULL == graph ) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    graph->nnodes = nnodes;

    /* Don't do any of the other initialization if we're not supposed
       to be part of the new communicator (because nnodes has been
       reset to 0, making things like index[nnodes-1] be junk).

       JMS: This should really be refactored to use
       comm_create_group(), because ompi_comm_allocate() still
       complains about 0-byte mallocs in debug builds for 0-member
       groups. */
    if (MPI_UNDEFINED != new_rank) {
        graph->index = (int*)malloc(sizeof(int) * nnodes);
        if (NULL == graph->index) {
            OBJ_RELEASE(graph);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        memcpy(graph->index, index, nnodes * sizeof(int));

        /* Graph communicator; copy the right data to the common information */
        graph->edges = (int*)malloc(sizeof(int) * index[nnodes-1]);
        if (NULL == graph->edges) {
            OBJ_RELEASE(graph);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        memcpy(graph->edges, edges, index[nnodes-1] * sizeof(int));

        topo_procs = (ompi_proc_t**)malloc(num_procs * sizeof(ompi_proc_t *));
        if (NULL == topo_procs) {
           OBJ_RELEASE(graph);
           return OMPI_ERR_OUT_OF_RESOURCE;
        }
        if(OMPI_GROUP_IS_DENSE(old_comm->c_local_group)) {
            memcpy(topo_procs,
                   old_comm->c_local_group->grp_proc_pointers,
                   num_procs * sizeof(ompi_proc_t *));
        } else {
            for(i = 0 ; i < num_procs; i++) {
                topo_procs[i] = ompi_group_peer_lookup(old_comm->c_local_group,i);
            }
        }
    }

    /* allocate a new communicator */
    new_comm = ompi_comm_allocate(nnodes, 0);
    if (NULL == new_comm) {
        free(topo_procs);
        OBJ_RELEASE(graph);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    ret = ompi_comm_enable(old_comm, new_comm,
                           new_rank, num_procs, topo_procs);
    if (OMPI_SUCCESS != ret) {
        free(topo_procs);
        OBJ_RELEASE(graph);
        if (MPI_COMM_NULL != new_comm) {
            new_comm->c_topo            = NULL;
            new_comm->c_flags          &= ~OMPI_COMM_GRAPH;
            ompi_comm_free (&new_comm);
        }
        return ret;
    }
    
    new_comm->c_topo            = topo;
    new_comm->c_topo->mtc.graph = graph;
    new_comm->c_flags          |= OMPI_COMM_GRAPH;
    new_comm->c_topo->reorder   = reorder;
    *comm_topo = new_comm;

    if( MPI_UNDEFINED == new_rank ) {
        ompi_comm_free(&new_comm);
        *comm_topo = MPI_COMM_NULL;
    }

    return OMPI_SUCCESS;
}

static void mca_topo_base_comm_graph_2_2_0_construct(mca_topo_base_comm_graph_2_2_0_t * graph) {
    graph->nnodes = 0;
    graph->index = NULL;
    graph->edges = NULL;
}

static void mca_topo_base_comm_graph_2_2_0_destruct(mca_topo_base_comm_graph_2_2_0_t * graph) {
    if (NULL != graph->index) {
        free(graph->index);
    }
    if (NULL != graph->edges) {
        free(graph->edges);
    }
}

OBJ_CLASS_INSTANCE(mca_topo_base_comm_graph_2_2_0_t, opal_object_t,
                   mca_topo_base_comm_graph_2_2_0_construct,
                   mca_topo_base_comm_graph_2_2_0_destruct);

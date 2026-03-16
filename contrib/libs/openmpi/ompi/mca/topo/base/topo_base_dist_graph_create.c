/*
 * Copyright (c) 2008      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2013 Inria.  All rights reserved.
 * Copyright (c) 2011-2013 Université Bordeaux 1
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation.  All rights reserved.
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/request/request.h"

#define IN_INDEX   0
#define OUT_INDEX  1
#define MCA_TOPO_BASE_TAG_DIST_EDGE_IN    -50
#define MCA_TOPO_BASE_TAG_DIST_EDGE_OUT   -51

typedef struct _dist_graph_elem {
    int in;
    int out;
} mca_topo_base_dist_graph_elem_t;

int mca_topo_base_dist_graph_distribute(mca_topo_base_module_t* module,
                                        ompi_communicator_t *comm,
                                        int n, const int nodes[],
                                        const int degrees[], const int targets[],
                                        const int weights[],
                                        mca_topo_base_comm_dist_graph_2_2_0_t** ptopo)
{
    int i, j, err, count, left_over, pending_reqs, current_pos, index, csize;
    int *rin = NULL, *rout, *temp = NULL;
    mca_topo_base_dist_graph_elem_t *pos, *cnt, *idx;
    size_t int_size, how_much;
    ompi_status_public_t status;
    ompi_request_t **reqs = NULL;
    mca_topo_base_comm_dist_graph_2_2_0_t* topo=NULL;

    ompi_datatype_type_size( (ompi_datatype_t*)&ompi_mpi_int, &int_size);

    csize = ompi_comm_size(comm);
    /**
     * We compress the counts: for each peer we maintain an in and an out.
     * In addition we compute 3 arrays (that are allocated in one go):
     * - cnt: the number of elements for a peer
     * - pos: the position of the first element for a peer
     * - idx: temporary indexes and message count after the reduce.
     */
    cnt = (mca_topo_base_dist_graph_elem_t*)calloc(3 * csize, sizeof(mca_topo_base_dist_graph_elem_t));
    if( NULL == cnt ) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto bail_out;
    }
    pos = cnt + csize;
    idx = pos + csize;

    for( index = i = 0; i < n; i++ ) {
        cnt[nodes[i]].out += degrees[i];
        for( j = 0; j < degrees[i]; ++j ) {
            cnt[targets[index]].in++;
            index++;
        }
    }

    /**
     * Prepare the positions array. The ith element is the corresponding
     * starting position of the ith neighbor in the global array.
     */
    pos[0].in  = 0;
    pos[0].out = 0;
    for( i = 0; i < (csize - 1); i++ ) {
        pos[i + 1].in  = pos[i].in  + cnt[i].in;
        pos[i + 1].out = pos[i].out + cnt[i].out;
    }

    rin = (int*)calloc(2 * (pos[csize - 1].in +  cnt[csize - 1].in +
                            pos[csize - 1].out + cnt[csize - 1].out), sizeof(int));
    if( NULL == rin ) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto bail_out;
    }
    rout = &rin[2 * (pos[csize - 1].in +  cnt[csize - 1].in)];

    for( index = i = 0; i < n; ++i ) {  /* for each of the nodes */
        for( j = 0; j < degrees[i]; ++j ) {  /* for each node's degree */
            int position = pos[nodes[i]].out + idx[nodes[i]].out;
            if( MPI_UNWEIGHTED != weights ) {
                position *= 2;
                rout[position + 1] = weights[index];
            }
            rout[position + 0] = targets[index];
            idx[nodes[i]].out++;

            position = pos[targets[index]].in + idx[targets[index]].in;
            if( MPI_UNWEIGHTED != weights ) {
                position *= 2;
                rin[position + 1] = weights[index];
            }
            rin[position + 0] = nodes[i];
            idx[targets[index]].in++;

            index++;
        }
    }

    err = comm->c_coll->coll_reduce_scatter_block( MPI_IN_PLACE, idx, 2,
                                                  (ompi_datatype_t*)&ompi_mpi_int, MPI_SUM, comm,
                                                  comm->c_coll->coll_reduce_scatter_block_module);
    /**
     * At this point in the indexes array we have:
     * - idx[0].in  total number of IN  edges
     * - idx[0].out total number of OUT edges
     */
    topo = OBJ_NEW(mca_topo_base_comm_dist_graph_2_2_0_t);
    if( NULL == topo ) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto bail_out;
    }
    topo->indegree  = idx[0].in;
    topo->outdegree = idx[0].out;
    topo->weighted = (weights != MPI_UNWEIGHTED);
    if (topo->indegree > 0) {
        topo->in = (int*)malloc(sizeof(int) * topo->indegree);
        if (NULL == topo->in) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto bail_out;
        }
        if (MPI_UNWEIGHTED != weights) {
            topo->inw = (int*)malloc(sizeof(int) * topo->indegree);
            if (NULL == topo->inw) {
                err = OMPI_ERR_OUT_OF_RESOURCE;
                goto bail_out;
            }
        }
    }
    if (topo->outdegree > 0) {
        topo->out = (int*)malloc(sizeof(int) * topo->outdegree);
        if (NULL == topo->out) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto bail_out;
        }
        if (MPI_UNWEIGHTED != weights) {
            topo->outw = (int*)malloc(sizeof(int) * topo->outdegree);
            if (NULL == topo->outw) {
                err = OMPI_ERR_OUT_OF_RESOURCE;
                goto bail_out;
            }
        }
    }

    reqs = (ompi_request_t**)malloc(sizeof(ompi_request_t*) * 2 * csize);
    for (pending_reqs = i = 0; i < csize; ++i) {
        int position;
        if( 0 != (count = cnt[i].in) ) {
            position = pos[i].in;
            if (MPI_UNWEIGHTED != weights) {
                count *= 2;  /* don't forget the weights */
                position *= 2;
            }
            err = MCA_PML_CALL(isend( &rin[position], count, (ompi_datatype_t*)&ompi_mpi_int,
                                      i, MCA_TOPO_BASE_TAG_DIST_EDGE_IN, MCA_PML_BASE_SEND_STANDARD,
                                      comm, &reqs[pending_reqs]));
            pending_reqs++;
        }
        if( 0 != (count = cnt[i].out) ) {
            position = pos[i].out;
            if (MPI_UNWEIGHTED != weights) {
                count *= 2;  /* don't forget the weights */
                position *= 2;
            }
            err = MCA_PML_CALL(isend(&rout[position], count, (ompi_datatype_t*)&ompi_mpi_int,
                                     i, MCA_TOPO_BASE_TAG_DIST_EDGE_OUT, MCA_PML_BASE_SEND_STANDARD,
                                     comm, &reqs[pending_reqs]));
            pending_reqs++;
        }
    }

    /**
     * Now let's receive the input edges in a temporary array
     * and then move them to their corresponding place.
     */
    count = topo->indegree;
    temp = topo->in;
    if (MPI_UNWEIGHTED != weights) {
        count *= 2;  /* don't forget the weights */
        if (count > 0) {
            /* Allocate an array big enough to hold the edges and
               their weights */
            temp = (int*)malloc(count*sizeof(int));
            if (NULL == temp) {
                err = OMPI_ERR_OUT_OF_RESOURCE;
                goto bail_out;
            }
        }
    }
    for( left_over = count, current_pos = i = 0; left_over > 0; i++ ) {

        MCA_PML_CALL(recv( &temp[count - left_over], left_over, (ompi_datatype_t*)&ompi_mpi_int,  /* keep receiving in the same buffer */
                           MPI_ANY_SOURCE, MCA_TOPO_BASE_TAG_DIST_EDGE_IN,
                           comm, &status ));
        how_much = status._ucount / int_size;
        if (MPI_UNWEIGHTED != weights) {
            for( j = 0; j < ((int)how_much >> 1); j++, current_pos++ ) {
                topo->in[current_pos]  = temp[2 * j + 0 + (count - left_over)];
                topo->inw[current_pos] = temp[2 * j + 1 + (count - left_over)];
            }
        }
        left_over -= how_much;
    }
    if (MPI_UNWEIGHTED != weights) {
        free(temp);
    }

    /**
     * Now let's receive the output edges in a temporary array
     * and then move them to their corresponding place.
     */
    count = topo->outdegree;
    temp = topo->out;
    if (MPI_UNWEIGHTED != weights) {
        count *= 2;  /* don't forget the weights */
        if (count > 0) {
            /* Allocate an array big enough to hold the edges and
               their weights */
            temp = (int*)malloc(count*sizeof(int));
            if (NULL == temp) {
                err = OMPI_ERR_OUT_OF_RESOURCE;
                goto bail_out;
            }
        }
    }
    for( left_over = count, current_pos = i = 0; left_over > 0; i++ ) {

        MCA_PML_CALL(recv( &temp[count - left_over], left_over, (ompi_datatype_t*)&ompi_mpi_int,  /* keep receiving in the same buffer */
                           MPI_ANY_SOURCE, MCA_TOPO_BASE_TAG_DIST_EDGE_OUT,
                           comm, &status ));
        how_much = status._ucount / int_size;

        if (MPI_UNWEIGHTED != weights) {
            for( j = 0; j < ((int)how_much >> 1); j++, current_pos++ ) {
                topo->out[current_pos]  = temp[2 * j + 0 + (count - left_over)];
                topo->outw[current_pos] = temp[2 * j + 1 + (count - left_over)];
            }
        }
        left_over -= how_much;
    }
    if (MPI_UNWEIGHTED != weights) {
        free(temp);
    }

    err = ompi_request_wait_all(pending_reqs, reqs, MPI_STATUSES_IGNORE);
    *ptopo = topo;
    topo = NULL;  /* don't free it below */

 bail_out:
    if( NULL != reqs ) {
        free(reqs);
    }
    if( NULL != rin ) {
        free(rin);
    }
    if( NULL != cnt ) {
        free(cnt);
    }
    if( NULL != topo ) {
        OBJ_RELEASE(topo);
    }
    return err;
}

int mca_topo_base_dist_graph_create(mca_topo_base_module_t* module,
                                    ompi_communicator_t *comm_old,
                                    int n, const int nodes[],
                                    const int degrees[], const int targets[],
                                    const int weights[],
                                    opal_info_t *info, int reorder,
                                    ompi_communicator_t **newcomm)
{
    int err;

    if( OMPI_SUCCESS != (err = ompi_comm_create(comm_old,
                                                comm_old->c_local_group,
                                                newcomm)) ) {
        OBJ_RELEASE(module);
        return err;
    }
    // But if there is an info object, the above call didn't make use
    // of it, so we'll do a dup-with-info to get the final comm and
    // free the above intermediate newcomm:
    if (info && info != &(MPI_INFO_NULL->super)) {
        ompi_communicator_t *intermediate_comm = *newcomm;
        ompi_comm_dup_with_info (intermediate_comm, info, newcomm);
        ompi_comm_free(&intermediate_comm);
    }

    assert(NULL == (*newcomm)->c_topo);
    (*newcomm)->c_topo             = module;
    (*newcomm)->c_topo->reorder    = reorder;
    (*newcomm)->c_flags           |= OMPI_COMM_DIST_GRAPH;

    err = mca_topo_base_dist_graph_distribute(module,
                                              *newcomm, 
                                              n, nodes,
                                              degrees, targets,
                                              weights,
                                              &((*newcomm)->c_topo->mtc.dist_graph));
    if( OMPI_SUCCESS != err ) {
        ompi_comm_free(newcomm);
    }
    return err;
}

static void mca_topo_base_comm_dist_graph_2_2_0_construct(mca_topo_base_comm_dist_graph_2_2_0_t * dist_graph) {
    dist_graph->in = NULL;
    dist_graph->inw = NULL;
    dist_graph->out = NULL;
    dist_graph->outw = NULL;
    dist_graph->indegree = 0;
    dist_graph->outdegree = 0;
    dist_graph->weighted = false;
}

static void mca_topo_base_comm_dist_graph_2_2_0_destruct(mca_topo_base_comm_dist_graph_2_2_0_t * dist_graph) {
    if (NULL != dist_graph->in) {
        free(dist_graph->in);
    }
    if (NULL != dist_graph->inw) {
        free(dist_graph->inw);
    }
    if (NULL != dist_graph->out) {
        free(dist_graph->out);
    }
    if (NULL != dist_graph->outw) {
        free(dist_graph->outw);
    }
}

OBJ_CLASS_INSTANCE(mca_topo_base_comm_dist_graph_2_2_0_t, opal_object_t,
                   mca_topo_base_comm_dist_graph_2_2_0_construct,
                   mca_topo_base_comm_dist_graph_2_2_0_destruct);

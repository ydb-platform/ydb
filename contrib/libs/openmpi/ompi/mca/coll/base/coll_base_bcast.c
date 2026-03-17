/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "coll_base_topo.h"
#include "coll_base_util.h"

int
ompi_coll_base_bcast_intra_generic( void* buffer,
                                     int original_count,
                                     struct ompi_datatype_t* datatype,
                                     int root,
                                     struct ompi_communicator_t* comm,
                                     mca_coll_base_module_t *module,
                                     uint32_t count_by_segment,
                                     ompi_coll_tree_t* tree )
{
    int err = 0, line, i, rank, segindex, req_index;
    int num_segments; /* Number of segments */
    int sendcount;    /* number of elements sent in this segment */
    size_t realsegsize, type_size;
    char *tmpbuf;
    ptrdiff_t extent, lb;
    ompi_request_t *recv_reqs[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
    ompi_request_t **send_reqs = NULL;

#if OPAL_ENABLE_DEBUG
    int size;
    size = ompi_comm_size(comm);
    assert( size > 1 );
#endif
    rank = ompi_comm_rank(comm);

    ompi_datatype_get_extent (datatype, &lb, &extent);
    ompi_datatype_type_size( datatype, &type_size );
    num_segments = (original_count + count_by_segment - 1) / count_by_segment;
    realsegsize = (ptrdiff_t)count_by_segment * extent;

    /* Set the buffer pointers */
    tmpbuf = (char *) buffer;

    if( tree->tree_nextsize != 0 ) {
        send_reqs = ompi_coll_base_comm_get_reqs(module->base_data, tree->tree_nextsize);
        if( NULL == send_reqs ) { err = OMPI_ERR_OUT_OF_RESOURCE; line = __LINE__; goto error_hndl; }
    }

    /* Root code */
    if( rank == root ) {
        /*
           For each segment:
           - send segment to all children.
           The last segment may have less elements than other segments.
        */
        sendcount = count_by_segment;
        for( segindex = 0; segindex < num_segments; segindex++ ) {
            if( segindex == (num_segments - 1) ) {
                sendcount = original_count - segindex * count_by_segment;
            }
            for( i = 0; i < tree->tree_nextsize; i++ ) {
                err = MCA_PML_CALL(isend(tmpbuf, sendcount, datatype,
                                         tree->tree_next[i],
                                         MCA_COLL_BASE_TAG_BCAST,
                                         MCA_PML_BASE_SEND_STANDARD, comm,
                                         &send_reqs[i]));
                if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
            }

            /* complete the sends before starting the next sends */
            err = ompi_request_wait_all( tree->tree_nextsize, send_reqs,
                                         MPI_STATUSES_IGNORE );
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

            /* update tmp buffer */
            tmpbuf += realsegsize;

        }
    }

    /* Intermediate nodes code */
    else if( tree->tree_nextsize > 0 ) {
        /*
           Create the pipeline.
           1) Post the first receive
           2) For segments 1 .. num_segments
           - post new receive
           - wait on the previous receive to complete
           - send this data to children
           3) Wait on the last segment
           4) Compute number of elements in last segment.
           5) Send the last segment to children
        */
        req_index = 0;
        err = MCA_PML_CALL(irecv(tmpbuf, count_by_segment, datatype,
                                 tree->tree_prev, MCA_COLL_BASE_TAG_BCAST,
                                 comm, &recv_reqs[req_index]));
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

        for( segindex = 1; segindex < num_segments; segindex++ ) {

            req_index = req_index ^ 0x1;

            /* post new irecv */
            err = MCA_PML_CALL(irecv( tmpbuf + realsegsize, count_by_segment,
                                      datatype, tree->tree_prev,
                                      MCA_COLL_BASE_TAG_BCAST,
                                      comm, &recv_reqs[req_index]));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

            /* wait for and forward the previous segment to children */
            err = ompi_request_wait( &recv_reqs[req_index ^ 0x1],
                                     MPI_STATUS_IGNORE );
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

            for( i = 0; i < tree->tree_nextsize; i++ ) {
                err = MCA_PML_CALL(isend(tmpbuf, count_by_segment, datatype,
                                         tree->tree_next[i],
                                         MCA_COLL_BASE_TAG_BCAST,
                                         MCA_PML_BASE_SEND_STANDARD, comm,
                                         &send_reqs[i]));
                if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
            }

            /* complete the sends before starting the next iteration */
            err = ompi_request_wait_all( tree->tree_nextsize, send_reqs,
                                         MPI_STATUSES_IGNORE );
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

            /* Update the receive buffer */
            tmpbuf += realsegsize;

        }

        /* Process the last segment */
        err = ompi_request_wait( &recv_reqs[req_index], MPI_STATUS_IGNORE );
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
        sendcount = original_count - (ptrdiff_t)(num_segments - 1) * count_by_segment;
        for( i = 0; i < tree->tree_nextsize; i++ ) {
            err = MCA_PML_CALL(isend(tmpbuf, sendcount, datatype,
                                     tree->tree_next[i],
                                     MCA_COLL_BASE_TAG_BCAST,
                                     MCA_PML_BASE_SEND_STANDARD, comm,
                                     &send_reqs[i]));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
        }

        err = ompi_request_wait_all( tree->tree_nextsize, send_reqs,
                                     MPI_STATUSES_IGNORE );
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
    }

    /* Leaf nodes */
    else {
        /*
           Receive all segments from parent in a loop:
           1) post irecv for the first segment
           2) for segments 1 .. num_segments
           - post irecv for the next segment
           - wait on the previous segment to arrive
           3) wait for the last segment
        */
        req_index = 0;
        err = MCA_PML_CALL(irecv(tmpbuf, count_by_segment, datatype,
                                 tree->tree_prev, MCA_COLL_BASE_TAG_BCAST,
                                 comm, &recv_reqs[req_index]));
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

        for( segindex = 1; segindex < num_segments; segindex++ ) {
            req_index = req_index ^ 0x1;
            tmpbuf += realsegsize;
            /* post receive for the next segment */
            err = MCA_PML_CALL(irecv(tmpbuf, count_by_segment, datatype,
                                     tree->tree_prev, MCA_COLL_BASE_TAG_BCAST,
                                     comm, &recv_reqs[req_index]));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
            /* wait on the previous segment */
            err = ompi_request_wait( &recv_reqs[req_index ^ 0x1],
                                     MPI_STATUS_IGNORE );
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
        }

        err = ompi_request_wait( &recv_reqs[req_index], MPI_STATUS_IGNORE );
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
    }

    return (MPI_SUCCESS);

 error_hndl:
    OPAL_OUTPUT( (ompi_coll_base_framework.framework_output,"%s:%4d\tError occurred %d, rank %2d",
                  __FILE__, line, err, rank) );
    (void)line;  // silence compiler warnings
    ompi_coll_base_free_reqs( recv_reqs, 2);
    if( NULL != send_reqs ) {
        ompi_coll_base_free_reqs(send_reqs, tree->tree_nextsize);
    }

    return err;
}

int
ompi_coll_base_bcast_intra_bintree ( void* buffer,
                                      int count,
                                      struct ompi_datatype_t* datatype,
                                      int root,
                                      struct ompi_communicator_t* comm,
                                      mca_coll_base_module_t *module,
                                      uint32_t segsize )
{
    int segcount = count;
    size_t typelng;
    mca_coll_base_comm_t *data = module->base_data;

    COLL_BASE_UPDATE_BINTREE( comm, module, root );

    /**
     * Determine number of elements sent per operation.
     */
    ompi_datatype_type_size( datatype, &typelng );
    COLL_BASE_COMPUTED_SEGCOUNT( segsize, typelng, segcount );

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"coll:base:bcast_intra_binary rank %d ss %5d typelng %lu segcount %d",
                 ompi_comm_rank(comm), segsize, (unsigned long)typelng, segcount));

    return ompi_coll_base_bcast_intra_generic( buffer, count, datatype, root, comm, module,
                                                segcount, data->cached_bintree );
}

int
ompi_coll_base_bcast_intra_pipeline( void* buffer,
                                      int count,
                                      struct ompi_datatype_t* datatype,
                                      int root,
                                      struct ompi_communicator_t* comm,
                                      mca_coll_base_module_t *module,
                                      uint32_t segsize )
{
    int segcount = count;
    size_t typelng;
    mca_coll_base_comm_t *data = module->base_data;

    COLL_BASE_UPDATE_PIPELINE( comm, module, root );

    /**
     * Determine number of elements sent per operation.
     */
    ompi_datatype_type_size( datatype, &typelng );
    COLL_BASE_COMPUTED_SEGCOUNT( segsize, typelng, segcount );

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"coll:base:bcast_intra_pipeline rank %d ss %5d typelng %lu segcount %d",
                 ompi_comm_rank(comm), segsize, (unsigned long)typelng, segcount));

    return ompi_coll_base_bcast_intra_generic( buffer, count, datatype, root, comm, module,
                                                segcount, data->cached_pipeline );
}

int
ompi_coll_base_bcast_intra_chain( void* buffer,
                                   int count,
                                   struct ompi_datatype_t* datatype,
                                   int root,
                                   struct ompi_communicator_t* comm,
                                   mca_coll_base_module_t *module,
                                   uint32_t segsize, int32_t chains )
{
    int segcount = count;
    size_t typelng;
    mca_coll_base_comm_t *data = module->base_data;

    COLL_BASE_UPDATE_CHAIN( comm, module, root, chains );

    /**
     * Determine number of elements sent per operation.
     */
    ompi_datatype_type_size( datatype, &typelng );
    COLL_BASE_COMPUTED_SEGCOUNT( segsize, typelng, segcount );

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"coll:base:bcast_intra_chain rank %d fo %d ss %5d typelng %lu segcount %d",
                 ompi_comm_rank(comm), chains, segsize, (unsigned long)typelng, segcount));

    return ompi_coll_base_bcast_intra_generic( buffer, count, datatype, root, comm, module,
                                                segcount, data->cached_chain );
}

int
ompi_coll_base_bcast_intra_binomial( void* buffer,
                                      int count,
                                      struct ompi_datatype_t* datatype,
                                      int root,
                                      struct ompi_communicator_t* comm,
                                      mca_coll_base_module_t *module,
                                      uint32_t segsize )
{
    int segcount = count;
    size_t typelng;
    mca_coll_base_comm_t *data = module->base_data;

    COLL_BASE_UPDATE_BMTREE( comm, module, root );

    /**
     * Determine number of elements sent per operation.
     */
    ompi_datatype_type_size( datatype, &typelng );
    COLL_BASE_COMPUTED_SEGCOUNT( segsize, typelng, segcount );

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"coll:base:bcast_intra_binomial rank %d ss %5d typelng %lu segcount %d",
                 ompi_comm_rank(comm), segsize, (unsigned long)typelng, segcount));

    return ompi_coll_base_bcast_intra_generic( buffer, count, datatype, root, comm, module,
                                                segcount, data->cached_bmtree );
}

int
ompi_coll_base_bcast_intra_split_bintree ( void* buffer,
                                            int count,
                                            struct ompi_datatype_t* datatype,
                                            int root,
                                            struct ompi_communicator_t* comm,
                                            mca_coll_base_module_t *module,
                                            uint32_t segsize )
{
    int err=0, line, rank, size, segindex, i, lr, pair;
    uint32_t counts[2];
    int segcount[2];       /* Number of elements sent with each segment */
    int num_segments[2];   /* Number of segmenets */
    int sendcount[2];      /* the same like segcount, except for the last segment */
    size_t realsegsize[2], type_size;
    char *tmpbuf[2];
    ptrdiff_t type_extent, lb;
    ompi_request_t *base_req, *new_req;
    ompi_coll_tree_t *tree;

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"ompi_coll_base_bcast_intra_split_bintree rank %d root %d ss %5d", rank, root, segsize));

    if (size == 1) {
        return MPI_SUCCESS;
    }

    /* setup the binary tree topology. */
    COLL_BASE_UPDATE_BINTREE( comm, module, root );
    tree = module->base_data->cached_bintree;

    err = ompi_datatype_type_size( datatype, &type_size );

    /* Determine number of segments and number of elements per segment */
    counts[0] = count/2;
    if (count % 2 != 0) counts[0]++;
    counts[1] = count - counts[0];
    if ( segsize > 0 ) {
        /* Note that ompi_datatype_type_size() will never return a negative
           value in typelng; it returns an int [vs. an unsigned type]
           because of the MPI spec. */
        if (segsize < ((uint32_t) type_size)) {
            segsize = type_size; /* push segsize up to hold one type */
        }
        segcount[0] = segcount[1] = segsize / type_size;
        num_segments[0] = counts[0]/segcount[0];
        if ((counts[0] % segcount[0]) != 0) num_segments[0]++;
        num_segments[1] = counts[1]/segcount[1];
        if ((counts[1] % segcount[1]) != 0) num_segments[1]++;
    } else {
        segcount[0]     = counts[0];
        segcount[1]     = counts[1];
        num_segments[0] = num_segments[1] = 1;
    }

    /* if the message is too small to be split into segments */
    if( (counts[0] == 0 || counts[1] == 0) ||
        (segsize > ((ptrdiff_t)counts[0] * type_size)) ||
        (segsize > ((ptrdiff_t)counts[1] * type_size)) ) {
        /* call linear version here ! */
        return (ompi_coll_base_bcast_intra_chain ( buffer, count, datatype,
                                                    root, comm, module,
                                                    segsize, 1 ));
    }

    err = ompi_datatype_get_extent (datatype, &lb, &type_extent);

    /* Determine real segment size */
    realsegsize[0] = (ptrdiff_t)segcount[0] * type_extent;
    realsegsize[1] = (ptrdiff_t)segcount[1] * type_extent;

    /* set the buffer pointers */
    tmpbuf[0] = (char *) buffer;
    tmpbuf[1] = (char *) buffer + (ptrdiff_t)counts[0] * type_extent;

    /* Step 1:
       Root splits the buffer in 2 and sends segmented message down the branches.
       Left subtree of the tree receives first half of the buffer, while right
       subtree receives the remaining message.
    */

    /* determine if I am left (0) or right (1), (root is right) */
    lr = ((rank + size - root)%size + 1)%2;

    /* root code */
    if( rank == root ) {
        /* determine segment count */
        sendcount[0] = segcount[0];
        sendcount[1] = segcount[1];
        /* for each segment */
        for (segindex = 0; segindex < num_segments[0]; segindex++) {
            /* for each child */
            for( i = 0; i < tree->tree_nextsize && i < 2; i++ ) {
                if (segindex >= num_segments[i]) { /* no more segments */
                    continue;
                }
                /* determine how many elements are being sent in this round */
                if(segindex == (num_segments[i] - 1))
                    sendcount[i] = counts[i] - segindex*segcount[i];
                /* send data */
                MCA_PML_CALL(send(tmpbuf[i], sendcount[i], datatype,
                                  tree->tree_next[i], MCA_COLL_BASE_TAG_BCAST,
                                  MCA_PML_BASE_SEND_STANDARD, comm));
                if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
                /* update tmp buffer */
                tmpbuf[i] += realsegsize[i];
            }
        }
    }

    /* intermediate nodes code */
    else if( tree->tree_nextsize > 0 ) {
        /* Intermediate nodes:
         * It will receive segments only from one half of the data.
         * Which one is determined by whether the node belongs to the "left" or "right"
         * subtree. Topoloby building function builds binary tree such that
         * odd "shifted ranks" ((rank + size - root)%size) are on the left subtree,
         * and even on the right subtree.
         *
         * Create the pipeline. We first post the first receive, then in the loop we
         * post the next receive and after that wait for the previous receive to complete
         * and we disseminating the data to all children.
         */
        sendcount[lr] = segcount[lr];
        err = MCA_PML_CALL(irecv(tmpbuf[lr], sendcount[lr], datatype,
                                 tree->tree_prev, MCA_COLL_BASE_TAG_BCAST,
                                 comm, &base_req));
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

        for( segindex = 1; segindex < num_segments[lr]; segindex++ ) {
            /* determine how many elements to expect in this round */
            if( segindex == (num_segments[lr] - 1))
                sendcount[lr] = counts[lr] - (ptrdiff_t)segindex * (ptrdiff_t)segcount[lr];
            /* post new irecv */
            err = MCA_PML_CALL(irecv( tmpbuf[lr] + realsegsize[lr], sendcount[lr],
                                      datatype, tree->tree_prev, MCA_COLL_BASE_TAG_BCAST,
                                      comm, &new_req));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

            /* wait for and forward the previous segment */
            err = ompi_request_wait( &base_req, MPI_STATUS_IGNORE );
            for( i = 0; i < tree->tree_nextsize; i++ ) {  /* send data to children (segcount[lr]) */
                err = MCA_PML_CALL(send( tmpbuf[lr], segcount[lr], datatype,
                                         tree->tree_next[i], MCA_COLL_BASE_TAG_BCAST,
                                         MCA_PML_BASE_SEND_STANDARD, comm));
                if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
            } /* end of for each child */

            /* upate the base request */
            base_req = new_req;
            /* go to the next buffer (ie. the one corresponding to the next recv) */
            tmpbuf[lr] += realsegsize[lr];
        } /* end of for segindex */

        /* wait for the last segment and forward current segment */
        err = ompi_request_wait( &base_req, MPI_STATUS_IGNORE );
        for( i = 0; i < tree->tree_nextsize; i++ ) {  /* send data to children */
            err = MCA_PML_CALL(send(tmpbuf[lr], sendcount[lr], datatype,
                                    tree->tree_next[i], MCA_COLL_BASE_TAG_BCAST,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
        } /* end of for each child */
    }

    /* leaf nodes */
    else {
        /* Just consume segments as fast as possible */
        sendcount[lr] = segcount[lr];
        for (segindex = 0; segindex < num_segments[lr]; segindex++) {
            /* determine how many elements to expect in this round */
            if (segindex == (num_segments[lr] - 1))
                sendcount[lr] = counts[lr] - (ptrdiff_t)segindex * (ptrdiff_t)segcount[lr];
            /* receive segments */
            err = MCA_PML_CALL(recv(tmpbuf[lr], sendcount[lr], datatype,
                                    tree->tree_prev, MCA_COLL_BASE_TAG_BCAST,
                                    comm, MPI_STATUS_IGNORE));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
            /* update the initial pointer to the buffer */
            tmpbuf[lr] += realsegsize[lr];
        }
    }

    /* reset the buffer pointers */
    tmpbuf[0] = (char *) buffer;
    tmpbuf[1] = (char *) buffer + (ptrdiff_t)counts[0] * type_extent;

    /* Step 2:
       Find your immediate pair (identical node in opposite subtree) and SendRecv
       data buffer with them.
       The tree building function ensures that
       if (we are not root)
       if we are in the left subtree (lr == 0) our pair is (rank+1)%size.
       if we are in the right subtree (lr == 1) our pair is (rank-1)%size
       If we have even number of nodes the rank (size-1) will pair up with root.
    */
    if (lr == 0) {
        pair = (rank+1)%size;
    } else {
        pair = (rank+size-1)%size;
    }

    if ( (size%2) != 0 && rank != root) {

        err = ompi_coll_base_sendrecv( tmpbuf[lr], counts[lr], datatype,
                                        pair, MCA_COLL_BASE_TAG_BCAST,
                                        tmpbuf[(lr+1)%2], counts[(lr+1)%2], datatype,
                                        pair, MCA_COLL_BASE_TAG_BCAST,
                                        comm, MPI_STATUS_IGNORE, rank);
        if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
    } else if ( (size%2) == 0 ) {
        /* root sends right buffer to the last node */
        if( rank == root ) {
            err = MCA_PML_CALL(send(tmpbuf[1], counts[1], datatype,
                                    (root+size-1)%size, MCA_COLL_BASE_TAG_BCAST,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }

        }
        /* last node receives right buffer from the root */
        else if (rank == (root+size-1)%size) {
            err = MCA_PML_CALL(recv(tmpbuf[1], counts[1], datatype,
                                    root, MCA_COLL_BASE_TAG_BCAST,
                                    comm, MPI_STATUS_IGNORE));
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
        }
        /* everyone else exchanges buffers */
        else {
            err = ompi_coll_base_sendrecv( tmpbuf[lr], counts[lr], datatype,
                                            pair, MCA_COLL_BASE_TAG_BCAST,
                                            tmpbuf[(lr+1)%2], counts[(lr+1)%2], datatype,
                                            pair, MCA_COLL_BASE_TAG_BCAST,
                                            comm, MPI_STATUS_IGNORE, rank);
            if (err != MPI_SUCCESS) { line = __LINE__; goto error_hndl; }
        }
    }
    return (MPI_SUCCESS);

 error_hndl:
    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"%s:%4d\tError occurred %d, rank %2d", __FILE__,line,err,rank));
    (void)line;  // silence compiler warning
    return (err);
}


/*
 * Linear functions are copied from the BASIC coll module
 * they do not segment the message and are simple implementations
 * but for some small number of nodes and/or small data sizes they
 * are just as fast as base/tree based segmenting operations
 * and as such may be selected by the decision functions
 * These are copied into this module due to the way we select modules
 * in V1. i.e. in V2 we will handle this differently and so will not
 * have to duplicate code.
 * GEF Oct05 after asking Jeff.
 */

/* copied function (with appropriate renaming) starts here */

/*
 *  bcast_lin_intra
 *
 *  Function:   - broadcast using O(N) algorithm
 *  Accepts:    - same arguments as MPI_Bcast()
 *  Returns:    - MPI_SUCCESS or error code
 */
int
ompi_coll_base_bcast_intra_basic_linear(void *buff, int count,
                                        struct ompi_datatype_t *datatype, int root,
                                        struct ompi_communicator_t *comm,
                                        mca_coll_base_module_t *module)
{
    int i, size, rank, err;
    ompi_request_t **preq, **reqs;

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"ompi_coll_base_bcast_intra_basic_linear rank %d root %d", rank, root));

    if (1 == size) return OMPI_SUCCESS;

    /* Non-root receive the data. */

    if (rank != root) {
        return MCA_PML_CALL(recv(buff, count, datatype, root,
                                 MCA_COLL_BASE_TAG_BCAST, comm,
                                 MPI_STATUS_IGNORE));
    }

    /* Root sends data to all others. */
    preq = reqs = ompi_coll_base_comm_get_reqs(module->base_data, size-1);
    if( NULL == reqs ) { err = OMPI_ERR_OUT_OF_RESOURCE; goto err_hndl; }

    for (i = 0; i < size; ++i) {
        if (i == rank) {
            continue;
        }

        err = MCA_PML_CALL(isend(buff, count, datatype, i,
                                 MCA_COLL_BASE_TAG_BCAST,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm, preq++));
        if (MPI_SUCCESS != err) { goto err_hndl; }
    }
    --i;

    /* Wait for them all.  If there's an error, note that we don't
     * care what the error was -- just that there *was* an error.  The
     * PML will finish all requests, even if one or more of them fail.
     * i.e., by the end of this call, all the requests are free-able.
     * So free them anyway -- even if there was an error, and return
     * the error after we free everything. */

    err = ompi_request_wait_all(i, reqs, MPI_STATUSES_IGNORE);
 err_hndl:
    if( MPI_SUCCESS != err ) {  /* Free the reqs */
        ompi_coll_base_free_reqs(reqs, i);
    }

    /* All done */
    return err;
}

/* copied function (with appropriate renaming) ends here */

/*
 * ompi_coll_base_bcast_intra_knomial
 *
 * Function:  Bcast using k-nomial tree algorithm
 * Accepts:   Same arguments as MPI_Bcast
 * Returns:   MPI_SUCCESS or error code
 * Parameters: radix -- k-nomial tree radix (>= 2)
 *
 * Time complexity: (radix - 1)O(\log_{radix}(comm_size))
 *
 * Example, comm_size=10
 *    radix=2         radix=3             radix=4
 *       0               0                   0
 *    / / \ \       / /  |  \ \         /   / \ \ \
 *   8 4   2 1     9 3   6   1 2       4   8  1 2 3
 *   | |\  |         |\  |\           /|\  |
 *   9 6 5 3         4 5 7 8         5 6 7 9
 *     |
 *     7
 */
int ompi_coll_base_bcast_intra_knomial(
    void *buf, int count, struct ompi_datatype_t *datatype, int root,
    struct ompi_communicator_t *comm, mca_coll_base_module_t *module,
    uint32_t segsize, int radix)
{
    int segcount = count;
    size_t typesize;
    mca_coll_base_comm_t *data = module->base_data;

    COLL_BASE_UPDATE_KMTREE(comm, module, root, radix);
    if (NULL == data->cached_kmtree) {
        /* Failed to build k-nomial tree for given radix */
        return ompi_coll_base_bcast_intra_binomial(buf, count, datatype, root, comm, module,
                                                   segcount);
    }

    /**
     * Determine number of elements sent per operation.
     */
    ompi_datatype_type_size(datatype, &typesize);
    COLL_BASE_COMPUTED_SEGCOUNT(segsize, typesize, segcount);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                 "coll:base:bcast_intra_knomial rank %d segsize %5d typesize %lu segcount %d",
                 ompi_comm_rank(comm), segsize, (unsigned long)typesize, segcount));

    return ompi_coll_base_bcast_intra_generic(buf, count, datatype, root, comm, module,
                                              segcount, data->cached_kmtree);
}

/*
 * ompi_coll_base_bcast_intra_scatter_allgather
 *
 * Function:  Bcast using a binomial tree scatter followed by a recursive
 *            doubling allgather.
 * Accepts:   Same arguments as MPI_Bcast
 * Returns:   MPI_SUCCESS or error code
 *
 * Limitations: count >= comm_size
 * Time complexity: O(\alpha\log(p) + \beta*m((p-1)/p))
 *   Binomial tree scatter: \alpha\log(p) + \beta*m((p-1)/p)
 *   Recursive doubling allgather: \alpha\log(p) + \beta*m((p-1)/p)
 *
 * Example, p=8, count=8, root=0
 *    Binomial tree scatter      Recursive doubling allgather
 * 0: --+  --+  --+  [0*******]  <-+ [01******]  <--+   [0123****] <--+
 * 1:   |   2|  <-+  [*1******]  <-+ [01******]  <--|-+ [0123****] <--+-+
 * 2:  4|  <-+  --+  [**2*****]  <-+ [**23****]  <--+ | [0123****] <--+-+-+
 * 3:   |       <-+  [***3****]  <-+ [**23****]  <----+ [0123****] <--+-+-+-+
 * 4: <-+  --+  --+  [****4***]  <-+ [****45**]  <--+   [****4567] <--+ | | |
 * 5:       2|  <-+  [*****5**]  <-+ [****45**]  <--|-+ [****4567] <----+ | |
 * 6:      <-+  --+  [******6*]  <-+ [******67]  <--+ | [****4567] <------+ |
 * 7:           <-+  [*******7]  <-+ [******67]  <--|-+ [****4567] <--------+
 */
int ompi_coll_base_bcast_intra_scatter_allgather(
    void *buf, int count, struct ompi_datatype_t *datatype, int root,
    struct ompi_communicator_t *comm, mca_coll_base_module_t *module,
    uint32_t segsize)
{
    int err = MPI_SUCCESS;
    ptrdiff_t lb, extent;
    size_t datatype_size;
    MPI_Status status;
    ompi_datatype_get_extent(datatype, &lb, &extent);
    ompi_datatype_type_size(datatype, &datatype_size);
    int comm_size = ompi_comm_size(comm);
    int rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                 "coll:base:bcast_intra_scatter_allgather: rank %d/%d",
                 rank, comm_size));
    if (comm_size < 2 || datatype_size == 0)
        return MPI_SUCCESS;

    if (count < comm_size) {
        OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                     "coll:base:bcast_intra_scatter_allgather: rank %d/%d "
                     "count %d switching to basic linear bcast",
                     rank, comm_size, count));
        return ompi_coll_base_bcast_intra_basic_linear(buf, count, datatype,
                                                       root, comm, module);
    }

    int vrank = (rank - root + comm_size) % comm_size;
    int recv_count = 0, send_count = 0;
    int scatter_count = (count + comm_size - 1) / comm_size; /* ceil(count / comm_size) */
    int curr_count = (rank == root) ? count : 0;

    /* Scatter by binomial tree: receive data from parent */
    int mask = 0x1;
    while (mask < comm_size) {
        if (vrank & mask) {
            int parent = (rank - mask + comm_size) % comm_size;
            /* Compute an upper bound on recv block size */
            recv_count = count - vrank * scatter_count;
            if (recv_count <= 0) {
                curr_count = 0;
            } else {
                /* Recv data from parent */
                err = MCA_PML_CALL(recv((char *)buf + (ptrdiff_t)vrank * scatter_count * extent,
                                        recv_count, datatype, parent,
                                        MCA_COLL_BASE_TAG_BCAST, comm, &status));
                if (MPI_SUCCESS != err) { goto cleanup_and_return; }
                /* Get received count */
                curr_count = (int)(status._ucount / datatype_size);
            }
            break;
        }
        mask <<= 1;
    }

    /* Scatter by binomial tree: send data to child processes */
    mask >>= 1;
    while (mask > 0) {
        if (vrank + mask < comm_size) {
            send_count = curr_count - scatter_count * mask;
            if (send_count > 0) {
                int child = (rank + mask) % comm_size;
                err = MCA_PML_CALL(send((char *)buf + (ptrdiff_t)scatter_count * (vrank + mask) * extent,
                                        send_count, datatype, child,
                                        MCA_COLL_BASE_TAG_BCAST,
                                        MCA_PML_BASE_SEND_STANDARD, comm));
                if (MPI_SUCCESS != err) { goto cleanup_and_return; }
                curr_count -= send_count;
            }
        }
        mask >>= 1;
    }

    /*
     * Allgather by recursive doubling
     * Each process has the curr_count elems in the buf[vrank * scatter_count, ...]
     */
    int rem_count = count - vrank * scatter_count;
    curr_count = (scatter_count < rem_count) ? scatter_count : rem_count;
    if (curr_count < 0)
        curr_count = 0;

    mask = 0x1;
    while (mask < comm_size) {
        int vremote = vrank ^ mask;
        int remote = (vremote + root) % comm_size;

        int vrank_tree_root = ompi_rounddown(vrank, mask);
        int vremote_tree_root = ompi_rounddown(vremote, mask);

        if (vremote < comm_size) {
            ptrdiff_t send_offset = vrank_tree_root * scatter_count * extent;
            ptrdiff_t recv_offset = vremote_tree_root * scatter_count * extent;
            recv_count = count - vremote_tree_root * scatter_count;
            if (recv_count < 0)
                recv_count = 0;
            err = ompi_coll_base_sendrecv((char *)buf + send_offset,
                                          curr_count, datatype, remote,
                                          MCA_COLL_BASE_TAG_BCAST,
                                          (char *)buf + recv_offset,
                                          recv_count, datatype, remote,
                                          MCA_COLL_BASE_TAG_BCAST,
                                          comm, &status, rank);
            if (MPI_SUCCESS != err) { goto cleanup_and_return; }
            recv_count = (int)(status._ucount / datatype_size);
            curr_count += recv_count;
        }

        /*
         * Non-power-of-two case: if process did not have destination process
         * to communicate with, we need to send him the current result.
         * Recursive halving algorithm is used for search of process.
         */
        if (vremote_tree_root + mask > comm_size) {
            int nprocs_alldata = comm_size - vrank_tree_root - mask;
            int offset = scatter_count * (vrank_tree_root + mask);
            for (int rhalving_mask = mask >> 1; rhalving_mask > 0; rhalving_mask >>= 1) {
                vremote = vrank ^ rhalving_mask;
                remote = (vremote + root) % comm_size;
                int tree_root = ompi_rounddown(vrank, rhalving_mask << 1);
                /*
                 * Send only if:
                 * 1) current process has data: (vremote > vrank) && (vrank < tree_root + nprocs_alldata)
                 * 2) remote process does not have data at any step: vremote >= tree_root + nprocs_alldata
                 */
                if ((vremote > vrank) && (vrank < tree_root + nprocs_alldata)
                    && (vremote >= tree_root + nprocs_alldata)) {
                    err = MCA_PML_CALL(send((char *)buf + (ptrdiff_t)offset * extent,
                                            recv_count, datatype, remote,
                                            MCA_COLL_BASE_TAG_BCAST,
                                            MCA_PML_BASE_SEND_STANDARD, comm));
                    if (MPI_SUCCESS != err) { goto cleanup_and_return; }

                } else if ((vremote < vrank) && (vremote < tree_root + nprocs_alldata)
                           && (vrank >= tree_root + nprocs_alldata)) {
                    err = MCA_PML_CALL(recv((char *)buf + (ptrdiff_t)offset * extent,
                                            count - offset, datatype, remote,
                                            MCA_COLL_BASE_TAG_BCAST,
                                            comm, &status));
                    if (MPI_SUCCESS != err) { goto cleanup_and_return; }
                    recv_count = (int)(status._ucount / datatype_size);
                    curr_count += recv_count;
                }
            }
        }
        mask <<= 1;
    }

cleanup_and_return:
    return err;
}

/*
 * ompi_coll_base_bcast_intra_scatter_allgather_ring
 *
 * Function:  Bcast using a binomial tree scatter followed by a ring allgather.
 * Accepts:   Same arguments as MPI_Bcast
 * Returns:   MPI_SUCCESS or error code
 *
 * Limitations: count >= comm_size
 * Time complexity: O(\alpha(\log(p) + p) + \beta*m((p-1)/p))
 *   Binomial tree scatter: \alpha\log(p) + \beta*m((p-1)/p)
 *   Ring allgather: 2(p-1)(\alpha + m/p\beta)
 *
 * Example, p=8, count=8, root=0
 *    Binomial tree scatter      Ring allgather: p - 1 steps
 * 0: --+  --+  --+  [0*******]  [0******7] [0*****67] [0****567] ... [01234567]
 * 1:   |   2|  <-+  [*1******]  [01******] [01*****7] [01****67] ... [01234567]
 * 2:  4|  <-+  --+  [**2*****]  [*12*****] [012*****] [012****7] ... [01234567]
 * 3:   |       <-+  [***3****]  [**23****] [*123****] [0123****] ... [01234567]
 * 4: <-+  --+  --+  [****4***]  [***34***] [**234***] [*1234***] ... [01234567]
 * 5:       2|  <-+  [*****5**]  [****45**] [***345**] [**2345**] ... [01234567]
 * 6:      <-+  --+  [******6*]  [*****56*] [****456*] [***3456*] ... [01234567]
 * 7:           <-+  [*******7]  [******67] [*****567] [****4567] ... [01234567]
 */
int ompi_coll_base_bcast_intra_scatter_allgather_ring(
    void *buf, int count, struct ompi_datatype_t *datatype, int root,
    struct ompi_communicator_t *comm, mca_coll_base_module_t *module,
    uint32_t segsize)
{
    int err = MPI_SUCCESS;
    ptrdiff_t lb, extent;
    size_t datatype_size;
    MPI_Status status;
    ompi_datatype_get_extent(datatype, &lb, &extent);
    ompi_datatype_type_size(datatype, &datatype_size);
    int comm_size = ompi_comm_size(comm);
    int rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                 "coll:base:bcast_intra_scatter_allgather_ring: rank %d/%d",
                 rank, comm_size));
    if (comm_size < 2 || datatype_size == 0)
        return MPI_SUCCESS;

    if (count < comm_size) {
        OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                     "coll:base:bcast_intra_scatter_allgather_ring: rank %d/%d "
                     "count %d switching to basic linear bcast",
                     rank, comm_size, count));
        return ompi_coll_base_bcast_intra_basic_linear(buf, count, datatype,
                                                       root, comm, module);
    }

    int vrank = (rank - root + comm_size) % comm_size;
    int recv_count = 0, send_count = 0;
    int scatter_count = (count + comm_size - 1) / comm_size; /* ceil(count / comm_size) */
    int curr_count = (rank == root) ? count : 0;

    /* Scatter by binomial tree: receive data from parent */
    int mask = 1;
    while (mask < comm_size) {
        if (vrank & mask) {
            int parent = (rank - mask + comm_size) % comm_size;
            /* Compute an upper bound on recv block size */
            recv_count = count - vrank * scatter_count;
            if (recv_count <= 0) {
                curr_count = 0;
            } else {
                /* Recv data from parent */
                err = MCA_PML_CALL(recv((char *)buf + (ptrdiff_t)vrank * scatter_count * extent,
                                        recv_count, datatype, parent,
                                        MCA_COLL_BASE_TAG_BCAST, comm, &status));
                if (MPI_SUCCESS != err) { goto cleanup_and_return; }
                /* Get received count */
                curr_count = (int)(status._ucount / datatype_size);
            }
            break;
        }
        mask <<= 1;
    }

    /* Scatter by binomial tree: send data to child processes */
    mask >>= 1;
    while (mask > 0) {
        if (vrank + mask < comm_size) {
            send_count = curr_count - scatter_count * mask;
            if (send_count > 0) {
                int child = (rank + mask) % comm_size;
                err = MCA_PML_CALL(send((char *)buf + (ptrdiff_t)scatter_count * (vrank + mask) * extent,
                                        send_count, datatype, child,
                                        MCA_COLL_BASE_TAG_BCAST,
                                        MCA_PML_BASE_SEND_STANDARD, comm));
                if (MPI_SUCCESS != err) { goto cleanup_and_return; }
                curr_count -= send_count;
            }
        }
        mask >>= 1;
    }

    /* Allgather by a ring algorithm */
    int left = (rank - 1 + comm_size) % comm_size;
    int right = (rank + 1) % comm_size;
    int send_block = vrank;
    int recv_block = (vrank - 1 + comm_size) % comm_size;

    for (int i = 1; i < comm_size; i++) {
        recv_count = (scatter_count < count - recv_block * scatter_count) ?
                      scatter_count : count - recv_block * scatter_count;
        if (recv_count < 0)
            recv_count = 0;
        ptrdiff_t recv_offset = recv_block * scatter_count * extent;

        send_count = (scatter_count < count - send_block * scatter_count) ?
                      scatter_count : count - send_block * scatter_count;
        if (send_count < 0)
            send_count = 0;
        ptrdiff_t send_offset = send_block * scatter_count * extent;

        err = ompi_coll_base_sendrecv((char *)buf + send_offset, send_count,
                                      datatype, right, MCA_COLL_BASE_TAG_BCAST,
                                      (char *)buf + recv_offset, recv_count,
                                      datatype, left, MCA_COLL_BASE_TAG_BCAST,
                                      comm, MPI_STATUS_IGNORE, rank);
        if (MPI_SUCCESS != err) { goto cleanup_and_return; }
        send_block = recv_block;
        recv_block = (recv_block - 1 + comm_size) % comm_size;
    }

cleanup_and_return:
    return err;
}

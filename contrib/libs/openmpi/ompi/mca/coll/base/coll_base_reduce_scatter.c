/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      University of Houston. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "mpi.h"
#include "opal/util/bit_ops.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/op/op.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "coll_base_topo.h"
#include "coll_base_util.h"

/*******************************************************************************
 * ompi_coll_base_reduce_scatter_intra_nonoverlapping
 *
 * This function just calls a reduce to rank 0, followed by an
 * appropriate scatterv call.
 */
int ompi_coll_base_reduce_scatter_intra_nonoverlapping(const void *sbuf, void *rbuf,
                                                        const int *rcounts,
                                                        struct ompi_datatype_t *dtype,
                                                        struct ompi_op_t *op,
                                                        struct ompi_communicator_t *comm,
                                                        mca_coll_base_module_t *module)
{
    int err, i, rank, size, total_count, *displs = NULL;
    const int root = 0;
    char *tmprbuf = NULL, *tmprbuf_free = NULL;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"coll:base:reduce_scatter_intra_nonoverlapping, rank %d", rank));

    for (i = 0, total_count = 0; i < size; i++) { total_count += rcounts[i]; }

    /* Reduce to rank 0 (root) and scatterv */
    tmprbuf = (char*) rbuf;
    if (MPI_IN_PLACE == sbuf) {
        /* rbuf on root (0) is big enough to hold whole data */
        if (root == rank) {
            err = comm->c_coll->coll_reduce (MPI_IN_PLACE, tmprbuf, total_count,
                                            dtype, op, root, comm, comm->c_coll->coll_reduce_module);
        } else {
            err = comm->c_coll->coll_reduce(tmprbuf, NULL, total_count,
                                           dtype, op, root, comm, comm->c_coll->coll_reduce_module);
        }
    } else {
        if (root == rank) {
            /* We must allocate temporary receive buffer on root to ensure that
               rbuf is big enough */
            ptrdiff_t dsize, gap = 0;
            dsize = opal_datatype_span(&dtype->super, total_count, &gap);

            tmprbuf_free = (char*) malloc(dsize);
            tmprbuf = tmprbuf_free - gap;
        }
        err = comm->c_coll->coll_reduce (sbuf, tmprbuf, total_count,
                                        dtype, op, root, comm, comm->c_coll->coll_reduce_module);
    }
    if (MPI_SUCCESS != err) {
        if (NULL != tmprbuf_free) free(tmprbuf_free);
        return err;
    }

    displs = (int*) malloc(size * sizeof(int));
    displs[0] = 0;
    for (i = 1; i < size; i++) {
        displs[i] = displs[i-1] + rcounts[i-1];
    }
    if (MPI_IN_PLACE == sbuf && root == rank) {
        err =  comm->c_coll->coll_scatterv (tmprbuf, rcounts, displs, dtype,
                                           MPI_IN_PLACE, 0, MPI_DATATYPE_NULL,
                                           root, comm, comm->c_coll->coll_scatterv_module);
    } else {
        err =  comm->c_coll->coll_scatterv (tmprbuf, rcounts, displs, dtype,
                                           rbuf, rcounts[rank], dtype,
                                           root, comm, comm->c_coll->coll_scatterv_module);
    }
    free(displs);
    if (NULL != tmprbuf_free) free(tmprbuf_free);

    return err;
}

/*
 * Recursive-halving function is (*mostly*) copied from the BASIC coll module.
 * I have removed the part which handles "large" message sizes
 * (non-overlapping version of reduce_Scatter).
 */

/* copied function (with appropriate renaming) starts here */

/*
 *  reduce_scatter_intra_basic_recursivehalving
 *
 *  Function:   - reduce scatter implementation using recursive-halving
 *                algorithm
 *  Accepts:    - same as MPI_Reduce_scatter()
 *  Returns:    - MPI_SUCCESS or error code
 *  Limitation: - Works only for commutative operations.
 */
int
ompi_coll_base_reduce_scatter_intra_basic_recursivehalving( const void *sbuf,
                                                            void *rbuf,
                                                            const int *rcounts,
                                                            struct ompi_datatype_t *dtype,
                                                            struct ompi_op_t *op,
                                                            struct ompi_communicator_t *comm,
                                                            mca_coll_base_module_t *module)
{
    int i, rank, size, count, err = OMPI_SUCCESS;
    int tmp_size, remain = 0, tmp_rank, *disps = NULL;
    ptrdiff_t extent, buf_size, gap = 0;
    char *recv_buf = NULL, *recv_buf_free = NULL;
    char *result_buf = NULL, *result_buf_free = NULL;

    /* Initialize */
    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,"coll:base:reduce_scatter_intra_basic_recursivehalving, rank %d", rank));

    /* Find displacements and the like */
    disps = (int*) malloc(sizeof(int) * size);
    if (NULL == disps) return OMPI_ERR_OUT_OF_RESOURCE;

    disps[0] = 0;
    for (i = 0; i < (size - 1); ++i) {
        disps[i + 1] = disps[i] + rcounts[i];
    }
    count = disps[size - 1] + rcounts[size - 1];

    /* short cut the trivial case */
    if (0 == count) {
        free(disps);
        return OMPI_SUCCESS;
    }

    /* get datatype information */
    ompi_datatype_type_extent(dtype, &extent);
    buf_size = opal_datatype_span(&dtype->super, count, &gap);

    /* Handle MPI_IN_PLACE */
    if (MPI_IN_PLACE == sbuf) {
        sbuf = rbuf;
    }

    /* Allocate temporary receive buffer. */
    recv_buf_free = (char*) malloc(buf_size);
    recv_buf = recv_buf_free - gap;
    if (NULL == recv_buf_free) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto cleanup;
    }

    /* allocate temporary buffer for results */
    result_buf_free = (char*) malloc(buf_size);
    result_buf = result_buf_free - gap;

    /* copy local buffer into the temporary results */
    err = ompi_datatype_sndrcv(sbuf, count, dtype, result_buf, count, dtype);
    if (OMPI_SUCCESS != err) goto cleanup;

    /* figure out power of two mapping: grow until larger than
       comm size, then go back one, to get the largest power of
       two less than comm size */
    tmp_size = opal_next_poweroftwo (size);
    tmp_size >>= 1;
    remain = size - tmp_size;

    /* If comm size is not a power of two, have the first "remain"
       procs with an even rank send to rank + 1, leaving a power of
       two procs to do the rest of the algorithm */
    if (rank < 2 * remain) {
        if ((rank & 1) == 0) {
            err = MCA_PML_CALL(send(result_buf, count, dtype, rank + 1,
                                    MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                    MCA_PML_BASE_SEND_STANDARD,
                                    comm));
            if (OMPI_SUCCESS != err) goto cleanup;

            /* we don't participate from here on out */
            tmp_rank = -1;
        } else {
            err = MCA_PML_CALL(recv(recv_buf, count, dtype, rank - 1,
                                    MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                    comm, MPI_STATUS_IGNORE));

            /* integrate their results into our temp results */
            ompi_op_reduce(op, recv_buf, result_buf, count, dtype);

            /* adjust rank to be the bottom "remain" ranks */
            tmp_rank = rank / 2;
        }
    } else {
        /* just need to adjust rank to show that the bottom "even
           remain" ranks dropped out */
        tmp_rank = rank - remain;
    }

    /* For ranks not kicked out by the above code, perform the
       recursive halving */
    if (tmp_rank >= 0) {
        int *tmp_disps = NULL, *tmp_rcounts = NULL;
        int mask, send_index, recv_index, last_index;

        /* recalculate disps and rcounts to account for the
           special "remainder" processes that are no longer doing
           anything */
        tmp_rcounts = (int*) malloc(tmp_size * sizeof(int));
        if (NULL == tmp_rcounts) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup;
        }
        tmp_disps = (int*) malloc(tmp_size * sizeof(int));
        if (NULL == tmp_disps) {
            free(tmp_rcounts);
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup;
        }

        for (i = 0 ; i < tmp_size ; ++i) {
            if (i < remain) {
                /* need to include old neighbor as well */
                tmp_rcounts[i] = rcounts[i * 2 + 1] + rcounts[i * 2];
            } else {
                tmp_rcounts[i] = rcounts[i + remain];
            }
        }

        tmp_disps[0] = 0;
        for (i = 0; i < tmp_size - 1; ++i) {
            tmp_disps[i + 1] = tmp_disps[i] + tmp_rcounts[i];
        }

        /* do the recursive halving communication.  Don't use the
           dimension information on the communicator because I
           think the information is invalidated by our "shrinking"
           of the communicator */
        mask = tmp_size >> 1;
        send_index = recv_index = 0;
        last_index = tmp_size;
        while (mask > 0) {
            int tmp_peer, peer, send_count, recv_count;
            struct ompi_request_t *request;

            tmp_peer = tmp_rank ^ mask;
            peer = (tmp_peer < remain) ? tmp_peer * 2 + 1 : tmp_peer + remain;

            /* figure out if we're sending, receiving, or both */
            send_count = recv_count = 0;
            if (tmp_rank < tmp_peer) {
                send_index = recv_index + mask;
                for (i = send_index ; i < last_index ; ++i) {
                    send_count += tmp_rcounts[i];
                }
                for (i = recv_index ; i < send_index ; ++i) {
                    recv_count += tmp_rcounts[i];
                }
            } else {
                recv_index = send_index + mask;
                for (i = send_index ; i < recv_index ; ++i) {
                    send_count += tmp_rcounts[i];
                }
                for (i = recv_index ; i < last_index ; ++i) {
                    recv_count += tmp_rcounts[i];
                }
            }

            /* actual data transfer.  Send from result_buf,
               receive into recv_buf */
            if (recv_count > 0) {
                err = MCA_PML_CALL(irecv(recv_buf + (ptrdiff_t)tmp_disps[recv_index] * extent,
                                         recv_count, dtype, peer,
                                         MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                         comm, &request));
                if (OMPI_SUCCESS != err) {
                    free(tmp_rcounts);
                    free(tmp_disps);
                    goto cleanup;
                }
            }
            if (send_count > 0) {
                err = MCA_PML_CALL(send(result_buf + (ptrdiff_t)tmp_disps[send_index] * extent,
                                        send_count, dtype, peer,
                                        MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                        MCA_PML_BASE_SEND_STANDARD,
                                        comm));
                if (OMPI_SUCCESS != err) {
                    free(tmp_rcounts);
                    free(tmp_disps);
                    goto cleanup;
                }
            }

            /* if we received something on this step, push it into
               the results buffer */
            if (recv_count > 0) {
                err = ompi_request_wait(&request, MPI_STATUS_IGNORE);
                if (OMPI_SUCCESS != err) {
                    free(tmp_rcounts);
                    free(tmp_disps);
                    goto cleanup;
                }

                ompi_op_reduce(op,
                               recv_buf + (ptrdiff_t)tmp_disps[recv_index] * extent,
                               result_buf + (ptrdiff_t)tmp_disps[recv_index] * extent,
                               recv_count, dtype);
            }

            /* update for next iteration */
            send_index = recv_index;
            last_index = recv_index + mask;
            mask >>= 1;
        }

        /* copy local results from results buffer into real receive buffer */
        if (0 != rcounts[rank]) {
            err = ompi_datatype_sndrcv(result_buf + disps[rank] * extent,
                                       rcounts[rank], dtype,
                                       rbuf, rcounts[rank], dtype);
            if (OMPI_SUCCESS != err) {
                free(tmp_rcounts);
                free(tmp_disps);
                goto cleanup;
            }
        }

        free(tmp_rcounts);
        free(tmp_disps);
    }

    /* Now fix up the non-power of two case, by having the odd
       procs send the even procs the proper results */
    if (rank < (2 * remain)) {
        if ((rank & 1) == 0) {
            if (rcounts[rank]) {
                err = MCA_PML_CALL(recv(rbuf, rcounts[rank], dtype, rank + 1,
                                        MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                        comm, MPI_STATUS_IGNORE));
                if (OMPI_SUCCESS != err) goto cleanup;
            }
        } else {
            if (rcounts[rank - 1]) {
                err = MCA_PML_CALL(send(result_buf + disps[rank - 1] * extent,
                                        rcounts[rank - 1], dtype, rank - 1,
                                        MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                        MCA_PML_BASE_SEND_STANDARD,
                                        comm));
                if (OMPI_SUCCESS != err) goto cleanup;
            }
        }
    }

 cleanup:
    if (NULL != disps) free(disps);
    if (NULL != recv_buf_free) free(recv_buf_free);
    if (NULL != result_buf_free) free(result_buf_free);

    return err;
}

/* copied function (with appropriate renaming) ends here */


/*
 *   ompi_coll_base_reduce_scatter_intra_ring
 *
 *   Function:       Ring algorithm for reduce_scatter operation
 *   Accepts:        Same as MPI_Reduce_scatter()
 *   Returns:        MPI_SUCCESS or error code
 *
 *   Description:    Implements ring algorithm for reduce_scatter:
 *                   the block sizes defined in rcounts are exchanged and
 8                    updated until they reach proper destination.
 *                   Algorithm requires 2 * max(rcounts) extra buffering
 *
 *   Limitations:    The algorithm DOES NOT preserve order of operations so it
 *                   can be used only for commutative operations.
 *         Example on 5 nodes:
 *         Initial state
 *   #      0              1             2              3             4
 *        [00]           [10]   ->     [20]           [30]           [40]
 *        [01]           [11]          [21]  ->       [31]           [41]
 *        [02]           [12]          [22]           [32]  ->       [42]
 *    ->  [03]           [13]          [23]           [33]           [43] --> ..
 *        [04]  ->       [14]          [24]           [34]           [44]
 *
 *        COMPUTATION PHASE
 *         Step 0: rank r sends block (r-1) to rank (r+1) and
 *                 receives block (r+1) from rank (r-1) [with wraparound].
 *   #      0              1             2              3             4
 *        [00]           [10]        [10+20]   ->     [30]           [40]
 *        [01]           [11]          [21]          [21+31]  ->     [41]
 *    ->  [02]           [12]          [22]           [32]         [32+42] -->..
 *      [43+03] ->       [13]          [23]           [33]           [43]
 *        [04]         [04+14]  ->     [24]           [34]           [44]
 *
 *         Step 1:
 *   #      0              1             2              3             4
 *        [00]           [10]        [10+20]       [10+20+30] ->     [40]
 *    ->  [01]           [11]          [21]          [21+31]      [21+31+41] ->
 *     [32+42+02] ->     [12]          [22]           [32]         [32+42]
 *        [03]        [43+03+13] ->    [23]           [33]           [43]
 *        [04]         [04+14]      [04+14+24]  ->    [34]           [44]
 *
 *         Step 2:
 *   #      0              1             2              3             4
 *     -> [00]           [10]        [10+20]       [10+20+30]   [10+20+30+40] ->
 *   [21+31+41+01]->     [11]          [21]          [21+31]      [21+31+41]
 *     [32+42+02]   [32+42+02+12]->    [22]           [32]         [32+42]
 *        [03]        [43+03+13]   [43+03+13+23]->    [33]           [43]
 *        [04]         [04+14]      [04+14+24]    [04+14+24+34] ->   [44]
 *
 *         Step 3:
 *   #      0             1              2              3             4
 * [10+20+30+40+00]     [10]         [10+20]       [10+20+30]   [10+20+30+40]
 *  [21+31+41+01] [21+31+41+01+11]     [21]          [21+31]      [21+31+41]
 *    [32+42+02]   [32+42+02+12] [32+42+02+12+22]     [32]         [32+42]
 *       [03]        [43+03+13]    [43+03+13+23] [43+03+13+23+33]    [43]
 *       [04]         [04+14]       [04+14+24]    [04+14+24+34] [04+14+24+34+44]
 *    DONE :)
 *
 */
int
ompi_coll_base_reduce_scatter_intra_ring( const void *sbuf, void *rbuf, const int *rcounts,
                                          struct ompi_datatype_t *dtype,
                                          struct ompi_op_t *op,
                                          struct ompi_communicator_t *comm,
                                          mca_coll_base_module_t *module)
{
    int ret, line, rank, size, i, k, recv_from, send_to, total_count, max_block_count;
    int inbi, *displs = NULL;
    char *tmpsend = NULL, *tmprecv = NULL, *accumbuf = NULL, *accumbuf_free = NULL;
    char *inbuf_free[2] = {NULL, NULL}, *inbuf[2] = {NULL, NULL};
    ptrdiff_t extent, max_real_segsize, dsize, gap = 0;
    ompi_request_t *reqs[2] = {NULL, NULL};

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                 "coll:base:reduce_scatter_intra_ring rank %d, size %d",
                 rank, size));

    /* Determine the maximum number of elements per node,
       corresponding block size, and displacements array.
    */
    displs = (int*) malloc(size * sizeof(int));
    if (NULL == displs) { ret = -1; line = __LINE__; goto error_hndl; }
    displs[0] = 0;
    total_count = rcounts[0];
    max_block_count = rcounts[0];
    for (i = 1; i < size; i++) {
        displs[i] = total_count;
        total_count += rcounts[i];
        if (max_block_count < rcounts[i]) max_block_count = rcounts[i];
    }

    /* Special case for size == 1 */
    if (1 == size) {
        if (MPI_IN_PLACE != sbuf) {
            ret = ompi_datatype_copy_content_same_ddt(dtype, total_count,
                                                      (char*)rbuf, (char*)sbuf);
            if (ret < 0) { line = __LINE__; goto error_hndl; }
        }
        free(displs);
        return MPI_SUCCESS;
    }

    /* Allocate and initialize temporary buffers, we need:
       - a temporary buffer to perform reduction (size total_count) since
       rbuf can be of rcounts[rank] size.
       - up to two temporary buffers used for communication/computation overlap.
    */
    ret = ompi_datatype_type_extent(dtype, &extent);
    if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }

    max_real_segsize = opal_datatype_span(&dtype->super, max_block_count, &gap);
    dsize = opal_datatype_span(&dtype->super, total_count, &gap);

    accumbuf_free = (char*)malloc(dsize);
    if (NULL == accumbuf_free) { ret = -1; line = __LINE__; goto error_hndl; }
    accumbuf = accumbuf_free - gap;

    inbuf_free[0] = (char*)malloc(max_real_segsize);
    if (NULL == inbuf_free[0]) { ret = -1; line = __LINE__; goto error_hndl; }
    inbuf[0] = inbuf_free[0] - gap;
    if (size > 2) {
        inbuf_free[1] = (char*)malloc(max_real_segsize);
        if (NULL == inbuf_free[1]) { ret = -1; line = __LINE__; goto error_hndl; }
        inbuf[1] = inbuf_free[1] - gap;
    }

    /* Handle MPI_IN_PLACE for size > 1 */
    if (MPI_IN_PLACE == sbuf) {
        sbuf = rbuf;
    }

    ret = ompi_datatype_copy_content_same_ddt(dtype, total_count,
                                              accumbuf, (char*)sbuf);
    if (ret < 0) { line = __LINE__; goto error_hndl; }

    /* Computation loop */

    /*
       For each of the remote nodes:
       - post irecv for block (r-2) from (r-1) with wrap around
       - send block (r-1) to (r+1)
       - in loop for every step k = 2 .. n
       - post irecv for block (r - 1 + n - k) % n
       - wait on block (r + n - k) % n to arrive
       - compute on block (r + n - k ) % n
       - send block (r + n - k) % n
       - wait on block (r)
       - compute on block (r)
       - copy block (r) to rbuf
       Note that we must be careful when computing the begining of buffers and
       for send operations and computation we must compute the exact block size.
    */
    send_to = (rank + 1) % size;
    recv_from = (rank + size - 1) % size;

    inbi = 0;
    /* Initialize first receive from the neighbor on the left */
    ret = MCA_PML_CALL(irecv(inbuf[inbi], max_block_count, dtype, recv_from,
                             MCA_COLL_BASE_TAG_REDUCE_SCATTER, comm,
                             &reqs[inbi]));
    if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }
    tmpsend = accumbuf + (ptrdiff_t)displs[recv_from] * extent;
    ret = MCA_PML_CALL(send(tmpsend, rcounts[recv_from], dtype, send_to,
                            MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                            MCA_PML_BASE_SEND_STANDARD, comm));
    if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }

    for (k = 2; k < size; k++) {
        const int prevblock = (rank + size - k) % size;

        inbi = inbi ^ 0x1;

        /* Post irecv for the current block */
        ret = MCA_PML_CALL(irecv(inbuf[inbi], max_block_count, dtype, recv_from,
                                 MCA_COLL_BASE_TAG_REDUCE_SCATTER, comm,
                                 &reqs[inbi]));
        if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }

        /* Wait on previous block to arrive */
        ret = ompi_request_wait(&reqs[inbi ^ 0x1], MPI_STATUS_IGNORE);
        if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }

        /* Apply operation on previous block: result goes to rbuf
           rbuf[prevblock] = inbuf[inbi ^ 0x1] (op) rbuf[prevblock]
        */
        tmprecv = accumbuf + (ptrdiff_t)displs[prevblock] * extent;
        ompi_op_reduce(op, inbuf[inbi ^ 0x1], tmprecv, rcounts[prevblock], dtype);

        /* send previous block to send_to */
        ret = MCA_PML_CALL(send(tmprecv, rcounts[prevblock], dtype, send_to,
                                MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                MCA_PML_BASE_SEND_STANDARD, comm));
        if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }
    }

    /* Wait on the last block to arrive */
    ret = ompi_request_wait(&reqs[inbi], MPI_STATUS_IGNORE);
    if (MPI_SUCCESS != ret) { line = __LINE__; goto error_hndl; }

    /* Apply operation on the last block (my block)
       rbuf[rank] = inbuf[inbi] (op) rbuf[rank] */
    tmprecv = accumbuf + (ptrdiff_t)displs[rank] * extent;
    ompi_op_reduce(op, inbuf[inbi], tmprecv, rcounts[rank], dtype);

    /* Copy result from tmprecv to rbuf */
    ret = ompi_datatype_copy_content_same_ddt(dtype, rcounts[rank], (char *)rbuf, tmprecv);
    if (ret < 0) { line = __LINE__; goto error_hndl; }

    if (NULL != displs) free(displs);
    if (NULL != accumbuf_free) free(accumbuf_free);
    if (NULL != inbuf_free[0]) free(inbuf_free[0]);
    if (NULL != inbuf_free[1]) free(inbuf_free[1]);

    return MPI_SUCCESS;

 error_hndl:
    OPAL_OUTPUT((ompi_coll_base_framework.framework_output, "%s:%4d\tRank %d Error occurred %d\n",
                 __FILE__, line, rank, ret));
    (void)line;  // silence compiler warning
    if (NULL != displs) free(displs);
    if (NULL != accumbuf_free) free(accumbuf_free);
    if (NULL != inbuf_free[0]) free(inbuf_free[0]);
    if (NULL != inbuf_free[1]) free(inbuf_free[1]);
    return ret;
}

/*
 * ompi_sum_counts: Returns sum of counts [lo, hi]
 *                  lo, hi in {0, 1, ..., nprocs_pof2 - 1}
 */
static int ompi_sum_counts(const int *counts, int *displs, int nprocs_rem, int lo, int hi)
{
    /* Adjust lo and hi for taking into account blocks of excluded processes */
    lo = (lo < nprocs_rem) ? lo * 2 : lo + nprocs_rem;
    hi = (hi < nprocs_rem) ? hi * 2 + 1 : hi + nprocs_rem;
    return displs[hi] + counts[hi] - displs[lo];
}

/*
 * ompi_coll_base_reduce_scatter_intra_butterfly
 *
 * Function:  Butterfly algorithm for reduce_scatter
 * Accepts:   Same as MPI_Reduce_scatter
 * Returns:   MPI_SUCCESS or error code
 *
 * Description:  Implements butterfly algorithm for MPI_Reduce_scatter [*].
 *               The algorithm can be used both by commutative and non-commutative
 *               operations, for power-of-two and non-power-of-two number of processes.
 *
 * [*] J.L. Traff. An improved Algorithm for (non-commutative) Reduce-scatter
 *     with an Application // Proc. of EuroPVM/MPI, 2005. -- pp. 129-137.
 *
 * Time complexity: O(m\lambda + log(p)\alpha + m\beta + m\gamma),
 *   where m = sum of rcounts[], p = comm_size
 * Memory requirements (per process): 2 * m * typesize + comm_size
 *
 * Example: comm_size=6, nprocs_pof2=4, nprocs_rem=2, rcounts[]=1, sbuf=[0,1,...,5]
 * Step 1. Reduce the number of processes to 4
 * rank 0: [0|1|2|3|4|5]: send to 1: vrank -1
 * rank 1: [0|1|2|3|4|5]: recv from 0, op: vrank 0: [0|2|4|6|8|10]
 * rank 2: [0|1|2|3|4|5]: send to 3: vrank -1
 * rank 3: [0|1|2|3|4|5]: recv from 2, op: vrank 1: [0|2|4|6|8|10]
 * rank 4: [0|1|2|3|4|5]: vrank 2: [0|1|2|3|4|5]
 * rank 5: [0|1|2|3|4|5]: vrank 3: [0|1|2|3|4|5]
 *
 * Step 2. Butterfly. Buffer of 6 elements is divided into 4 blocks.
 * Round 1 (mask=1, nblocks=2)
 * 0: vrank -1
 * 1: vrank  0 [0 2|4 6|8|10]: exch with 1: send [2,3], recv [0,1]: [0 4|8 12|*|*]
 * 2: vrank -1
 * 3: vrank  1 [0 2|4 6|8|10]: exch with 0: send [0,1], recv [2,3]: [**|**|16|20]
 * 4: vrank  2 [0 1|2 3|4|5] : exch with 3: send [2,3], recv [0,1]: [0 2|4 6|*|*]
 * 5: vrank  3 [0 1|2 3|4|5] : exch with 2: send [0,1], recv [2,3]: [**|**|8|10]
 *
 * Round 2 (mask=2, nblocks=1)
 * 0: vrank -1
 * 1: vrank  0 [0 4|8 12|*|*]: exch with 2: send [1], recv [0]: [0 6|**|*|*]
 * 2: vrank -1
 * 3: vrank  1 [**|**|16|20] : exch with 3: send [3], recv [2]: [**|**|24|*]
 * 4: vrank  2 [0 2|4 6|*|*] : exch with 0: send [0], recv [1]: [**|12 18|*|*]
 * 5: vrank  3 [**|**|8|10]  : exch with 1: send [2], recv [3]: [**|**|*|30]
 *
 * Step 3. Exchange with remote process according to a mirror permutation:
 *         mperm(0)=0, mperm(1)=2, mperm(2)=1, mperm(3)=3
 * 0: vrank -1: recv "0" from process 0
 * 1: vrank  0 [0 6|**|*|*]: send "0" to 0, copy "6" to rbuf (mperm(0)=0)
 * 2: vrank -1: recv result "12" from process 4
 * 3: vrank  1 [**|**|24|*]
 * 4: vrank  2 [**|12 18|*|*]: send "12" to 2, send "18" to 3, recv "24" from 3
 * 5: vrank  3 [**|**|*|30]: copy "30" to rbuf (mperm(3)=3)
 */
int
ompi_coll_base_reduce_scatter_intra_butterfly(
    const void *sbuf, void *rbuf, const int *rcounts, struct ompi_datatype_t *dtype,
    struct ompi_op_t *op, struct ompi_communicator_t *comm,
    mca_coll_base_module_t *module)
{
    char *tmpbuf[2] = {NULL, NULL}, *psend, *precv;
    int *displs = NULL, index;
    ptrdiff_t span, gap, totalcount, extent;
    int err = MPI_SUCCESS;
    int comm_size = ompi_comm_size(comm);
    int rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                 "coll:base:reduce_scatter_intra_butterfly: rank %d/%d",
                 rank, comm_size));
    if (comm_size < 2)
        return MPI_SUCCESS;

    displs = malloc(sizeof(*displs) * comm_size);
    if (NULL == displs) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto cleanup_and_return;
    }
    displs[0] = 0;
    for (int i = 1; i < comm_size; i++) {
        displs[i] = displs[i - 1] + rcounts[i - 1];
    }
    totalcount = displs[comm_size - 1] + rcounts[comm_size - 1];

    ompi_datatype_type_extent(dtype, &extent);
    span = opal_datatype_span(&dtype->super, totalcount, &gap);
    tmpbuf[0] = malloc(span);
    tmpbuf[1] = malloc(span);
    if (NULL == tmpbuf[0] || NULL == tmpbuf[1]) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto cleanup_and_return;
    }
    psend = tmpbuf[0] - gap;
    precv = tmpbuf[1] - gap;

    if (sbuf != MPI_IN_PLACE) {
        err = ompi_datatype_copy_content_same_ddt(dtype, totalcount, psend, (char *)sbuf);
        if (MPI_SUCCESS != err) { goto cleanup_and_return; }
    } else {
        err = ompi_datatype_copy_content_same_ddt(dtype, totalcount, psend, rbuf);
        if (MPI_SUCCESS != err) { goto cleanup_and_return; }
    }

    /*
     * Step 1. Reduce the number of processes to the nearest lower power of two
     * p' = 2^{\floor{\log_2 p}} by removing r = p - p' processes.
     * In the first 2r processes (ranks 0 to 2r - 1), all the even ranks send
     * the input vector to their neighbor (rank + 1) and all the odd ranks recv
     * the input vector and perform local reduction.
     * The odd ranks (0 to 2r - 1) contain the reduction with the input
     * vector on their neighbors (the even ranks). The first r odd
     * processes and the p - 2r last processes are renumbered from
     * 0 to 2^{\floor{\log_2 p}} - 1. Even ranks do not participate in the
     * rest of the algorithm.
     */

    /* Find nearest power-of-two less than or equal to comm_size */
    int nprocs_pof2 = opal_next_poweroftwo(comm_size);
    nprocs_pof2 >>= 1;
    int nprocs_rem = comm_size - nprocs_pof2;
    int log2_size = opal_cube_dim(nprocs_pof2);

    int vrank = -1;
    if (rank < 2 * nprocs_rem) {
        if ((rank % 2) == 0) {
            /* Even process */
            err = MCA_PML_CALL(send(psend, totalcount, dtype, rank + 1,
                                    MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
            if (OMPI_SUCCESS != err) { goto cleanup_and_return; }
            /* This process does not participate in the rest of the algorithm */
            vrank = -1;
        } else {
            /* Odd process */
            err = MCA_PML_CALL(recv(precv, totalcount, dtype, rank - 1,
                                    MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                    comm, MPI_STATUS_IGNORE));
            if (OMPI_SUCCESS != err) { goto cleanup_and_return; }
            ompi_op_reduce(op, precv, psend, totalcount, dtype);
            /* Adjust rank to be the bottom "remain" ranks */
            vrank = rank / 2;
        }
    } else {
        /* Adjust rank to show that the bottom "even remain" ranks dropped out */
        vrank = rank - nprocs_rem;
    }

    if (vrank != -1) {
        /*
         * Now, psend vector of size totalcount is divided into nprocs_pof2 blocks:
         * block 0:   rcounts[0] and rcounts[1] -- for process 0 and 1
         * block 1:   rcounts[2] and rcounts[3] -- for process 2 and 3
         * ...
         * block r-1: rcounts[2*(r-1)] and rcounts[2*(r-1)+1]
         * block r:   rcounts[r+r]
         * block r+1: rcounts[r+r+1]
         * ...
         * block nprocs_pof2 - 1: rcounts[r+nprocs_pof2-1]
         */
        int nblocks = nprocs_pof2, send_index = 0, recv_index = 0;
        for (int mask = 1; mask < nprocs_pof2; mask <<= 1) {
            int vpeer = vrank ^ mask;
            int peer = (vpeer < nprocs_rem) ? vpeer * 2 + 1 : vpeer + nprocs_rem;

            nblocks /= 2;
            if ((vrank & mask) == 0) {
                /* Send the upper half of reduction buffer, recv the lower half */
                send_index += nblocks;
            } else {
                /* Send the upper half of reduction buffer, recv the lower half */
                recv_index += nblocks;
            }

            /* Send blocks: [send_index, send_index + nblocks - 1] */
            int send_count = ompi_sum_counts(rcounts, displs, nprocs_rem,
                                             send_index, send_index + nblocks - 1);
            index = (send_index < nprocs_rem) ? 2 * send_index : nprocs_rem + send_index;
            ptrdiff_t sdispl = displs[index];

            /* Recv blocks: [recv_index, recv_index + nblocks - 1] */
            int recv_count = ompi_sum_counts(rcounts, displs, nprocs_rem,
                                             recv_index, recv_index + nblocks - 1);
            index = (recv_index < nprocs_rem) ? 2 * recv_index : nprocs_rem + recv_index;
            ptrdiff_t rdispl = displs[index];

            err = ompi_coll_base_sendrecv(psend + (ptrdiff_t)sdispl * extent, send_count,
                                          dtype, peer, MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                          precv + (ptrdiff_t)rdispl * extent, recv_count,
                                          dtype, peer, MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                          comm, MPI_STATUS_IGNORE, rank);
            if (MPI_SUCCESS != err) { goto cleanup_and_return; }

            if (vrank < vpeer) {
                /* precv = psend <op> precv */
                ompi_op_reduce(op, psend + (ptrdiff_t)rdispl * extent,
                               precv + (ptrdiff_t)rdispl * extent, recv_count, dtype);
                char *p = psend;
                psend = precv;
                precv = p;
            } else {
                /* psend = precv <op> psend */
                ompi_op_reduce(op, precv + (ptrdiff_t)rdispl * extent,
                               psend + (ptrdiff_t)rdispl * extent, recv_count, dtype);
            }
            send_index = recv_index;
        }
        /*
         * psend points to the result block [send_index]
         * Exchange results with remote process according to a mirror permutation.
         */
        int vpeer = ompi_mirror_perm(vrank, log2_size);
        int peer = (vpeer < nprocs_rem) ? vpeer * 2 + 1 : vpeer + nprocs_rem;
        index = (send_index < nprocs_rem) ? 2 * send_index : nprocs_rem + send_index;

        if (vpeer < nprocs_rem) {
            /*
             * Process has two blocks: for excluded process and own.
             * Send the first block to excluded process.
             */
            err = MCA_PML_CALL(send(psend + (ptrdiff_t)displs[index] * extent,
                                    rcounts[index], dtype, peer - 1,
                                    MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
            if (MPI_SUCCESS != err) { goto cleanup_and_return; }
        }

        /* If process has two blocks, then send the second block (own block) */
        if (vpeer < nprocs_rem)
            index++;
        if (vpeer != vrank) {
            err = ompi_coll_base_sendrecv(psend + (ptrdiff_t)displs[index] * extent,
                                          rcounts[index], dtype, peer,
                                          MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                          rbuf, rcounts[rank], dtype, peer,
                                          MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                          comm, MPI_STATUS_IGNORE, rank);
            if (MPI_SUCCESS != err) { goto cleanup_and_return; }
        } else {
            err = ompi_datatype_copy_content_same_ddt(dtype, rcounts[rank], rbuf,
                                                      psend + (ptrdiff_t)displs[rank] * extent);
            if (MPI_SUCCESS != err) { goto cleanup_and_return; }
        }

    } else {
        /* Excluded process: receive result */
        int vpeer = ompi_mirror_perm((rank + 1) / 2, log2_size);
        int peer = (vpeer < nprocs_rem) ? vpeer * 2 + 1 : vpeer + nprocs_rem;
        err = MCA_PML_CALL(recv(rbuf, rcounts[rank], dtype, peer,
                                MCA_COLL_BASE_TAG_REDUCE_SCATTER, comm,
                                MPI_STATUS_IGNORE));
        if (OMPI_SUCCESS != err) { goto cleanup_and_return; }
    }

cleanup_and_return:
    if (displs)
        free(displs);
    if (tmpbuf[0])
        free(tmpbuf[0]);
    if (tmpbuf[1])
        free(tmpbuf[1]);
    return err;
}

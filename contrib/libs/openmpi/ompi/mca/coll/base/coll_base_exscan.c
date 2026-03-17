/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Siberian State University of Telecommunications
 *                         and Information Science. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
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
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/coll/base/coll_base_util.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/op/op.h"

/*
 * ompi_coll_base_exscan_intra_linear
 *
 * Function:  Linear algorithm for exclusive scan.
 * Accepts:   Same as MPI_Exscan
 * Returns:   MPI_SUCCESS or error code
 */
int
ompi_coll_base_exscan_intra_linear(const void *sbuf, void *rbuf, int count,
                                  struct ompi_datatype_t *dtype,
                                  struct ompi_op_t *op,
                                  struct ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module)
{
    int size, rank, err;
    ptrdiff_t dsize, gap;
    char *free_buffer = NULL;
    char *reduce_buffer = NULL;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    /* For MPI_IN_PLACE, just adjust send buffer to point to
     * receive buffer. */
    if (MPI_IN_PLACE == sbuf) {
        sbuf = rbuf;
    }

    /* If we're rank 0, then just send our sbuf to the next rank, and
     * we are done. */
    if (0 == rank) {
        return MCA_PML_CALL(send(sbuf, count, dtype, rank + 1,
                                 MCA_COLL_BASE_TAG_EXSCAN,
                                 MCA_PML_BASE_SEND_STANDARD, comm));
    }

    /* If we're the last rank, then just receive the result from the
     * prior rank, and we are done. */
    else if ((size - 1) == rank) {
        return MCA_PML_CALL(recv(rbuf, count, dtype, rank - 1,
                                 MCA_COLL_BASE_TAG_EXSCAN, comm,
                                 MPI_STATUS_IGNORE));
    }

    /* Otherwise, get the result from the prior rank, combine it with my
     * data, and send it to the next rank */

    /* Get a temporary buffer to perform the reduction into.  Rationale
     * for malloc'ing this size is provided in coll_basic_reduce.c. */
    dsize = opal_datatype_span(&dtype->super, count, &gap);

    free_buffer = (char*)malloc(dsize);
    if (NULL == free_buffer) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    reduce_buffer = free_buffer - gap;
    err = ompi_datatype_copy_content_same_ddt(dtype, count,
                                              reduce_buffer, (char*)sbuf);

    /* Receive the reduced value from the prior rank */
    err = MCA_PML_CALL(recv(rbuf, count, dtype, rank - 1,
                            MCA_COLL_BASE_TAG_EXSCAN, comm, MPI_STATUS_IGNORE));
    if (MPI_SUCCESS != err) {
        goto error;
    }

    /* Now reduce the prior rank's result with my source buffer.  The source
     * buffer had been previously copied into the temporary reduce_buffer. */
    ompi_op_reduce(op, rbuf, reduce_buffer, count, dtype);

    /* Send my result off to the next rank */
    err = MCA_PML_CALL(send(reduce_buffer, count, dtype, rank + 1,
                            MCA_COLL_BASE_TAG_EXSCAN,
                            MCA_PML_BASE_SEND_STANDARD, comm));
    /* Error */
  error:
    free(free_buffer);

    /* All done */
    return err;
}


/*
 * ompi_coll_base_exscan_intra_recursivedoubling
 *
 * Function:  Recursive doubling algorithm for exclusive scan.
 * Accepts:   Same as MPI_Exscan
 * Returns:   MPI_SUCCESS or error code
 *
 * Description:  Implements recursive doubling algorithm for MPI_Exscan.
 *               The algorithm preserves order of operations so it can
 *               be used both by commutative and non-commutative operations.
 *
 * Example for 5 processes and commutative operation MPI_SUM:
 * Process:  0                 1             2              3               4
 * recvbuf:  -                 -             -              -               -
 *   psend: [0]               [1]           [2]            [3]             [4]
 *
 *  Step 1:
 * recvbuf:  -                [0]            -             [2]              -
 *   psend: [1+0]             [0+1]         [3+2]          [2+3]           [4]
 *
 *  Step 2:
 * recvbuf:  -                [0]            [1+0]          [(0+1)+2]       -
 *   psend: [(3+2)+(1+0)]     [(2+3)+(0+1)]  [(1+0)+(3+2)]  [(1+0)+(2+3)]  [4]
 *
 *  Step 3:
 * recvbuf:  -                [0]            [1+0]          [(0+1)+2]      [(3+2)+(1+0)]
 *   psend: [4+((3+2)+(1+0))]                                              [((3+2)+(1+0))+4]
 *
 * Time complexity (worst case): \ceil(\log_2(p))(2\alpha + 2m\beta + 2m\gamma)
 * Memory requirements (per process): 2 * count * typesize = O(count)
 * Limitations: intra-communicators only
 */
int ompi_coll_base_exscan_intra_recursivedoubling(
    const void *sendbuf, void *recvbuf, int count, struct ompi_datatype_t *datatype,
    struct ompi_op_t *op, struct ompi_communicator_t *comm,
    mca_coll_base_module_t *module)
{
    int err = MPI_SUCCESS;
    char *tmpsend_raw = NULL, *tmprecv_raw = NULL;
    int comm_size = ompi_comm_size(comm);
    int rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output, "coll:base:exscan_intra_recursivedoubling: rank %d/%d",
                 rank, comm_size));
    if (count == 0)
        return MPI_SUCCESS;
    if (comm_size < 2)
        return MPI_SUCCESS;

    ptrdiff_t dsize, gap;
    dsize = opal_datatype_span(&datatype->super, count, &gap);
    tmpsend_raw = malloc(dsize);
    tmprecv_raw = malloc(dsize);
    if (NULL == tmpsend_raw || NULL == tmprecv_raw) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto cleanup_and_return;
    }
    char *psend = tmpsend_raw - gap;
    char *precv = tmprecv_raw - gap;
    if (sendbuf != MPI_IN_PLACE) {
        err = ompi_datatype_copy_content_same_ddt(datatype, count, psend, (char *)sendbuf);
        if (MPI_SUCCESS != err) { goto cleanup_and_return; }
    } else {
        err = ompi_datatype_copy_content_same_ddt(datatype, count, psend, recvbuf);
        if (MPI_SUCCESS != err) { goto cleanup_and_return; }
    }
    int is_commute = ompi_op_is_commute(op);
    int is_first_block = 1;

    for (int mask = 1; mask < comm_size; mask <<= 1) {
        int remote = rank ^ mask;
        if (remote < comm_size) {
            err = ompi_coll_base_sendrecv(psend, count, datatype, remote,
                                          MCA_COLL_BASE_TAG_EXSCAN,
                                          precv, count, datatype, remote,
                                          MCA_COLL_BASE_TAG_EXSCAN, comm,
                                          MPI_STATUS_IGNORE, rank);
            if (MPI_SUCCESS != err) { goto cleanup_and_return; }

            if (rank > remote) {
                /* Assertion: rank > 0 and rbuf is valid */
                if (is_first_block) {
                    err = ompi_datatype_copy_content_same_ddt(datatype, count,
                                                              recvbuf, precv);
                    if (MPI_SUCCESS != err) { goto cleanup_and_return; }
                    is_first_block = 0;
                } else {
                    /* Accumulate prefix reduction: recvbuf = precv <op> recvbuf */
                    ompi_op_reduce(op, precv, recvbuf, count, datatype);
                }
                /* Partial result: psend = precv <op> psend */
                ompi_op_reduce(op, precv, psend, count, datatype);
            } else {
                if (is_commute) {
                    /* psend = precv <op> psend */
                    ompi_op_reduce(op, precv, psend, count, datatype);
                } else {
                    /* precv = psend <op> precv */
                    ompi_op_reduce(op, psend, precv, count, datatype);
                    char *tmp = psend;
                    psend = precv;
                    precv = tmp;
                }
            }
        }
    }

cleanup_and_return:
    if (NULL != tmpsend_raw)
        free(tmpsend_raw);
    if (NULL != tmprecv_raw)
        free(tmprecv_raw);
    return err;
}

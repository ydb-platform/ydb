/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_basic.h"

#include <stdio.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/op/op.h"


/*
 *	reduce_log_intra
 *
 *	Function:	- reduction using O(log N) algorithm
 *	Accepts:	- same as MPI_Reduce()
 *	Returns:	- MPI_SUCCESS or error code
 *
 *
 *      Performing reduction on each dimension of the hypercube.
 *	An example for 8 procs (dimensions = 3):
 *
 *      Stage 1, reduce on X dimension,  1 -> 0, 3 -> 2, 5 -> 4, 7 -> 6
 *
 *          6----<---7		proc_0: 0+1
 *         /|       /|		proc_1: 1
 *        / |      / |		proc_2: 2+3
 *       /  |     /  |		proc_3: 3
 *      4----<---5   |		proc_4: 4+5
 *      |   2--< |---3		proc_5: 5
 *      |  /     |  /		proc_6: 6+7
 *      | /      | /		proc_7: 7
 *      |/       |/
 *      0----<---1
 *
 *	Stage 2, reduce on Y dimension, 2 -> 0, 6 -> 4
 *
 *          6--------7		proc_0: 0+1+2+3
 *         /|       /|		proc_1: 1
 *        v |      / |		proc_2: 2+3
 *       /  |     /  |		proc_3: 3
 *      4--------5   |		proc_4: 4+5+6+7
 *      |   2--- |---3		proc_5: 5
 *      |  /     |  /		proc_6: 6+7
 *      | v      | /		proc_7: 7
 *      |/       |/
 *      0--------1
 *
 *	Stage 3, reduce on Z dimension, 4 -> 0
 *
 *          6--------7		proc_0: 0+1+2+3+4+5+6+7
 *         /|       /|		proc_1: 1
 *        / |      / |		proc_2: 2+3
 *       /  |     /  |		proc_3: 3
 *      4--------5   |		proc_4: 4+5+6+7
 *      |   2--- |---3		proc_5: 5
 *      v  /     |  /		proc_6: 6+7
 *      | /      | /		proc_7: 7
 *      |/       |/
 *      0--------1
 *
 *
 */
int
mca_coll_basic_reduce_log_intra(const void *sbuf, void *rbuf, int count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                int root, struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module)
{
    int i, size, rank, vrank;
    int err, peer, dim, mask;
    ptrdiff_t lb, extent, dsize, gap;
    char *free_buffer = NULL;
    char *free_rbuf = NULL;
    char *pml_buffer = NULL;
    char *snd_buffer = NULL;
    char *rcv_buffer = (char*)rbuf;
    char *inplace_temp = NULL;

    /* JMS Codearound for now -- if the operations is not communative,
     * just call the linear algorithm.  Need to talk to Edgar / George
     * about fixing this algorithm here to work with non-communative
     * operations. */

    if (!ompi_op_is_commute(op)) {
        return ompi_coll_base_reduce_intra_basic_linear(sbuf, rbuf, count, dtype,
                                                        op, root, comm, module);
    }

    /* Some variables */
    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);
    vrank = ompi_op_is_commute(op) ? (rank - root + size) % size : rank;
    dim = comm->c_cube_dim;

    /* Allocate the incoming and resulting message buffers.  See lengthy
     * rationale above. */

    ompi_datatype_get_extent(dtype, &lb, &extent);
    dsize = opal_datatype_span(&dtype->super, count, &gap);

    free_buffer = (char*)malloc(dsize);
    if (NULL == free_buffer) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    pml_buffer = free_buffer - gap;
    /* read the comment about commutative operations (few lines down
     * the page) */
    if (ompi_op_is_commute(op)) {
        rcv_buffer = pml_buffer;
    }

    /* Allocate sendbuf in case the MPI_IN_PLACE option has been used. See lengthy
     * rationale above. */

    if (MPI_IN_PLACE == sbuf) {
        inplace_temp = (char*)malloc(dsize);
        if (NULL == inplace_temp) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup_and_return;
        }
        sbuf = inplace_temp - gap;
        err = ompi_datatype_copy_content_same_ddt(dtype, count, (char*)sbuf, (char*)rbuf);
    }
    snd_buffer = (char*)sbuf;

    if (rank != root && 0 == (vrank & 1)) {
        /* root is the only one required to provide a valid rbuf.
         * Assume rbuf is invalid for all other ranks, so fix it up
         * here to be valid on all non-leaf ranks */
        free_rbuf = (char*)malloc(dsize);
        if (NULL == free_rbuf) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup_and_return;
        }
        rbuf = free_rbuf - gap;
    }

    /* Loop over cube dimensions. High processes send to low ones in the
     * dimension. */

    for (i = 0, mask = 1; i < dim; ++i, mask <<= 1) {

        /* A high-proc sends to low-proc and stops. */
        if (vrank & mask) {
            peer = vrank & ~mask;
            if (ompi_op_is_commute(op)) {
                peer = (peer + root) % size;
            }

            err = MCA_PML_CALL(send(snd_buffer, count,
                                    dtype, peer, MCA_COLL_BASE_TAG_REDUCE,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
            if (MPI_SUCCESS != err) {
                goto cleanup_and_return;
            }
            snd_buffer = (char*)rbuf;
            break;
        }

        /* A low-proc receives, reduces, and moves to a higher
         * dimension. */

        else {
            peer = vrank | mask;
            if (peer >= size) {
                continue;
            }
            if (ompi_op_is_commute(op)) {
                peer = (peer + root) % size;
            }

            /* Most of the time (all except the first one for commutative
             * operations) we receive in the user provided buffer
             * (rbuf). But the exception is here to allow us to dont have
             * to copy from the sbuf to a temporary location. If the
             * operation is commutative we dont care in which order we
             * apply the operation, so for the first time we can receive
             * the data in the pml_buffer and then apply to operation
             * between this buffer and the user provided data. */

            err = MCA_PML_CALL(recv(rcv_buffer, count, dtype, peer,
                                    MCA_COLL_BASE_TAG_REDUCE, comm,
                                    MPI_STATUS_IGNORE));
            if (MPI_SUCCESS != err) {
                goto cleanup_and_return;
            }
            /* Perform the operation. The target is always the user
             * provided buffer We do the operation only if we receive it
             * not in the user buffer */
            if (snd_buffer != sbuf) {
                /* the target buffer is the locally allocated one */
                ompi_op_reduce(op, rcv_buffer, pml_buffer, count, dtype);
            } else {
                /* If we're commutative, we don't care about the order of
                 * operations and we can just reduce the operations now.
                 * If we are not commutative, we have to copy the send
                 * buffer into a temp buffer (pml_buffer) and then reduce
                 * what we just received against it. */
                if (!ompi_op_is_commute(op)) {
                    ompi_datatype_copy_content_same_ddt(dtype, count, pml_buffer,
                                                   (char*)sbuf);
                    ompi_op_reduce(op, rbuf, pml_buffer, count, dtype);
                } else {
                    ompi_op_reduce(op, (void *)sbuf, pml_buffer, count, dtype);
                }
                /* now we have to send the buffer containing the computed data */
                snd_buffer = pml_buffer;
                /* starting from now we always receive in the user
                 * provided buffer */
                rcv_buffer = (char*)rbuf;
            }
        }
    }

    /* Get the result to the root if needed. */
    err = MPI_SUCCESS;
    if (0 == vrank) {
        if (root == rank) {
            ompi_datatype_copy_content_same_ddt(dtype, count, (char*)rbuf, snd_buffer);
        } else {
            err = MCA_PML_CALL(send(snd_buffer, count,
                                    dtype, root, MCA_COLL_BASE_TAG_REDUCE,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
        }
    } else if (rank == root) {
        err = MCA_PML_CALL(recv(rcv_buffer, count, dtype, 0,
                                MCA_COLL_BASE_TAG_REDUCE,
                                comm, MPI_STATUS_IGNORE));
        if (rcv_buffer != rbuf) {
            ompi_op_reduce(op, rcv_buffer, rbuf, count, dtype);
        }
    }

  cleanup_and_return:
    if (NULL != inplace_temp) {
        free(inplace_temp);
    }
    if (NULL != free_buffer) {
        free(free_buffer);
    }
    if (NULL != free_rbuf) {
        free(free_rbuf);
    }

    /* All done */

    return err;
}


/*
 *	reduce_lin_inter
 *
 *	Function:	- reduction using O(N) algorithm
 *	Accepts:	- same as MPI_Reduce()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_reduce_lin_inter(const void *sbuf, void *rbuf, int count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                int root, struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module)
{
    int i, err, size;
    ptrdiff_t dsize, gap;
    char *free_buffer = NULL;
    char *pml_buffer = NULL;

    /* Initialize */
    size = ompi_comm_remote_size(comm);

    if (MPI_PROC_NULL == root) {
        /* do nothing */
        err = OMPI_SUCCESS;
    } else if (MPI_ROOT != root) {
        /* If not root, send data to the root. */
        err = MCA_PML_CALL(send(sbuf, count, dtype, root,
                                MCA_COLL_BASE_TAG_REDUCE,
                                MCA_PML_BASE_SEND_STANDARD, comm));
    } else {
        /* Root receives and reduces messages  */
        dsize = opal_datatype_span(&dtype->super, count, &gap);

        free_buffer = (char*)malloc(dsize);
        if (NULL == free_buffer) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        pml_buffer = free_buffer - gap;


        /* Initialize the receive buffer. */
        err = MCA_PML_CALL(recv(rbuf, count, dtype, 0,
                                MCA_COLL_BASE_TAG_REDUCE, comm,
                                MPI_STATUS_IGNORE));
        if (MPI_SUCCESS != err) {
            if (NULL != free_buffer) {
                free(free_buffer);
            }
            return err;
        }

        /* Loop receiving and calling reduction function (C or Fortran). */
        for (i = 1; i < size; i++) {
            err = MCA_PML_CALL(recv(pml_buffer, count, dtype, i,
                                    MCA_COLL_BASE_TAG_REDUCE, comm,
                                    MPI_STATUS_IGNORE));
            if (MPI_SUCCESS != err) {
                if (NULL != free_buffer) {
                    free(free_buffer);
                }
                return err;
            }

            /* Perform the reduction */
            ompi_op_reduce(op, pml_buffer, rbuf, count, dtype);
        }

        if (NULL != free_buffer) {
            free(free_buffer);
        }
    }

    /* All done */
    return err;
}


/*
 *	reduce_log_inter
 *
 *	Function:	- reduction using O(N) algorithm
 *	Accepts:	- same as MPI_Reduce()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_reduce_log_inter(const void *sbuf, void *rbuf, int count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                int root, struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module)
{
    return OMPI_ERR_NOT_IMPLEMENTED;
}

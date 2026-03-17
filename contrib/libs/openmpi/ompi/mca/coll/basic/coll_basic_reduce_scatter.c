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
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
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
#include <errno.h>

#include "mpi.h"
#include "opal/util/bit_ops.h"
#include "ompi/constants.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/datatype/ompi_datatype.h"
#include "coll_basic.h"
#include "ompi/op/op.h"

#define COMMUTATIVE_LONG_MSG (8 * 1024 * 1024)

/*
 *	reduce_scatter
 *
 *	Function:	- reduce then scatter
 *	Accepts:	- same as MPI_Reduce_scatter()
 *	Returns:	- MPI_SUCCESS or error code
 *
 * Algorithm:
 *   Cummutative, reasonable sized messages
 *     recursive halving algorithm
 *   Others:
 *     reduce and scatterv (needs to be cleaned
 *     up at some point)
 *
 * NOTE: that the recursive halving algorithm should be faster than
 * the reduce/scatter for all message sizes.  However, the memory
 * usage for the recusive halving is msg_size + 2 * comm_size greater
 * for the recursive halving, so I've limited where the recursive
 * halving is used to be nice to the app memory wise.  There are much
 * better algorithms for large messages with commutative operations,
 * so this should be investigated further.
 */
int
mca_coll_basic_reduce_scatter_intra(const void *sbuf, void *rbuf, const int *rcounts,
                                    struct ompi_datatype_t *dtype,
                                    struct ompi_op_t *op,
                                    struct ompi_communicator_t *comm,
                                    mca_coll_base_module_t *module)
{
    int i, rank, size, count, err = OMPI_SUCCESS;
    ptrdiff_t extent, buf_size, gap;
    int *disps = NULL;
    char *recv_buf = NULL, *recv_buf_free = NULL;
    char *result_buf = NULL, *result_buf_free = NULL;
    /* Initialize */
    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

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

    if ((op->o_flags & OMPI_OP_FLAGS_COMMUTE) &&
        (buf_size < COMMUTATIVE_LONG_MSG)) {
        int tmp_size, remain = 0, tmp_rank;

        /* temporary receive buffer.  See coll_basic_reduce.c for details on sizing */
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
        tmp_size = opal_next_poweroftwo(size);
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
                if (OMPI_SUCCESS != err) goto cleanup;

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
                    err = MCA_PML_CALL(irecv(recv_buf + tmp_disps[recv_index] * extent,
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
                    err = MCA_PML_CALL(send(result_buf + tmp_disps[send_index] * extent,
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
                                   recv_buf + tmp_disps[recv_index] * extent,
                                   result_buf + tmp_disps[recv_index] * extent,
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
        if (rank < 2 * remain) {
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

    } else {
        if (0 == rank) {
            /* temporary receive buffer.  See coll_basic_reduce.c for
               details on sizing */
            recv_buf_free = (char*) malloc(buf_size);
            recv_buf = recv_buf_free - gap;
            if (NULL == recv_buf_free) {
                err = OMPI_ERR_OUT_OF_RESOURCE;
                goto cleanup;
            }
        }

        /* reduction */
        err =
            comm->c_coll->coll_reduce(sbuf, recv_buf, count, dtype, op, 0,
                                     comm, comm->c_coll->coll_reduce_module);

        /* scatter */
        if (MPI_SUCCESS == err) {
            err = comm->c_coll->coll_scatterv(recv_buf, rcounts, disps, dtype,
                                             rbuf, rcounts[rank], dtype, 0,
                                             comm, comm->c_coll->coll_scatterv_module);
        }
    }

 cleanup:
    if (NULL != disps) free(disps);
    if (NULL != recv_buf_free) free(recv_buf_free);
    if (NULL != result_buf_free) free(result_buf_free);

    return err;
}


/*
 *	reduce_scatter_inter
 *
 *	Function:	- reduce/scatter operation
 *	Accepts:	- same arguments as MPI_Reduce_scatter()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_reduce_scatter_inter(const void *sbuf, void *rbuf, const int *rcounts,
                                    struct ompi_datatype_t *dtype,
                                    struct ompi_op_t *op,
                                    struct ompi_communicator_t *comm,
                                    mca_coll_base_module_t *module)
{
    int err, i, rank, root = 0, rsize, lsize, totalcounts;
    char *tmpbuf = NULL, *tmpbuf2 = NULL, *lbuf = NULL, *buf;
    ptrdiff_t gap, span;
    ompi_request_t *req;
    int *disps = NULL;

    rank = ompi_comm_rank(comm);
    rsize = ompi_comm_remote_size(comm);
    lsize = ompi_comm_size(comm);

    /* Figure out the total amount of data for the reduction. */
    for (totalcounts = 0, i = 0; i < lsize; i++) {
        totalcounts += rcounts[i];
    }

    /*
     * The following code basically does an interreduce followed by a
     * intrascatterv.  This is implemented by having the roots of each
     * group exchange their sbuf.  Then, the roots receive the data
     * from each of the remote ranks and execute the reduce.  When
     * this is complete, they have the reduced data available to them
     * for doing the scatterv.  They do this on the local communicator
     * associated with the intercommunicator.
     *
     * Note: There are other ways to implement MPI_Reduce_scatter on
     * intercommunicators.  For example, one could do a MPI_Reduce locally,
     * then send the results to the other root which could scatter it.
     *
     * Note: It is also worth pointing out that the rcounts argument
     * represents how the data is going to be scatter locally.  Therefore,
     * its size is the same as the local communicator size.
     */
    if (rank == root) {
        span = opal_datatype_span(&dtype->super, totalcounts, &gap);

        /* Generate displacements for the scatterv part */
        disps = (int*) malloc(sizeof(int) * lsize);
        if (NULL == disps) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        disps[0] = 0;
        for (i = 0; i < (lsize - 1); ++i) {
            disps[i + 1] = disps[i] + rcounts[i];
        }

        tmpbuf = (char *) malloc(span);
        tmpbuf2 = (char *) malloc(span);
        if (NULL == tmpbuf || NULL == tmpbuf2) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        lbuf = tmpbuf - gap;
        buf = tmpbuf2 - gap;

        /* Do a send-recv between the two root procs. to avoid deadlock */
        err = MCA_PML_CALL(isend(sbuf, totalcounts, dtype, 0,
                                 MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                 MCA_PML_BASE_SEND_STANDARD, comm, &req));
        if (OMPI_SUCCESS != err) {
            goto exit;
        }

        err = MCA_PML_CALL(recv(lbuf, totalcounts, dtype, 0,
                                MCA_COLL_BASE_TAG_REDUCE_SCATTER, comm,
                                MPI_STATUS_IGNORE));
        if (OMPI_SUCCESS != err) {
            goto exit;
        }

        err = ompi_request_wait( &req, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != err) {
            goto exit;
        }


        /* Loop receiving and calling reduction function (C or Fortran)
         * The result of this reduction operations is then in
         * lbuf.
         */
        for (i = 1; i < rsize; i++) {
            char *tbuf;
            err = MCA_PML_CALL(recv(buf, totalcounts, dtype, i,
                                    MCA_COLL_BASE_TAG_REDUCE_SCATTER, comm,
                                    MPI_STATUS_IGNORE));
            if (MPI_SUCCESS != err) {
                goto exit;
            }

            /* Perform the reduction */
            ompi_op_reduce(op, lbuf, buf, totalcounts, dtype);
            /* swap the buffers */
            tbuf = lbuf; lbuf = buf; buf = tbuf;
        }
    } else {
        /* If not root, send data to the root. */
        err = MCA_PML_CALL(send(sbuf, totalcounts, dtype, root,
                                MCA_COLL_BASE_TAG_REDUCE_SCATTER,
                                MCA_PML_BASE_SEND_STANDARD, comm));
        if (OMPI_SUCCESS != err) {
            goto exit;
        }
    }

    /* Now do a scatterv on the local communicator */
    err = comm->c_local_comm->c_coll->coll_scatterv(lbuf, rcounts, disps, dtype,
                                                   rbuf, rcounts[rank], dtype, 0,
                                                   comm->c_local_comm,
                                                   comm->c_local_comm->c_coll->coll_scatterv_module);

  exit:
    if (NULL != tmpbuf) {
        free(tmpbuf);
    }

    if (NULL != tmpbuf2) {
        free(tmpbuf2);
    }

    if (NULL != disps) {
        free(disps);
    }

    return err;
}

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
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_basic.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "opal/util/bit_ops.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "coll_basic.h"


/*
 *	barrier_intra_log
 *
 *	Function:	- barrier using O(log(N)) algorithm
 *	Accepts:	- same as MPI_Barrier()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_barrier_intra_log(struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module)
{
    int i;
    int err;
    int peer;
    int dim;
    int hibit;
    int mask;
    int size = ompi_comm_size(comm);
    int rank = ompi_comm_rank(comm);

    /* Send null-messages up and down the tree.  Synchronization at the
     * root (rank 0). */

    dim = comm->c_cube_dim;
    hibit = opal_hibit(rank, dim);
    --dim;

    /* Receive from children. */

    for (i = dim, mask = 1 << i; i > hibit; --i, mask >>= 1) {
        peer = rank | mask;
        if (peer < size) {
            err = MCA_PML_CALL(recv(NULL, 0, MPI_BYTE, peer,
                                    MCA_COLL_BASE_TAG_BARRIER,
                                    comm, MPI_STATUS_IGNORE));
            if (MPI_SUCCESS != err) {
                return err;
            }
        }
    }

    /* Send to and receive from parent. */

    if (rank > 0) {
        peer = rank & ~(1 << hibit);
        err =
            MCA_PML_CALL(send
                         (NULL, 0, MPI_BYTE, peer,
                          MCA_COLL_BASE_TAG_BARRIER,
                          MCA_PML_BASE_SEND_STANDARD, comm));
        if (MPI_SUCCESS != err) {
            return err;
        }

        err = MCA_PML_CALL(recv(NULL, 0, MPI_BYTE, peer,
                                MCA_COLL_BASE_TAG_BARRIER,
                                comm, MPI_STATUS_IGNORE));
        if (MPI_SUCCESS != err) {
            return err;
        }
    }

    /* Send to children. */

    for (i = hibit + 1, mask = 1 << i; i <= dim; ++i, mask <<= 1) {
        peer = rank | mask;
        if (peer < size) {
            err = MCA_PML_CALL(send(NULL, 0, MPI_BYTE, peer,
                                    MCA_COLL_BASE_TAG_BARRIER,
                                    MCA_PML_BASE_SEND_STANDARD, comm));
            if (MPI_SUCCESS != err) {
                return err;
            }
        }
    }

    /* All done */

    return MPI_SUCCESS;
}


/*
 *	barrier_inter_lin
 *
 *	Function:	- barrier using O(log(N)) algorithm
 *	Accepts:	- same as MPI_Barrier()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_barrier_inter_lin(struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module)
{
    int rank;
    int result;

    rank = ompi_comm_rank(comm);
    return comm->c_coll->coll_allreduce(&rank, &result, 1, MPI_INT, MPI_MAX,
                                       comm, comm->c_coll->coll_allreduce_module);
}

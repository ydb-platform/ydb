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
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/communicator/communicator.h"

/*
 * function - mca_topo_base_cart_map
 *
 *  @param comm input communicator (handle)
 *  @param ndims number of dimensions of cartesian structure (integer)
 *  @param dims integer array of size 'ndims' specifying the number of
 *              processes in each coordinate direction
 *  @param periods logical array of size 'ndims' specifying the
 *                 periodicity specification in each coordinate direction
 *  @param newrank reordered rank of the calling process; 'MPI_UNDEFINED'
 *                 if calling process does not belong to grid (integer)
 *
 *  @retval MPI_SUCCESS
 *  @retval MPI_ERR_DIMS
 */

int mca_topo_base_cart_map(ompi_communicator_t* comm,
                           int ndims,
                           const int *dims, const int *periods, int *newrank)
{
    int nprocs, rank, size, i;

    /*
     * Compute the # of processes in the grid.
     */
    nprocs = 1;
    for (i = 0 ; i < ndims; ++i) {
        if (dims[i] <= 0) {
            return MPI_ERR_DIMS;
        }
        nprocs *= dims[i];
    }
    /*
     * Check that number of processes <= size of communicator.
     */
    size = ompi_comm_size(comm);
    if (nprocs > size) {
        return MPI_ERR_DIMS;
    }
    /*
     * Compute my new rank.
     */
    rank = ompi_comm_rank(comm);
    *newrank = ((rank < 0) || (rank >= nprocs)) ? MPI_UNDEFINED : rank;

    return MPI_SUCCESS;
}

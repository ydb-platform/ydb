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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
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
 * function - determines process coords in cartesian topology given
 *            rank in group
 *
 * @param comm - communicator with cartesian structure (handle)
 * @param rank - rank of a process within group of 'comm' (integer)
 * @param maxdims - length of vector 'coords' in the calling program (integer)
 * @param coords - integer array (of size 'ndims') containing the cartesian
 *                   coordinates of specified process (integer)
 *
 * @retval MPI_SUCCESS
 */

int mca_topo_base_cart_coords(ompi_communicator_t* comm,
                              int rank,
                              int maxdims,
                              int *coords)
{
    int dim, remprocs, i, *d;

    /*
     * loop computing the co-ordinates
     */
    d = comm->c_topo->mtc.cart->dims;
    remprocs = ompi_comm_size(comm);

    for (i = 0;
        (i < comm->c_topo->mtc.cart->ndims) && (i < maxdims);
        ++i, ++d) {
        dim = *d;
        remprocs /= dim;
        *coords++ = rank / remprocs;
        rank %= remprocs;
    }

    return MPI_SUCCESS;
}

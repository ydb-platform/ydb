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

#include <string.h>

#include "ompi/mca/topo/base/base.h"
#include "ompi/communicator/communicator.h"

/*
 * function - retrieves Cartesian topology information associated with a
 *            communicator
 *
 * @param comm communicator with cartesian structure (handle)
 * @param maxdims length of vectors  'dims', 'periods', and 'coords'
 *                 in the calling program (integer)
 * @param dims number of processes for each cartesian dimension (array of integer)
 * @param periods periodicity (true/false) for each cartesian dimension
 *                (array of logical)
 * @param coords coordinates of calling process in cartesian structure
 *               (array of integer)
 *
 * @retval MPI_SUCCESS
 */
int mca_topo_base_cart_get(ompi_communicator_t* comm,
                           int maxdims,
                           int *dims,
                           int *periods,
                           int *coords)
{
    int m = (maxdims <= comm->c_topo->mtc.cart->ndims) ?
        maxdims : comm->c_topo->mtc.cart->ndims;

    memcpy(dims, comm->c_topo->mtc.cart->dims, m * sizeof(int));
    memcpy(periods, comm->c_topo->mtc.cart->periods, m * sizeof(int));
    memcpy(coords, comm->c_topo->mtc.cart->coords, m * sizeof(int));

    return MPI_SUCCESS;
}

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
 * function - Determines process rank in communicator given Cartesian
 *                 location
 *
 * @param comm communicator with cartesian structure (handle)
 * @param coords integer array (of size  'ndims') specifying the cartesian
 *               coordinates of a process
 * @param rank rank of specified process (integer)
 *
 * @retval MPI_SUCCESS
 * @retval MPI_ERR_COMM
 * @retval MPI_ERR_TOPOLOGY
 * @retval MPI_ERR_ARG
 */

int mca_topo_base_cart_rank(ompi_communicator_t* comm,
                            const int *coords,
                            int *rank)
{
   int prank;
   int dim;
   int ord;
   int factor;
   int i;
   int *d;

   /*
    * Loop over coordinates computing the rank.
    */
    factor = 1;
    prank = 0;

    i = comm->c_topo->mtc.cart->ndims - 1;
    d = comm->c_topo->mtc.cart->dims + i;

   for (; i >= 0; --i, --d) {
       dim = *d;
       ord = coords[i];
       /* Per MPI-2.1 7.5.4 (description of MPI_CART_RANK), if the
          dimension is periodic and the coordinate is outside of 0 <=
          coord(i) < dim, then normalize it.  If the dimension is not
          periodic, it's an error. */
       if ((ord < 0) || (ord >= dim)) {
           ord %= dim;
           if (ord < 0) {
               ord += dim;
           }
       }
       prank += factor * ord;
       factor *= dim;
    }
    *rank = prank;

    return(MPI_SUCCESS);
}

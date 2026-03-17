/*
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdlib.h>

#include "ompi/constants.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/communicator/communicator.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/mca/coll/base/coll_base_util.h"

int
ompi_init_preconnect_mpi(void)
{
    int comm_size = ompi_comm_size(MPI_COMM_WORLD);
    int comm_rank =  ompi_comm_rank(MPI_COMM_WORLD);
    int param, next, prev, i, ret = OMPI_SUCCESS;
    char inbuf[1], outbuf[1];
    const bool *value = NULL;

    param = mca_base_var_find("ompi", "mpi", NULL, "preconnect_mpi");
    if (0 > param) return OMPI_SUCCESS;
    ret = mca_base_var_get_value(param, &value, NULL, NULL);
    if (OMPI_SUCCESS != ret || (NULL != value && 0 == value[0])) {
        return OMPI_SUCCESS;
    }

    inbuf[0] = outbuf[0] = '\0';

    /* Each iteration, every process sends to its neighbor i hops to
       the right and receives from its neighbor i hops to the left.
       Because send_complete is used, there will only ever be one
       outstanding send and one outstanding receive in the network at
       a time for any given process.  This limits any "flooding"
       effect that can occur with other connection algorithms.  While
       the flooding algorithms may be a more efficient use of
       resources, they can overwhelm the out-of-band connection system
       used to wire up some networks, leading to poor performance and
       hangs. */
    for (i = 1 ; i <= comm_size / 2 ; ++i) {
        next = (comm_rank + i) % comm_size;
        prev = (comm_rank - i + comm_size) % comm_size;

        ret = ompi_coll_base_sendrecv_actual(outbuf, 1, MPI_CHAR,
                                             next, 1,
                                             inbuf, 1, MPI_CHAR,
                                             prev, 1,
                                             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if(OMPI_SUCCESS != ret) return ret;
    }

    return ret;
}

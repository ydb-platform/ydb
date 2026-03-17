/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/constants.h"
#include "coll_self.h"


/*
 *	barrier_intra
 *
 *	Function:	- barrier
 *	Accepts:	- same as MPI_Barrier()
 *	Returns:	- MPI_SUCCESS
 */
int mca_coll_self_barrier_intra(struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module)
{
    /* Since there is only one process, this is a no-op */

    return MPI_SUCCESS;
}

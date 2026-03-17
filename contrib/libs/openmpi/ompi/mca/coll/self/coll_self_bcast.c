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
#include "ompi/datatype/ompi_datatype.h"
#include "coll_self.h"


/*
 *	bcast_lin_intra
 *
 *	Function:	- broadcast
 *	Accepts:	- same arguments as MPI_Bcast()
 *	Returns:	- MPI_SUCCESS
 */
int mca_coll_self_bcast_intra(void *buff, int count,
                              struct ompi_datatype_t *datatype, int root,
                              struct ompi_communicator_t *comm,
                              mca_coll_base_module_t *module)
{
    /* Since there's only one process, there's nothing to do */

    return MPI_SUCCESS;
}

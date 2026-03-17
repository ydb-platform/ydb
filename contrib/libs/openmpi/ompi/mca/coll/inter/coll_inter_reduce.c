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
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_inter.h"

#include <stdio.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/op/op.h"
#include "ompi/mca/pml/pml.h"

/*
 *	reduce_inter
 *
 *	Function:	- reduction using the local_comm
 *	Accepts:	- same as MPI_Reduce()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_inter_reduce_inter(const void *sbuf, void *rbuf, int count,
                            struct ompi_datatype_t *dtype,
                            struct ompi_op_t *op,
                            int root, struct ompi_communicator_t *comm,
                            mca_coll_base_module_t *module)
{
    int rank, err;

    /* Initialize */
    rank = ompi_comm_rank(comm);

    if (MPI_PROC_NULL == root) {
        /* do nothing */
        err = OMPI_SUCCESS;
    } else if (MPI_ROOT != root) {
        ptrdiff_t gap, span;
        char *free_buffer = NULL;
        char *pml_buffer = NULL;

	/* Perform the reduce locally with the first process as root */
        span = opal_datatype_span(&dtype->super, count, &gap);

	free_buffer = (char*)malloc(span);
	if (NULL == free_buffer) {
	    return OMPI_ERR_OUT_OF_RESOURCE;
	}
	pml_buffer = free_buffer - gap;

	err = comm->c_local_comm->c_coll->coll_reduce(sbuf, pml_buffer, count,
						     dtype, op, 0, comm->c_local_comm,
                                                     comm->c_local_comm->c_coll->coll_reduce_module);
	if (0 == rank) {
	    /* First process sends the result to the root */
	    err = MCA_PML_CALL(send(pml_buffer, count, dtype, root,
				    MCA_COLL_BASE_TAG_REDUCE,
				    MCA_PML_BASE_SEND_STANDARD, comm));
	    if (OMPI_SUCCESS != err) {
                return err;
            }
	}

	if (NULL != free_buffer) {
	    free(free_buffer);
	}
    } else {
        /* Root receives the reduced message from the first process  */
	err = MCA_PML_CALL(recv(rbuf, count, dtype, 0,
				MCA_COLL_BASE_TAG_REDUCE, comm,
				MPI_STATUS_IGNORE));
	if (OMPI_SUCCESS != err) {
	    return err;
	}
    }
    /* All done */
    return err;
}

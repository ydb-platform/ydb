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

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"


/*
 *	gather_inter
 *
 *	Function:	- basic gather operation
 *	Accepts:	- same arguments as MPI_Gather()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_inter_gather_inter(const void *sbuf, int scount,
                            struct ompi_datatype_t *sdtype,
                            void *rbuf, int rcount,
                            struct ompi_datatype_t *rdtype,
                            int root, struct ompi_communicator_t *comm,
                            mca_coll_base_module_t *module)
{
    int err;
    int rank;
    int size;

    size = ompi_comm_remote_size(comm);
    rank = ompi_comm_rank(comm);

    if (MPI_PROC_NULL == root) {
        /* do nothing */
        err = OMPI_SUCCESS;
    } else if (MPI_ROOT != root) {
	/* Perform the gather locally with the first process as root */
        char *ptmp_free = NULL, *ptmp;
        int size_local;
        ptrdiff_t gap, span;

        size_local = ompi_comm_size(comm->c_local_comm);
        span = opal_datatype_span(&sdtype->super, (int64_t)scount*(int64_t)size_local, &gap);

        ptmp_free = (char*)malloc(span);
        if (NULL == ptmp_free) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        ptmp = ptmp_free - gap;

	err = comm->c_local_comm->c_coll->coll_gather(sbuf, scount, sdtype,
						     ptmp, scount, sdtype,
						     0, comm->c_local_comm,
                                                     comm->c_local_comm->c_coll->coll_gather_module);
	if (0 == rank) {
	    /* First process sends data to the root */
	    err = MCA_PML_CALL(send(ptmp, scount*size_local, sdtype, root,
				    MCA_COLL_BASE_TAG_GATHER,
				    MCA_PML_BASE_SEND_STANDARD, comm));
	    if (OMPI_SUCCESS != err) {
                return err;
            }
	}
        free(ptmp_free);
    } else {
        /* I am the root, loop receiving the data. */
	err = MCA_PML_CALL(recv(rbuf, rcount*size, rdtype, 0,
				MCA_COLL_BASE_TAG_GATHER,
				comm, MPI_STATUS_IGNORE));
	if (OMPI_SUCCESS != err) {
	    return err;
	}
    }

    /* All done */
    return err;
}

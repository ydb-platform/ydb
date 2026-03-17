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
 * Copyright (c) 2006-2010 University of Houston. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_inter.h"

#include <stdlib.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/coll/base/coll_base_util.h"

/*
 *	allgather_inter
 *
 *	Function:	- allgather using other MPI collections
 *	Accepts:	- same as MPI_Allgather()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_inter_allgather_inter(const void *sbuf, int scount,
                               struct ompi_datatype_t *sdtype,
                               void *rbuf, int rcount,
                               struct ompi_datatype_t *rdtype,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module)
{
    int rank, root = 0, size, rsize, err = OMPI_SUCCESS;
    char *ptmp_free = NULL, *ptmp = NULL;
    ptrdiff_t gap, span;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm->c_local_comm);
    rsize = ompi_comm_remote_size(comm);

    /* Perform the gather locally at the root */
    if ( scount > 0 ) {
        span = opal_datatype_span(&sdtype->super, (int64_t)scount*(int64_t)size, &gap);
	ptmp_free = (char*)malloc(span);
	if (NULL == ptmp_free) {
	    return OMPI_ERR_OUT_OF_RESOURCE;
	}
        ptmp = ptmp_free - gap;

	err = comm->c_local_comm->c_coll->coll_gather(sbuf, scount, sdtype,
						     ptmp, scount, sdtype,
						     0, comm->c_local_comm,
						     comm->c_local_comm->c_coll->coll_gather_module);
	if (OMPI_SUCCESS != err) {
	    goto exit;
	}
    }

    if (rank == root) {
	/* Do a send-recv between the two root procs. to avoid deadlock */
        err = ompi_coll_base_sendrecv_actual(ptmp, scount*size, sdtype, 0,
                                             MCA_COLL_BASE_TAG_ALLGATHER,
                                             rbuf, rcount*rsize, rdtype, 0,
                                             MCA_COLL_BASE_TAG_ALLGATHER,
                                             comm, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != err) {
            goto exit;
        }
    }
    /* bcast the message to all the local processes */
    if ( rcount > 0 ) {
	err = comm->c_local_comm->c_coll->coll_bcast(rbuf, rcount*rsize, rdtype,
						    root, comm->c_local_comm,
						    comm->c_local_comm->c_coll->coll_bcast_module);
	if (OMPI_SUCCESS != err) {
	    goto exit;
	}
    }

 exit:
    if (NULL != ptmp_free) {
        free(ptmp_free);
    }

    return err;
}

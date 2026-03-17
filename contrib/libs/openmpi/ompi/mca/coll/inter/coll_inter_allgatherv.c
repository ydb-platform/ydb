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

#include "mpi.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/constants.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/coll/base/coll_base_util.h"
#include "ompi/mca/pml/pml.h"


/*
 *	allgatherv_inter
 *
 *	Function:	- allgatherv using other MPI collectives
 *	Accepts:	- same as MPI_Allgatherv()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_inter_allgatherv_inter(const void *sbuf, int scount,
                                struct ompi_datatype_t *sdtype,
                                void *rbuf, const int *rcounts, const int *disps,
                                struct ompi_datatype_t *rdtype,
                                struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module)
{
    int i, rank, size, size_local, total=0, err;
    int *count=NULL,*displace=NULL;
    char *ptmp_free=NULL, *ptmp=NULL;
    ompi_datatype_t *ndtype = NULL;

    rank = ompi_comm_rank(comm);
    size_local = ompi_comm_size(comm->c_local_comm);
    size = ompi_comm_remote_size(comm);

    if (0 == rank) {
	count = (int *)malloc(sizeof(int) * size_local);
	displace = (int *)malloc(sizeof(int) * size_local);
	if ((NULL == count) || (NULL == displace)) {
            err = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
	}
    }
    /* Local gather to get the scount of each process */
    err = comm->c_local_comm->c_coll->coll_gather(&scount, 1, MPI_INT,
						 count, 1, MPI_INT,
						 0, comm->c_local_comm,
                                                 comm->c_local_comm->c_coll->coll_gather_module);
    if (OMPI_SUCCESS != err) {
        goto exit;
    }
    if(0 == rank) {
	displace[0] = 0;
	for (i = 1; i < size_local; i++) {
	    displace[i] = displace[i-1] + count[i-1];
	}
	total = 0;
	for (i = 0; i < size_local; i++) {
	    total = total + count[i];
	}
	if ( total > 0 ) {
            ptrdiff_t gap, span;
            span = opal_datatype_span(&sdtype->super, total, &gap);
	    ptmp_free = (char*)malloc(span);
	    if (NULL == ptmp_free) {
                err = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
	    }
            ptmp = ptmp_free - gap;
	}
    }
    err = comm->c_local_comm->c_coll->coll_gatherv(sbuf, scount, sdtype,
						  ptmp, count, displace,
						  sdtype,0, comm->c_local_comm,
                                                  comm->c_local_comm->c_coll->coll_gatherv_module);
    if (OMPI_SUCCESS != err) {
        goto exit;
    }

    ompi_datatype_create_indexed(size,rcounts,disps,rdtype,&ndtype);
    ompi_datatype_commit(&ndtype);

    if (0 == rank) {
	/* Exchange data between roots */
        err = ompi_coll_base_sendrecv_actual(ptmp, total, sdtype, 0,
                                             MCA_COLL_BASE_TAG_ALLGATHERV,
	                                     rbuf, 1, ndtype, 0,
                                             MCA_COLL_BASE_TAG_ALLGATHERV,
                                             comm, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != err) {
            goto exit;
        }
    }

    /* bcast the message to all the local processes */
    err = comm->c_local_comm->c_coll->coll_bcast(rbuf, 1, ndtype,
						0, comm->c_local_comm,
                                                comm->c_local_comm->c_coll->coll_bcast_module);
  exit:
    if( NULL != ndtype ) {
        ompi_datatype_destroy(&ndtype);
    }
    if (NULL != ptmp_free) {
        free(ptmp_free);
    }
    if (NULL != displace) {
        free(displace);
    }
    if (NULL != count) {
        free(count);
    }

    return err;

}

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2013      FUJITSU LIMITED.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_basic.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"


/*
 *	alltoallv_inter
 *
 *	Function:	- MPI_Alltoallv
 *	Accepts:	- same as MPI_Alltoallv()
 *	Returns:	- MPI_SUCCESS or an MPI error code
 */
int
mca_coll_basic_alltoallv_inter(const void *sbuf, const int *scounts, const int *sdisps,
                               struct ompi_datatype_t *sdtype, void *rbuf,
                               const int *rcounts, const int *rdisps,
                               struct ompi_datatype_t *rdtype,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module)
{
    int i;
    int rsize;
    int err;
    char *psnd;
    char *prcv;
    size_t nreqs;
    MPI_Aint sndextent;
    MPI_Aint rcvextent;

    ompi_request_t **preq;

    /* Initialize. */

    rsize = ompi_comm_remote_size(comm);

    ompi_datatype_type_extent(sdtype, &sndextent);
    ompi_datatype_type_extent(rdtype, &rcvextent);

    /* Initiate all send/recv to/from others. */
    nreqs = rsize * 2;
    preq = ompi_coll_base_comm_get_reqs(module->base_data, nreqs);
    if( NULL == preq ) { return OMPI_ERR_OUT_OF_RESOURCE; }

    /* Post all receives first  */
    /* A simple optimization: do not send and recv msgs of length zero */
    for (i = 0; i < rsize; ++i) {
        prcv = ((char *) rbuf) + (rdisps[i] * rcvextent);
        if (rcounts[i] > 0) {
            err = MCA_PML_CALL(irecv(prcv, rcounts[i], rdtype,
                                     i, MCA_COLL_BASE_TAG_ALLTOALLV, comm,
                                     &preq[i]));
            if (MPI_SUCCESS != err) {
                ompi_coll_base_free_reqs(preq, i + 1);
                return err;
            }
        }
    }

    /* Now post all sends */
    for (i = 0; i < rsize; ++i) {
        psnd = ((char *) sbuf) + (sdisps[i] * sndextent);
        if (scounts[i] > 0) {
            err = MCA_PML_CALL(isend(psnd, scounts[i], sdtype,
                                     i, MCA_COLL_BASE_TAG_ALLTOALLV,
                                     MCA_PML_BASE_SEND_STANDARD, comm,
                                     &preq[rsize + i]));
            if (MPI_SUCCESS != err) {
                ompi_coll_base_free_reqs(preq, rsize + i + 1);
                return err;
            }
        }
    }

    err = ompi_request_wait_all(nreqs, preq, MPI_STATUSES_IGNORE);
    if (MPI_SUCCESS != err) {
        ompi_coll_base_free_reqs(preq, nreqs);
    }

    /* All done */
    return err;
}

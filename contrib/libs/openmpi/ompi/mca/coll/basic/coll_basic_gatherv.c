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
 * Copyright (c) 2015      Research Organization for Information Science
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
 *	gatherv_intra
 *
 *	Function:	- basic gatherv operation
 *	Accepts:	- same arguments as MPI_Gatherv()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_gatherv_intra(const void *sbuf, int scount,
                             struct ompi_datatype_t *sdtype,
                             void *rbuf, const int *rcounts, const int *disps,
                             struct ompi_datatype_t *rdtype, int root,
                             struct ompi_communicator_t *comm,
                            mca_coll_base_module_t *module)
{
    int i, rank, size, err;
    char *ptmp;
    ptrdiff_t lb, extent;

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    /* Everyone but root sends data and returns.  Don't send anything
       for sendcounts of 0 (even though MPI_Gatherv has a guard for 0
       counts, this routine is used elsewhere, like the implementation
       of allgatherv, so it's possible to get here with a scount of
       0) */

    if (rank != root) {
        if (scount > 0) {
            return MCA_PML_CALL(send(sbuf, scount, sdtype, root,
                                     MCA_COLL_BASE_TAG_GATHERV,
                                     MCA_PML_BASE_SEND_STANDARD, comm));
        }
        return MPI_SUCCESS;
    }

    /* I am the root, loop receiving data. */

    err = ompi_datatype_get_extent(rdtype, &lb, &extent);
    if (OMPI_SUCCESS != err) {
        return OMPI_ERROR;
    }

    for (i = 0; i < size; ++i) {
        ptmp = ((char *) rbuf) + (extent * disps[i]);

        if (i == rank) {
            /* simple optimization */
            if (MPI_IN_PLACE != sbuf && (0 < scount) && (0 < rcounts[i])) {
                err = ompi_datatype_sndrcv(sbuf, scount, sdtype,
                                      ptmp, rcounts[i], rdtype);
            }
        } else {
            /* Only receive if there is something to receive */
            if (rcounts[i] > 0) {
                err = MCA_PML_CALL(recv(ptmp, rcounts[i], rdtype, i,
                                        MCA_COLL_BASE_TAG_GATHERV,
                                        comm, MPI_STATUS_IGNORE));
            }
        }

        if (MPI_SUCCESS != err) {
            return err;
        }
    }

    /* All done */

    return MPI_SUCCESS;
}


/*
 *	gatherv_inter
 *
 *	Function:	- basic gatherv operation
 *	Accepts:	- same arguments as MPI_Gatherv()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_gatherv_inter(const void *sbuf, int scount,
                             struct ompi_datatype_t *sdtype,
                             void *rbuf, const int *rcounts, const int *disps,
                             struct ompi_datatype_t *rdtype, int root,
                             struct ompi_communicator_t *comm,
                             mca_coll_base_module_t *module)
{
    int i, size, err;
    char *ptmp;
    ptrdiff_t lb, extent;
    ompi_request_t **reqs = NULL;

    size = ompi_comm_remote_size(comm);

    /* If not root, receive data.  Note that we will only get here if
     * scount > 0 or rank == root. */

    if (MPI_PROC_NULL == root) {
        /* do nothing */
        err = OMPI_SUCCESS;
    } else if (MPI_ROOT != root) {
        /* Everyone but root sends data and returns. */
        err = MCA_PML_CALL(send(sbuf, scount, sdtype, root,
                                MCA_COLL_BASE_TAG_GATHERV,
                                MCA_PML_BASE_SEND_STANDARD, comm));
    } else {
        /* I am the root, loop receiving data. */
        err = ompi_datatype_get_extent(rdtype, &lb, &extent);
        if (OMPI_SUCCESS != err) {
            return OMPI_ERROR;
        }

        reqs = ompi_coll_base_comm_get_reqs(module->base_data, size);
        if( NULL == reqs ) { return OMPI_ERR_OUT_OF_RESOURCE; }

        for (i = 0; i < size; ++i) {
            ptmp = ((char *) rbuf) + (extent * disps[i]);
            err = MCA_PML_CALL(irecv(ptmp, rcounts[i], rdtype, i,
                                     MCA_COLL_BASE_TAG_GATHERV,
                                     comm, &reqs[i]));
            if (OMPI_SUCCESS != err) {
                ompi_coll_base_free_reqs(reqs, i + 1);
                return err;
            }
        }

        err = ompi_request_wait_all(size, reqs, MPI_STATUSES_IGNORE);
        if (OMPI_SUCCESS != err) {
            ompi_coll_base_free_reqs(reqs, size);
        }
    }

    /* All done */
    return err;
}

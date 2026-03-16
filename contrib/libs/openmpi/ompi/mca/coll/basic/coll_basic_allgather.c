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
 * Copyright (c) 2014-2016 Research Organization for Information Science
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

#include <stdlib.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "coll_basic.h"


/*
 *	allgather_inter
 *
 *	Function:	- allgather using other MPI collections
 *	Accepts:	- same as MPI_Allgather()
 *	Returns:	- MPI_SUCCESS or error code
 */
int
mca_coll_basic_allgather_inter(const void *sbuf, int scount,
                               struct ompi_datatype_t *sdtype,
                               void *rbuf, int rcount,
                               struct ompi_datatype_t *rdtype,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module)
{
    int rank, root = 0, size, rsize, err, i, line;
    char *tmpbuf_free = NULL, *tmpbuf, *ptmp;
    ptrdiff_t rlb, rextent, incr;
    ptrdiff_t gap, span;
    ompi_request_t *req;
    ompi_request_t **reqs = NULL;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    rsize = ompi_comm_remote_size(comm);

    /* Algorithm:
     * - a gather to the root in remote group (simultaniously executed,
     * thats why we cannot use coll_gather).
     * - exchange the temp-results between two roots
     * - inter-bcast (again simultanious).
     */

    /* Step one: gather operations: */
    if (rank != root) {
        /* send your data to root */
        err = MCA_PML_CALL(send(sbuf, scount, sdtype, root,
                                MCA_COLL_BASE_TAG_ALLGATHER,
                                MCA_PML_BASE_SEND_STANDARD, comm));
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }
    } else {
        /* receive a msg. from all other procs. */
        err = ompi_datatype_get_extent(rdtype, &rlb, &rextent);
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

        /* Get a requests arrays of the right size */
        reqs = ompi_coll_base_comm_get_reqs(module->base_data, rsize + 1);
        if( NULL == reqs ) { line = __LINE__; err = OMPI_ERR_OUT_OF_RESOURCE; goto exit; }

        /* Do a send-recv between the two root procs. to avoid deadlock */
        err = MCA_PML_CALL(isend(sbuf, scount, sdtype, 0,
                                 MCA_COLL_BASE_TAG_ALLGATHER,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm, &reqs[rsize]));
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

        err = MCA_PML_CALL(irecv(rbuf, rcount, rdtype, 0,
                                 MCA_COLL_BASE_TAG_ALLGATHER, comm,
                                 &reqs[0]));
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

        incr = rextent * rcount;
        ptmp = (char *) rbuf + incr;
        for (i = 1; i < rsize; ++i, ptmp += incr) {
            err = MCA_PML_CALL(irecv(ptmp, rcount, rdtype, i,
                                     MCA_COLL_BASE_TAG_ALLGATHER,
                                     comm, &reqs[i]));
            if (MPI_SUCCESS != err) { line = __LINE__; goto exit; }
        }

        err = ompi_request_wait_all(rsize + 1, reqs, MPI_STATUSES_IGNORE);
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

        /* Step 2: exchange the resuts between the root processes */
        span = opal_datatype_span(&sdtype->super, (int64_t)scount * (int64_t)size, &gap);
        tmpbuf_free = (char *) malloc(span);
        if (NULL == tmpbuf_free) { line = __LINE__; err = OMPI_ERR_OUT_OF_RESOURCE; goto exit; }
        tmpbuf = tmpbuf_free - gap;

        err = MCA_PML_CALL(isend(rbuf, rsize * rcount, rdtype, 0,
                                 MCA_COLL_BASE_TAG_ALLGATHER,
                                 MCA_PML_BASE_SEND_STANDARD, comm, &req));
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

        err = MCA_PML_CALL(recv(tmpbuf, size * scount, sdtype, 0,
                                MCA_COLL_BASE_TAG_ALLGATHER, comm,
                                MPI_STATUS_IGNORE));
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

        err = ompi_request_wait( &req, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }
    }


    /* Step 3: bcast the data to the remote group. This
     * happens in both groups simultaneously, thus we can
     * not use coll_bcast (this would deadlock).
     */
    if (rank != root) {
        /* post the recv */
        err = MCA_PML_CALL(recv(rbuf, rsize * rcount, rdtype, 0,
                                MCA_COLL_BASE_TAG_ALLGATHER, comm,
                                MPI_STATUS_IGNORE));
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }

    } else {
        /* Send the data to every other process in the remote group
         * except to rank zero. which has it already. */
        for (i = 1; i < rsize; i++) {
            err = MCA_PML_CALL(isend(tmpbuf, size * scount, sdtype, i,
                                     MCA_COLL_BASE_TAG_ALLGATHER,
                                     MCA_PML_BASE_SEND_STANDARD,
                                     comm, &reqs[i - 1]));
            if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }
        }

        err = ompi_request_wait_all(rsize - 1, reqs, MPI_STATUSES_IGNORE);
        if (OMPI_SUCCESS != err) { line = __LINE__; goto exit; }
    }

  exit:
    if( MPI_SUCCESS != err ) {
        OPAL_OUTPUT( (ompi_coll_base_framework.framework_output,"%s:%4d\tError occurred %d, rank %2d",
                      __FILE__, line, err, rank) );
        (void)line;  // silence compiler warning
        if( NULL != reqs ) ompi_coll_base_free_reqs(reqs, rsize+1);
    }
    if (NULL != tmpbuf_free) {
        free(tmpbuf_free);
    }

    return err;
}

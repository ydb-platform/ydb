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
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2013      FUJITSU LIMITED.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
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


static int
mca_coll_basic_alltoallw_intra_inplace(const void *rbuf, const int *rcounts, const int *rdisps,
                                       struct ompi_datatype_t * const *rdtypes,
                                       struct ompi_communicator_t *comm,
                                       mca_coll_base_module_t *module)
{
    int i, j, size, rank, err = MPI_SUCCESS, max_size;
    ompi_request_t *req;
    char *tmp_buffer, *save_buffer = NULL;
    ptrdiff_t ext, gap = 0;

    /* Initialize. */

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    /* If only one process, we're done. */
    if (1 == size) {
        return MPI_SUCCESS;
    }

    /* Find the largest receive amount */
    for (i = 0, max_size = 0 ; i < size ; ++i) {
        ext = opal_datatype_span(&rdtypes[i]->super, rcounts[i], &gap);

        max_size = ext > max_size ? ext : max_size;
    }

    /* Allocate a temporary buffer */
    tmp_buffer = save_buffer = calloc (max_size, 1);
    if (NULL == tmp_buffer) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    tmp_buffer -= gap;

    /* in-place alltoallw slow algorithm (but works) */
    for (i = 0 ; i < size ; ++i) {
        size_t msg_size_i;
        ompi_datatype_type_size(rdtypes[i], &msg_size_i);
        msg_size_i *= rcounts[i];
        for (j = i+1 ; j < size ; ++j) {
            size_t msg_size_j;
            ompi_datatype_type_size(rdtypes[j], &msg_size_j);
            msg_size_j *= rcounts[j];

            /* Initiate all send/recv to/from others. */
            if (i == rank && msg_size_j != 0) {
                /* Copy the data into the temporary buffer */
                err = ompi_datatype_copy_content_same_ddt (rdtypes[j], rcounts[j],
                                                           tmp_buffer, (char *) rbuf + rdisps[j]);
                if (MPI_SUCCESS != err) { goto error_hndl; }

                /* Exchange data with the peer */
                err = MCA_PML_CALL(irecv ((char *) rbuf + rdisps[j], rcounts[j], rdtypes[j],
                                          j, MCA_COLL_BASE_TAG_ALLTOALLW, comm, &req));
                if (MPI_SUCCESS != err) { goto error_hndl; }

                err = MCA_PML_CALL(send ((void *) tmp_buffer,  rcounts[j], rdtypes[j],
                                          j, MCA_COLL_BASE_TAG_ALLTOALLW, MCA_PML_BASE_SEND_STANDARD,
                                          comm));
                if (MPI_SUCCESS != err) { goto error_hndl; }
            } else if (j == rank && msg_size_i != 0) {
                /* Copy the data into the temporary buffer */
                err = ompi_datatype_copy_content_same_ddt (rdtypes[i], rcounts[i],
                                                           tmp_buffer, (char *) rbuf + rdisps[i]);
                if (MPI_SUCCESS != err) { goto error_hndl; }

                /* Exchange data with the peer */
                err = MCA_PML_CALL(irecv ((char *) rbuf + rdisps[i], rcounts[i], rdtypes[i],
                                          i, MCA_COLL_BASE_TAG_ALLTOALLW, comm, &req));
                if (MPI_SUCCESS != err) { goto error_hndl; }

                err = MCA_PML_CALL(send ((void *) tmp_buffer,  rcounts[i], rdtypes[i],
                                          i, MCA_COLL_BASE_TAG_ALLTOALLW, MCA_PML_BASE_SEND_STANDARD,
                                          comm));
                if (MPI_SUCCESS != err) { goto error_hndl; }
            } else {
                continue;
            }

            /* Wait for the requests to complete */
            err = ompi_request_wait (&req, MPI_STATUSES_IGNORE);
            if (MPI_SUCCESS != err) { goto error_hndl; }
        }
    }

 error_hndl:
    /* Free the temporary buffer */
    free (save_buffer);

    /* All done */

    return err;
}


/*
 *	alltoallw_intra
 *
 *	Function:	- MPI_Alltoallw
 *	Accepts:	- same as MPI_Alltoallw()
 *	Returns:	- MPI_SUCCESS or an MPI error code
 */
int
mca_coll_basic_alltoallw_intra(const void *sbuf, const int *scounts, const int *sdisps,
                               struct ompi_datatype_t * const *sdtypes,
                               void *rbuf, const int *rcounts, const int *rdisps,
                               struct ompi_datatype_t * const *rdtypes,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module)
{
    int i, size, rank, err, nreqs;
    char *psnd, *prcv;
    ompi_request_t **preq, **reqs;

    /* Initialize. */
    if (MPI_IN_PLACE == sbuf) {
        return mca_coll_basic_alltoallw_intra_inplace (rbuf, rcounts, rdisps,
                                                       rdtypes, comm, module);
    }

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    /* simple optimization */

    psnd = ((char *) sbuf) + sdisps[rank];
    prcv = ((char *) rbuf) + rdisps[rank];

    err = ompi_datatype_sndrcv(psnd, scounts[rank], sdtypes[rank],
                               prcv, rcounts[rank], rdtypes[rank]);
    if (MPI_SUCCESS != err) {
        return err;
    }

    /* If only one process, we're done. */

    if (1 == size) {
        return MPI_SUCCESS;
    }

    /* Initiate all send/recv to/from others. */

    nreqs = 0;
    reqs = preq = ompi_coll_base_comm_get_reqs(module->base_data, 2 * size);
    if( NULL == reqs ) { return OMPI_ERR_OUT_OF_RESOURCE; }

    /* Post all receives first -- a simple optimization */

    for (i = 0; i < size; ++i) {
        size_t msg_size;
        ompi_datatype_type_size(rdtypes[i], &msg_size);
        msg_size *= rcounts[i];

        if (i == rank || 0 == msg_size)
            continue;

        prcv = ((char *) rbuf) + rdisps[i];
        err = MCA_PML_CALL(irecv_init(prcv, rcounts[i], rdtypes[i],
                                      i, MCA_COLL_BASE_TAG_ALLTOALLW, comm,
                                      preq++));
        ++nreqs;
        if (MPI_SUCCESS != err) {
            ompi_coll_base_free_reqs(reqs, nreqs);
            return err;
        }
    }

    /* Now post all sends */

    for (i = 0; i < size; ++i) {
        size_t msg_size;
        ompi_datatype_type_size(sdtypes[i], &msg_size);
        msg_size *= scounts[i];

        if (i == rank || 0 == msg_size)
            continue;

        psnd = ((char *) sbuf) + sdisps[i];
        err = MCA_PML_CALL(isend_init(psnd, scounts[i], sdtypes[i],
                                      i, MCA_COLL_BASE_TAG_ALLTOALLW,
                                      MCA_PML_BASE_SEND_STANDARD, comm,
                                      preq++));
        ++nreqs;
        if (MPI_SUCCESS != err) {
            ompi_coll_base_free_reqs(reqs, nreqs);
            return err;
        }
    }

    /* Start your engines.  This will never return an error. */

    MCA_PML_CALL(start(nreqs, reqs));

    /* Wait for them all.  If there's an error, note that we don't care
     * what the error was -- just that there *was* an error.  The PML
     * will finish all requests, even if one or more of them fail.
     * i.e., by the end of this call, all the requests are free-able.
     * So free them anyway -- even if there was an error, and return the
     * error after we free everything. */

    err = ompi_request_wait_all(nreqs, reqs, MPI_STATUSES_IGNORE);
    /* Free the requests in all cases as they are persistent */
    ompi_coll_base_free_reqs(reqs, nreqs);

    /* All done */
    return err;
}


/*
 *	alltoallw_inter
 *
 *	Function:	- MPI_Alltoallw
 *	Accepts:	- same as MPI_Alltoallw()
 *	Returns:	- MPI_SUCCESS or an MPI error code
 */
int
mca_coll_basic_alltoallw_inter(const void *sbuf, const int *scounts, const int *sdisps,
                               struct ompi_datatype_t * const *sdtypes,
                               void *rbuf, const int *rcounts, const int *rdisps,
                               struct ompi_datatype_t * const *rdtypes,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module)
{
    int i, size, err, nreqs;
    char *psnd, *prcv;
    ompi_request_t **preq, **reqs;

    /* Initialize. */
    size = ompi_comm_remote_size(comm);

    /* Initiate all send/recv to/from others. */
    nreqs = 0;
    reqs = preq = ompi_coll_base_comm_get_reqs(module->base_data, 2 * size);
    if( NULL == reqs ) { return OMPI_ERR_OUT_OF_RESOURCE; }

    /* Post all receives first -- a simple optimization */
    for (i = 0; i < size; ++i) {
        size_t msg_size;
        ompi_datatype_type_size(rdtypes[i], &msg_size);
        msg_size *= rcounts[i];

        if (0 == msg_size)
            continue;

        prcv = ((char *) rbuf) + rdisps[i];
        err = MCA_PML_CALL(irecv_init(prcv, rcounts[i], rdtypes[i],
                                      i, MCA_COLL_BASE_TAG_ALLTOALLW,
                                      comm, preq++));
        ++nreqs;
        if (OMPI_SUCCESS != err) {
            ompi_coll_base_free_reqs(reqs, nreqs);
            return err;
        }
    }

    /* Now post all sends */
    for (i = 0; i < size; ++i) {
        size_t msg_size;
        ompi_datatype_type_size(sdtypes[i], &msg_size);
        msg_size *= scounts[i];

        if (0 == msg_size)
            continue;

        psnd = ((char *) sbuf) + sdisps[i];
        err = MCA_PML_CALL(isend_init(psnd, scounts[i], sdtypes[i],
                                      i, MCA_COLL_BASE_TAG_ALLTOALLW,
                                      MCA_PML_BASE_SEND_STANDARD, comm,
                                      preq++));
        ++nreqs;
        if (OMPI_SUCCESS != err) {
            ompi_coll_base_free_reqs(reqs, nreqs);
            return err;
        }
    }

    /* Start your engines.  This will never return an error. */
    MCA_PML_CALL(start(nreqs, reqs));

    /* Wait for them all.  If there's an error, note that we don't care
     * what the error was -- just that there *was* an error.  The PML
     * will finish all requests, even if one or more of them fail.
     * i.e., by the end of this call, all the requests are free-able.
     * So free them anyway -- even if there was an error, and return the
     * error after we free everything. */
    err = ompi_request_wait_all(nreqs, reqs, MPI_STATUSES_IGNORE);

    /* Free the requests in all cases as they are persistent */
    ompi_coll_base_free_reqs(reqs, nreqs);

    /* All done */
    return err;
}

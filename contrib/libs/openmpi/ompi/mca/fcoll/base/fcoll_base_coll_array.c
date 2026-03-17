/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/pml.h"
#include "opal/datatype/opal_datatype.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"

#include <math.h>
#include "ompi/mca/fcoll/base/fcoll_base_coll_array.h"
#include "ompi/mca/common/ompio/common_ompio.h"


int ompi_fcoll_base_coll_allgatherv_array (void *sbuf,
                                      int scount,
                                      ompi_datatype_t *sdtype,
                                      void *rbuf,
                                      int *rcounts,
                                      int *disps,
                                      ompi_datatype_t *rdtype,
                                      int root_index,
                                      int *procs_in_group,
                                      int procs_per_group,
                                      ompi_communicator_t *comm)
{
    int err = OMPI_SUCCESS;
    ptrdiff_t extent, lb;
    int i, rank, j;
    char *send_buf = NULL;
    struct ompi_datatype_t *newtype, *send_type;

    rank = ompi_comm_rank (comm);
    for (j = 0; j < procs_per_group; j++) {
        if (procs_in_group[j] == rank) {
            break;
        }
    }

    if (MPI_IN_PLACE == sbuf) {
        err = opal_datatype_get_extent (&rdtype->super, &lb, &extent);
        if (OMPI_SUCCESS != err) {
            return OMPI_ERROR;
        }
        send_type = rdtype;
        send_buf = (char*)rbuf;

        for (i = 0; i < j; i++) {
            send_buf += (rcounts[i] * extent);
        }
    }
    else {
        send_buf = (char*)sbuf;
        send_type = sdtype;
    }

    err = ompi_fcoll_base_coll_gatherv_array (send_buf,
                                         rcounts[j],
                                         send_type,
                                         rbuf,
                                         rcounts,
                                         disps,
                                         rdtype,
                                         root_index,
                                         procs_in_group,
                                         procs_per_group,
                                         comm);
    if (OMPI_SUCCESS != err) {
        return err;
    }

    err = ompi_datatype_create_indexed (procs_per_group,
                                        rcounts,
                                        disps,
                                        rdtype,
                                        &newtype);
    if (MPI_SUCCESS != err) {
        return err;
    }
    err = ompi_datatype_commit (&newtype);
    if(MPI_SUCCESS != err) {
        return err;
    }
    
    ompi_fcoll_base_coll_bcast_array (rbuf,
                                 1,
                                 newtype,
                                 root_index,
                                 procs_in_group,
                                 procs_per_group,
                                 comm);
    
    ompi_datatype_destroy (&newtype);

    return OMPI_SUCCESS;
}

int ompi_fcoll_base_coll_gatherv_array (void *sbuf,
                                   int scount,
                                   ompi_datatype_t *sdtype,
                                   void *rbuf,
                                   int *rcounts,
                                   int *disps,
                                   ompi_datatype_t *rdtype,
                                   int root_index,
                                   int *procs_in_group,
                                   int procs_per_group,
                                   struct ompi_communicator_t *comm)
{
    int i, rank;
    int err = OMPI_SUCCESS;
    char *ptmp;
    ptrdiff_t extent, lb;
    ompi_request_t **reqs=NULL;

    rank = ompi_comm_rank (comm);

    if (procs_in_group[root_index] != rank)  {
        if (scount > 0) {
            return MCA_PML_CALL(send(sbuf,
                                     scount,
                                     sdtype,
                                     procs_in_group[root_index],
                                     FCOLL_TAG_GATHERV,
                                     MCA_PML_BASE_SEND_STANDARD,
                                     comm));
        }
        return err;
    }

    /* writer processes, loop receiving data from proceses
       belonging to each corresponding root */

    err = opal_datatype_get_extent (&rdtype->super, &lb, &extent);
    if (OMPI_SUCCESS != err) {
        return OMPI_ERROR;
    }
    reqs = (ompi_request_t **) malloc ( procs_per_group *sizeof(ompi_request_t *));
    if ( NULL == reqs ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }
    for (i=0; i<procs_per_group; i++) {
        ptmp = ((char *) rbuf) + (extent * disps[i]);

        if (procs_in_group[i] == rank) {
            if (MPI_IN_PLACE != sbuf &&
                (0 < scount) &&
                (0 < rcounts[i])) {
                err = ompi_datatype_sndrcv (sbuf,
                                            scount,
                                            sdtype,
                                            ptmp,
                                            rcounts[i],
                                            rdtype);
            }
	    reqs[i] = MPI_REQUEST_NULL;
        }
        else {
            /* Only receive if there is something to receive */
            if (rcounts[i] > 0) {
                err = MCA_PML_CALL(irecv(ptmp,
                                         rcounts[i],
                                         rdtype,
                                         procs_in_group[i],
                                         FCOLL_TAG_GATHERV,
                                         comm,
                                         &reqs[i]));
            }
	    else {
		reqs[i] = MPI_REQUEST_NULL;
	    }
        }

        if (OMPI_SUCCESS != err) {
	    free ( reqs );
            return err;
        }
    }
    /* All done */
    err = ompi_request_wait_all ( procs_per_group, reqs, MPI_STATUSES_IGNORE );
    if ( NULL != reqs ) {
	free ( reqs );
    }
    return err;
}

int ompi_fcoll_base_coll_scatterv_array (void *sbuf,
                                    int *scounts,
                                    int *disps,
                                    ompi_datatype_t *sdtype,
                                    void *rbuf,
                                    int rcount,
                                    ompi_datatype_t *rdtype,
                                    int root_index,
                                    int *procs_in_group,
                                    int procs_per_group,
                                    struct ompi_communicator_t *comm)
{
    int i, rank;
    int err = OMPI_SUCCESS;
    char *ptmp;
    ptrdiff_t extent, lb;
    ompi_request_t ** reqs=NULL;

    rank = ompi_comm_rank (comm);

    if (procs_in_group[root_index] != rank) {
        if (rcount > 0) {
            err = MCA_PML_CALL(recv(rbuf,
                                    rcount,
                                    rdtype,
                                    procs_in_group[root_index],
                                    FCOLL_TAG_SCATTERV,
                                    comm,
                                    MPI_STATUS_IGNORE));
        }
        return err;
    }

    /* writer processes, loop sending data to proceses
       belonging to each corresponding root */

    err = opal_datatype_get_extent (&sdtype->super, &lb, &extent);
    if (OMPI_SUCCESS != err) {
        return OMPI_ERROR;
    }
    reqs = ( ompi_request_t **) malloc ( procs_per_group * sizeof ( ompi_request_t *));
    if (NULL == reqs ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (i=0 ; i<procs_per_group ; ++i) {
        ptmp = ((char *) sbuf) + (extent * disps[i]);

        if (procs_in_group[i] == rank) {
            if (MPI_IN_PLACE != sbuf &&
                (0 < scounts[i]) &&
                (0 < rcount)) {
                err = ompi_datatype_sndrcv (ptmp,
                                            scounts[i],
                                            sdtype,
                                            rbuf,
                                            rcount,
                                            rdtype);
            }
	    reqs[i] = MPI_REQUEST_NULL;
        }
        else {
            /* Only receive if there is something to receive */
            if (scounts[i] > 0) {
                err = MCA_PML_CALL(isend(ptmp,
                                         scounts[i],
                                         sdtype,
                                         procs_in_group[i],
                                         FCOLL_TAG_SCATTERV,
                                         MCA_PML_BASE_SEND_STANDARD,
                                         comm,
				         &reqs[i]));
            }
	    else {
		reqs[i] = MPI_REQUEST_NULL;
            }
        }
        if (OMPI_SUCCESS != err) {
            free ( reqs );
            return err;
        }
    }
    /* All done */
    err = ompi_request_wait_all ( procs_per_group, reqs, MPI_STATUSES_IGNORE );
    if ( NULL != reqs ) {
	free ( reqs );
    }
    return err;
}

int ompi_fcoll_base_coll_allgather_array (void *sbuf,
                                     int scount,
                                     ompi_datatype_t *sdtype,
                                     void *rbuf,
                                     int rcount,
                                     ompi_datatype_t *rdtype,
                                     int root_index,
                                     int *procs_in_group,
                                     int procs_per_group,
                                     ompi_communicator_t *comm)
{
    int err = OMPI_SUCCESS;
    int rank;
    ptrdiff_t extent, lb;

    rank = ompi_comm_rank (comm);

    if (((void *) 1) == sbuf && 0 != rank) {
        err = opal_datatype_get_extent (&rdtype->super, &lb, &extent);
        if (OMPI_SUCCESS != err) {
            return OMPI_ERROR;
        }
        sbuf = ((char*) rbuf) + (rank * extent * rcount);
        sdtype = rdtype;
        scount = rcount;
    }

    /* Gather and broadcast. */
    err = ompi_fcoll_base_coll_gather_array (sbuf,
                                        scount,
                                        sdtype,
                                        rbuf,
                                        rcount,
                                        rdtype,
                                        root_index,
                                        procs_in_group,
                                        procs_per_group,
                                        comm);
    
    if (OMPI_SUCCESS == err) {
        err = ompi_fcoll_base_coll_bcast_array (rbuf,
                                           rcount * procs_per_group,
                                           rdtype,
                                           root_index,
                                           procs_in_group,
                                           procs_per_group,
                                           comm);
    }
    /* All done */

    return err;
}

int ompi_fcoll_base_coll_gather_array (void *sbuf,
                                  int scount,
                                  ompi_datatype_t *sdtype,
                                  void *rbuf,
                                  int rcount,
                                  ompi_datatype_t *rdtype,
                                  int root_index,
                                  int *procs_in_group,
                                  int procs_per_group,
                                  struct ompi_communicator_t *comm)
{
    int i;
    int rank;
    char *ptmp;
    ptrdiff_t incr;
    ptrdiff_t extent, lb;
    int err = OMPI_SUCCESS;
    ompi_request_t ** reqs=NULL;

    rank = ompi_comm_rank (comm);

    /* Everyone but the writers sends data and returns. */
    if (procs_in_group[root_index] != rank) {
        err = MCA_PML_CALL(send(sbuf,
                                scount,
                                sdtype,
                                procs_in_group[root_index],
                                FCOLL_TAG_GATHER,
                                MCA_PML_BASE_SEND_STANDARD,
                                comm));
        return err;
    }

    /* writers, loop receiving the data. */
    opal_datatype_get_extent (&rdtype->super, &lb, &extent);
    incr = extent * rcount;

    reqs = ( ompi_request_t **) malloc ( procs_per_group * sizeof ( ompi_request_t *));
    if (NULL == reqs ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (i = 0, ptmp = (char *) rbuf;
         i < procs_per_group;
         ++i, ptmp += incr) {
        if (procs_in_group[i] == rank) {
            if (MPI_IN_PLACE != sbuf) {
                err = ompi_datatype_sndrcv (sbuf,
                                            scount,
                                            sdtype ,
                                            ptmp,
                                            rcount,
                                            rdtype);
            }
            else {
                err = OMPI_SUCCESS;
            }
	    reqs[i] = MPI_REQUEST_NULL;
        }
        else {
            err = MCA_PML_CALL(irecv(ptmp,
                                     rcount,
                                     rdtype,
                                     procs_in_group[i],
                                     FCOLL_TAG_GATHER,
                                     comm,
                                     &reqs[i]));
            /*
            for (k=0 ; k<4 ; k++)
                printf ("RECV %p  %d \n",
                        ((struct iovec *)ptmp)[k].iov_base,
                        ((struct iovec *)ptmp)[k].iov_len);
            */
        }

        if (OMPI_SUCCESS != err) {
	    free ( reqs );
            return err;
        }
    }

    /* All done */
    err = ompi_request_wait_all ( procs_per_group, reqs, MPI_STATUSES_IGNORE );
    if ( NULL != reqs ) {
	free ( reqs );
    }

    return err;
}

int ompi_fcoll_base_coll_bcast_array (void *buff,
                                 int count,
                                 ompi_datatype_t *datatype,
                                 int root_index,
                                 int *procs_in_group,
                                 int procs_per_group,
                                 ompi_communicator_t *comm)
{
    int i, rank;
    int err = OMPI_SUCCESS;
    ompi_request_t ** reqs=NULL;

    rank = ompi_comm_rank (comm);

    /* Non-writers receive the data. */
    if (procs_in_group[root_index] != rank) {
        err = MCA_PML_CALL(recv(buff,
                                count,
                                datatype,
                                procs_in_group[root_index],
                                FCOLL_TAG_BCAST,
                                comm,
                                MPI_STATUS_IGNORE));
        return err;
    }

    /* Writers sends data to all others. */
    reqs = ( ompi_request_t **) malloc ( procs_per_group * sizeof ( ompi_request_t *));
    if (NULL == reqs ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (i=0 ; i<procs_per_group ; i++) {
        if (procs_in_group[i] == rank) {
	    reqs[i] = MPI_REQUEST_NULL;
            continue;
        }

        err = MCA_PML_CALL(isend(buff,
                                 count,
                                 datatype,
                                 procs_in_group[i],
                                 FCOLL_TAG_BCAST,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm,
			         &reqs[i]));
        if (OMPI_SUCCESS != err) {
	    free ( reqs );
            return err;
        }
    }
    err = ompi_request_wait_all ( procs_per_group, reqs, MPI_STATUSES_IGNORE );
    if ( NULL != reqs ) {
	free ( reqs );
    }

    return err;
}

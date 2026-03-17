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
 * Copyright (c) 2013-2015 University of Houston. All rights reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "sharedfp_sm.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

/*use a semaphore to lock the shared memory*/
#include <semaphore.h>

int mca_sharedfp_sm_request_position(ompio_file_t *fh, 
                                     int bytes_requested,
                                     OMPI_MPI_OFFSET_TYPE *offset)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE position = 0;
    OMPI_MPI_OFFSET_TYPE old_offset;
    struct mca_sharedfp_sm_data * sm_data = NULL;
    struct mca_sharedfp_sm_offset * sm_offset_ptr = NULL;
    struct mca_sharedfp_base_data_t *sh = NULL;

    sh = fh->f_sharedfp_data;
    sm_data = sh->selected_module_data;

    *offset = 0;
    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "Aquiring lock, rank=%d...",fh->f_rank);
    }

    sm_offset_ptr = sm_data->sm_offset_ptr;

    /* Aquire an exclusive lock */

    sem_wait(sm_data->mutex);

    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "Succeeded! Acquired sm lock.for rank=%d\n",fh->f_rank);
    }

    old_offset=sm_offset_ptr->offset;
    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "Read last_offset=%lld!\n",old_offset);
    }

    position = old_offset + bytes_requested;
    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "old_offset=%lld, bytes_requested=%d, new offset=%lld!\n",old_offset,bytes_requested,position);
    }
    sm_offset_ptr->offset=position;


    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "Releasing sm lock...rank=%d",fh->f_rank);
    }

    sem_post(sm_data->mutex);
    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "Released lock! released lock.for rank=%d\n",fh->f_rank);
    }

    *offset = old_offset;

    return ret;
}

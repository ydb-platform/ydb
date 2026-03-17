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
 * Copyright (c) 2013-2018 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

int
mca_sharedfp_sm_get_position(ompio_file_t *fh,
                             OMPI_MPI_OFFSET_TYPE * offset)
{
    if(fh->f_sharedfp_data==NULL){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_sm_write - module not initialized\n");
        return OMPI_ERROR;
    }

    /*Requesting the offset to write 0 bytes,
     *returns the current offset w/o updating it
     */

    return mca_sharedfp_sm_request_position(fh,0,offset);
}

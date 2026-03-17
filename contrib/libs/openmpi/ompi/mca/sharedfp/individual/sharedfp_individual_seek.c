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
 * Copyright (c) 2013      University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "sharedfp_individual.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"

int mca_sharedfp_individual_usage_counter=0;

int mca_sharedfp_individual_seek (ompio_file_t *fh,
                         OMPI_MPI_OFFSET_TYPE offset, int whence)
{
    if ( 0 == mca_sharedfp_individual_usage_counter  ) {
        /* As long as nobody using this module 'inapproprialy', its is ok.
        ** to call this function, since it might come out of File_set_view.
        ** however, we have to return an error as soon as people are calling
        ** this function on this component and expect it to work. 
        */
        
        return OMPI_SUCCESS;
    }

    opal_output(0,"mca_sharedfp_individual_seek: NOT IMPLEMENTED\n");
    return OMPI_ERROR;
}

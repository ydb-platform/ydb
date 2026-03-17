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
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 *  $COPYRIGHT$
 *
 *  Additional copyrights may follow
 *
 *  $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "ompi/file/file.h"
#include "ompi/mca/pml/pml.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/datatype/ompi_datatype.h"
#include <stdlib.h>
#include <stdio.h>

#include <unistd.h>
#include "io_ompio.h"

static int datatype_duplicate (ompi_datatype_t *oldtype, ompi_datatype_t **newtype );
static int datatype_duplicate  (ompi_datatype_t *oldtype, ompi_datatype_t **newtype )
{
    ompi_datatype_t *type;
    if( ompi_datatype_is_predefined(oldtype) ) {
        OBJ_RETAIN(oldtype);
        *newtype = oldtype;
        return OMPI_SUCCESS;
    }

    if ( OMPI_SUCCESS != ompi_datatype_duplicate (oldtype, &type)){
        ompi_datatype_destroy (&type);
        return MPI_ERR_INTERN;
    }
    
    ompi_datatype_set_args( type, 0, NULL, 0, NULL, 1, &oldtype, MPI_COMBINER_DUP );

    *newtype = type;
    return OMPI_SUCCESS;
}

int mca_io_ompio_file_set_view (ompi_file_t *fp,
                                OMPI_MPI_OFFSET_TYPE disp,
                                ompi_datatype_t *etype,
                                ompi_datatype_t *filetype,
                                const char *datarep,
                                opal_info_t *info)
{
    int ret=OMPI_SUCCESS;
    mca_common_ompio_data_t *data;
    ompio_file_t *fh;

    if ( (strcmp(datarep, "native") && strcmp(datarep, "NATIVE"))) {
        return MPI_ERR_UNSUPPORTED_DATAREP;
    }

    data = (mca_common_ompio_data_t *) fp->f_io_selected_data;

    /* we need to call the internal file set view twice: once for the individual
       file pointer, once for the shared file pointer (if it is existent)
    */
    fh = &data->ompio_fh;
  
    OPAL_THREAD_LOCK(&fp->f_lock);
    ret = mca_common_ompio_set_view(fh, disp, etype, filetype, datarep, info);
    OPAL_THREAD_UNLOCK(&fp->f_lock);
    return ret;
}

int mca_io_ompio_file_get_view (struct ompi_file_t *fp,
                                OMPI_MPI_OFFSET_TYPE *disp,
                                struct ompi_datatype_t **etype,
                                struct ompi_datatype_t **filetype,
                                char *datarep)
{
    mca_common_ompio_data_t *data;
    ompio_file_t *fh;

    data = (mca_common_ompio_data_t *) fp->f_io_selected_data;
    fh = &data->ompio_fh;

    OPAL_THREAD_LOCK(&fp->f_lock);
    *disp = fh->f_disp;
    datatype_duplicate (fh->f_etype, etype);
    datatype_duplicate (fh->f_orig_filetype, filetype);
    strcpy (datarep, fh->f_datarep);
    OPAL_THREAD_UNLOCK(&fp->f_lock);

    return OMPI_SUCCESS;
}


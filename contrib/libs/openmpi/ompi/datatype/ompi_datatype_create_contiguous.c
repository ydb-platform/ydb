/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "mpi.h"

int32_t ompi_datatype_create_contiguous( int count, const ompi_datatype_t* oldType,
                                         ompi_datatype_t** newType )
{
    ompi_datatype_t* pdt;

    if( 0 == count ) {
        pdt = ompi_datatype_create( 0 );
        ompi_datatype_add( pdt, &ompi_mpi_datatype_null.dt, 0, 0, 0 );
    } else {
        pdt = ompi_datatype_create( oldType->super.desc.used + 2 );
        opal_datatype_add( &(pdt->super), &(oldType->super), count, 0, (oldType->super.ub - oldType->super.lb) );
    }
    *newType = pdt;
    return OMPI_SUCCESS;
}

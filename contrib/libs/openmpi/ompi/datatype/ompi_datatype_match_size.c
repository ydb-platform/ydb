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
#include "opal/class/opal_pointer_array.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"

extern int32_t ompi_datatype_number_of_predefined_data;

const ompi_datatype_t* ompi_datatype_match_size( int size, uint16_t datakind, uint16_t datalang )
{
    int32_t i;
    const ompi_datatype_t* datatype;

    /* If we're not looking for a complex C++ type then set the default type to C */
    if( datalang == OMPI_DATATYPE_FLAG_DATA_CPP ) {
        if( datakind != OMPI_DATATYPE_FLAG_DATA_COMPLEX )
            datalang = OMPI_DATATYPE_FLAG_DATA_C;
    }

    for( i = 0; i < ompi_datatype_number_of_predefined_data; i++ ) {

        datatype = (ompi_datatype_t*)opal_pointer_array_get_item(&ompi_datatype_f_to_c_table, i);

        if( (datatype->super.flags & OMPI_DATATYPE_FLAG_DATA_LANGUAGE) != datalang )
            continue;
        if( (datatype->super.flags & OMPI_DATATYPE_FLAG_DATA_TYPE) != datakind )
            continue;
        if( (size_t)size == datatype->super.size ) {
            return datatype;
        }
    }
    return &ompi_mpi_datatype_null.dt;
}

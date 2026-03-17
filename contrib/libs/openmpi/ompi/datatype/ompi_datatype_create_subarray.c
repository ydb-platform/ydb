/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stddef.h>

#include "ompi/datatype/ompi_datatype.h"

int32_t ompi_datatype_create_subarray(int ndims,
                                      int const* size_array,
                                      int const* subsize_array,
                                      int const* start_array,
                                      int order,
                                      const ompi_datatype_t* oldtype,
                                      ompi_datatype_t** newtype)
{
    ompi_datatype_t *last_type;
    int32_t i, step, end_loop;
    MPI_Aint size, displ, extent;

    /**
     * If the oldtype contains the original MPI_LB and MPI_UB markers then we
     * are forced to follow the MPI standard suggestion and reset these 2
     * markers (MPI 3.0 page 96 line 37).  Otherwise we can simply resize the
     * datatype.
     */
    ompi_datatype_type_extent( oldtype, &extent );

    /* If the ndims is zero then return the NULL datatype */
    if( ndims < 2 ) {
        if( 0 == ndims ) {
            ompi_datatype_duplicate(&ompi_mpi_datatype_null.dt, newtype);
            return MPI_SUCCESS;
        }
        ompi_datatype_create_contiguous( subsize_array[0], oldtype, &last_type );
        size = size_array[0];
        displ = start_array[0];
        goto replace_subarray_type;
    }

    if( MPI_ORDER_C == order ) {
        i = ndims - 1;
        step = -1;
        end_loop = -1;
    } else {
        i = 0;
        step = 1;
        end_loop = ndims;
    }

    /* As we know that the ndims is at least 1 we can start by creating the
     * first dimension data outside the loop, such that we dont have to create
     * a duplicate of the oldtype just to be able to free it.
     */
    ompi_datatype_create_vector( subsize_array[i+step], subsize_array[i], size_array[i],
                                 oldtype, newtype );

    last_type = *newtype;
    size = (MPI_Aint)size_array[i] * (MPI_Aint)size_array[i+step];
    displ = (MPI_Aint)start_array[i] + (MPI_Aint)start_array[i+step] * (MPI_Aint)size_array[i];
    for( i += 2 * step; i != end_loop; i += step ) {
        ompi_datatype_create_hvector( subsize_array[i], 1, size * extent,
                                      last_type, newtype );
        ompi_datatype_destroy( &last_type );

        displ += size * start_array[i];
        size *= size_array[i];
        last_type = *newtype;
    }

 replace_subarray_type:
    /**
      * We need to shift the content (useful data) of the datatype, so
      * we need to force the displacement to be moved. Therefore, we
      * cannot use resize as it will only set the soft lb and ub
      * markers without moving the data. Instead, we have to create a
      * new data, and insert the last_Type with the correct
      * displacement.
      */
    *newtype = ompi_datatype_create( last_type->super.desc.used );
    ompi_datatype_add( *newtype, last_type, 1, displ * extent, size * extent);
    ompi_datatype_destroy( &last_type );
    opal_datatype_resize( &(*newtype)->super, 0, size * extent );

    return OMPI_SUCCESS;
}

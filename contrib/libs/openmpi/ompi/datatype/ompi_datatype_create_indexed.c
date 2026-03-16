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
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2019      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stddef.h>

#include "ompi/datatype/ompi_datatype.h"


/* We try to merge together data that are contiguous */
int32_t ompi_datatype_create_indexed( int count, const int* pBlockLength, const int* pDisp,
                                      const ompi_datatype_t* oldType, ompi_datatype_t** newType )
{
    ompi_datatype_t* pdt;
    int i;
    ptrdiff_t extent, disp, endat;
    size_t dLength;

    if( 0 == count ) {
        return ompi_datatype_duplicate( &ompi_mpi_datatype_null.dt, newType);
    }

    disp = pDisp[0];
    dLength = pBlockLength[0];
    endat = disp + dLength;
    ompi_datatype_type_extent( oldType, &extent );

    pdt = ompi_datatype_create( count * (2 + oldType->super.desc.used) );
    for( i = 1; i < count; i++ ) {
        if( endat == pDisp[i] ) {
            /* contiguous with the previsious */
            dLength += pBlockLength[i];
            endat += pBlockLength[i];
        } else {
            ompi_datatype_add( pdt, oldType, dLength, disp * extent, extent );
            disp = pDisp[i];
            dLength = pBlockLength[i];
            endat = disp + pBlockLength[i];
        }
    }
    ompi_datatype_add( pdt, oldType, dLength, disp * extent, extent );

    *newType = pdt;
    return OMPI_SUCCESS;
}


int32_t ompi_datatype_create_hindexed( int count, const int* pBlockLength, const ptrdiff_t* pDisp,
                                       const ompi_datatype_t* oldType, ompi_datatype_t** newType )
{
    ompi_datatype_t* pdt;
    int i;
    ptrdiff_t extent, disp, endat;
    size_t dLength;

    if( 0 == count ) {
        *newType = ompi_datatype_create( 0 );
        ompi_datatype_add( *newType, &ompi_mpi_datatype_null.dt, 0, 0, 0);
        return OMPI_SUCCESS;
    }

    ompi_datatype_type_extent( oldType, &extent );
    pdt = ompi_datatype_create( count * (2 + oldType->super.desc.used) );
    disp = pDisp[0];
    dLength = pBlockLength[0];
    endat = disp + dLength * extent;

    for( i = 1; i < count; i++ ) {
        if( endat == pDisp[i] ) {
            /* contiguous with the previsious */
            dLength += pBlockLength[i];
            endat += pBlockLength[i] * extent;
        } else {
            ompi_datatype_add( pdt, oldType, dLength, disp, extent );
            disp = pDisp[i];
            dLength = pBlockLength[i];
            endat = disp + pBlockLength[i] * extent;
        }
    }
    ompi_datatype_add( pdt, oldType, dLength, disp, extent );

    *newType = pdt;
    return OMPI_SUCCESS;
}


int32_t ompi_datatype_create_indexed_block( int count, int bLength, const int* pDisp,
                                            const ompi_datatype_t* oldType, ompi_datatype_t** newType )
{
    ompi_datatype_t* pdt;
    int i;
    ptrdiff_t extent, disp, endat;
    size_t dLength;

    ompi_datatype_type_extent( oldType, &extent );
    if( (count == 0) || (bLength == 0) ) {
        if( 0 == count ) {
            return ompi_datatype_duplicate(&ompi_mpi_datatype_null.dt, newType);
        } else {
            *newType = ompi_datatype_create(1);
            ompi_datatype_add( *newType, oldType, 0, pDisp[0] * extent, extent );
            return OMPI_SUCCESS;
        }
    }
    pdt = ompi_datatype_create( count * (2 + oldType->super.desc.used) );
    disp = pDisp[0];
    dLength = bLength;
    endat = disp + dLength;
    for( i = 1; i < count; i++ ) {
        if( endat == pDisp[i] ) {
            /* contiguous with the previsious */
            dLength += bLength;
            endat += bLength;
        } else {
            ompi_datatype_add( pdt, oldType, dLength, disp * extent, extent );
            disp = pDisp[i];
            dLength = bLength;
            endat = disp + bLength;
        }
    }
    ompi_datatype_add( pdt, oldType, dLength, disp * extent, extent );

    *newType = pdt;
    return OMPI_SUCCESS;
}

int32_t ompi_datatype_create_hindexed_block( int count, int bLength, const ptrdiff_t* pDisp,
                                             const ompi_datatype_t* oldType, ompi_datatype_t** newType )
{
    ompi_datatype_t* pdt;
    int i;
    ptrdiff_t extent, disp, endat;
    size_t dLength;

    ompi_datatype_type_extent( oldType, &extent );
    if( (count == 0) || (bLength == 0) ) {
        *newType = ompi_datatype_create(1);
        if( 0 == count )
            ompi_datatype_add( *newType, &ompi_mpi_datatype_null.dt, 0, 0, 0 );
        else
            ompi_datatype_add( *newType, oldType, 0, pDisp[0] * extent, extent );
        return OMPI_SUCCESS;
    }
    pdt = ompi_datatype_create( count * (2 + oldType->super.desc.used) );
    disp = pDisp[0];
    dLength = bLength;
    endat = disp + dLength;
    for( i = 1; i < count; i++ ) {
        if( endat == pDisp[i] ) {
            /* contiguous with the previsious */
            dLength += bLength;
            endat += bLength;
        } else {
            ompi_datatype_add( pdt, oldType, dLength, disp, extent );
            disp = pDisp[i];
            dLength = bLength;
            endat = disp + bLength;
        }
    }
    ompi_datatype_add( pdt, oldType, dLength, disp, extent );

    *newType = pdt;
    return OMPI_SUCCESS;
}

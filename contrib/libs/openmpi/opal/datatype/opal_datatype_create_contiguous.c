/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
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

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"

int32_t opal_datatype_create_contiguous( int count, const opal_datatype_t* oldType,
                                         opal_datatype_t** newType )
{
    opal_datatype_t* pdt;

    if( 0 == count ) {
        pdt = opal_datatype_create( 0 );
        opal_datatype_add( pdt, &opal_datatype_empty, 0, 0, 0 );
    } else {
        pdt = opal_datatype_create( oldType->desc.used + 2 );
        opal_datatype_add( pdt, oldType, count, 0, (oldType->ub - oldType->lb) );
    }
    *newType = pdt;
    return OPAL_SUCCESS;
}

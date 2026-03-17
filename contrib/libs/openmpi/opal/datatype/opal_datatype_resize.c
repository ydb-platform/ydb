/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

int32_t opal_datatype_resize( opal_datatype_t* type, ptrdiff_t lb, ptrdiff_t extent )
{
    type->lb = lb;
    type->ub = lb + extent;

    type->flags &= ~OPAL_DATATYPE_FLAG_NO_GAPS;
    if( (extent == (ptrdiff_t)type->size) &&
        (type->flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) ) {
        type->flags |= OPAL_DATATYPE_FLAG_NO_GAPS;
    }
    return OPAL_SUCCESS;
}

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdio.h>
#include <limits.h>

#include "ompi/runtime/params.h"
#include "ompi/datatype/ompi_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"

int ompi_datatype_get_elements (ompi_datatype_t *datatype, size_t ucount, size_t *count)
{
    size_t internal_count, size, total;
    int rc, i;

    *count = 0;
    if (OMPI_SUCCESS != (rc = ompi_datatype_type_size (datatype, &size))) {
        return OMPI_ERR_BAD_PARAM;
    }

    if (size == 0) {
        /* If the size of the datatype is zero let's return a count of zero */
        return OMPI_SUCCESS;
    }

    internal_count = ucount / size;    /* how many full types? */
    size = ucount - internal_count * size;  /* leftover bytes */

    /* if basic type we should return the same result as MPI_Get_count (internal_count) if
       there are no leftover bytes */
    if (!ompi_datatype_is_predefined(datatype)) {
        if (0 != internal_count) {
            opal_datatype_compute_ptypes(&datatype->super);
            /* count the basic elements in the datatype */
            for (i = OPAL_DATATYPE_FIRST_TYPE, total = 0 ; i < OPAL_DATATYPE_MAX_PREDEFINED ; ++i) {
                total += datatype->super.ptypes[i];
            }
            internal_count = total * internal_count;
        }
        if (size > 0) {
            /* If there are any leftover bytes, compute the number of predefined
             * types in the datatype that can fit in these bytes.
             */
            if (-1 == (i = ompi_datatype_get_element_count (datatype, size))) {
                return OMPI_ERR_VALUE_OUT_OF_BOUNDS;
            }

            internal_count += i;
        }
    } else if (0 != size) {
        /* no leftover is supported for predefined types */
        return OMPI_ERR_VALUE_OUT_OF_BOUNDS;
    }

    *count = internal_count;

    return OMPI_SUCCESS;
}

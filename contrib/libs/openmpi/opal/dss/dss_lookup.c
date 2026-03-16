/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/dss/dss_internal.h"

char *opal_dss_lookup_data_type(opal_data_type_t type)
{
    opal_dss_type_info_t *info;
    char *name;

    info = (opal_dss_type_info_t*)opal_pointer_array_get_item(&opal_dss_types, type);
    if (NULL != info) { /* type found on list */
        name = strdup(info->odti_name);
        return name;
    }

    return NULL;
}

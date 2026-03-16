/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
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

#include "opal/util/output.h"

#include "opal/dss/dss_internal.h"

int opal_dss_dump(int output_stream, void *src, opal_data_type_t type)
{
    char *sptr;
    int rc;

    if (OPAL_SUCCESS != (rc = opal_dss.print(&sptr, NULL, src, type))) {
        return rc;
    }

    opal_output(output_stream, "%s", sptr);
    free(sptr);

    return OPAL_SUCCESS;
}


void opal_dss_dump_data_types(int output)
{
    opal_dss_type_info_t *ptr;
    opal_data_type_t j;
    int32_t i;

    opal_output(output, "DUMP OF REGISTERED DATA TYPES");

    j = 0;
    for (i=0; i < opal_pointer_array_get_size(&opal_dss_types); i++) {
        ptr = opal_pointer_array_get_item(&opal_dss_types, i);
        if (NULL != ptr) {
            j++;
            /* print out the info */
            opal_output(output, "\tIndex: %lu\tData type: %lu\tName: %s",
                        (unsigned long)j,
                        (unsigned long)ptr->odti_type,
                        ptr->odti_name);
        }
    }
}


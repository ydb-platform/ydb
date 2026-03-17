/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/mca/base/base.h"
#include "opal/class/opal_list.h"
#include "ompi/constants.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/io/base/base.h"


int
mca_io_base_register_datarep(const char *datarep,
                             MPI_Datarep_conversion_function* read_fn,
                             MPI_Datarep_conversion_function* write_fn,
                             MPI_Datarep_extent_function* extent_fn,
                             void* state)
{
    mca_base_component_list_item_t *cli;
    const mca_base_component_t *component;
    const mca_io_base_component_2_0_0_t *v200;
    int tmp, ret = OMPI_SUCCESS;

    /* Find the maximum additional number of bytes required by all io
       components for requests and make that the request size */

    OPAL_LIST_FOREACH(cli, &ompi_io_base_framework.framework_components, mca_base_component_list_item_t) {
        component = cli->cli_component;

        /* Only know how to handle v2.0.0 components for now */
        if (component->mca_type_major_version == 2 &&
            component->mca_type_minor_version == 0 &&
            component->mca_type_release_version == 0) {
            v200 = (mca_io_base_component_2_0_0_t *) component;

            /* return first non-good error-code */
            tmp = v200->io_register_datarep(datarep, read_fn, write_fn,
                                            extent_fn, state);
            ret = (ret == OMPI_SUCCESS) ? tmp : ret;
        }
    }

    return ret;
}


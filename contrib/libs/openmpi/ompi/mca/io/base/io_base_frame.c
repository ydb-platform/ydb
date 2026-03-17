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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"

#include "ompi/mca/io/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "ompi/mca/io/base/static-components.h"

static int mca_io_base_open(mca_base_open_flag_t flags)
{
    int ret;

    if (OPAL_SUCCESS !=
        (ret = mca_base_framework_components_open(&ompi_io_base_framework, flags))) {
        return ret;
    }

    return mca_io_base_find_available(OPAL_ENABLE_PROGRESS_THREADS, 1);
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, io, "I/O", NULL, mca_io_base_open, NULL,
                           mca_io_base_static_components, 0);

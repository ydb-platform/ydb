/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Inria.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>

#include "ompi/constants.h"
#include "ompi/mca/topo/base/base.h"


/* This function is invoked by the top-level MPI API functions to
   lazily load the topo framework components (if it wasn't already --
   it's safe to invoke this function multiple times).  We do this
   because most MPI apps don't use MPI topology functions, so we might
   as well not load them unless we have to. */
int mca_topo_base_lazy_init(void)
{
    int err;

    if (!mca_base_framework_is_open (&ompi_topo_base_framework)) {
        /**
         * Register and open all available components, giving them a chance to access the MCA parameters.
         */

        err = mca_base_framework_open (&ompi_topo_base_framework, MCA_BASE_OPEN_DEFAULT);
        if (OMPI_SUCCESS != err) {
            return err;
        }

        if (OMPI_SUCCESS !=
            (err = mca_topo_base_find_available(OPAL_ENABLE_PROGRESS_THREADS, 1))) {
            return err;
        }
    }

    return OMPI_SUCCESS;
}


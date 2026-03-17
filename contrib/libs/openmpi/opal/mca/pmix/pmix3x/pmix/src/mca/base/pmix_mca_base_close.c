/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include "src/util/output.h"
#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_component_repository.h"
#include "pmix_common.h"

extern int pmix_mca_base_opened;

/*
 * Main MCA shutdown.
 */
int pmix_mca_base_close(void)
{
    assert (pmix_mca_base_opened);
    if (!--pmix_mca_base_opened) {
        /* deregister all MCA base parameters */
        int group_id = pmix_mca_base_var_group_find ("pmix", "mca", "base");

        if (-1 < group_id) {
            pmix_mca_base_var_group_deregister (group_id);
        }

        /* release the default paths */
        if (NULL != pmix_mca_base_system_default_path) {
            free(pmix_mca_base_system_default_path);
        }
        if (NULL != pmix_mca_base_user_default_path) {
            free(pmix_mca_base_user_default_path);
        }

        /* Close down the component repository */
        pmix_mca_base_component_repository_finalize();

        /* Shut down the dynamic component finder */
        pmix_mca_base_component_find_finalize();

        /* Close pmix output stream 0 */
        pmix_output_close(0);
    }

    /* All done */
    return PMIX_SUCCESS;
}

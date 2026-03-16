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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/mca/shmem/base/base.h"
#include "opal/util/output.h"

/* ////////////////////////////////////////////////////////////////////////// */
int
opal_shmem_base_close(void)
{
    /* if there is a selected shmem module, finalize it */
    if (NULL != opal_shmem_base_module &&
        NULL != opal_shmem_base_module->module_finalize) {
        opal_shmem_base_module->module_finalize();
    }

    opal_shmem_base_selected = false;
    opal_shmem_base_component = NULL;
    opal_shmem_base_module = NULL;

    return mca_base_framework_components_close (&opal_shmem_base_framework,
                                                NULL);
}


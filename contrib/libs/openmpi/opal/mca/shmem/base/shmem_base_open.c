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
 * Copyright (c) 2008-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/mca/shmem/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "opal/mca/shmem/base/static-components.h"

/**
 * globals
 */
char *opal_shmem_base_RUNTIME_QUERY_hint = NULL;

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * Register some shmem-wide MCA params
 */
static int
opal_shmem_base_register (mca_base_register_flag_t flags)
{
    int ret;

    /* register an INTERNAL parameter used to provide a component selection
     * hint to the shmem framework.
     */
    opal_shmem_base_RUNTIME_QUERY_hint = NULL;
    ret = mca_base_framework_var_register (&opal_shmem_base_framework, "RUNTIME_QUERY_hint",
                                           "Internal OMPI parameter used to provide a "
                                           "component selection hint to the shmem "
                                           "framework.  The value of this parameter "
                                           "is the name of the component that is "
                                           "available, selectable, and meets our "
                                           "run-time behavior requirements.",
                                           MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                           MCA_BASE_VAR_FLAG_INTERNAL,
                                           OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                           &opal_shmem_base_RUNTIME_QUERY_hint);

    return (0 > ret) ? ret : OPAL_SUCCESS;
}

/* Use the default open function */
MCA_BASE_FRAMEWORK_DECLARE(opal, shmem, "shared memory", opal_shmem_base_register, NULL,
                           opal_shmem_base_close, mca_shmem_base_static_components, 0);

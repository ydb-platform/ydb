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
 * Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/constants.h"
#include "opal/util/sys_limits.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "opal/mca/mpool/base/static-components.h"

#include "mpool_base_tree.h"
/*
 * Global variables
 */

opal_list_t mca_mpool_base_modules = {{0}};
static char *mca_mpool_base_default_hints;

int mca_mpool_base_default_priority = 50;

OBJ_CLASS_INSTANCE(mca_mpool_base_selected_module_t, opal_list_item_t, NULL, NULL);

static int mca_mpool_base_register (mca_base_register_flag_t flags)
{
    mca_mpool_base_default_hints = NULL;
    (void) mca_base_var_register ("opal", "mpool", "base", "default_hints",
                                  "Hints to use when selecting the default memory pool",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_LOCAL,
                                  &mca_mpool_base_default_hints);

    mca_mpool_base_default_priority = 50;
    (void) mca_base_var_register ("opal", "mpool", "base", "default_priority",
                                  "Priority of the default mpool module",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_LOCAL,
                                  &mca_mpool_base_default_priority);

    return OPAL_SUCCESS;
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int mca_mpool_base_open(mca_base_open_flag_t flags)
{
    /* Open up all available components - and populate the
       opal_mpool_base_framework.framework_components list */
    if (OPAL_SUCCESS !=
        mca_base_framework_components_open(&opal_mpool_base_framework, flags)) {
        return OPAL_ERROR;
    }

    if (mca_mpool_base_default_hints) {
        mca_mpool_base_default_module = mca_mpool_base_module_lookup (mca_mpool_base_default_hints);
    }

     /* Initialize the list so that in mca_mpool_base_close(), we can
        iterate over it (even if it's empty, as in the case of opal_info) */
    OBJ_CONSTRUCT(&mca_mpool_base_modules, opal_list_t);

    /* setup tree for tracking MPI_Alloc_mem */
    mca_mpool_base_tree_init();

    return OPAL_SUCCESS;
}

static int mca_mpool_base_close(void)
{
  opal_list_item_t *item;
  mca_mpool_base_selected_module_t *sm;

  /* Finalize all the mpool components and free their list items */

  while(NULL != (item = opal_list_remove_first(&mca_mpool_base_modules))) {
    sm = (mca_mpool_base_selected_module_t *) item;

    /* Blatently ignore the return code (what would we do to recover,
       anyway?  This component is going away, so errors don't matter
       anymore).  Note that it's legal for the module to have NULL for
       the finalize function. */

    if (NULL != sm->mpool_module->mpool_finalize) {
        sm->mpool_module->mpool_finalize(sm->mpool_module);
    }
    OBJ_RELEASE(sm);
  }

  /* Close all remaining available components (may be one if this is a
     OMPI RTE program, or [possibly] multiple if this is opal_info) */
  (void) mca_base_framework_components_close(&opal_mpool_base_framework, NULL);

  mca_mpool_base_tree_fini();

  return OPAL_SUCCESS;
}

MCA_BASE_FRAMEWORK_DECLARE(opal, mpool, "Memory pools", mca_mpool_base_register, mca_mpool_base_open,
                           mca_mpool_base_close, mca_mpool_base_static_components, 0);

/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 */
#include <src/include/pmix_config.h>

#include <pmix_common.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_list.h"
#include "src/util/argv.h"

#include "src/mca/base/base.h"
#include "src/mca/gds/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "src/mca/gds/base/static-components.h"

/* Instantiate the global vars */
pmix_gds_globals_t pmix_gds_globals = {{{0}}};
int pmix_gds_base_output = -1;

static pmix_status_t pmix_gds_close(void)
{
  pmix_gds_base_active_module_t *active, *prev;

    if (!pmix_gds_globals.initialized) {
        return PMIX_SUCCESS;
    }
    pmix_gds_globals.initialized = false;

    PMIX_LIST_FOREACH_SAFE(active, prev, &pmix_gds_globals.actives, pmix_gds_base_active_module_t) {
      pmix_list_remove_item(&pmix_gds_globals.actives, &active->super);
      if (NULL != active->module->finalize) {
        active->module->finalize();
      }
      PMIX_RELEASE(active);
    }
    PMIX_DESTRUCT(&pmix_gds_globals.actives);

    if (NULL != pmix_gds_globals.all_mods) {
        free(pmix_gds_globals.all_mods);
    }
    return pmix_mca_base_framework_components_close(&pmix_gds_base_framework, NULL);
}

static pmix_status_t pmix_gds_open(pmix_mca_base_open_flag_t flags)
{
    pmix_status_t rc;

    /* initialize globals */
    pmix_gds_globals.initialized = true;
    pmix_gds_globals.all_mods = NULL;
    PMIX_CONSTRUCT(&pmix_gds_globals.actives, pmix_list_t);

    /* Open up all available components */
    rc = pmix_mca_base_framework_components_open(&pmix_gds_base_framework, flags);
    pmix_gds_base_output = pmix_gds_base_framework.framework_output;
    return rc;
}

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, gds, "PMIx Generalized Data Store",
                                NULL, pmix_gds_open, pmix_gds_close,
                                mca_gds_base_static_components, 0);

PMIX_CLASS_INSTANCE(pmix_gds_base_active_module_t,
                    pmix_list_item_t,
                    NULL, NULL);

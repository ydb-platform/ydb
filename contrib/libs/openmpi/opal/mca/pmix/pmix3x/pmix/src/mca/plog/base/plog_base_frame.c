/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include <src/include/pmix_config.h>

#include <pmix_common.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_list.h"
#include "src/threads/threads.h"
#include "src/util/argv.h"
#include "src/mca/base/base.h"
#include "src/mca/plog/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "src/mca/plog/base/static-components.h"

/* Instantiate the global vars */
pmix_plog_globals_t pmix_plog_globals = {{0}};
pmix_plog_API_module_t pmix_plog = {
    .log = pmix_plog_base_log
};

static char *order = NULL;
static int pmix_plog_register(pmix_mca_base_register_flag_t flags)
{
    pmix_mca_base_var_register("pmix", "plog", "base", "order",
                               "Comma-delimited, prioritized list of logging channels",
                               PMIX_MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                               PMIX_INFO_LVL_2,
                               PMIX_MCA_BASE_VAR_SCOPE_READONLY,
                               &order);
    if (NULL != order) {
        pmix_plog_globals.channels = pmix_argv_split(order, ',');
    }
    return PMIX_SUCCESS;
}

static pmix_status_t pmix_plog_close(void)
{
    pmix_plog_base_active_module_t *active;
    int n;

    if (!pmix_plog_globals.initialized) {
        return PMIX_SUCCESS;
    }
    pmix_plog_globals.initialized = false;

    for (n=0; n < pmix_plog_globals.actives.size; n++) {
        if (NULL == (active = (pmix_plog_base_active_module_t*)pmix_pointer_array_get_item(&pmix_plog_globals.actives, n))) {
            continue;
        }
        if (NULL != active->module->finalize) {
          active->module->finalize();
        }
        PMIX_RELEASE(active);
        pmix_pointer_array_set_item(&pmix_plog_globals.actives, n, NULL);
    }
    PMIX_DESTRUCT(&pmix_plog_globals.actives);

    PMIX_DESTRUCT_LOCK(&pmix_plog_globals.lock);

    return pmix_mca_base_framework_components_close(&pmix_plog_base_framework, NULL);
}

static pmix_status_t pmix_plog_open(pmix_mca_base_open_flag_t flags)
{
    /* initialize globals */
    pmix_plog_globals.initialized = true;
    pmix_plog_globals.channels = NULL;
    PMIX_CONSTRUCT(&pmix_plog_globals.actives, pmix_pointer_array_t);
    pmix_pointer_array_init(&pmix_plog_globals.actives, 1, INT_MAX, 1);
    PMIX_CONSTRUCT_LOCK(&pmix_plog_globals.lock);
    pmix_plog_globals.lock.active = false;

    /* Open up all available components */
    return pmix_mca_base_framework_components_open(&pmix_plog_base_framework, flags);
}

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, plog, "PMIx Logging Operations",
                                pmix_plog_register, pmix_plog_open, pmix_plog_close,
                                mca_plog_base_static_components, 0);

static void acon(pmix_plog_base_active_module_t *p)
{
    p->reqd = false;
    p->added = false;
}
PMIX_CLASS_INSTANCE(pmix_plog_base_active_module_t,
                    pmix_list_item_t,
                    acon, NULL);

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/patcher/patcher.h"
#include "opal/mca/patcher/base/base.h"
#include "opal/mca/patcher/base/static-components.h"

/*
 * Local variables
 */
static mca_patcher_base_module_t empty_module;

/*
 * Globals
 */
mca_patcher_base_module_t *opal_patcher = &empty_module;

int opal_patcher_base_select (void)
{
    mca_patcher_base_module_t *best_module;
    mca_patcher_base_component_t *best_component;
    int rc, priority;

    rc = mca_base_select ("patcher", opal_patcher_base_framework.framework_output,
                          &opal_patcher_base_framework.framework_components,
                          (mca_base_module_t **) &best_module, (mca_base_component_t **) &best_component,
                          &priority);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    OBJ_CONSTRUCT(&best_module->patch_list, opal_list_t);
    OBJ_CONSTRUCT(&best_module->patch_list_mutex, opal_mutex_t);

    if (best_module->patch_init) {
        rc = best_module->patch_init ();
        if (OPAL_SUCCESS != rc) {
            return rc;
        }
    }

    opal_patcher = best_module;

    return OPAL_SUCCESS;
}

static int opal_patcher_base_close (void)
{
    if (opal_patcher == &empty_module) {
        return OPAL_SUCCESS;
    }

    mca_patcher_base_patch_t *patch;
    OPAL_LIST_FOREACH_REV(patch, &opal_patcher->patch_list, mca_patcher_base_patch_t) {
        patch->patch_restore (patch);
    }

    OPAL_LIST_DESTRUCT(&opal_patcher->patch_list);
    OBJ_DESTRUCT(&opal_patcher->patch_list_mutex);

    if (opal_patcher->patch_fini) {
        return opal_patcher->patch_fini ();
    }

    return OPAL_SUCCESS;
}

/* Use default register/open functions */
MCA_BASE_FRAMEWORK_DECLARE(opal, patcher, "runtime code patching", NULL, NULL,
                           opal_patcher_base_close, mca_patcher_base_static_components,
                           0);

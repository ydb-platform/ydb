/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <pmix_common.h>
#include "src/include/pmix_globals.h"

#include "src/class/pmix_list.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/mca/ptl/base/base.h"

#include "src/mca/psec/base/base.h"


char* pmix_psec_base_get_available_modules(void)
{
    pmix_psec_base_active_module_t *active;
    char **tmp=NULL, *reply=NULL;

    if (!pmix_psec_globals.initialized) {
        return NULL;
    }

    PMIX_LIST_FOREACH(active, &pmix_psec_globals.actives, pmix_psec_base_active_module_t) {
        pmix_argv_append_nosize(&tmp, active->component->base.pmix_mca_component_name);
    }
    if (NULL != tmp) {
        reply = pmix_argv_join(tmp, ',');
        pmix_argv_free(tmp);
    }
    return reply;
}

pmix_psec_module_t* pmix_psec_base_assign_module(const char *options)
{
    pmix_psec_base_active_module_t *active;
    pmix_psec_module_t *mod;
    char **tmp=NULL;
    int i;

    if (!pmix_psec_globals.initialized) {
        return NULL;
    }

    if (NULL != options) {
        tmp = pmix_argv_split(options, ',');
    }

    PMIX_LIST_FOREACH(active, &pmix_psec_globals.actives, pmix_psec_base_active_module_t) {
        if (NULL == tmp) {
            if (NULL != (mod = active->component->assign_module())) {
                return mod;
            }
        } else {
            for (i=0; NULL != tmp[i]; i++) {
                if (0 == strcmp(tmp[i], active->component->base.pmix_mca_component_name)) {
                    if (NULL != (mod = active->component->assign_module())) {
                        pmix_argv_free(tmp);
                        return mod;
                    }
                }
            }
        }
    }

    /* we only get here if nothing was found */
    if (NULL != tmp) {
        pmix_argv_free(tmp);
    }
    return NULL;
}

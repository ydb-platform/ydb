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
 * Copyright (c) 2015-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>


#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/include/pmix_globals.h"

#include "src/mca/bfrops/base/base.h"

PMIX_EXPORT const char* PMIx_Data_type_string(pmix_data_type_t type)
{
    pmix_bfrops_base_active_module_t *active;
    char *reply;

    if (!pmix_bfrops_globals.initialized) {
        return "NOT INITIALIZED";
    }

    PMIX_LIST_FOREACH(active, &pmix_bfrops_globals.actives, pmix_bfrops_base_active_module_t) {
        if (NULL != active->module->data_type_string) {
            if (NULL != (reply = (char*)active->module->data_type_string(type))) {
                return reply;
            }
        }
    }
    return "UNKNOWN";
}

char* pmix_bfrops_base_get_available_modules(void)
{
    pmix_bfrops_base_active_module_t *active;
    char **tmp=NULL, *reply=NULL;

    if (!pmix_bfrops_globals.initialized) {
        return NULL;
    }

    PMIX_LIST_FOREACH(active, &pmix_bfrops_globals.actives, pmix_bfrops_base_active_module_t) {
        pmix_argv_append_nosize(&tmp, active->component->base.pmix_mca_component_name);
    }
    if (NULL != tmp) {
        reply = pmix_argv_join(tmp, ',');
        pmix_argv_free(tmp);
    }
    return reply;
}

pmix_bfrops_module_t* pmix_bfrops_base_assign_module(const char *version)
{
    pmix_bfrops_base_active_module_t *active;
    pmix_bfrops_module_t *mod;
    char **tmp=NULL;
    int i;

    if (!pmix_bfrops_globals.initialized) {
        return NULL;
    }

    if (NULL != version) {
        tmp = pmix_argv_split(version, ',');
    }

    PMIX_LIST_FOREACH(active, &pmix_bfrops_globals.actives, pmix_bfrops_base_active_module_t) {
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

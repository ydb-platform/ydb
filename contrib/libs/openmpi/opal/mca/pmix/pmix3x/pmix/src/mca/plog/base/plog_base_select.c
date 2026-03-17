/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include <string.h>

#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/util/show_help.h"

#include "src/mca/plog/base/base.h"

static bool selected = false;

/* Function for selecting a prioritized array of components
 * from all those that are available. */
int pmix_plog_base_select(void)
{
    pmix_mca_base_component_list_item_t *cli = NULL;
    pmix_mca_base_component_t *component = NULL;
    pmix_mca_base_module_t *module = NULL;
    pmix_plog_module_t *nmodule;
    pmix_plog_base_active_module_t *newmodule, *mod, *default_mod = NULL;
    int rc, priority, n;
    bool inserted, default_added, reqd;
    pmix_list_t actives;
    char *ptr;
    size_t len;

    if (selected) {
        /* ensure we don't do this twice */
        return PMIX_SUCCESS;
    }
    selected = true;

    PMIX_CONSTRUCT(&actives, pmix_list_t);

    /* Query all available components and ask if they have a module */
    PMIX_LIST_FOREACH(cli, &pmix_plog_base_framework.framework_components, pmix_mca_base_component_list_item_t) {
        component = (pmix_mca_base_component_t *) cli->cli_component;

        pmix_output_verbose(5, pmix_plog_base_framework.framework_output,
                            "mca:plog:select: checking available component %s", component->pmix_mca_component_name);

        /* If there's no query function, skip it */
        if (NULL == component->pmix_mca_query_component) {
            pmix_output_verbose(5, pmix_plog_base_framework.framework_output,
                                "mca:plog:select: Skipping component [%s]. It does not implement a query function",
                                component->pmix_mca_component_name );
            continue;
        }

        /* Query the component */
        pmix_output_verbose(5, pmix_plog_base_framework.framework_output,
                            "mca:plog:select: Querying component [%s]",
                            component->pmix_mca_component_name);
        rc = component->pmix_mca_query_component(&module, &priority);

        /* If no module was returned, then skip component */
        if (PMIX_SUCCESS != rc || NULL == module) {
            pmix_output_verbose(5, pmix_plog_base_framework.framework_output,
                                "mca:plog:select: Skipping component [%s]. Query failed to return a module",
                                component->pmix_mca_component_name );
            continue;
        }

        /* If we got a module, keep it */
        nmodule = (pmix_plog_module_t*) module;
        /* let it initialize */
        if (NULL != nmodule->init && PMIX_SUCCESS != nmodule->init()) {
            continue;
        }
        /* add to the list of selected modules */
        newmodule = PMIX_NEW(pmix_plog_base_active_module_t);
        newmodule->pri = priority;
        newmodule->module = nmodule;
        newmodule->component = (pmix_plog_base_component_t*)cli->cli_component;

        /* maintain priority order */
        inserted = false;
        PMIX_LIST_FOREACH(mod, &actives, pmix_plog_base_active_module_t) {
            if (priority > mod->pri) {
                pmix_list_insert_pos(&actives, (pmix_list_item_t*)mod, &newmodule->super);
                inserted = true;
                break;
            }
        }
        if (!inserted) {
            /* must be lowest priority - add to end */
            pmix_list_append(&actives, &newmodule->super);
        }

        /* if this is the default module, track it */
        if (0 == strcmp(newmodule->module->name, "default")) {
            default_mod = newmodule;
        }
    }

    /* if they gave us a desired ordering, then impose it here */
    if (NULL != pmix_plog_globals.channels) {
        default_added = false;
        for (n=0; NULL != pmix_plog_globals.channels[n]; n++) {
            len = strlen(pmix_plog_globals.channels[n]);
            /* check for the "req" modifier */
            reqd = false;
            ptr = strrchr(pmix_plog_globals.channels[n], ':');
            if (NULL != ptr) {
                /* get the length of the remaining string so we
                 * can constrain our comparison of the channel
                 * name itself */
                len = len - strlen(ptr);
                /* move over the ':' */
                ++ptr;
                /* we accept anything that starts with "req" */
                if (0 == strncasecmp(ptr, "req", 3)) {
                    reqd = true;
                }
            }
            /* now search for this channel in our list of actives */
            inserted = false;
            PMIX_LIST_FOREACH(mod, &actives, pmix_plog_base_active_module_t) {
                if (0 == strncasecmp(pmix_plog_globals.channels[n], mod->module->name, len)) {
                    pmix_list_remove_item(&actives, &mod->super);
                    pmix_pointer_array_add(&pmix_plog_globals.actives, mod);
                    mod->reqd = reqd;
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                /* we didn't find a supporting module - this
                 * still might be okay because it could be something
                 * the RM itself supports, so just insert the default
                 * module here if it hasn't already been inserted */
                if (!default_added) {
                    /* if the default module isn't available and this
                     * channel isn't optional, then there is nothing
                     * we can do except report an error */
                    if (NULL == default_mod && reqd) {
                        pmix_show_help("help-pmix-plog.txt", "reqd-not-found",
                                       true, pmix_plog_globals.channels[n]);
                        PMIX_LIST_DESTRUCT(&actives);
                        return PMIX_ERR_NOT_FOUND;
                    } else if (NULL != default_mod) {
                        pmix_pointer_array_add(&pmix_plog_globals.actives, default_mod);
                        default_added = true;
                        default_mod->reqd = reqd;
                    }
                } else if (reqd) {
                    /* if we already added it, we still have to check the
                     * reqd status - if any citation requires that the
                     * default be used, then we set it, but be sure we
                     * don't overwrite it with a "not required" if it
                     * was already set as "required" */
                    default_mod->reqd = reqd;
                }
            }
        }
        /* if there are any modules left over, we need to discard them */
        PMIX_LIST_DESTRUCT(&actives);
    } else {
        /* insert the modules into the global array in priority order */
        while (NULL != (mod = (pmix_plog_base_active_module_t*)pmix_list_remove_first(&actives))) {
            pmix_pointer_array_add(&pmix_plog_globals.actives, mod);
        }
        PMIX_DESTRUCT(&actives);
    }

    if (4 < pmix_output_get_verbosity(pmix_plog_base_framework.framework_output)) {
        pmix_output(0, "Final plog order");
        /* show the prioritized order */
        for (n=0; n < pmix_plog_globals.actives.size; n++) {
            if (NULL != (mod = (pmix_plog_base_active_module_t*)pmix_pointer_array_get_item(&pmix_plog_globals.actives, n))) {
                pmix_output(0, "\tplog[%d]: %s", n, mod->component->base.pmix_mca_component_name);
            }
        }
    }


    return PMIX_SUCCESS;;
}

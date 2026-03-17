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
#include <string.h>

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/rcache/rcache.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/rcache/base/rcache_base_mem_cb.h"
#include "rcache_base_vma_tree.h"
#include "opal/util/show_help.h"
#include "opal/util/proc.h"

#include "opal/runtime/opal_params.h"
#include "opal/memoryhooks/memory.h"

mca_rcache_base_module_t* mca_rcache_base_module_create (const char* name, void *user_data,
                                                         struct mca_rcache_base_resources_t* resources)
{
    mca_rcache_base_component_t* component = NULL;
    mca_rcache_base_module_t* module = NULL;
    mca_base_component_list_item_t *cli;
    mca_rcache_base_selected_module_t *sm;

    /* on the very first creation of a module we init the memory
       callback */
    if (!mca_rcache_base_used_mem_hooks) {
        /* Use the memory hooks if leave_pinned or
         * leave_pinned_pipeline is enabled (note that either of these
         * leave_pinned variables may have been set by a user MCA
         * param or elsewhere in the code base).  Yes, we could havexc
         * coded this more succinctly, but this is more clear. Do not
         * check memory hooks if the rcache does not provide an
         * range invalidation function.. */
        if (opal_leave_pinned != 0 || opal_leave_pinned_pipeline) {
            /* open the memory manager components.  Memory hooks may be
               triggered before this (any time after mem_free_init(),
               actually).  This is a hook available for memory manager hooks
               without good initialization routine support */
            (void) mca_base_framework_open (&opal_memory_base_framework, 0);

            if ((OPAL_MEMORY_FREE_SUPPORT | OPAL_MEMORY_MUNMAP_SUPPORT) ==
                ((OPAL_MEMORY_FREE_SUPPORT | OPAL_MEMORY_MUNMAP_SUPPORT) &
                 opal_mem_hooks_support_level())) {
                if (-1 == opal_leave_pinned) {
                    opal_leave_pinned = !opal_leave_pinned_pipeline;
                }
                opal_mem_hooks_register_release(mca_rcache_base_mem_cb, NULL);
            } else if (1 == opal_leave_pinned || opal_leave_pinned_pipeline) {
                opal_show_help("help-rcache-base.txt", "leave pinned failed",
                               true, name, OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                               opal_proc_local_get()->proc_hostname);
                return NULL;
            }

            /* Set this to true so that rcache_base_close knows to
               cleanup */
            mca_rcache_base_used_mem_hooks = 1;
        }
    }

    OPAL_LIST_FOREACH(cli, &opal_rcache_base_framework.framework_components, mca_base_component_list_item_t) {
         component = (mca_rcache_base_component_t *) cli->cli_component;
         if(0 == strcmp(component->rcache_version.mca_component_name, name)) {
             module = component->rcache_init (resources);
             break;
         }
    }

    if ( NULL == module ) {
        return NULL;
    }

    sm = OBJ_NEW(mca_rcache_base_selected_module_t);
    sm->rcache_component = component;
    sm->rcache_module = module;
    sm->user_data = user_data;
    opal_list_append(&mca_rcache_base_modules, (opal_list_item_t*) sm);

    return module;
}

int mca_rcache_base_module_destroy(mca_rcache_base_module_t *module)
{
    mca_rcache_base_selected_module_t *sm, *next;

    OPAL_LIST_FOREACH_SAFE(sm, next, &mca_rcache_base_modules, mca_rcache_base_selected_module_t) {
        if (module == sm->rcache_module) {
            opal_list_remove_item(&mca_rcache_base_modules, (opal_list_item_t*)sm);
            if (NULL != sm->rcache_module->rcache_finalize) {
                sm->rcache_module->rcache_finalize(sm->rcache_module);
            }
            OBJ_RELEASE(sm);
            return OPAL_SUCCESS;
        }
    }

    return OPAL_ERR_NOT_FOUND;
}

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
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_DISABLE_ENABLE_MEM_DEBUG 1
#include "opal_config.h"
#include "opal/mca/base/base.h"
#include "rcache_rgpusm.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

/*
 * Local functions
 */
static int rgpusm_open(void);
static int rgpusm_close(void);
static int rgpusm_register(void);
static mca_rcache_base_module_t* rgpusm_init(struct mca_rcache_base_resources_t* resources);

static int opal_rcache_rgpusm_verbose = 0;

mca_rcache_rgpusm_component_t mca_rcache_rgpusm_component = {
    {
        /* First, the mca_base_component_t struct containing meta
           information about the component itself */

        .rcache_version = {
            MCA_RCACHE_BASE_VERSION_3_0_0,

            .mca_component_name = "rgpusm",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),
            .mca_open_component = rgpusm_open,
            .mca_close_component = rgpusm_close,
            .mca_register_component_params = rgpusm_register,
        },
        .rcache_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .rcache_init = rgpusm_init
    }
};

/**
  * component open/close/init function
  */
static int rgpusm_open(void)
{
    mca_rcache_rgpusm_component.output = opal_output_open(NULL);
    opal_output_set_verbosity(mca_rcache_rgpusm_component.output, opal_rcache_rgpusm_verbose);

    return OPAL_SUCCESS;
}


static int rgpusm_register(void)
{
    mca_rcache_rgpusm_component.rcache_name = "vma";
    (void) mca_base_component_var_register(&mca_rcache_rgpusm_component.super.rcache_version,
                                           "rcache_name",
                                           "The name of the registration cache the rcache should use",
                                           MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rcache_rgpusm_component.rcache_name);
    mca_rcache_rgpusm_component.rcache_size_limit = 0;
    (void) mca_base_component_var_register(&mca_rcache_rgpusm_component.super.rcache_version,
                                           "rcache_size_limit",
                                           "the maximum size of registration cache in bytes. "
                                           "0 is unlimited (default 0)",
                                           MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rcache_rgpusm_component.rcache_size_limit);

    mca_rcache_rgpusm_component.leave_pinned = 1;
    (void) mca_base_component_var_register(&mca_rcache_rgpusm_component.super.rcache_version,
                                           "leave_pinned",
                                           "Whether to keep memory handles around or release them when done. ",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rcache_rgpusm_component.leave_pinned);

    mca_rcache_rgpusm_component.print_stats = false;
    (void) mca_base_component_var_register(&mca_rcache_rgpusm_component.super.rcache_version,
                                           "print_stats",
                                           "print pool usage statistics at the end of the run",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rcache_rgpusm_component.print_stats);

    /* Set different levels of verbosity in the rgpusm related code. */
    opal_rcache_rgpusm_verbose = 0;
    (void) mca_base_component_var_register(&mca_rcache_rgpusm_component.super.rcache_version,
                                           "verbose", "Set level of rcache rgpusm verbosity",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &opal_rcache_rgpusm_verbose);

    /* Force emptying of entire registration cache when it gets full */
    mca_rcache_rgpusm_component.empty_cache = false;
    (void) mca_base_component_var_register(&mca_rcache_rgpusm_component.super.rcache_version,
                                           "empty_cache", "When set, empty entire registration cache when it is full",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_rcache_rgpusm_component.empty_cache);

    return OPAL_SUCCESS;
}


static int rgpusm_close(void)
{
    return OPAL_SUCCESS;
}


static mca_rcache_base_module_t* rgpusm_init(
     struct mca_rcache_base_resources_t *resources)
{
    mca_rcache_rgpusm_module_t* rcache_module;

    /* ignore passed in resource structure */
    (void) resources;

    rcache_module = (mca_rcache_rgpusm_module_t *) calloc (1, sizeof (*rcache_module));
    if (NULL == rcache_module) {
        return NULL;
    }

    mca_rcache_rgpusm_module_init(rcache_module);

    return &rcache_module->super;
}

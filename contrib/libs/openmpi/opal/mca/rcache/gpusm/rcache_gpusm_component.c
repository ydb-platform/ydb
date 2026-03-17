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
 * Copyright (c) 2012      NVIDIA Corporation.  All rights reserved.
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
#include "rcache_gpusm.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

/*
 * Local functions
 */
static int gpusm_open(void);
static int gpusm_close(void);
static int gpusm_register(void);
static mca_rcache_base_module_t* gpusm_init(struct mca_rcache_base_resources_t* resources);

mca_rcache_gpusm_component_t mca_rcache_gpusm_component = {
    {
      /* First, the mca_base_component_t struct containing meta
         information about the component itself */

        .rcache_version = {
            MCA_RCACHE_BASE_VERSION_3_0_0,

            .mca_component_name = "gpusm",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),
            .mca_open_component = gpusm_open,
            .mca_close_component = gpusm_close,
            .mca_register_component_params = gpusm_register,
        },
        .rcache_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .rcache_init = gpusm_init,
    }
};

/**
  * Component open/close/init/register functions.  Most do not do anything,
  * but keep around for placeholders.
  */
static int gpusm_open(void)
{
    return OPAL_SUCCESS;
}


static int gpusm_register(void)
{
	return OPAL_SUCCESS;
}


static int gpusm_close(void)
{
    return OPAL_SUCCESS;
}


static mca_rcache_base_module_t* gpusm_init(struct mca_rcache_base_resources_t *resources)
{
    mca_rcache_gpusm_module_t* rcache_module;

    (void) resources;

    rcache_module = (mca_rcache_gpusm_module_t *) calloc (1, sizeof (*rcache_module));
    if (NULL == rcache_module) {
        return NULL;
    }

    mca_rcache_gpusm_module_init(rcache_module);

    return &rcache_module->super;
}

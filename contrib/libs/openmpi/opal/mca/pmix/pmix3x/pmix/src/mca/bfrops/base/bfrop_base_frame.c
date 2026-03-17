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
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
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

#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/class/pmix_list.h"
#include "src/mca/bfrops/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "src/mca/bfrops/base/static-components.h"

/* Instantiate the global vars */
pmix_bfrops_globals_t pmix_bfrops_globals = {{{0}}};
int pmix_bfrops_base_output = 0;

static int pmix_bfrop_register(pmix_mca_base_register_flag_t flags)
{
    pmix_bfrops_globals.initial_size = PMIX_BFROP_DEFAULT_INITIAL_SIZE;
    pmix_mca_base_var_register("pmix", "bfrops", "base", "initial_size",
                               "Initial size of a buffer",
                               PMIX_MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                               PMIX_INFO_LVL_2,
                               PMIX_MCA_BASE_VAR_SCOPE_READONLY,
                               &pmix_bfrops_globals.initial_size);

    pmix_bfrops_globals.threshold_size = PMIX_BFROP_DEFAULT_THRESHOLD_SIZE;
    pmix_mca_base_var_register("pmix", "bfrops", "base", "threshold_size",
                               "Size at which we switch from extending a buffer by doubling to extending by a smaller value",
                               PMIX_MCA_BASE_VAR_TYPE_SIZE_T, NULL, 0, 0,
                               PMIX_INFO_LVL_2,
                               PMIX_MCA_BASE_VAR_SCOPE_READONLY,
                               &pmix_bfrops_globals.threshold_size);

#if PMIX_ENABLE_DEBUG
    pmix_bfrops_globals.default_type = PMIX_BFROP_BUFFER_FULLY_DESC;
#else
    pmix_bfrops_globals.default_type = PMIX_BFROP_BUFFER_NON_DESC;
#endif
    pmix_mca_base_var_register("pmix", "bfrops", "base", "default_type",
                               "Default type for buffers",
                               PMIX_MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                               PMIX_INFO_LVL_2,
                               PMIX_MCA_BASE_VAR_SCOPE_READONLY,
                               &pmix_bfrops_globals.default_type);
    return PMIX_SUCCESS;
}

static pmix_status_t pmix_bfrop_close(void)
{
    if (!pmix_bfrops_globals.initialized) {
        return PMIX_SUCCESS;
    }
    pmix_bfrops_globals.initialized = false;

    /* the components will cleanup when closed */
    PMIX_LIST_DESTRUCT(&pmix_bfrops_globals.actives);

    return pmix_mca_base_framework_components_close(&pmix_bfrops_base_framework, NULL);
}

static pmix_status_t pmix_bfrop_open(pmix_mca_base_open_flag_t flags)
{
    pmix_status_t rc;

    /* initialize globals */
    pmix_bfrops_globals.initialized = true;
    PMIX_CONSTRUCT(&pmix_bfrops_globals.actives, pmix_list_t);

    /* Open up all available components */
    rc = pmix_mca_base_framework_components_open(&pmix_bfrops_base_framework, flags);
    pmix_bfrops_base_output = pmix_bfrops_base_framework.framework_output;
    return rc;
}

PMIX_MCA_BASE_FRAMEWORK_DECLARE(pmix, bfrops, "PMIx Buffer Operations",
                                pmix_bfrop_register, pmix_bfrop_open, pmix_bfrop_close,
                                mca_bfrops_base_static_components, 0);

static void moddes(pmix_bfrops_base_active_module_t *p)
{
    if (NULL != p->module->finalize) {
        p->module->finalize();
    }
}
PMIX_CLASS_INSTANCE(pmix_bfrops_base_active_module_t,
                    pmix_list_item_t,
                    NULL, moddes);

/**
 * Object constructors, destructors, and instantiations
 */
/** Value **/
static void pmix_buffer_construct (pmix_buffer_t* buffer)
{
    /** set the default buffer type */
    buffer->type = PMIX_BFROP_BUFFER_UNDEF;

    /* Make everything NULL to begin with */
    buffer->base_ptr = buffer->pack_ptr = buffer->unpack_ptr = NULL;
    buffer->bytes_allocated = buffer->bytes_used = 0;
}

static void pmix_buffer_destruct (pmix_buffer_t* buffer)
{
    if (NULL != buffer->base_ptr) {
        free (buffer->base_ptr);
    }
}

PMIX_CLASS_INSTANCE(pmix_buffer_t,
                   pmix_object_t,
                   pmix_buffer_construct,
                   pmix_buffer_destruct);


static void pmix_bfrop_type_info_construct(pmix_bfrop_type_info_t *obj)
{
    obj->odti_name = NULL;
    obj->odti_pack_fn = NULL;
    obj->odti_unpack_fn = NULL;
    obj->odti_copy_fn = NULL;
    obj->odti_print_fn = NULL;
}

static void pmix_bfrop_type_info_destruct(pmix_bfrop_type_info_t *obj)
{
    if (NULL != obj->odti_name) {
        free(obj->odti_name);
    }
}

PMIX_CLASS_INSTANCE(pmix_bfrop_type_info_t, pmix_object_t,
                   pmix_bfrop_type_info_construct,
                   pmix_bfrop_type_info_destruct);

static void kvcon(pmix_kval_t *k)
{
    k->key = NULL;
    k->value = NULL;
}
static void kvdes(pmix_kval_t *k)
{
    if (NULL != k->key) {
        free(k->key);
    }
    if (NULL != k->value) {
        PMIX_VALUE_RELEASE(k->value);
    }
}
PMIX_CLASS_INSTANCE(pmix_kval_t,
                   pmix_list_item_t,
                   kvcon, kvdes);

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"
#include <stdio.h>

#include "opal/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"

mca_base_var_enum_flag_t *mca_btl_base_flag_enum = NULL;
mca_base_var_enum_flag_t *mca_btl_base_atomic_enum = NULL;

mca_base_var_enum_value_flag_t mca_btl_base_flag_enum_flags[] = {
    {MCA_BTL_FLAGS_SEND, "send", 0},
    {MCA_BTL_FLAGS_PUT, "put", 0},
    {MCA_BTL_FLAGS_GET, "get", 0},
    {MCA_BTL_FLAGS_SEND_INPLACE, "inplace", 0},
    {MCA_BTL_FLAGS_SIGNALED, "signaled", 0},
    {MCA_BTL_FLAGS_ATOMIC_OPS, "atomics", 0},
    {MCA_BTL_FLAGS_ATOMIC_FOPS, "fetching-atomics", 0},
    {MCA_BTL_FLAGS_SINGLE_ADD_PROCS, "static", 0},
    {MCA_BTL_FLAGS_CUDA_PUT, "cuda-put", 0},
    {MCA_BTL_FLAGS_CUDA_GET, "cuda-get", 0},
    {MCA_BTL_FLAGS_CUDA_COPY_ASYNC_SEND, "cuda-async-send", 0},
    {MCA_BTL_FLAGS_CUDA_COPY_ASYNC_RECV, "cuda-async-recv", 0},
    {MCA_BTL_FLAGS_FAILOVER_SUPPORT, "failover", 0},
    {MCA_BTL_FLAGS_NEED_ACK, "need-ack", 0},
    {MCA_BTL_FLAGS_NEED_CSUM, "need-csum", 0},
    {MCA_BTL_FLAGS_HETEROGENEOUS_RDMA, "hetero-rdma", 0},
    {MCA_BTL_FLAGS_RDMA_FLUSH, "rdma-flush", 0},
    {0, NULL, 0}
};

mca_base_var_enum_value_flag_t mca_btl_base_atomic_enum_flags[] = {
  {MCA_BTL_ATOMIC_SUPPORTS_ADD, "add", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_AND, "and", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_OR, "or", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_XOR, "xor", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_LAND, "land", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_LOR, "lor", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_LXOR, "lxor", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_SWAP, "swap", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_MIN, "min", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_MAX, "max", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_CSWAP, "compare-and-swap", 0},
  {MCA_BTL_ATOMIC_SUPPORTS_GLOB, "global"},
  {0, NULL, 0}
};

mca_btl_active_message_callback_t mca_btl_base_active_message_trigger[MCA_BTL_TAG_MAX] = {{0}};

/*
 *  mca_btl_base_descriptor_t
 */

static void mca_btl_base_descriptor_constructor(mca_btl_base_descriptor_t* des)
{
    des->des_segments = NULL;
    des->des_segment_count = 0;
    des->des_cbfunc = NULL;
    des->des_cbdata = NULL;
    des->des_flags = 0;
}

static void mca_btl_base_descriptor_destructor(mca_btl_base_descriptor_t* des)
{
}

OBJ_CLASS_INSTANCE(
    mca_btl_base_descriptor_t,
    opal_list_item_t,
    mca_btl_base_descriptor_constructor,
    mca_btl_base_descriptor_destructor);


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "opal/mca/btl/base/static-components.h"
#include "btl_base_error.h"

/*
 * Global variables
 */
char* mca_btl_base_include = NULL;
char* mca_btl_base_exclude = NULL;
int mca_btl_base_warn_component_unused = 1;
opal_list_t mca_btl_base_modules_initialized = {{0}};
bool mca_btl_base_thread_multiple_override = false;

static int mca_btl_base_register(mca_base_register_flag_t flags)
{
    /* Override the per-BTL "don't run if THREAD_MULTIPLE selected"
       embargo? */
    mca_btl_base_thread_multiple_override = false;
    (void) mca_base_var_register("opal", "btl", "base", "thread_multiple_override",
                                 "Enable BTLs that are not normally enabled when MPI_THREAD_MULTIPLE is enabled (THIS IS FOR DEVELOPERS ONLY!  SHOULD NOT BE USED BY END USERS!)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0,
                                 MCA_BASE_VAR_FLAG_INTERNAL,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_btl_base_thread_multiple_override);

    (void) mca_base_var_register("opal", "btl", "base", "include", NULL,
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_btl_base_include);
    (void) mca_base_var_register("opal", "btl", "base", "exclude", NULL,
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_btl_base_exclude);
    (void) mca_base_var_register("opal", "btl", "base", "warn_component_unused",
                                 "This parameter is used to turn on warning messages when certain NICs are not used",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_btl_base_warn_component_unused);

    (void) mca_base_var_enum_create_flag ("btl_flags", mca_btl_base_flag_enum_flags, &mca_btl_base_flag_enum);
    (void) mca_base_var_enum_register("opal", "btl", "base", "btl_flags",
                                 &mca_btl_base_flag_enum);
    (void) mca_base_var_enum_create_flag ("btl_atomic_flags", mca_btl_base_atomic_enum_flags, &mca_btl_base_atomic_enum);
    (void) mca_base_var_enum_register("opal", "btl", "base", "btl_atomic_flags",
                                 &mca_btl_base_atomic_enum);

    return OPAL_SUCCESS;
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int mca_btl_base_open(mca_base_open_flag_t flags)
{
    int ret;

    /* Open up all available components */

    if (OPAL_SUCCESS !=
        (ret = mca_base_framework_components_open(&opal_btl_base_framework, flags))) {
        return ret;
    }

  /* Initialize the list so that in mca_btl_base_close(), we can
     iterate over it (even if it's empty, as in the case of
     opal_info) */

  OBJ_CONSTRUCT(&mca_btl_base_modules_initialized, opal_list_t);

  /* get the verbosity so that BTL_VERBOSE will work */
  mca_btl_base_verbose = opal_output_get_verbosity(opal_btl_base_framework.framework_output);

  /* All done */
  return OPAL_SUCCESS;
}

static int mca_btl_base_close(void)
{
    mca_btl_base_selected_module_t *sm, *next;

#if 0
    /* disable event processing while cleaning up btls */
    opal_event_disable();
#endif
    /* Finalize all the btl components and free their list items */

    OPAL_LIST_FOREACH_SAFE(sm, next, &mca_btl_base_modules_initialized, mca_btl_base_selected_module_t) {
        /* Blatently ignore the return code (what would we do to recover,
           anyway?  This component is going away, so errors don't matter
           anymore) */

        sm->btl_module->btl_finalize(sm->btl_module);
        opal_list_remove_item(&mca_btl_base_modules_initialized, &sm->super);
        free(sm);
    }

    (void) mca_base_framework_components_close(&opal_btl_base_framework, NULL);

    OBJ_DESTRUCT(&mca_btl_base_modules_initialized);

#if 0
    /* restore event processing */
    opal_event_enable();
#endif
    /* All done */
    return OPAL_SUCCESS;
}

MCA_BASE_FRAMEWORK_DECLARE(opal, btl, "Byte Transport Layer", mca_btl_base_register,
                           mca_btl_base_open, mca_btl_base_close, mca_btl_base_static_components,
                           0);

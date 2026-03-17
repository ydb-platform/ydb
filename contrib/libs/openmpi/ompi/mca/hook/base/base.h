/*
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_HOOK_BASE_H
#define OMPI_HOOK_BASE_H

#include "ompi_config.h"

#include "ompi/mca/mca.h"
#include "opal/mca/base/mca_base_framework.h"

#include "ompi/mca/hook/hook.h"

BEGIN_C_DECLS

/**
 * Framework struct declaration for this framework
 */
OMPI_DECLSPEC extern mca_base_framework_t ompi_hook_base_framework;


/**
 * Dynamically register function pointers to be called from outside of the hook
 * framework. For example, a collective component could register a callback
 * at the bottom of init to perform some action.
 */
OMPI_DECLSPEC int ompi_hook_base_register_callbacks(ompi_hook_base_component_t *comp);
OMPI_DECLSPEC int ompi_hook_base_deregister_callbacks(ompi_hook_base_component_t *comp);

/**
 * Wrapper functions matching the interface functions
 */
OMPI_DECLSPEC void ompi_hook_base_mpi_initialized_top(int *flag);
OMPI_DECLSPEC void ompi_hook_base_mpi_initialized_bottom(int *flag);

OMPI_DECLSPEC void ompi_hook_base_mpi_init_thread_top(int *argc, char ***argv, int required, int *provided);
OMPI_DECLSPEC void ompi_hook_base_mpi_init_thread_bottom(int *argc, char ***argv, int required, int *provided);

OMPI_DECLSPEC void ompi_hook_base_mpi_finalized_top(int *flag);
OMPI_DECLSPEC void ompi_hook_base_mpi_finalized_bottom(int *flag);

OMPI_DECLSPEC void ompi_hook_base_mpi_init_top(int argc, char **argv, int requested, int *provided);
OMPI_DECLSPEC void ompi_hook_base_mpi_init_top_post_opal(int argc, char **argv, int requested, int *provided);
OMPI_DECLSPEC void ompi_hook_base_mpi_init_bottom(int argc, char **argv, int requested, int *provided);
OMPI_DECLSPEC void ompi_hook_base_mpi_init_error(int argc, char **argv, int requested, int *provided);

OMPI_DECLSPEC void ompi_hook_base_mpi_finalize_top(void);
OMPI_DECLSPEC void ompi_hook_base_mpi_finalize_bottom(void);

END_C_DECLS

#endif /* OMPI_BASE_HOOK_H */

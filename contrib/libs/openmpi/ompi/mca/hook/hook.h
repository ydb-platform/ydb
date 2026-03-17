/*
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * The hook framework is designed to allow a component of the hook framework
 * to be called a designated points in the MPI process lifecycle.
 *
 * There is not module structure in this framework since the components apply to
 * the whole process. Further, the component might need to active before mca_init.
 *
 * Why not a PMPI library?
 *  - The desire is to allow a component to be built statically into the library
 *    for licensing and disgnostic purposes. As such we need the ability for
 *    the component to be statically linked into the libmpi library.
 *
 * Why is there not a hook for location XYZ?
 *  - The number of possible places for hooks is quite large. This framework
 *    supports a set that we think will provide the best coverage for the
 *    common use cases. If you have another use case we are always open to
 *    discussing extensions on the mailing list.
 */
#ifndef MCA_HOOK_H
#define MCA_HOOK_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"

BEGIN_C_DECLS

/*
 * Some functions are specially marked. See decoder below.
 *
 * *** Static Only (Always) ***
 *  Only static components will ever see this callback triggered due to the
 *  position of the hook relative to ompi_mpi_init and ompi_mpi_finalize.
 *
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *  Only static components will ever see this callback triggered when outside
 *  of the ompi_mpi_init and ompi_mpi_finalize region. Others may see this
 *  triggered if the user's program makes additional calls while inside
 *  that region.
 *
 * *** Everyone (Inside MPI) ***
 *  Everyone registered will see this callback triggered. It is only triggered
 *  between ompi_mpi_init and ompi_mpi_finalize.
 * 
 */
/* ******************************************************************** */

/**
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *
 * Location: mpi/c/initialized.c
 *           At the top of function (outside of mutex)
 */
typedef void (*ompi_hook_base_component_mpi_initialized_top_fn_t)(int *flag);

/**
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *
 * Location: mpi/c/initialized.c
 *           At the bottom of function (outside of mutex)
 */
typedef void (*ompi_hook_base_component_mpi_initialized_bottom_fn_t)(int *flag);

/**
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *
 * Location: mpi/c/init_thread.c
 *           At the top of function
 */
typedef void (*ompi_hook_base_component_mpi_init_thread_top_fn_t)(int *argc, char ***argv, int required, int *provided);

/**
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *
 * Location: mpi/c/init_thread.c
 *           At the bottom of function
 */
typedef void (*ompi_hook_base_component_mpi_init_thread_bottom_fn_t)(int *argc, char ***argv, int required, int *provided);

/**
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *
 * Location: mpi/c/finalized.c
 *           At the top of function (outside of mutex)
 */
typedef void (*ompi_hook_base_component_mpi_finalized_top_fn_t)(int *flag);

/**
 * *** Static Only (Outside MPI), Everyone (Inside MPI) ***
 *
 * Location: mpi/c/finalized.c
 *           At the bottom of function (outside of mutex)
 */
typedef void (*ompi_hook_base_component_mpi_finalized_bottom_fn_t)(int *flag);

/**
 * *** Static Only (Always) ***
 *
 * Location: runtime/ompi_mpi_init.c
 *           At top of function (outside of mutex)
 */
typedef void (*ompi_hook_base_component_mpi_init_top_fn_t)(int argc, char **argv, int requested, int *provided);

/**
 * *** Everyone (Inside MPI) ***
 *
 * Location: runtime/ompi_mpi_init.c
 *           Just after opal_init_util and MCA initialization. (inside mutex)
 * Notes:
 *           This framework has been opened as have its components.
 */
typedef void (*ompi_hook_base_component_mpi_init_top_post_opal_fn_t)(int argc, char **argv, int requested, int *provided);

/**
 * *** Everyone (Inside MPI) ***
 *
 * Location: runtime/ompi_mpi_init.c
 *           At the bottom of the function. (outside mutex)
 * Notes:
 *           This framework has been opened as have its components.
 *           Can safely use all MPI functionality.
 */
typedef void (*ompi_hook_base_component_mpi_init_bottom_fn_t)(int argc, char **argv, int requested, int *provided);

/**
 * *** Everyone (Inside MPI) ***
 *
 * Location: runtime/ompi_mpi_init.c
 *           At the bottom of the error path. (outside mutex)
 * Notes:
 *           This framework has been opened as have its components.
 */
typedef void (*ompi_hook_base_component_mpi_init_error_fn_t)(int argc, char **argv, int requested, int *provided);

/**
 * *** Everyone (Inside MPI) ***
 *
 * Location: runtime/ompi_mpi_finalize.c
 *           At the top of the function. (outside mutex)
 * Notes:
 *           This framework has been opened as have its components.
 *           Can safely use all MPI functionality.
 */
typedef void (*ompi_hook_base_component_mpi_finalize_top_fn_t)(void);

/**
 * *** Static Only (Always) ***
 *
 * Location: runtime/ompi_mpi_finalize.c
 *           At top of function (outside of mutex)
 * Notes:
 *           This framework has been closed.
 */
typedef void (*ompi_hook_base_component_mpi_finalize_bottom_fn_t)(void);


/* ******************************************************************** */

/**
 * Hook component version and interface functions.
 */
struct ompi_hook_base_component_1_0_0_t {
    mca_base_component_t hookm_version;
    mca_base_component_data_t hookm_data;

    /* MPI_Initialized */
    ompi_hook_base_component_mpi_initialized_top_fn_t hookm_mpi_initialized_top;
    ompi_hook_base_component_mpi_initialized_bottom_fn_t hookm_mpi_initialized_bottom;

    /* MPI_Init_thread */
    ompi_hook_base_component_mpi_init_thread_top_fn_t hookm_mpi_init_thread_top;
    ompi_hook_base_component_mpi_init_thread_bottom_fn_t hookm_mpi_init_thread_bottom;

    /* MPI_Finalized */
    ompi_hook_base_component_mpi_finalized_top_fn_t hookm_mpi_finalized_top;
    ompi_hook_base_component_mpi_finalized_bottom_fn_t hookm_mpi_finalized_bottom;

    /* ompi_mpi_init */
    ompi_hook_base_component_mpi_init_top_fn_t hookm_mpi_init_top;
    ompi_hook_base_component_mpi_init_top_post_opal_fn_t hookm_mpi_init_top_post_opal;
    ompi_hook_base_component_mpi_init_bottom_fn_t hookm_mpi_init_bottom;
    ompi_hook_base_component_mpi_init_error_fn_t hookm_mpi_init_error;

    /* ompi_mpi_finalize */
    ompi_hook_base_component_mpi_finalize_top_fn_t hookm_mpi_finalize_top;
    ompi_hook_base_component_mpi_finalize_bottom_fn_t hookm_mpi_finalize_bottom;
};
typedef struct ompi_hook_base_component_1_0_0_t ompi_hook_base_component_1_0_0_t;
typedef ompi_hook_base_component_1_0_0_t ompi_hook_base_component_t;
/* 
 * Note: We do -not- expose a component object for this framework.
 * All interation with the component should go through the base/base.h interfaces.
 * See that header for more information on calling functions.
 */

/* ******************************************************************** */

/*
 * Macro for use in components that are of type hook
 */
#define OMPI_HOOK_BASE_VERSION_1_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("hook", 1, 0, 0)

END_C_DECLS

#endif /* MCA_HOOK_H */

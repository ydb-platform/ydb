/*
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <sys/types.h>
#include <unistd.h>

#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/runtime/opal.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/class/opal_list.h"
#include "opal/include/opal/prefetch.h"

#include "ompi/constants.h"
#include "ompi/mca/hook/hook.h"
#include "ompi/mca/hook/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "ompi/mca/hook/base/static-components.h"


// Is the framework open - or has it been closed and we need to reopen it.
static bool ompi_hook_is_framework_open = false;

static opal_list_t *additional_callback_components = NULL;


static int ompi_hook_base_register( mca_base_register_flag_t flags )
{
    return OMPI_SUCCESS;
}

static int ompi_hook_base_open( mca_base_open_flag_t flags )
{
    int ret;
    const mca_base_component_t **static_components = ompi_hook_base_framework.framework_static_components;
    mca_base_component_list_item_t *cli = NULL;
    mca_base_component_t *component = NULL;
    bool found = false;

    additional_callback_components = OBJ_NEW(opal_list_t);

    /* Open up all available components */
    ret = mca_base_framework_components_open( &ompi_hook_base_framework, flags );
    if (ret != OMPI_SUCCESS) {
        return ret;
    }

    /*
     * Make sure that the `MCA_BASE_COMPONENT_FLAG_REQUIRED` components defined
     * as static are loaded. If we find one that was avoided then error out.
     */
    if( NULL != static_components ) {
        for (int i = 0 ; NULL != static_components[i]; ++i) {
            if( static_components[i]->mca_component_flags & MCA_BASE_COMPONENT_FLAG_REQUIRED ) {
                // Make sure that this component is in the list of components that
                // were included in the earlier framework_components_open() call.
                found = false;
                OPAL_LIST_FOREACH(cli, &ompi_hook_base_framework.framework_components, mca_base_component_list_item_t) {
                    component = (mca_base_component_t*)cli->cli_component;
                    if( component == static_components[i] ) {
                        found = true;
                        break;
                    }
                }
                if( !found ) {
                    opal_show_help("help-mca-hook-base.txt", "hook:missing-required-component", true,
                                   ompi_hook_base_framework.framework_name,
                                   static_components[i]->mca_component_name);
                    return OPAL_ERR_NOT_SUPPORTED;
                }
            }
        }
    }

    /* Assume that if the component is present then it wants to be used.
     * It has the option to have NULL as the function pointer for any
     * functions call hook locations they do not want to hear about
     */

    /*
     * There are three classes of components - we want them processed in this order
     * 1) static components
     * 2) dynamic components
     * 3) internal 'component' hooks (from other places in the code)
     *
     * The ordering of (1) and (2) is managed by mca_base_component_find().
     * We keep a separate list for the 'internal' hooks.
     */

    ompi_hook_is_framework_open = true;

    /* All done */
    return OMPI_SUCCESS;
}

static int ompi_hook_base_close( void )
{
    int ret;

    /*
     * Close our components
     */
    ret = mca_base_framework_components_close( &ompi_hook_base_framework, NULL );
    if( OMPI_SUCCESS != ret ) {
        return ret;
    }
    OBJ_RELEASE(additional_callback_components);
    ompi_hook_is_framework_open = false;

    return OMPI_SUCCESS;
}


int ompi_hook_base_register_callbacks(ompi_hook_base_component_t *comp)
{
    mca_base_component_list_item_t *cli;

    // Check if it is already there
    OPAL_LIST_FOREACH(cli, additional_callback_components, mca_base_component_list_item_t) {
        if( cli->cli_component == (mca_base_component_t*)comp ) {
            return OMPI_SUCCESS;
        }
    }

    // Not found, so add it to the list
    cli = OBJ_NEW(mca_base_component_list_item_t);
    cli->cli_component = (mca_base_component_t*)comp;
    opal_list_append(additional_callback_components, (opal_list_item_t*) cli);

    return OMPI_SUCCESS;
}

int ompi_hook_base_deregister_callbacks(ompi_hook_base_component_t *comp)
{
    mca_base_component_list_item_t *cli;

    // Check if it is already there
    OPAL_LIST_FOREACH(cli, additional_callback_components, mca_base_component_list_item_t) {
        if( cli->cli_component == (mca_base_component_t*)comp ) {
            opal_list_remove_item(additional_callback_components, (opal_list_item_t*) cli);
            OBJ_RELEASE(cli);
            return OMPI_SUCCESS;
        }
    }

    return OMPI_ERR_NOT_FOUND;
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, hook, "hook hooks",
                           ompi_hook_base_register,
                           ompi_hook_base_open,
                           ompi_hook_base_close,
                           mca_hook_base_static_components, 0);


/* ***********************************************************************
 * ***********************************************************************
 * *********************************************************************** */

/*
 * If the framework has not been opened, then we can only use the static components.
 *
 * Otherwise we would need to initialize opal outside of ompi_mpi_init and possibly
 * after ompi_mpi_finalize which gets messy (especially when trying to cleanup).
 */
#define HOOK_CALL_COMMON_HOOK_NOT_INITIALIZED(fn_name, ...)             \
    do {                                                                \
        ompi_hook_base_component_t *component;                          \
        int idx;                                                        \
                                                                        \
        for(idx = 0; NULL != mca_hook_base_static_components[idx]; ++idx ) { \
            component = (ompi_hook_base_component_t*)mca_hook_base_static_components[idx]; \
            if( NULL != component->hookm_ ## fn_name &&                 \
                ompi_hook_base_ ## fn_name != component->hookm_ ## fn_name ) { \
                component->hookm_ ## fn_name ( __VA_ARGS__ );           \
            }                                                           \
        }                                                               \
    } while(0)

/*
 * Once the framework is open then call all available components with
 * the approprate function pointer. Call order:
 * 1) static components
 * 2) dynamic components
 * 3) 'registered' components (those registered by ompi_hook_base_register_callbacks)
 */
#define HOOK_CALL_COMMON_HOOK_INITIALIZED(fn_name, ...)                 \
    do {                                                                \
        mca_base_component_list_item_t *cli;                            \
        ompi_hook_base_component_t *component;                          \
                                                                        \
        OPAL_LIST_FOREACH(cli, &ompi_hook_base_framework.framework_components, mca_base_component_list_item_t) { \
            component = (ompi_hook_base_component_t*)cli->cli_component; \
            if( NULL != component->hookm_ ## fn_name &&                 \
                ompi_hook_base_ ## fn_name != component->hookm_ ## fn_name ) { \
                component->hookm_ ## fn_name ( __VA_ARGS__ );           \
            }                                                           \
        }                                                               \
                                                                        \
        OPAL_LIST_FOREACH(cli, additional_callback_components, mca_base_component_list_item_t) { \
            component = (ompi_hook_base_component_t*)cli->cli_component; \
            if( NULL != component->hookm_ ## fn_name &&                 \
                ompi_hook_base_ ## fn_name != component->hookm_ ## fn_name ) { \
                component->hookm_ ## fn_name ( __VA_ARGS__ );           \
            }                                                           \
        }                                                               \
    } while(0)

#define HOOK_CALL_COMMON(fn_name, ...)                                  \
    do {                                                                \
        if( OPAL_LIKELY(ompi_hook_is_framework_open) ) {                \
            HOOK_CALL_COMMON_HOOK_INITIALIZED(fn_name, __VA_ARGS__);    \
        }                                                               \
        else {                                                          \
            HOOK_CALL_COMMON_HOOK_NOT_INITIALIZED(fn_name, __VA_ARGS__); \
        }                                                               \
    } while(0)



void ompi_hook_base_mpi_initialized_top(int *flag)
{
    HOOK_CALL_COMMON( mpi_initialized_top, flag );
}

void ompi_hook_base_mpi_initialized_bottom(int *flag)
{
    HOOK_CALL_COMMON( mpi_initialized_bottom, flag );
}


void ompi_hook_base_mpi_init_thread_top(int *argc, char ***argv, int required, int *provided)
{
    HOOK_CALL_COMMON( mpi_init_thread_top, argc, argv, required, provided );
}

void ompi_hook_base_mpi_init_thread_bottom(int *argc, char ***argv, int required, int *provided)
{
    HOOK_CALL_COMMON( mpi_init_thread_bottom, argc, argv, required, provided );
}


void ompi_hook_base_mpi_finalized_top(int *flag)
{
    HOOK_CALL_COMMON( mpi_finalized_top, flag );
}

void ompi_hook_base_mpi_finalized_bottom(int *flag)
{
    HOOK_CALL_COMMON( mpi_finalized_bottom, flag );
}


void ompi_hook_base_mpi_init_top(int argc, char **argv, int requested, int *provided)
{
    HOOK_CALL_COMMON( mpi_init_top, argc, argv, requested, provided);
}

void ompi_hook_base_mpi_init_top_post_opal(int argc, char **argv, int requested, int *provided)
{
    HOOK_CALL_COMMON( mpi_init_top_post_opal, argc, argv, requested, provided);
}

void ompi_hook_base_mpi_init_bottom(int argc, char **argv, int requested, int *provided)
{
    HOOK_CALL_COMMON( mpi_init_bottom, argc, argv, requested, provided);
}

void ompi_hook_base_mpi_init_error(int argc, char **argv, int requested, int *provided)
{
    HOOK_CALL_COMMON( mpi_init_error, argc, argv, requested, provided);
}


void ompi_hook_base_mpi_finalize_top(void)
{
    HOOK_CALL_COMMON( mpi_finalize_top, );
}

void ompi_hook_base_mpi_finalize_bottom(void)
{
    HOOK_CALL_COMMON( mpi_finalize_bottom, );
}

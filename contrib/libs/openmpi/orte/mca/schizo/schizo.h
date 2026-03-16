/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The Open RTE Personality Framework (schizo)
 *
 * Multi-select framework so that multiple personalities can be
 * simultaneously supported
 *
 */

#ifndef ORTE_MCA_SCHIZO_H
#define ORTE_MCA_SCHIZO_H

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/mca.h"

#include "orte/runtime/orte_globals.h"


BEGIN_C_DECLS

/*
 * schizo module functions
 */

/**
* SCHIZO module functions - the modules are accessed via
* the base stub functions
*/

/* initialize the module - allow it to do whatever one-time
 * things it requires */
typedef int (*orte_schizo_base_module_init_fn_t)(void);

/* provide an opportunity for components to add personality and/or
 * environment-specific command line options. The OPAL cli tools
 * will add provided options to the CLI definition, and so the
 * resulting CLI array will include the _union_ of options provided
 * by the various components. Where there is overlap (i.e., an option
 * is added that was also defined earlier in the stack), then the
 * first definition is used. This reflects the higher priority of
 * the original definition - note that this only impacts the help
 * message that will be displayed */
typedef int (*orte_schizo_base_module_define_cli_fn_t)(opal_cmd_line_t *cli);

/* parse a tool command line
 * starting from the given location according to the cmd line options
 * known to this module's personality. First, of course, check that
 * this module is included in the base array of personalities, or is
 * automatically recognizable! */
typedef int (*orte_schizo_base_module_parse_cli_fn_t)(int argc, int start,
                                                      char **argv);

/* parse the environment of the
 * tool to extract any personality-specific envars that need to be
 * forward to the app's environment upon execution */
typedef int (*orte_schizo_base_module_parse_env_fn_t)(char *path,
                                                      opal_cmd_line_t *cmd_line,
                                                      char **srcenv,
                                                      char ***dstenv);

/* do whatever preparation work
 * is required to setup the app for execution. This is intended to be
 * used by orterun and other launcher tools to, for example, change
 * an executable's relative-path to an absolute-path, or add a command
 * required for starting a particular kind of application (e.g., adding
 * "java" to start a Java application) */
typedef int (*orte_schizo_base_module_setup_app_fn_t)(orte_app_context_t *app);

/* add any personality-specific envars required at the job level prior
 * to beginning to execute local procs */
typedef int (*orte_schizo_base_module_setup_fork_fn_t)(orte_job_t *jdata,
                                                       orte_app_context_t *context);

/* add any personality-specific envars required for this specific local
 * proc upon execution */
typedef int (*orte_schizo_base_module_setup_child_fn_t)(orte_job_t *jdata,
                                                        orte_proc_t *child,
                                                        orte_app_context_t *app,
                                                        char ***env);


typedef enum {
    ORTE_SCHIZO_UNDETERMINED,
    ORTE_SCHIZO_NATIVE_LAUNCHED,
    ORTE_SCHIZO_UNMANAGED_SINGLETON,
    ORTE_SCHIZO_DIRECT_LAUNCHED,
    ORTE_SCHIZO_MANAGED_SINGLETON
} orte_schizo_launch_environ_t;


/* check if this process was directly launched by a managed environment, and
 * do whatever the module wants to do under those conditions. The module
 * can push any required envars into the local environment, but must remember
 * to "unset" them during finalize. The module then returns a flag indicating
 * the launch environment of the process */
typedef orte_schizo_launch_environ_t (*orte_schizo_base_module_ck_launch_environ_fn_t)(void);

/* give the component a chance to cleanup */
typedef void (*orte_schizo_base_module_finalize_fn_t)(void);


/* request time remaining in this allocation - only one module
 * capable of supporting this operation should be available
 * in a given environment. However, if a module is available
 * and decides it cannot provide the info in the current situation,
 * then it can return ORTE_ERR_TAKE_NEXT_OPTION to indicate that
 * another module should be tried */
typedef int (*orte_schizo_base_module_get_rem_time_fn_t)(uint32_t *timeleft);

/*
 * schizo module version 1.3.0
 */
typedef struct {
    orte_schizo_base_module_init_fn_t                   init;
    orte_schizo_base_module_define_cli_fn_t             define_cli;
    orte_schizo_base_module_parse_cli_fn_t              parse_cli;
    orte_schizo_base_module_parse_env_fn_t              parse_env;
    orte_schizo_base_module_setup_app_fn_t              setup_app;
    orte_schizo_base_module_setup_fork_fn_t             setup_fork;
    orte_schizo_base_module_setup_child_fn_t            setup_child;
    orte_schizo_base_module_ck_launch_environ_fn_t      check_launch_environment;
    orte_schizo_base_module_get_rem_time_fn_t           get_remaining_time;
    orte_schizo_base_module_finalize_fn_t               finalize;
} orte_schizo_base_module_t;

ORTE_DECLSPEC extern orte_schizo_base_module_t orte_schizo;

/*
 * schizo component
 */

/**
 * schizo component version 1.3.0
 */
typedef struct {
    /** Base MCA structure */
    mca_base_component_t base_version;
    /** Base MCA data */
    mca_base_component_data_t base_data;
} orte_schizo_base_component_t;

/**
 * Macro for use in components that are of type schizo
 */
#define MCA_SCHIZO_BASE_VERSION_1_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("schizo", 1, 0, 0)


END_C_DECLS

#endif

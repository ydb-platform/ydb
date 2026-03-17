/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_MCA_BASE_H
#define PMIX_MCA_BASE_H

#include <src/include/pmix_config.h>

#include "src/class/pmix_object.h"
#include "src/class/pmix_list.h"

/*
 * These units are large enough to warrant their own .h files
 */
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/util/cmd_line.h"
#include "src/util/output.h"

BEGIN_C_DECLS

/*
 * Structure for making plain lists of components
 */
struct pmix_mca_base_component_list_item_t {
    pmix_list_item_t super;
    const pmix_mca_base_component_t *cli_component;
};
typedef struct pmix_mca_base_component_list_item_t pmix_mca_base_component_list_item_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_mca_base_component_list_item_t);

/*
 * Structure for making priority lists of components
 */
struct pmix_mca_base_component_priority_list_item_t {
    pmix_mca_base_component_list_item_t super;
    int cpli_priority;
};
typedef struct pmix_mca_base_component_priority_list_item_t
    pmix_mca_base_component_priority_list_item_t;

PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_mca_base_component_priority_list_item_t);

/*
 * Public variables
 */
PMIX_EXPORT extern char *pmix_mca_base_component_path;
PMIX_EXPORT extern bool pmix_mca_base_component_show_load_errors;
PMIX_EXPORT extern bool pmix_mca_base_component_track_load_errors;
PMIX_EXPORT extern bool pmix_mca_base_component_disable_dlopen;
PMIX_EXPORT extern char *pmix_mca_base_system_default_path;
PMIX_EXPORT extern char *pmix_mca_base_user_default_path;

/*
 * Standard verbosity levels
 */
enum {
    /** total silence */
    PMIX_MCA_BASE_VERBOSE_NONE  = -1,
    /** only errors are printed */
    PMIX_MCA_BASE_VERBOSE_ERROR = 0,
    /** emit messages about component selection, open, and unloading */
    PMIX_MCA_BASE_VERBOSE_COMPONENT = 10,
    /** also emit warnings */
    PMIX_MCA_BASE_VERBOSE_WARN  = 20,
    /** also emit general, user-relevant information, such as rationale as to why certain choices
     * or code paths were taken, information gleaned from probing the local system, etc. */
    PMIX_MCA_BASE_VERBOSE_INFO  = 40,
    /** also emit relevant tracing information (e.g., which functions were invoked /
     * call stack entry/exit info) */
    PMIX_MCA_BASE_VERBOSE_TRACE = 60,
    /** also emit PMIX-developer-level (i.e,. highly detailed) information */
    PMIX_MCA_BASE_VERBOSE_DEBUG = 80,
    /** also output anything else that might be useful */
    PMIX_MCA_BASE_VERBOSE_MAX   = 100,
};

/*
 * Public functions
 */

/**
 * First function called in the MCA.
 *
 * @return PMIX_SUCCESS Upon success
 * @return PMIX_ERROR Upon failure
 *
 * This function starts up the entire MCA.  It initializes a bunch
 * of built-in MCA parameters, and initialized the MCA component
 * repository.
 *
 * It must be the first MCA function invoked.  It is normally
 * invoked during the initialization stage and specifically
 * invoked in the special case of the *_info command.
 */
PMIX_EXPORT int pmix_mca_base_open(void);

/**
 * Last function called in the MCA
 *
 * @return PMIX_SUCCESS Upon success
 * @return PMIX_ERROR Upon failure
 *
 * This function closes down the entire MCA.  It clears all MCA
 * parameters and closes down the MCA component respository.
 *
 * It must be the last MCA function invoked.  It is normally invoked
 * during the finalize stage.
 */
PMIX_EXPORT int pmix_mca_base_close(void);

/**
 * A generic select function
 *
 */
PMIX_EXPORT int pmix_mca_base_select(const char *type_name, int output_id,
                                     pmix_list_t *components_available,
                                     pmix_mca_base_module_t **best_module,
                                     pmix_mca_base_component_t **best_component,
                                     int *priority_out);

/**
 * A function for component query functions to discover if they have
 * been explicitly required to or requested to be selected.
 *
 * exclusive: If the specified component is the only component that is
 *            available for selection.
 *
 */
PMIX_EXPORT int pmix_mca_base_is_component_required(pmix_list_t *components_available,
                                                    pmix_mca_base_component_t *component,
                                                    bool exclusive,
                                                    bool *is_required);

/* mca_base_cmd_line.c */

PMIX_EXPORT int pmix_mca_base_cmd_line_setup(pmix_cmd_line_t *cmd);
PMIX_EXPORT int pmix_mca_base_cmd_line_process_args(pmix_cmd_line_t *cmd,
                                                 char ***app_env,
                                                 char ***global_env);
PMIX_EXPORT void pmix_mca_base_cmd_line_wrap_args(char **args);

/* pmix_mca_base_component_compare.c */

PMIX_EXPORT int pmix_mca_base_component_compare_priority(pmix_mca_base_component_priority_list_item_t *a,
                                                         pmix_mca_base_component_priority_list_item_t *b);
PMIX_EXPORT int pmix_mca_base_component_compare(const pmix_mca_base_component_t *a,
                                                const pmix_mca_base_component_t *b);
PMIX_EXPORT int pmix_mca_base_component_compatible(const pmix_mca_base_component_t *a,
                                                   const pmix_mca_base_component_t *b);
PMIX_EXPORT char * pmix_mca_base_component_to_string(const pmix_mca_base_component_t *a);

/* pmix_mca_base_component_find.c */

PMIX_EXPORT int pmix_mca_base_component_find (const char *directory, pmix_mca_base_framework_t *framework,
                                              bool ignore_requested, bool open_dso_components);

/**
 * Parse the requested component string and return an pmix_argv of the requested
 * (or not requested) components.
 */
PMIX_EXPORT int pmix_mca_base_component_parse_requested (const char *requested, bool *include_mode,
                                                         char ***requested_component_names);

/**
 * Filter a list of components based on a comma-delimted list of names and/or
 * a set of meta-data flags.
 *
 * @param[in,out] components List of components to filter
 * @param[in] output_id Output id to write to for error/warning/debug messages
 * @param[in] filter_names Comma delimited list of components to use. Negate with ^.
 * May be NULL.
 * @param[in] filter_flags Metadata flags components are required to have set (CR ready)
 *
 * @returns PMIX_SUCCESS On success
 * @returns PMIX_ERR_NOT_FOUND If some component in {filter_names} is not found in
 * {components}. Does not apply to negated filters.
 * @returns pmix error code On other error.
 *
 * This function closes and releases any components that do not match the filter_name and
 * filter flags.
 */
PMIX_EXPORT int pmix_mca_base_components_filter (pmix_mca_base_framework_t *framework, uint32_t filter_flags);



/* Safely release some memory allocated by pmix_mca_base_component_find()
   (i.e., is safe to call even if you never called
   pmix_mca_base_component_find()). */
PMIX_EXPORT int pmix_mca_base_component_find_finalize(void);

/* pmix_mca_base_components_register.c */
PMIX_EXPORT int pmix_mca_base_framework_components_register (struct pmix_mca_base_framework_t *framework,
                                                             pmix_mca_base_register_flag_t flags);

/* pmix_mca_base_components_open.c */
PMIX_EXPORT int pmix_mca_base_framework_components_open (struct pmix_mca_base_framework_t *framework,
                                                         pmix_mca_base_open_flag_t flags);

PMIX_EXPORT int pmix_mca_base_components_open(const char *type_name, int output_id,
                                              const pmix_mca_base_component_t **static_components,
                                              pmix_list_t *components_available,
                                              bool open_dso_components);

/* pmix_mca_base_components_close.c */
/**
 * Close and release a component.
 *
 * @param[in] component Component to close
 * @param[in] output_id Output id for debugging output
 *
 * After calling this function the component may no longer be used.
 */
PMIX_EXPORT void pmix_mca_base_component_close (const pmix_mca_base_component_t *component, int output_id);

/**
 * Release a component without closing it.
 * @param[in] component Component to close
 * @param[in] output_id Output id for debugging output
 *
 * After calling this function the component may no longer be used.
 */
PMIX_EXPORT void pmix_mca_base_component_unload (const pmix_mca_base_component_t *component, int output_id);

PMIX_EXPORT int pmix_mca_base_components_close(int output_id, pmix_list_t *components_available,
                                               const pmix_mca_base_component_t *skip);

PMIX_EXPORT int pmix_mca_base_framework_components_close (struct pmix_mca_base_framework_t *framework,
                                                          const pmix_mca_base_component_t *skip);

END_C_DECLS

#endif /* MCA_BASE_H */

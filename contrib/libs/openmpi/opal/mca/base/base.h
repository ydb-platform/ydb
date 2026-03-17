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
 * Copyright (c) 2017 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_BASE_H
#define MCA_BASE_H

#include "opal_config.h"

#include "opal/class/opal_object.h"
#include "opal/class/opal_list.h"

/*
 * These units are large enough to warrant their own .h files
 */
#include "opal/mca/mca.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/util/cmd_line.h"
#include "opal/util/output.h"

BEGIN_C_DECLS

/*
 * Structure for making plain lists of components
 */
struct mca_base_component_list_item_t {
    opal_list_item_t super;
    const mca_base_component_t *cli_component;
};
typedef struct mca_base_component_list_item_t mca_base_component_list_item_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_base_component_list_item_t);

/*
 * Structure for making priority lists of components
 */
struct mca_base_component_priority_list_item_t {
    mca_base_component_list_item_t super;
    int cpli_priority;
};
typedef struct mca_base_component_priority_list_item_t
    mca_base_component_priority_list_item_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_base_component_priority_list_item_t);

/*
 * Public variables
 */
OPAL_DECLSPEC extern char *mca_base_component_path;
OPAL_DECLSPEC extern bool mca_base_component_show_load_errors;
OPAL_DECLSPEC extern bool mca_base_component_track_load_errors;
OPAL_DECLSPEC extern bool mca_base_component_disable_dlopen;
OPAL_DECLSPEC extern char *mca_base_system_default_path;
OPAL_DECLSPEC extern char *mca_base_user_default_path;

/*
 * Standard verbosity levels
 */
enum {
    /** total silence */
    MCA_BASE_VERBOSE_NONE  = -1,
    /** only errors are printed */
    MCA_BASE_VERBOSE_ERROR = 0,
    /** emit messages about component selection, open, and unloading */
    MCA_BASE_VERBOSE_COMPONENT = 10,
    /** also emit warnings */
    MCA_BASE_VERBOSE_WARN  = 20,
    /** also emit general, user-relevant information, such as rationale as to why certain choices
     * or code paths were taken, information gleaned from probing the local system, etc. */
    MCA_BASE_VERBOSE_INFO  = 40,
    /** also emit relevant tracing information (e.g., which functions were invoked /
     * call stack entry/exit info) */
    MCA_BASE_VERBOSE_TRACE = 60,
    /** also emit Open MPI-developer-level (i.e,. highly detailed) information */
    MCA_BASE_VERBOSE_DEBUG = 80,
    /** also output anything else that might be useful */
    MCA_BASE_VERBOSE_MAX   = 100,
};

/*
 * Public functions
 */

/**
 * First function called in the MCA.
 *
 * @return OPAL_SUCCESS Upon success
 * @return OPAL_ERROR Upon failure
 *
 * This function starts up the entire MCA.  It initializes a bunch
 * of built-in MCA parameters, and initialized the MCA component
 * repository.
 *
 * It must be the first MCA function invoked.  It is normally
 * invoked during the initialization stage and specifically
 * invoked in the special case of the *_info command.
 */
OPAL_DECLSPEC int mca_base_open(void);

/**
 * Last function called in the MCA
 *
 * @return OPAL_SUCCESS Upon success
 * @return OPAL_ERROR Upon failure
 *
 * This function closes down the entire MCA.  It clears all MCA
 * parameters and closes down the MCA component respository.
 *
 * It must be the last MCA function invoked.  It is normally invoked
 * during the finalize stage.
 */
OPAL_DECLSPEC int mca_base_close(void);

/**
 * A generic select function
 *
 */
OPAL_DECLSPEC int mca_base_select(const char *type_name, int output_id,
                                  opal_list_t *components_available,
                                  mca_base_module_t **best_module,
                                  mca_base_component_t **best_component,
                                  int *priority_out);

/**
 * A function for component query functions to discover if they have
 * been explicitly required to or requested to be selected.
 *
 * exclusive: If the specified component is the only component that is
 *            available for selection.
 *
 */
OPAL_DECLSPEC int mca_base_is_component_required(opal_list_t *components_available,
                                                 mca_base_component_t *component,
                                                 bool exclusive,
                                                 bool *is_required);

/* mca_base_cmd_line.c */

OPAL_DECLSPEC int mca_base_cmd_line_setup(opal_cmd_line_t *cmd);
OPAL_DECLSPEC int mca_base_cmd_line_process_args(opal_cmd_line_t *cmd,
                                                 char ***app_env,
                                                 char ***global_env);
OPAL_DECLSPEC void mca_base_cmd_line_wrap_args(char **args);

/* mca_base_component_compare.c */

OPAL_DECLSPEC int mca_base_component_compare_priority(mca_base_component_priority_list_item_t *a,
                                                      mca_base_component_priority_list_item_t *b);
OPAL_DECLSPEC int mca_base_component_compare(const mca_base_component_t *a,
                                             const mca_base_component_t *b);
OPAL_DECLSPEC int mca_base_component_compatible(const mca_base_component_t *a,
                                                const mca_base_component_t *b);
OPAL_DECLSPEC char * mca_base_component_to_string(const mca_base_component_t *a);

/* mca_base_component_find.c */

OPAL_DECLSPEC int mca_base_component_find (const char *directory, mca_base_framework_t *framework,
                                           bool ignore_requested, bool open_dso_components);

/**
 * Parse the requested component string and return an opal_argv of the requested
 * (or not requested) components.
 */
int mca_base_component_parse_requested (const char *requested, bool *include_mode,
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
 * @returns OPAL_SUCCESS On success
 * @returns OPAL_ERR_NOT_FOUND If some component in {filter_names} is not found in
 * {components}. Does not apply to negated filters.
 * @returns opal error code On other error.
 *
 * This function closes and releases any components that do not match the filter_name and
 * filter flags.
 */
OPAL_DECLSPEC int mca_base_components_filter (mca_base_framework_t *framework, uint32_t filter_flags);



/* Safely release some memory allocated by mca_base_component_find()
   (i.e., is safe to call even if you never called
   mca_base_component_find()). */
OPAL_DECLSPEC int mca_base_component_find_finalize(void);

/* mca_base_components_register.c */
OPAL_DECLSPEC int mca_base_framework_components_register (struct mca_base_framework_t *framework,
                                                          mca_base_register_flag_t flags);

/* mca_base_components_open.c */
OPAL_DECLSPEC int mca_base_framework_components_open (struct mca_base_framework_t *framework,
                                                      mca_base_open_flag_t flags);

OPAL_DECLSPEC int mca_base_components_open(const char *type_name, int output_id,
                                           const mca_base_component_t **static_components,
                                           opal_list_t *components_available,
                                           bool open_dso_components);

/* mca_base_components_close.c */
/**
 * Close and release a component.
 *
 * @param[in] component Component to close
 * @param[in] output_id Output id for debugging output
 *
 * After calling this function the component may no longer be used.
 */
OPAL_DECLSPEC void mca_base_component_close (const mca_base_component_t *component, int output_id);

/**
 * Release a component without closing it.
 * @param[in] component Component to close
 * @param[in] output_id Output id for debugging output
 *
 * After calling this function the component may no longer be used.
 */
void mca_base_component_unload (const mca_base_component_t *component, int output_id);

OPAL_DECLSPEC int mca_base_components_close(int output_id, opal_list_t *components_available,
                                            const mca_base_component_t *skip);

OPAL_DECLSPEC int mca_base_framework_components_close (struct mca_base_framework_t *framework,
						       const mca_base_component_t *skip);

END_C_DECLS

#endif /* MCA_BASE_H */

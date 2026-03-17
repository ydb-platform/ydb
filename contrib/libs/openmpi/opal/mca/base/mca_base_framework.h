/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OPAL_MCA_BASE_FRAMEWORK_H)
#define OPAL_MCA_BASE_FRAMEWORK_H

#include "opal/mca/mca.h"
#include "opal/class/opal_list.h"

/*
 * Register and open flags
 */
enum mca_base_register_flag_t {
    MCA_BASE_REGISTER_DEFAULT     = 0,
    /** Register all components (ignore selection MCA variables) */
    MCA_BASE_REGISTER_ALL         = 1,
    /** Do not register DSO components */
    MCA_BASE_REGISTER_STATIC_ONLY = 2
};

typedef enum mca_base_register_flag_t mca_base_register_flag_t;

enum mca_base_open_flag_t {
    MCA_BASE_OPEN_DEFAULT         = 0,
    /** Find components in mca_base_components_find. Used by
     mca_base_framework_open() when NOREGISTER is specified
     by the framework */
    MCA_BASE_OPEN_FIND_COMPONENTS = 1,
    /** Do not open DSO components */
    MCA_BASE_OPEN_STATIC_ONLY     = 2,
};

typedef enum mca_base_open_flag_t mca_base_open_flag_t;


/**
 * Register the MCA framework parameters
 *
 * @param[in] flags Registration flags (see mca/base/base.h)
 *
 * @retval OPAL_SUCCESS on success
 * @retval opal error code on failure
 *
 * This function registers all framework MCA parameters. This
 * function should not call mca_base_framework_components_register().
 *
 * Frameworks are NOT required to provide this function. It
 * may be NULL.
 */
typedef int (*mca_base_framework_register_params_fn_t) (mca_base_register_flag_t flags);

/**
 * Initialize the MCA framework
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR Upon failure
 *
 * This must be the first function invoked in the MCA framework.
 * It initializes the MCA framework, finds and opens components,
 * populates the components list, etc.
 *
 * This function is invoked during opal_init() and during the
 * initialization of the special case of the ompi_info command.
 *
 * This function fills in the components framework value, which
 * is a list of all components that were successfully opened.
 * This variable should \em only be used by other framework base
 * functions or by ompi_info -- it is not considered a public
 * interface member -- and is only mentioned here for completeness.
 *
 * Any resources allocated by this function must be freed
 * in the framework close function.
 *
 * Frameworks are NOT required to provide this function. It may
 * be NULL. If a framework does not provide an open function the
 * default behavior of mca_base_framework_open() is to call
 * mca_base_framework_components_open(). If a framework provides
 * an open function it will need to call mca_base_framework_components_open()
 * if it needs to open any components.
 */
typedef int (*mca_base_framework_open_fn_t) (mca_base_open_flag_t flags);

/**
 * Shut down the MCA framework.
 *
 * @retval OPAL_SUCCESS Always
 *
 * This function should shut downs everything in the MCA
 * framework, and is called during opal_finalize() and the
 * special case of the ompi_info command.
 *
 * It must be the last function invoked on the MCA framework.
 *
 * Frameworks are NOT required to provide this function. It may
 * be NULL. If a framework does not provide a close function the
 * default behavior of mca_base_framework_close() is to call
 * mca_base_framework_components_close(). If a framework provide
 * a close function it will need to call mca_base_framework_components_close()
 * if any components were opened.
 */
typedef int (*mca_base_framework_close_fn_t) (void);

typedef enum {
    MCA_BASE_FRAMEWORK_FLAG_DEFAULT    = 0,
    /** Don't register any variables for this framework */
    MCA_BASE_FRAMEWORK_FLAG_NOREGISTER = 1,
    /** Internal. Don't set outside mca_base_framework.h */
    MCA_BASE_FRAMEWORK_FLAG_REGISTERED = 2,
    /** Framework does not have any DSO components */
    MCA_BASE_FRAMEWORK_FLAG_NO_DSO     = 4,
    /** Internal. Don't set outside mca_base_framework.h */
    MCA_BASE_FRAMEWORK_FLAG_OPEN       = 8,
    /**
     * The upper 16 bits are reserved for project specific flags.
     */
} mca_base_framework_flags_t;

typedef struct mca_base_framework_t {
    /** Project name for this component (ex "opal") */
    char                                    *framework_project;
    /** Framework name */
    char                                    *framework_name;
    /** Description of this framework or NULL */
    const char                              *framework_description;
    /** Framework register function or NULL if the framework
        and all its components have nothing to register */
    mca_base_framework_register_params_fn_t  framework_register;
    /** Framework open function or NULL */
    mca_base_framework_open_fn_t             framework_open;
    /** Framework close function or NULL */
    mca_base_framework_close_fn_t            framework_close;
    /** Framework flags (future use) set to 0 */
    mca_base_framework_flags_t               framework_flags;
    /** Framework open count */
    int                                      framework_refcnt;
    /** List of static components */
    const mca_base_component_t             **framework_static_components;
    /** Component selection. This will be registered with the MCA
        variable system and should be either NULL (all components) or
        a heap allocated, comma-delimited list of components. */
    char                                    *framework_selection;
    /** Verbosity level (0-100) */
    int                                      framework_verbose;
    /** Opal output for this framework (or -1) */
    int                                      framework_output;
    /** List of selected components (filled in by mca_base_framework_register()
        or mca_base_framework_open() */
    opal_list_t                              framework_components;
    /** List of components that failed to load */
    opal_list_t                              framework_failed_components;
} mca_base_framework_t;


/**
 * Register a framework with MCA.
 *
 * @param[in] framework framework to register
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR Upon failure
 *
 * Call a framework's register function.
 */
OPAL_DECLSPEC int mca_base_framework_register (mca_base_framework_t *framework,
                                               mca_base_register_flag_t flags);

/**
 * Open a framework
 *
 * @param[in] framework framework to open
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR Upon failure
 *
 * Call a framework's open function.
 */
OPAL_DECLSPEC int mca_base_framework_open (mca_base_framework_t *framework,
                                           mca_base_open_flag_t flags);

/**
 * Close a framework
 *
 * @param[in] framework framework to close
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR Upon failure
 *
 * Call a framework's close function.
 */
OPAL_DECLSPEC int mca_base_framework_close (mca_base_framework_t *framework);


/**
 * Check if a framework is already registered
 *
 * @param[in] framework framework to query
 *
 * @retval true if the framework's mca variables are registered
 * @retval false if not
 */
OPAL_DECLSPEC bool mca_base_framework_is_registered (struct mca_base_framework_t *framework);


/**
 * Check if a framework is already open
 *
 * @param[in] framework framework to query
 *
 * @retval true if the framework is open
 * @retval false if not
 */
OPAL_DECLSPEC bool mca_base_framework_is_open (struct mca_base_framework_t *framework);


/**
 * Macro to declare an MCA framework
 *
 * Example:
 *  MCA_BASE_FRAMEWORK_DECLARE(opal, foo, NULL, opal_foo_open, opal_foo_close, MCA_BASE_FRAMEWORK_FLAG_LAZY)
 */
#define MCA_BASE_FRAMEWORK_DECLARE(project, name, description, registerfn, openfn, closefn, static_components, flags) \
    mca_base_framework_t project##_##name##_base_framework = {   \
        .framework_project           = #project,                        \
        .framework_name              = #name,                           \
        .framework_description       = description,                     \
        .framework_register          = registerfn,                      \
        .framework_open              = openfn,                          \
        .framework_close             = closefn,                         \
        .framework_flags             = flags,                           \
        .framework_refcnt            = 0,                               \
        .framework_static_components = static_components,               \
        .framework_selection         = NULL,                            \
        .framework_verbose           = 0,                               \
        .framework_output            = -1}

#endif /* OPAL_MCA_BASE_FRAMEWORK_H */

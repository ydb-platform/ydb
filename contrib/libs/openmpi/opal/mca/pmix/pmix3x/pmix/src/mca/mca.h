/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Top-level interface for all pmix MCA components.
 */

#ifndef PMIX_MCA_H
#define PMIX_MCA_H

#include <src/include/pmix_config.h>

/**
 * Common type for all MCA modules.
 *
 * An instance of this type is always the first element in MCA
 * modules, allowing the module to be associated with a
 * particular version of a specific framework, and to publish its own
 * name and version.
 */
struct pmix_mca_base_module_2_0_0_t {
    int dummy_value;
};
/** Unversioned convenience typedef; use this name in
    frameworks/components to stay forward source-compatible */
typedef struct pmix_mca_base_module_2_0_0_t pmix_mca_base_module_t;
/** Versioned convenience typedef */
typedef struct pmix_mca_base_module_2_0_0_t pmix_mca_base_module_2_0_0_t;


/**
 * MCA component open function.
 *
 * @retval PMIX_SUCCESS This component can be used in the process.
 *
 * @retval PMIX_ERR_NOT_AVAILABLE Silently ignore this component for
 * the duration of the process (it may even be unloaded from the
 * process).
 *
 * @retval anything_else The MCA base will print an error message
 * ignore this component for the duration of the process (it may even
 * be unloaded from the process).
 *
 * All MCA components can have an "open" function that is invoked once
 * per process, when the component is located and loaded.
 *
 * This function should avoid registering MCA parameters (use the
 * component "register" function for that; i.e.,
 * mca_base_register_component_params_2_0_0_fn_t for that).  Legacy
 * components still register MCA params in their component "open"
 * function, but their authors should update them to use the component
 * "register" function.
 *
 * This function can also be used to allocate any resources necessary
 * for the component (e.g., heap memory).
 *
 * This function should return PMIX_SUCCESS if it wishes to remain
 * loaded in the process.  Any other return value will cause the MCA
 * base to unload the component.  Although most components do not use
 * this mechanism to force themselves to be unloaded (because if they
 * are immediately unloaded, ompi_info will not display them), the
 * mechanism is available should the need arise.
 *
 * If the component a) has no MCA parameters to register, b) no
 * resources to allocate, and c) can always be used in a process
 * (albiet perhaps not selected), it may provide NULL for this
 * function.  In this cause, the MCA will act as if it called the open
 * function and it returned PMIX_SUCCESS.
 */
typedef int (*pmix_mca_base_open_component_1_0_0_fn_t)(void);

/**
 * MCA component close function.
 *
 * @retval PMIX_SUCCESS The component successfully shut down.
 *
 * @retval any_other_value Some error occurred, but is likely to be
 * ignored.
 *
 * This function is invoked on a component after all of its modules
 * have been finalized (according to the rules of its framework) and
 * the component will never be used in the process again; the
 * component may be unloaded from the process memory after the close
 * function has been invoked.
 *
 * This function is typically used to release any resources still in
 * use by the component.
 *
 * If the component has no resources to free, it may provide NULL for
 * this function.  In this case, the MCA will act as if it called the
 * close function and it returned PMIX_SUCCESS.
 */
typedef int (*pmix_mca_base_close_component_1_0_0_fn_t)(void);

/**
 * MCA component query function.
 *
 * @retval PMIX_SUCCESS The component successfully queried.
 *
 * @retval any_other_value Some error occurred, but is likely to be
 * ignored.
 *
 * @param module The module to be used if this component is selected.
 *
 * @param priority The priority of this component.
 *
 * This function is used by the mca_base_select function to find the
 * highest priority component to select. Frameworks are free to
 * implement their own query function, but must also implment their
 * own select function as a result.
 */
typedef int (*pmix_mca_base_query_component_2_0_0_fn_t)(pmix_mca_base_module_2_0_0_t **module, int *priority);

/**
 * MCA component parameter registration function.
 *
 * @retval PMIX_SUCCESS This component successfully registered its
 * parameters and can be used in this process.
 * @retval PMIX_ERR_BAD_PARAM Indicates that the register function
 * failed because an MCA parameter got an invalid/incorrect value.
 *
 * @retval anything_else The MCA will ignore this component for the
 * duration of the process.
 *
 * If a component has a non-NULL parameter registration function, it
 * will be invoked to register all MCA parameters associated with the
 * component.  This function is invoked *before* the component "open"
 * function is invoked.
 *
 * The registration function should not allocate any resources that
 * need to be freed (aside from registering MCA parameters).
 * Specifically, strings that are passed to the MCA parameter
 * registration functions are all internally copied; there's no need
 * for the caller to keep them after registering a parameter.  Hence,
 * it is possible that the registration function will be the *only*
 * function invoked on a component; component authors should take care
 * that no resources are leaked in this case.
 *
 * This function should return PMIX_SUCCESS if it wishes to remain
 * loaded in the process.  Any other return value will cause the MCA
 * base to unload the component.  Although most components do not use
 * this mechanism to force themselves to be unloaded (because if they
 * are immediately unloaded, ompi_info will not display them), the
 * mechanism is available should the need arise.
 *
 * Note that if the function returns PMIX_ERR_BAD_PARAM, it is
 * possible (likely?) that the component didn't register all of its
 * parameters.  When this happens, ompi_info (and friends) will stop
 * execution and print out all existing registered parameters from the
 * entire framework (since ompi_info doesn't track individual
 * component register failures).  This allows a user to know exactly
 * what value is incorrect, and from where it was set (e.g., via an
 * MCA params file).
 *
 * If the component a) has no MCA parameters to register, b) no
 * resources to allocate, and c) can always be used in a process
 * (albiet perhaps not selected), it may provide NULL for this
 * function.  In this cause, the MCA will act as if it called the
 * registration function and it returned PMIX_SUCCESS.
 */
typedef int (*pmix_mca_base_register_component_params_2_0_0_fn_t)(void);


/**
 * Maximum length of MCA project string names.
 */
#define PMIX_MCA_BASE_MAX_PROJECT_NAME_LEN 15
/**
 * Maximum length of MCA framework string names.
 */
#define PMIX_MCA_BASE_MAX_TYPE_NAME_LEN 31
/**
 * Maximum length of MCA component string names.
 */
#define PMIX_MCA_BASE_MAX_COMPONENT_NAME_LEN 63

/**
 * Common type for all MCA components.
 *
 * An instance of this type is always the first element in MCA
 * components, allowing the component to be associated with a
 * particular version of a specific framework, and to publish its own
 * name and version.
 */
struct pmix_mca_base_component_2_1_0_t {

  int pmix_mca_major_version;
  /**< Major number of the MCA. */
  int pmix_mca_minor_version;
  /**< Minor number of the MCA. */
  int pmix_mca_release_version;
  /**< Release number of the MCA. */

  char pmix_mca_project_name[PMIX_MCA_BASE_MAX_PROJECT_NAME_LEN + 1];
  /**< String name of the project that this component belongs to. */
  int pmix_mca_project_major_version;
  /**< Major version number of the project that this component
     belongs to. */
  int pmix_mca_project_minor_version;
  /**< Minor version number of the project that this component
     belongs to. */
  int pmix_mca_project_release_version;
  /**< Release version number of the project that this component
     belongs to. */

  char pmix_mca_type_name[PMIX_MCA_BASE_MAX_TYPE_NAME_LEN + 1];
  /**< String name of the framework that this component belongs to. */
  int pmix_mca_type_major_version;
  /**< Major version number of the framework that this component
     belongs to. */
  int pmix_mca_type_minor_version;
  /**< Minor version number of the framework that this component
     belongs to. */
  int pmix_mca_type_release_version;
  /**< Release version number of the framework that this component
     belongs to. */

  char pmix_mca_component_name[PMIX_MCA_BASE_MAX_COMPONENT_NAME_LEN + 1];
  /**< This comopnent's string name. */
  int pmix_mca_component_major_version;
  /**< This component's major version number. */
  int pmix_mca_component_minor_version;
  /**< This component's minor version number. */
  int pmix_mca_component_release_version;
  /**< This component's release version number. */

  pmix_mca_base_open_component_1_0_0_fn_t pmix_mca_open_component;
  /**< Method for opening this component. */
  pmix_mca_base_close_component_1_0_0_fn_t pmix_mca_close_component;
  /**< Method for closing this component. */
  pmix_mca_base_query_component_2_0_0_fn_t pmix_mca_query_component;
  /**< Method for querying this component. */
  pmix_mca_base_register_component_params_2_0_0_fn_t pmix_mca_register_component_params;
  /**< Method for registering the component's MCA parameters */

  /** Extra space to allow for expansion in the future without
      breaking older components. */
  char reserved[32];
};
/** Unversioned convenience typedef; use this name in
    frameworks/components to stay forward source-compatible */
typedef struct pmix_mca_base_component_2_1_0_t pmix_mca_base_component_t;
/** Versioned convenience typedef */
typedef struct pmix_mca_base_component_2_1_0_t pmix_mca_base_component_2_1_0_t;

/*
 * Metadata Bit field parameters
 */
#define PMIX_MCA_BASE_METADATA_PARAM_NONE        (uint32_t)0x00 /**< No Metadata flags */
#define PMIX_MCA_BASE_METADATA_PARAM_CHECKPOINT  (uint32_t)0x02 /**< Checkpoint enabled Component */
#define PMIX_MCA_BASE_METADATA_PARAM_DEBUG       (uint32_t)0x04 /**< Debug enabled/only Component */

/**
 * Meta data for MCA v2.0.0 components.
 */
struct pmix_mca_base_component_data_2_0_0_t {
    uint32_t param_field;
    /**< Metadata parameter bit field filled in by the parameters
         defined above */

    /** Extra space to allow for expansion in the future without
        breaking older components. */
    char reserved[32];
};
/** Unversioned convenience typedef; use this name in
    frameworks/components to stay forward source-compatible */
typedef struct pmix_mca_base_component_data_2_0_0_t pmix_mca_base_component_data_t;
/** Versioned convenience typedef */
typedef struct pmix_mca_base_component_data_2_0_0_t pmix_mca_base_component_data_2_0_0_t;

/**
 * Macro for framework author convenience.
 *
 * This macro is used by frameworks defining their component types,
 * indicating that they subscribe to the MCA version 2.0.0.  See
 * component header files (e.g., coll.h) for examples of its usage.
 */
#define PMIX_MCA_BASE_VERSION_MAJOR 2
#define PMIX_MCA_BASE_VERSION_MINOR 1
#define PMIX_MCA_BASE_VERSION_RELEASE 0

#define PMIX_MCA_BASE_MAKE_VERSION(level, MAJOR, MINOR, RELEASE) \
    .pmix_mca_## level ##_major_version = MAJOR,                 \
    .pmix_mca_## level ##_minor_version = MINOR,                 \
    .pmix_mca_## level ##_release_version = RELEASE


#define PMIX_MCA_BASE_VERSION_2_1_0(PROJECT, project_major, project_minor, project_release, TYPE, type_major, type_minor, type_release) \
    .pmix_mca_major_version = PMIX_MCA_BASE_VERSION_MAJOR,                        \
    .pmix_mca_minor_version = PMIX_MCA_BASE_VERSION_MINOR,                        \
    .pmix_mca_release_version = PMIX_MCA_BASE_VERSION_RELEASE,                    \
    .pmix_mca_project_name = PROJECT,                                        \
    PMIX_MCA_BASE_MAKE_VERSION(project, project_major, project_minor, project_release), \
    .pmix_mca_type_name = TYPE,                                              \
    PMIX_MCA_BASE_MAKE_VERSION(type, type_major, type_minor, type_release)


#define PMIX_MCA_BASE_VERSION_1_0_0(type, type_major, type_minor, type_release) \
    PMIX_MCA_BASE_VERSION_2_1_0("pmix", PMIX_MAJOR_VERSION, PMIX_MINOR_VERSION,      \
                                PMIX_RELEASE_VERSION, type, type_major, type_minor, \
                                type_release)

#endif /* PMIX_MCA_H */

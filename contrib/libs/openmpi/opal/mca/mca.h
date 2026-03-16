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
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Top-level interface for \em all MCA components.
 *
 * Historical notes:
 *
 * Open MPI originally used a v1.0.0 of the MCA component structs, but
 * did not have a version number in the struct name.  If I recall
 * correctly, this is because we simply didn't think through (or never
 * envisioned) changing the MCA base component struct itself.  Oops.
 *
 * We made some changes in the base struct in Open MPI v1.3, and
 * decided the following at the same time:
 *
 * - Bump the MCA version number to 2.0.0 and add some "reserved"
 *   space at the end of the struct.
 * - The major MCA version number is essentially tied to the space
 *   that the struct occupies; if we make future changes in the struct
 *   by just using some of the reserved space, it may be possible to
 *   just increment the minor version number (depending on the scope of
 *   the change).  If we need to add more space to the struct, we'll
 *   increment the major version number.
 * - The MCA base component struct now has a version number in it
 *   (starting with Open MPI v1.3, it is 2.0.0).
 * - As was an unstated assumption in prior versions of Open MPI, the
 *   unversioned versions of struct names (both in the MCA base and in
 *   individual framework bases) are intended for components who want
 *   to be forward source-compatible.  That is, the unversioned struct
 *   name always represents the most recent interface version.  If you
 *   need to use an older version, you can explicitly use that older
 *   struct version name.  Please note, however, the Open MPI
 *   developers may not generally provide older versions of framework
 *   interface structs unless they know if someone outside of the Open
 *   MPI community needs it.
 *
 *   ***IF YOU NEED BACKWARDS SOURCE OR BINARY COMPATIBILITY, you must
 *   let us know!***
 *
 * - We are currently only aware of one external developer making Open
 *   MPI components for the v1.2 series.  He already knows that there
 *   are major changes coming in the v1.3 series, and does not expect to
 *   be able to use his v1.2 DSO binaries in v1.3.  As such, we are
 *   breaking backwards binary compatibility in v1.3: there is no
 *   possibility of loading an MCA v1.0 binary component in Open MPI
 *   v1.3 or beyond (source compatibility is much easier -- the binary
 *   "refuse to load MCA components <v2.0.0" policy is enforced in
 *   mca_base_component_find.c).
 *
 *   ***IF YOU NEED BACKWARDS BINARY COMPATIBILITY, please let us
 *   know!***
 *
 * - Note that we decided that framework version numbers are *not*
 *   related to the MCA version number.  It is permissible to bump the
 *   MCA version number and leave all the framework version numbers
 *   they same.  Specifically: a component is uniquely identified by
 *   its (MCA version, framework version, component version) tuple.
 *   So a component that is simply compiled with two different MCA
 *   base versions is still considered "different" because the tuple
 *   first member is different.
 * - Per the discussion above, we decided to have MCA v2.0 no longer
 *   load <v2.0.0 components, and therefore avoided the "how to upcast
 *   a component in memory" issue.  After v2.0.0, it is slightly
 *   easier because the MCA component structs have "reserved" space at
 *   the end that may account for future version data fields.
 */

#ifndef OPAL_MCA_H
#define OPAL_MCA_H

#include "opal_config.h"


/**
 * Common type for all MCA modules.
 *
 * An instance of this type is always the first element in MCA
 * modules, allowing the module to be associated with a
 * particular version of a specific framework, and to publish its own
 * name and version.
 */
struct mca_base_module_2_0_0_t {
    int dummy_value;
};
/** Unversioned convenience typedef; use this name in
    frameworks/components to stay forward source-compatible */
typedef struct mca_base_module_2_0_0_t mca_base_module_t;
/** Versioned convenience typedef */
typedef struct mca_base_module_2_0_0_t mca_base_module_2_0_0_t;


/**
 * MCA component open function.
 *
 * @retval OPAL_SUCCESS This component can be used in the process.
 *
 * @retval OPAL_ERR_NOT_AVAILABLE Silently ignore this component for
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
 * This function should return OPAL_SUCCESS if it wishes to remain
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
 * function and it returned OPAL_SUCCESS.
 */
typedef int (*mca_base_open_component_1_0_0_fn_t)(void);

/**
 * MCA component close function.
 *
 * @retval OPAL_SUCCESS The component successfully shut down.
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
 * close function and it returned OPAL_SUCCESS.
 */
typedef int (*mca_base_close_component_1_0_0_fn_t)(void);

/**
 * MCA component query function.
 *
 * @retval OPAL_SUCCESS The component successfully queried.
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
typedef int (*mca_base_query_component_2_0_0_fn_t)(mca_base_module_2_0_0_t **module, int *priority);

/**
 * MCA component parameter registration function.
 *
 * @retval OPAL_SUCCESS This component successfully registered its
 * parameters and can be used in this process.
 * @retval OPAL_ERR_BAD_PARAM Indicates that the register function
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
 * This function should return OPAL_SUCCESS if it wishes to remain
 * loaded in the process.  Any other return value will cause the MCA
 * base to unload the component.  Although most components do not use
 * this mechanism to force themselves to be unloaded (because if they
 * are immediately unloaded, ompi_info will not display them), the
 * mechanism is available should the need arise.
 *
 * Note that if the function returns OPAL_ERR_BAD_PARAM, it is
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
 * registration function and it returned OPAL_SUCCESS.
 */
typedef int (*mca_base_register_component_params_2_0_0_fn_t)(void);


/**
 * Maximum length of MCA project string names.
 */
#define MCA_BASE_MAX_PROJECT_NAME_LEN 15
/**
 * Maximum length of MCA framework string names.
 */
#define MCA_BASE_MAX_TYPE_NAME_LEN 31
/**
 * Maximum length of MCA component string names.
 */
#define MCA_BASE_MAX_COMPONENT_NAME_LEN 63

/**
 * Component flags (mca_component_flags field)
 */
enum {
    /** Always consider this component for selection. For this flag to
     * work properly the component must always be built statically.
     *
     * Enforecement of this flag is left to the individual frameworks
     * so as to limit its use. See discussion from the Open MPI
     * face-to-face meeting Jan. 2017 */
    MCA_BASE_COMPONENT_FLAG_REQUIRED = 1,
};

/**
 * Common type for all MCA components.
 *
 * An instance of this type is always the first element in MCA
 * components, allowing the component to be associated with a
 * particular version of a specific framework, and to publish its own
 * name and version.
 */
struct mca_base_component_2_1_0_t {

  int mca_major_version;
  /**< Major number of the MCA. */
  int mca_minor_version;
  /**< Minor number of the MCA. */
  int mca_release_version;
  /**< Release number of the MCA. */

  char mca_project_name[MCA_BASE_MAX_PROJECT_NAME_LEN + 1];
  /**< String name of the project that this component belongs to. */
  int mca_project_major_version;
  /**< Major version number of the project that this component
     belongs to. */
  int mca_project_minor_version;
  /**< Minor version number of the project that this component
     belongs to. */
  int mca_project_release_version;
  /**< Release version number of the project that this component
     belongs to. */

  char mca_type_name[MCA_BASE_MAX_TYPE_NAME_LEN + 1];
  /**< String name of the framework that this component belongs to. */
  int mca_type_major_version;
  /**< Major version number of the framework that this component
     belongs to. */
  int mca_type_minor_version;
  /**< Minor version number of the framework that this component
     belongs to. */
  int mca_type_release_version;
  /**< Release version number of the framework that this component
     belongs to. */

  char mca_component_name[MCA_BASE_MAX_COMPONENT_NAME_LEN + 1];
  /**< This comopnent's string name. */
  int mca_component_major_version;
  /**< This component's major version number. */
  int mca_component_minor_version;
  /**< This component's minor version number. */
  int mca_component_release_version;
  /**< This component's release version number. */

  mca_base_open_component_1_0_0_fn_t mca_open_component;
  /**< Method for opening this component. */
  mca_base_close_component_1_0_0_fn_t mca_close_component;
  /**< Method for closing this component. */
  mca_base_query_component_2_0_0_fn_t mca_query_component;
  /**< Method for querying this component. */
  mca_base_register_component_params_2_0_0_fn_t mca_register_component_params;
  /**< Method for registering the component's MCA parameters */

  int32_t mca_component_flags;
  /**< flags for this component */

  /** Extra space to allow for expansion in the future without
      breaking older components. */
  char reserved[28];
};
/** Unversioned convenience typedef; use this name in
    frameworks/components to stay forward source-compatible */
typedef struct mca_base_component_2_1_0_t mca_base_component_t;
/** Versioned convenience typedef */
typedef struct mca_base_component_2_1_0_t mca_base_component_2_1_0_t;

/*
 * Metadata Bit field parameters
 */
#define MCA_BASE_METADATA_PARAM_NONE        (uint32_t)0x00 /**< No Metadata flags */
#define MCA_BASE_METADATA_PARAM_CHECKPOINT  (uint32_t)0x02 /**< Checkpoint enabled Component */
#define MCA_BASE_METADATA_PARAM_DEBUG       (uint32_t)0x04 /**< Debug enabled/only Component */

/**
 * Meta data for MCA v2.0.0 components.
 */
struct mca_base_component_data_2_0_0_t {
    uint32_t param_field;
    /**< Metadata parameter bit field filled in by the parameters
         defined above */

    /** Extra space to allow for expansion in the future without
        breaking older components. */
    char reserved[32];
};
/** Unversioned convenience typedef; use this name in
    frameworks/components to stay forward source-compatible */
typedef struct mca_base_component_data_2_0_0_t mca_base_component_data_t;
/** Versioned convenience typedef */
typedef struct mca_base_component_data_2_0_0_t mca_base_component_data_2_0_0_t;

/**
 * Macro for framework author convenience.
 *
 * This macro is used by frameworks defining their component types,
 * indicating that they subscribe to the MCA version 2.0.0.  See
 * component header files (e.g., coll.h) for examples of its usage.
 */
#define MCA_BASE_VERSION_MAJOR 2
#define MCA_BASE_VERSION_MINOR 1
#define MCA_BASE_VERSION_RELEASE 0

#define MCA_BASE_MAKE_VERSION(level, MAJOR, MINOR, RELEASE) \
    .mca_## level ##_major_version = MAJOR,                 \
    .mca_## level ##_minor_version = MINOR,                 \
    .mca_## level ##_release_version = RELEASE


#define MCA_BASE_VERSION_2_1_0(PROJECT, project_major, project_minor, project_release, TYPE, type_major, type_minor, type_release) \
    .mca_major_version = MCA_BASE_VERSION_MAJOR,                        \
    .mca_minor_version = MCA_BASE_VERSION_MINOR,                        \
    .mca_release_version = MCA_BASE_VERSION_RELEASE,                    \
    .mca_project_name = PROJECT,                                        \
    MCA_BASE_MAKE_VERSION(project, project_major, project_minor, project_release), \
    .mca_type_name = TYPE,                                              \
    MCA_BASE_MAKE_VERSION(type, type_major, type_minor, type_release)

#define OPAL_MCA_BASE_VERSION_2_1_0(type, type_major, type_minor, type_release) \
    MCA_BASE_VERSION_2_1_0("opal", OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION, OPAL_RELEASE_VERSION, type, type_major, type_minor, type_release)

#endif /* OPAL_MCA_H */

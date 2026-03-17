/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 * This file presents the MCA variable interface.
 *
 * Note that there are two scopes for MCA variables: "normal" and
 * attributes.  Specifically, all MCA variables are "normal" -- some
 * are special and may also be found on attributes on communicators,
 * datatypes, or windows.
 *
 * In general, these functions are intended to be used as follows:
 *
 * - Creating MCA variables
 * -# Register a variable, get an index back
 * - Using MCA variables
 * -# Lookup a "normal" variable value on a specific index, or
 * -# Lookup an attribute variable on a specific index and
 *    communicator / datatype / window.
 *
 * MCA variables can be defined in multiple different places.  As
 * such, variables are \em resolved to find their value.  The order
 * of resolution is as follows:
 *
 * - An "override" location that is only available to be set via the
 *   pmix_mca_base_param API.
 * - Look for an environment variable corresponding to the MCA
 *   variable.
 * - See if a file contains the MCA variable (MCA variable files are
 *   read only once -- when the first time any mca_param_t function is
 *   invoked).
 * - If nothing else was found, use the variable's default value.
 *
 * Note that there is a second header file (pmix_mca_base_vari.h)
 * that contains several internal type delcarations for the variable
 * system.  The internal file is only used within the variable system
 * itself; it should not be required by any other PMIX entities.
 */

#ifndef PMIX_MCA_BASE_VAR_H
#define PMIX_MCA_BASE_VAR_H

#include <src/include/pmix_config.h>

#include "src/class/pmix_list.h"
#include "src/class/pmix_value_array.h"
#include "src/mca/base/pmix_mca_base_var_enum.h"
#include "src/mca/base/pmix_mca_base_var_group.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/mca/mca.h"

/**
 * The types of MCA variables.
 */
typedef enum {
    /** The variable is of type int. */
    PMIX_MCA_BASE_VAR_TYPE_INT,
    /** The variable is of type unsigned int */
    PMIX_MCA_BASE_VAR_TYPE_UNSIGNED_INT,
    /** The variable is of type unsigned long */
    PMIX_MCA_BASE_VAR_TYPE_UNSIGNED_LONG,
    /** The variable is of type unsigned long long */
    PMIX_MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG,
    /** The variable is of type size_t */
    PMIX_MCA_BASE_VAR_TYPE_SIZE_T,
    /** The variable is of type string. */
    PMIX_MCA_BASE_VAR_TYPE_STRING,
    /** The variable is of type string and contains version. */
    PMIX_MCA_BASE_VAR_TYPE_VERSION_STRING,
    /** The variable is of type bool */
    PMIX_MCA_BASE_VAR_TYPE_BOOL,
    /** The variable is of type double */
    PMIX_MCA_BASE_VAR_TYPE_DOUBLE,
    /** Maximum variable type. */
    PMIX_MCA_BASE_VAR_TYPE_MAX
} pmix_mca_base_var_type_t;

PMIX_EXPORT extern const char *pmix_var_type_names[];

/**
 * Source of an MCA variable's value
 */
typedef enum {
    /** The default value */
    PMIX_MCA_BASE_VAR_SOURCE_DEFAULT,
    /** The value came from the command line */
    PMIX_MCA_BASE_VAR_SOURCE_COMMAND_LINE,
    /** The value came from the environment */
    PMIX_MCA_BASE_VAR_SOURCE_ENV,
    /** The value came from a file */
    PMIX_MCA_BASE_VAR_SOURCE_FILE,
    /** The value came a "set" API call */
    PMIX_MCA_BASE_VAR_SOURCE_SET,
    /** The value came from the override file */
    PMIX_MCA_BASE_VAR_SOURCE_OVERRIDE,

    /** Maximum source type */
    PMIX_MCA_BASE_VAR_SOURCE_MAX
} pmix_mca_base_var_source_t;

/**
 * MCA variable scopes
 *
 * Equivalent to MPI_T scopes with the same base name (e.g.,
 * MCA_BASE_VAR_SCOPE_CONSTANT corresponts to MPI_T_SCOPE_CONSTANT).
 */
typedef enum {
    /** The value of this variable will not change after it is
        registered.  This flag is incompatible with
        MCA_BASE_VAR_FLAG_SETTABLE, and also implies
        MCA_BASE_VAR_SCOPE_READONLY. */
    PMIX_MCA_BASE_VAR_SCOPE_CONSTANT,
    /** Setting the READONLY flag means that the pmix_mca_base_var_set()
        function cannot be used to set the value of this variable
        (e.g., the MPI_T_cvar_write() MPI_T function). */
    PMIX_MCA_BASE_VAR_SCOPE_READONLY,
    /** The value of this variable may be changed locally. */
    PMIX_MCA_BASE_VAR_SCOPE_LOCAL,
    /** The value of this variable must be set to a consistent value
        within a group */
    PMIX_MCA_BASE_VAR_SCOPE_GROUP,
    /** The value of this variable must be set to the same value
        within a group */
    PMIX_MCA_BASE_VAR_SCOPE_GROUP_EQ,
    /** The value of this variable must be set to a consistent value
        for all processes */
    PMIX_MCA_BASE_VAR_SCOPE_ALL,
    /** The value of this variable must be set to the same value
        for all processes */
    PMIX_MCA_BASE_VAR_SCOPE_ALL_EQ,
    PMIX_MCA_BASE_VAR_SCOPE_MAX
} pmix_mca_base_var_scope_t;

typedef enum {
    PMIX_INFO_LVL_1,
    PMIX_INFO_LVL_2,
    PMIX_INFO_LVL_3,
    PMIX_INFO_LVL_4,
    PMIX_INFO_LVL_5,
    PMIX_INFO_LVL_6,
    PMIX_INFO_LVL_7,
    PMIX_INFO_LVL_8,
    PMIX_INFO_LVL_9,
    PMIX_INFO_LVL_MAX
} pmix_mca_base_var_info_lvl_t;

typedef enum {
    PMIX_MCA_BASE_VAR_SYN_FLAG_DEPRECATED = 0x0001,
    PMIX_MCA_BASE_VAR_SYN_FLAG_INTERNAL   = 0x0002
} pmix_mca_base_var_syn_flag_t;

typedef enum {
    /** Variable is internal (hidden from *_info/MPIT) */
    PMIX_MCA_BASE_VAR_FLAG_INTERNAL     = 0x0001,
    /** Variable will always be the default value. Implies
        !MCA_BASE_VAR_FLAG_SETTABLE */
    PMIX_MCA_BASE_VAR_FLAG_DEFAULT_ONLY = 0x0002,
    /** Variable can be set with pmix_mca_base_var_set() */
    PMIX_MCA_BASE_VAR_FLAG_SETTABLE     = 0x0004,
    /** Variable is deprecated */
    PMIX_MCA_BASE_VAR_FLAG_DEPRECATED   = 0x0008,
    /** Variable has been overridden */
    PMIX_MCA_BASE_VAR_FLAG_OVERRIDE     = 0x0010,
    /** Variable may not be set from a file */
    PMIX_MCA_BASE_VAR_FLAG_ENVIRONMENT_ONLY = 0x0020,
    /** Variable should be deregistered when the group is deregistered
        (DWG = "deregister with group").  This flag is set
        automatically when you register a variable with
        pmix_mca_base_component_var_register(), but can also be set
        manually when you register a variable with
        pmix_mca_base_var_register().  Analogous to the
        MCA_BASE_PVAR_FLAG_IWG. */
    PMIX_MCA_BASE_VAR_FLAG_DWG          = 0x0040
} pmix_mca_base_var_flag_t;


/**
 * Types for MCA parameters.
 */
typedef union {
    /** integer value */
    int intval;
    /** unsigned int value */
    unsigned int uintval;
    /** string value */
    char *stringval;
    /** boolean value */
    bool boolval;
    /** unsigned long value */
    unsigned long ulval;
    /** unsigned long long value */
    unsigned long long ullval;
    /** size_t value */
    size_t sizetval;
    /** double value */
    double lfval;
} pmix_mca_base_var_storage_t;


/**
 * Entry for holding information about an MCA variable.
 */
struct pmix_mca_base_var_t {
    /** Allow this to be an PMIX OBJ */
    pmix_object_t super;

    /** Variable index. This will remain constant until pmix_mca_base_var_finalize()
        is called. */
    int mbv_index;
    /** Group index. This will remain constant until pmix_mca_base_var_finalize()
        is called. This variable will be deregistered if the associated group
        is deregistered with pmix_mca_base_var_group_deregister() */
    int mbv_group_index;

    /** Info level of this variable */
    pmix_mca_base_var_info_lvl_t mbv_info_lvl;

    /** Enum indicating the type of the variable (integer, string, boolean) */
     pmix_mca_base_var_type_t mbv_type;

    /** String of the variable name */
    char *mbv_variable_name;
    /** Full variable name, in case it is not <framework>_<component>_<param> */
    char *mbv_full_name;
    /** Long variable name <project>_<framework>_<component>_<name> */
    char *mbv_long_name;

    /** List of synonym names for this variable.  This *must* be a
        pointer (vs. a plain pmix_list_t) because we copy this whole
        struct into a new var for permanent storage
        (pmix_vale_array_append_item()), and the internal pointers in
        the pmix_list_t will be invalid when that happens.  Hence, we
        simply keep a pointer to an external pmix_list_t.  Synonyms
        are uncommon enough that this is not a big performance hit. */
    pmix_value_array_t mbv_synonyms;

    /** Variable flags */
    pmix_mca_base_var_flag_t  mbv_flags;

    /** Variable scope */
    pmix_mca_base_var_scope_t  mbv_scope;

    /** Source of the current value */
    pmix_mca_base_var_source_t mbv_source;

    /** Synonym for */
    int mbv_synonym_for;

    /** Variable description */
    char *mbv_description;

    /** File the value came from */
    char *mbv_source_file;

    /** Value enumerator (only valid for integer variables) */
    pmix_mca_base_var_enum_t *mbv_enumerator;

    /** Bind value for this variable (0 - none) */
    int mbv_bind;

    /** Storage for this variable */
    pmix_mca_base_var_storage_t *mbv_storage;

    /** File value structure */
    void *mbv_file_value;
};
/**
 * Convenience typedef.
 */
typedef struct pmix_mca_base_var_t pmix_mca_base_var_t;

/*
 * Global functions for MCA
 */

BEGIN_C_DECLS

/**
 * Object declarayion for pmix_mca_base_var_t
 */
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_mca_base_var_t);

/**
 * Initialize the MCA variable system.
 *
 * @retval PMIX_SUCCESS
 *
 * This function initalizes the MCA variable system.  It is
 * invoked internally (by pmix_mca_base_open()) and is only documented
 * here for completeness.
 */
PMIX_EXPORT int pmix_mca_base_var_init(void);

/**
 * Register an MCA variable
 *
 * @param[in] project_name The name of the project associated with
 * this variable
 * @param[in] framework_name The name of the framework associated with
 * this variable
 * @param[in] component_name The name of the component associated with
 * this variable
 * @param[in] variable_name The name of this variable
 * @param[in] description A string describing the use and valid
 * values of the variable (string).
 * @param[in] type The type of this variable (string, int, bool).
 * @param[in] enumerator Enumerator describing valid values.
 * @param[in] bind Hint for MPIT to specify type of binding (0 = none)
 * @param[in] flags Flags for this variable.
 * @param[in] info_lvl Info level of this variable
 * @param[in] scope Indicates the scope of this variable
 * @param[in,out] storage Pointer to the value's location.
 *
 * @retval index Index value representing this variable.
 * @retval PMIX_ERR_OUT_OF_RESOURCE Upon failure to allocate memory.
 * @retval PMIX_ERROR Upon failure to register the variable.
 *
 * This function registers an MCA variable and associates it
 * with a specific group.
 *
 * {description} is a string of arbitrary length (verbose is good!)
 * for explaining what the variable is for and what its valid values
 * are.  This message is used in help messages, such as the output
 * from the ompi_info executable.  The {description} string is copied
 * internally; the caller can free {description} upon successful
 * return.
 *
 * {enumerator} is either NULL or a handle that was created via
 * pmix_mca_base_var_enum_create(), and describes the valid values of an
 * integer variable (i.e., one with type MCA_BASE_VAR_TYPE_INT).  When
 * a non-NULL {enumerator} is used, the value set for this variable by
 * the user will be compared against the values in the enumerator.
 * The MCA variable system will allow the parameter to be set to
 * either one of the enumerator values (0, 1, 2, etc) or a string
 * representing one of those values.  {enumerator} is retained until
 * either the variable is deregistered using
 * pmix_mca_base_var_deregister(), pmix_mca_base_var_group_deregister(), or
 * pmix_mca_base_var_finalize().  {enumerator} should be NULL for
 * parameters that do not support enumerated values.
 *
 * {flags} indicate attributes of this variable (internal, settable,
 * default only, etc.), as listed below.
 *
 * If MCA_BASE_VAR_FLAG_SETTABLE is set in {flags}, this variable may
 * be set using pmix_mca_base_var_set_value() (i.e., the MPI_T interface).
 *
 * If MCA_BASE_VAR_FLAG_INTERNAL is set in {flags}, this variable
 * is not shown by default in the output of ompi_info.  That is,
 * this variable is considered internal to the PMIX implementation
 * and is not supposed to be viewed / changed by the user.
 *
 * If MCA_BASE_VAR_FLAG_DEFAULT_ONLY is set in {flags}, then the value
 * provided in storage will not be modified by the MCA variable system
 * (i.e., users cannot set the value of this variable via CLI
 * parameter, environment variable, file, etc.). It is up to the
 * caller to specify (using the scope) if this value may change
 * (MCA_BASE_VAR_SCOPE_READONLY) or remain constant
 * (MCA_BASE_VAR_SCOPE_CONSTANT).  MCA_BASE_VAR_FLAG_DEFAULT_ONLY must
 * not be specified with MCA_BASE_VAR_FLAG_SETTABLE.
 *
 * Set MCA_BASE_VAR_FLAG_DEPRECATED in {flags} to indicate that
 * this variable name is deprecated. The user will get a warning
 * if they set this variable.
 *
 * {scope} is for informational purposes to indicate how this variable
 * can be set, or if it is considered constant or readonly (which, by
 * MPI_T's definitions, are different things).  See the comments in
 * the description of pmix_mca_base_var_scope_t for information about the
 * different scope meanings.
 *
 * {storage} points to a (char *), (int), or (bool) where the value of
 * this variable is stored ({type} indicates the type of this
 * pointer).  The location pointed to by {storage} must exist until
 * the variable is deregistered.  Note that the initial value in
 * {storage} may be overwritten if the MCA_BASE_VAR_FLAG_DEFAULT_ONLY
 * flag is not set (e.g., if the user sets this variable via CLI
 * option, environment variable, or file value).  If input value of
 * {storage} points to a (char *), the pointed-to string will be
 * duplicated and maintained internally by the MCA variable system;
 * the caller may free the original string after this function returns
 * successfully.
 */
PMIX_EXPORT int pmix_mca_base_var_register (const char *project_name, const char *framework_name,
                                const char *component_name, const char *variable_name,
                                const char *description, pmix_mca_base_var_type_t type,
                                pmix_mca_base_var_enum_t *enumerator, int bind, pmix_mca_base_var_flag_t flags,
                                pmix_mca_base_var_info_lvl_t info_lvl,
                                pmix_mca_base_var_scope_t scope, void *storage);

/**
 * Convenience function for registering a variable associated with a
 * component.
 *
 * While quite similar to pmix_mca_base_var_register(), there is one key
 * difference: vars registered this this function will automatically
 * be unregistered / made unavailable when that component is closed by
 * its framework.
 */
PMIX_EXPORT int pmix_mca_base_component_var_register (const pmix_mca_base_component_t *component,
                                          const char *variable_name, const char *description,
                                          pmix_mca_base_var_type_t type, pmix_mca_base_var_enum_t *enumerator,
                                          int bind, pmix_mca_base_var_flag_t flags,
                                          pmix_mca_base_var_info_lvl_t info_lvl,
                                          pmix_mca_base_var_scope_t scope, void *storage);

/**
 * Convenience function for registering a variable associated with a framework. This
 * function is equivalent to pmix_mca_base_var_register with component_name = "base" and
 * with the MCA_BASE_VAR_FLAG_DWG set. See pmix_mca_base_var_register().
 */
PMIX_EXPORT int pmix_mca_base_framework_var_register (const pmix_mca_base_framework_t *framework,
                                          const char *variable_name,
                                          const char *help_msg, pmix_mca_base_var_type_t type,
                                          pmix_mca_base_var_enum_t *enumerator, int bind,
                                          pmix_mca_base_var_flag_t flags,
                                          pmix_mca_base_var_info_lvl_t info_level,
                                          pmix_mca_base_var_scope_t scope, void *storage);

/**
 * Register a synonym name for an MCA variable.
 *
 * @param[in] synonym_for The index of the original variable. This index
 * must not refer to a synonym.
 * @param[in] project_name The project this synonym belongs to. Should
 * not be NULL (except for legacy reasons).
 * @param[in] framework_name The framework this synonym belongs to.
 * @param[in] component_name The component this synonym belongs to.
 * @param[in] synonym_name The synonym name.
 * @param[in] flags Flags for this synonym.
 *
 * @returns index Variable index for new synonym on success.
 * @returns PMIX_ERR_BAD_VAR If synonym_for does not reference a valid
 * variable.
 * @returns PMIX_ERR_OUT_OF_RESOURCE If memory could not be allocated.
 * @returns PMIX_ERROR For all other errors.
 *
 * Upon success, this function creates a synonym MCA variable
 * that will be treated almost exactly like the original.  The
 * type (int or string) is irrelevant; this function simply
 * creates a new name that by which the same variable value is
 * accessible.
 *
 * Note that the original variable name has precendence over all
 * synonyms.  For example, consider the case if variable is
 * originally registered under the name "A" and is later
 * registered with synonyms "B" and "C".  If the user sets values
 * for both MCA variable names "A" and "B", the value associated
 * with the "A" name will be used and the value associated with
 * the "B" will be ignored (and will not even be visible by the
 * pmix_mca_base_var_*() API).  If the user sets values for both MCA
 * variable names "B" and "C" (and does *not* set a value for
 * "A"), it is undefined as to which value will be used.
 */
PMIX_EXPORT int pmix_mca_base_var_register_synonym (int synonym_for, const char *project_name,
                                        const char *framework_name,
                                        const char *component_name,
                                        const char *synonym_name,
                                        pmix_mca_base_var_syn_flag_t flags);

/**
 * Deregister a MCA variable or synonym
 *
 * @param vari Index returned from pmix_mca_base_var_register() or
 * pmix_mca_base_var_register_synonym().
 *
 * Deregistering a variable does not free the variable or any memory assoicated
 * with it. All memory will be freed and the variable index released when
 * pmix_mca_base_var_finalize() is called.
 *
 * If an enumerator is associated with this variable it will be dereferenced.
 */
PMIX_EXPORT int pmix_mca_base_var_deregister(int vari);


/**
 * Get the current value of an MCA variable.
 *
 * @param[in] vari Index of variable
 * @param[in,out] value Pointer to copy the value to. Can be NULL.
 * @param[in,out] value_size Size of memory pointed to by value.
 * copied size will be returned in value_size.
 * @param[out] source Source of current value. Can be NULL.
 * @param[out] source_file Source file for the current value if
 * it was set from a file.
 *
 * @return PMIX_ERROR Upon failure.  The contents of value are
 * undefined.
 * @return PMIX_SUCCESS Upon success. value (if not NULL) will be filled
 * with the variable's current value. value_size will contain the size
 * copied. source (if not NULL) will contain the source of the variable.
 *
 * Note: The value can be changed by the registering code without using
 * the pmix_mca_base_var_* interface so the source may be incorrect.
 */
PMIX_EXPORT int pmix_mca_base_var_get_value (int vari, void *value,
                                             pmix_mca_base_var_source_t *source,
                                             const char **source_file);

/**
 * Sets an "override" value for an integer MCA variable.
 *
 * @param[in] vari Index of MCA variable to set
 * @param[in] value Pointer to the value to set. Should point to
 * a char * for string variables or a int * for integer variables.
 * @param[in] size Size of value.
 * @param[in] source Source of this value.
 * @param[in] source_file Source file if source is MCA_BASE_VAR_SOURCE_FILE.
 *
 * @retval PMIX_SUCCESS  Upon success.
 * @retval PMIX_ERR_PERM If the variable is not settable.
 * @retval PMIX_ERR_BAD_PARAM If the variable does not exist or has
 * been deregistered.
 * @retval PMIX_ERROR On other error.
 *
 * This function sets the value of an MCA variable. This value will
 * overwrite the current value of the variable (or if index represents
 * a synonym the variable the synonym represents) if the value is
 * settable.
 */
PMIX_EXPORT int pmix_mca_base_var_set_value (int vari, const void *value, size_t size,
                                             pmix_mca_base_var_source_t source,
                                             const char *source_file);

/**
 * Get the string name corresponding to the MCA variable
 * value in the environment.
 *
 * @param param_name Name of the type containing the variable.
 *
 * @retval string A string suitable for setenv() or appending to
 * an environ-style string array.
 * @retval NULL Upon failure.
 *
 * The string that is returned is owned by the caller; if
 * appropriate, it must be eventually freed by the caller.
 */
PMIX_EXPORT int pmix_mca_base_var_env_name(const char *param_name,
                                           char **env_name);

/**
 * Find the index for an MCA variable based on its names.
 *
 * @param project_name   Name of the project
 * @param type_name      Name of the type containing the variable.
 * @param component_name Name of the component containing the variable.
 * @param param_name     Name of the variable.
 *
 * @retval PMIX_ERROR If the variable was not found.
 * @retval vari If the variable was found.
 *
 * It is not always convenient to widely propagate a variable's index
 * value, or it may be necessary to look up the variable from a
 * different component. This function can be used to look up the index
 * of any registered variable.  The returned index can be used with
 * pmix_mca_base_var_get() and pmix_mca_base_var_get_value().
 */
PMIX_EXPORT int pmix_mca_base_var_find (const char *project_name,
                                        const char *type_name,
                                        const char *component_name,
                                        const char *param_name);

/**
 * Find the index for a variable based on its full name
 *
 * @param full_name [in] Full name of the variable
 * @param vari [out]    Index of the variable
 *
 * See pmix_mca_base_var_find().
 */
PMIX_EXPORT int pmix_mca_base_var_find_by_name (const char *full_name, int *vari);

/**
 * Check that two MCA variables were not both set to non-default
 * values.
 *
 * @param type_a [in] Framework name of variable A (string).
 * @param component_a [in] Component name of variable A (string).
 * @param param_a [in] Variable name of variable A (string.
 * @param type_b [in] Framework name of variable A (string).
 * @param component_b [in] Component name of variable A (string).
 * @param param_b [in] Variable name of variable A (string.
 *
 * This function is useful for checking that the user did not set both
 * of 2 mutually-exclusive MCA variables.
 *
 * This function will print an pmix_show_help() message and return
 * PMIX_ERR_BAD_VAR if it finds that the two variables both have
 * value sources that are not MCA_BASE_VAR_SOURCE_DEFAULT.  This
 * means that both variables have been set by the user (i.e., they're
 * not default values).
 *
 * Note that pmix_show_help() allows itself to be hooked, so if this
 * happens after the aggregated pmix_show_help() system is
 * initialized, the messages will be aggregated (w00t).
 *
 * @returns PMIX_ERR_BAD_VAR if the two variables have sources that
 * are not MCA_BASE_VAR_SOURCE_DEFAULT.
 * @returns PMIX_SUCCESS otherwise.
 */
PMIX_EXPORT int pmix_mca_base_var_check_exclusive (const char *project,
                                                   const char *type_a,
                                                   const char *component_a,
                                                   const char *param_a,
                                                   const char *type_b,
                                                   const char *component_b,
                                                   const char *param_b);

/**
 * Set or unset a flag on a variable.
 *
 * @param[in] vari Index of variable
 * @param[in] flag Flag(s) to set or unset.
 * @param[in] set Boolean indicating whether to set flag(s).
 *
 * @returns PMIX_SUCCESS If the flags are set successfully.
 * @returns PMIX_ERR_BAD_PARAM If the variable is not registered.
 * @returns PMIX_ERROR Otherwise
 */
PMIX_EXPORT int pmix_mca_base_var_set_flag(int vari, pmix_mca_base_var_flag_t flag,
                                           bool set);

/**
 * Obtain basic info on a single variable (name, help message, etc)
 *
 * @param[in] vari Valid variable index.
 * @param[out] var Storage for the variable pointer.
 *
 * @retval PMIX_SUCCESS Upon success.
 * @retval pmix error code Upon failure.
 *
 * The returned pointer belongs to the MCA variable system. Do not
 * modify/free/retain the pointer.
 */
PMIX_EXPORT int pmix_mca_base_var_get (int vari, const pmix_mca_base_var_t **var);

/**
 * Obtain the number of variables that have been registered.
 *
 * @retval count on success
 * @return pmix error code on error
 *
 * Note: This function does not return the number of valid MCA variables as
 * pmix_mca_base_var_deregister() has no impact on the variable count. The count
 * returned is equal to the number of calls to pmix_mca_base_var_register with
 * unique names. ie. two calls with the same name will not affect the count.
 */
PMIX_EXPORT int pmix_mca_base_var_get_count (void);

/**
 * Obtain a list of enironment variables describing the all
 * valid (non-default) MCA variables and their sources.
 *
 * @param[out] env A pointer to an argv-style array of key=value
 * strings, suitable for use in an environment
 * @param[out] num_env A pointer to an int, containing the length
 * of the env array (not including the final NULL entry).
 * @param[in] internal Whether to include internal variables.
 *
 * @retval PMIX_SUCCESS Upon success.
 * @retval PMIX_ERROR Upon failure.
 *
 * This function is similar to pmix_mca_base_var_dump() except that
 * its output is in terms of an argv-style array of key=value
 * strings, suitable for using in an environment.
 */
PMIX_EXPORT int pmix_mca_base_var_build_env(char ***env, int *num_env,
                                            bool internal);

/**
 * Shut down the MCA variable system (normally only invoked by the
 * MCA framework itself).
 *
 * @returns PMIX_SUCCESS This function never fails.
 *
 * This function shuts down the MCA variable repository and frees all
 * associated memory.  No other pmix_mca_base_var*() functions can be
 * invoked after this function.
 *
 * This function is normally only invoked by the MCA framework itself
 * when the process is shutting down (e.g., during MPI_FINALIZE).  It
 * is only documented here for completeness.
 */
PMIX_EXPORT int pmix_mca_base_var_finalize(void);

typedef enum {
    /* Dump human-readable strings */
    PMIX_MCA_BASE_VAR_DUMP_READABLE = 0,
    /* Dump easily parsable strings */
    PMIX_MCA_BASE_VAR_DUMP_PARSABLE = 1,
    /* Dump simple name=value string */
    PMIX_MCA_BASE_VAR_DUMP_SIMPLE   = 2
} pmix_mca_base_var_dump_type_t;

/**
 * Dump strings describing the MCA variable at an index.
 *
 * @param[in]  vari        Variable index
 * @param[out] out         Array of strings describing this variable
 * @param[in]  output_type Type of output desired
 *
 * This function returns an array of strings describing the variable. All strings
 * and the array must be freed by the caller.
 */
PMIX_EXPORT int pmix_mca_base_var_dump(int vari, char ***out, pmix_mca_base_var_dump_type_t output_type);

#define MCA_COMPILETIME_VER "print_compiletime_version"
#define MCA_RUNTIME_VER "print_runtime_version"

PMIX_EXPORT int pmix_mca_base_var_cache_files (bool rel_path_search);

/*
 * Parse a provided list of envars and add their local value, or
 * their assigned value, to the provided argv
 */
PMIX_EXPORT int pmix_mca_base_var_process_env_list(char ***argv);
PMIX_EXPORT int pmix_mca_base_var_process_env_list_from_file(char ***argv);

END_C_DECLS

#endif /* PMIX_MCA_BASE_VAR_H */

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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * This is the private declarations for the MCA variable system.
 * This file is internal to the MCA variable system and should not
 * need to be used by any other elements in Open MPI except the
 * special case of the ompi_info command.
 *
 * All the rest of the doxygen documentation in this file is marked as
 * "internal" and won't show up unless you specifically tell doxygen
 * to generate internal documentation (by default, it is skipped).
 */

#ifndef OPAL_MCA_BASE_VAR_INTERNAL_H
#define OPAL_MCA_BASE_VAR_INTERNAL_H

#include "opal_config.h"

#include "opal/class/opal_object.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_value_array.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_hash_table.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/mca/base/mca_base_pvar.h"

BEGIN_C_DECLS

/* Internal flags start at bit 16 */
#define MCA_BASE_VAR_FLAG_EXTERNAL_MASK 0x0000ffff

typedef enum {
    /** Variable is valid */
    MCA_BASE_VAR_FLAG_VALID   = 0x00010000,
    /** Variable is a synonym */
    MCA_BASE_VAR_FLAG_SYNONYM = 0x00020000,
    /** mbv_source_file needs to be freed */
    MCA_BASE_VAR_FLAG_SOURCE_FILE_NEEDS_FREE = 0x00040000
} mca_base_var_flag_internal_t;

#define VAR_FLAG_ISSET(var, flag) (!!((var).mbp_flags & (flag)))

#define VAR_IS_VALID(var) (!!((var).mbv_flags & MCA_BASE_VAR_FLAG_VALID))
#define VAR_IS_SYNONYM(var) (!!((var).mbv_flags & MCA_BASE_VAR_FLAG_SYNONYM))
#define VAR_IS_INTERNAL(var) (!!((var).mbv_flags & MCA_BASE_VAR_FLAG_INTERNAL))
#define VAR_IS_DEFAULT_ONLY(var) (!!((var).mbv_flags & MCA_BASE_VAR_FLAG_DEFAULT_ONLY))
#define VAR_IS_SETTABLE(var) (!!((var).mbv_flags & MCA_BASE_VAR_FLAG_SETTABLE))
#define VAR_IS_DEPRECATED(var) (!!((var).mbv_flags & MCA_BASE_VAR_FLAG_DEPRECATED))

extern const char *ompi_var_type_names[];
extern const size_t ompi_var_type_sizes[];
extern bool mca_base_var_initialized;

/**
 * \internal
 *
 * Structure for holding param names and values read in from files.
 */
struct mca_base_var_file_value_t {
    /** Allow this to be an OPAL OBJ */
    opal_list_item_t super;

    /** Parameter name */
    char *mbvfv_var;
    /** Parameter value */
    char *mbvfv_value;
    /** File it came from */
    char *mbvfv_file;
    /** Line it came from */
    int mbvfv_lineno;
};

/**
 * \internal
 *
 * Convenience typedef
 */
typedef struct mca_base_var_file_value_t mca_base_var_file_value_t;

/**
 * Object declaration for mca_base_var_file_value_t
 */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_base_var_file_value_t);

/**
 * \internal
 *
 * Get a group
 *
 * @param[in]  group_index Group index
 * @param[out] group       Returned group if it exists
 * @param[in]  invalidok   Return group even if it has been deregistered
 */
OPAL_DECLSPEC int mca_base_var_group_get_internal (const int group_index, mca_base_var_group_t **group, bool invalidok);

/**
 * \internal
 *
 * Parse a parameter file.
 */
OPAL_DECLSPEC int mca_base_parse_paramfile(const char *paramfile, opal_list_t *list);

/**
 * \internal
 *
 * Add a variable to a group
 */
OPAL_DECLSPEC int mca_base_var_group_add_var (const int group_index, const int param_index);

/**
 * \internal
 *
 * Add a performance variable to a group
 */
OPAL_DECLSPEC int mca_base_var_group_add_pvar (const int group_index, const int param_index);

/**
 * \internal
 *
 * Add an enum to a group
 */
OPAL_DECLSPEC int mca_base_var_group_add_enum (const int group_index, const void *storage);

/**
 * \internal
 *
 * Generate a full name with _ between all of the non-NULL arguments
 */
OPAL_DECLSPEC int mca_base_var_generate_full_name4 (const char *project, const char *framework,
                                                    const char *component, const char *variable,
                                                    char **full_name);

/**
 * \internal
 *
 * Call save_value callback for generated internal mca parameter storing env variables
 */
OPAL_DECLSPEC int mca_base_internal_env_store(void);

/**
 * \internal
 *
 * Initialize/finalize MCA variable groups
 */
OPAL_DECLSPEC int mca_base_var_group_init (void);
OPAL_DECLSPEC int mca_base_var_group_finalize (void);

/**
 * \internal
 *
 * Initialize MCA performance variables
 */
OPAL_DECLSPEC int mca_base_pvar_init (void);
OPAL_DECLSPEC int mca_base_pvar_finalize (void);

END_C_DECLS

#endif /* OPAL_MCA_BASE_VAR_INTERNAL_H */

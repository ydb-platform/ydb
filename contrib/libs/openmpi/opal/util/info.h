/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017-2018 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_INFO_H
#define OPAL_INFO_H

#include <string.h>

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/threads/mutex.h"
#include "opal/mca/base/mca_base_var_enum.h"

/**
 * \internal
 * opal_info_t structure. MPI_Info is a pointer to this structure
 */

struct opal_info_t {
  opal_list_t   super;
  opal_mutex_t	*i_lock;
};

/**
 * \internal
 * Convenience typedef
 */
typedef struct opal_info_t opal_info_t;


/**
 * Table for Fortran <-> C translation table
 */
extern opal_pointer_array_t ompi_info_f_to_c_table;


/**
 * \internal
 *
 * opal_info_entry_t object. Each item in opal_info_list is of this
 * type. It contains (key,value) pairs
 */
struct opal_info_entry_t {
    opal_list_item_t super; /**< required for opal_list_t type */
    char *ie_value; /**< value part of the (key, value) pair.
                  * Maximum length is MPI_MAX_INFO_VAL */
    char ie_key[OPAL_MAX_INFO_KEY + 1]; /**< "key" part of the (key, value)
                                     * pair */
};
/**
 * \internal
 * Convenience typedef
 */
typedef struct opal_info_entry_t opal_info_entry_t;

BEGIN_C_DECLS

/**
 * \internal
 * Some declarations needed to use OBJ_NEW and OBJ_DESTRUCT macros
 */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_info_t);

/**
 * \internal
 * Some declarations needed to use OBJ_NEW and OBJ_DESTRUCT macros
 */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_info_entry_t);


int opal_mpiinfo_init(void*);

/**
 *   opal_info_dup - Duplicate an 'MPI_Info' object
 *
 *   @param info source info object (handle)
 *   @param newinfo pointer to the new info object (handle)
 *
 *   @retval OPAL_SUCCESS upon success
 *   @retval OPAL_ERR_OUT_OF_RESOURCE if out of memory
 *
 *   Not only will the (key, value) pairs be duplicated, the order
 *   of keys will be the same in 'newinfo' as it is in 'info'.  When
 *   an info object is no longer being used, it should be freed with
 *   'MPI_Info_free'.
 */
int opal_info_dup (opal_info_t *info, opal_info_t **newinfo);

// Comments might still say __IN_<key>, but the code should be using the
// below macro instead.
#define OPAL_INFO_SAVE_PREFIX "_OMPI_IN_"

/**
 *   opal_info_dup_mpistandard - Duplicate an 'MPI_Info' object
 *
 *   @param info source info object (handle)
 *   @param newinfo pointer to the new info object (handle)
 *
 *   @retval OPAL_SUCCESS upon success
 *   @retval OPAL_ERR_OUT_OF_RESOURCE if out of memory
 *
 *   The user sets an info object with key/value pairs and once processed,
 *   we keep key/val pairs that might have been modified vs what the user
 *   provided, and some user inputs might have been ignored too.  The original
 *   user inpust are kept as __IN_<key>/<val>.
 *
 *   This routine then outputs key/value pairs as:
 *
 *   if <key> and __IN_<key> both exist:
 *       This means the user set a k/v pair and it was used.
 *       output: <key> / value(__IN_<key>), the original user input
 *   if <key> exists but __IN_<key> doesn't:
 *       This is a system-provided setting.
 *       output: <key>/value(<key>)
 *   if __IN_<key> exists but <key> doesn't:
 *       The user provided a setting that was rejected (ignored) by the system
 *       output: nothing for this key
 */
int opal_info_dup_mpistandard (opal_info_t *info, opal_info_t **newinfo);

/**
 * Set a new key,value pair on info.
 *
 * @param info pointer to opal_info_t object
 * @param key pointer to the new key object
 * @param value pointer to the new value object
 *
 * @retval OPAL_SUCCESS upon success
 * @retval OPAL_ERR_OUT_OF_RESOURCE if out of memory
 */
OPAL_DECLSPEC int opal_info_set (opal_info_t *info, const char *key, const char *value);

/**
 * Set a new key,value pair from a variable enumerator.
 *
 * @param info pointer to opal_info_t object
 * @param key pointer to the new key object
 * @param value integer value of the info key (must be valid in var_enum)
 * @param var_enum variable enumerator
 *
 * @retval OPAL_SUCCESS upon success
 * @retval OPAL_ERR_OUT_OF_RESOURCE if out of memory
 * @retval OPAL_ERR_VALUE_OUT_OF_BOUNDS if the value is not valid in the enumerator
 */
OPAL_DECLSPEC int opal_info_set_value_enum (opal_info_t *info, const char *key, int value,
                                            mca_base_var_enum_t *var_enum);

/**
 * opal_info_free - Free an 'MPI_Info' object.
 *
 *   @param info pointer to info (opal_info_t *) object to be freed (handle)
 *
 *   @retval OPAL_SUCCESS
 *   @retval OPAL_ERR_BAD_PARAM
 *
 *   Upon successful completion, 'info' will be set to
 *   'MPI_INFO_NULL'.  Free the info handle and all of its keys and
 *   values.
 */
int opal_info_free (opal_info_t **info);

  /**
   *   Get a (key, value) pair from an 'MPI_Info' object and assign it
   *   into a boolen output.
   *
   *   @param info Pointer to opal_info_t object
   *   @param key null-terminated character string of the index key
   *   @param value Boolean output value
   *   @param flag true (1) if 'key' defined on 'info', false (0) if not
   *               (logical)
   *
   *   @retval OPAL_SUCCESS
   *
   *   If found, the string value will be cast to the boolen output in
   *   the following manner:
   *
   *   - If the string value is digits, the return value is "(bool)
   *     atoi(value)"
   *   - If the string value is (case-insensitive) "yes" or "true", the
   *     result is true
   *   - If the string value is (case-insensitive) "no" or "false", the
   *     result is false
   *   - All other values are false
   */
OPAL_DECLSPEC int opal_info_get_bool (opal_info_t *info, char *key, bool *value,
                                      int *flag);

/**
 *   Get a (key, value) pair from an 'MPI_Info' object and assign it
 *   into an integer output based on the enumerator value.
 *
 *   @param info Pointer to opal_info_t object
 *   @param key null-terminated character string of the index key
 *   @param value integer output value
 *   @param default_value value to use if the string does not conform to the
 *          values accepted by the enumerator
 *   @param var_enum variable enumerator for the value
 *   @param flag true (1) if 'key' defined on 'info', false (0) if not
 *               (logical)
 *
 *   @retval OPAL_SUCCESS
 */

OPAL_DECLSPEC int opal_info_get_value_enum (opal_info_t *info, const char *key,
                                            int *value, int default_value,
                                            mca_base_var_enum_t *var_enum, int *flag);

/**
 *   Get a (key, value) pair from an 'MPI_Info' object
 *
 *   @param info Pointer to opal_info_t object
 *   @param key null-terminated character string of the index key
 *   @param valuelen maximum length of 'value' (integer)
 *   @param value null-terminated character string of the value
 *   @param flag true (1) if 'key' defined on 'info', false (0) if not
 *               (logical)
 *
 *   @retval OPAL_SUCCESS
 *
 *   In C and C++, 'valuelen' should be one less than the allocated
 *   space to allow for for the null terminator.
 */
OPAL_DECLSPEC int opal_info_get (opal_info_t *info, const char *key, int valuelen,
                                 char *value, int *flag);

/**
 * Delete a (key,value) pair from "info"
 *
 * @param info opal_info_t pointer on which we need to operate
 * @param key The key portion of the (key,value) pair that
 *            needs to be deleted
 *
 * @retval OPAL_SUCCESS
 * @retval OPAL_ERR_NOT_FOUND
 */
int opal_info_delete(opal_info_t *info, const char *key);

/**
 *   @param info - opal_info_t pointer object (handle)
 *   @param key - null-terminated character string of the index key
 *   @param valuelen - length of the value associated with 'key' (integer)
 *   @param flag - true (1) if 'key' defined on 'info', false (0) if not
 *   (logical)
 *
 *   @retval OPAL_SUCCESS
 *   @retval OPAL_ERR_BAD_PARAM
 *   @retval MPI_ERR_INFO_KEY
 *
 *   The length returned in C and C++ does not include the end-of-string
 *   character.  If the 'key' is not found on 'info', 'valuelen' is left
 *   alone.
 */
OPAL_DECLSPEC int opal_info_get_valuelen (opal_info_t *info, const char *key, int *valuelen,
                                          int *flag);

/**
 *   opal_info_get_nthkey - Get a key indexed by integer from an 'MPI_Info' o
 *
 *   @param info Pointer to opal_info_t object
 *   @param n index of key to retrieve (integer)
 *   @param key character string of at least 'MPI_MAX_INFO_KEY' characters
 *
 *   @retval OPAL_SUCCESS
 *   @retval OPAL_ERR_BAD_PARAM
 */
int opal_info_get_nthkey (opal_info_t *info, int n, char *key);

/**
 * Convert value string to boolean
 *
 * Convert value string \c value into a boolean, using the
 * interpretation rules specified in MPI-2 Section 4.10.  The
 * strings "true", "false", and integer numbers can be converted
 * into booleans.  All others will return \c OMPI_ERR_BAD_PARAM
 *
 * @param value Value string for info key to interpret
 * @param interp returned interpretation of the value key
 *
 * @retval OPAL_SUCCESS string was successfully interpreted
 * @retval OPAL_ERR_BAD_PARAM string was not able to be interpreted
 */
OPAL_DECLSPEC int opal_info_value_to_bool(char *value, bool *interp);

/**
 * Convert value string to integer
 *
 * Convert value string \c value into a integer, using the
 * interpretation rules specified in MPI-2 Section 4.10.
 * All others will return \c OPAL_ERR_BAD_PARAM
 *
 * @param value Value string for info key to interpret
 * @param interp returned interpretation of the value key
 *
 * @retval OPAL_SUCCESS string was successfully interpreted
 * @retval OPAL_ERR_BAD_PARAM string was not able to be interpreted
 */
int opal_info_value_to_int(char *value, int *interp);

END_C_DECLS

/**
 * Get the number of keys defined on on an MPI_Info object
 * @param info Pointer to opal_info_t object.
 * @param nkeys Pointer to nkeys, which needs to be filled up.
 *
 * @retval The number of keys defined on info
 */
static inline int
opal_info_get_nkeys(opal_info_t *info, int *nkeys)
{
    *nkeys = (int) opal_list_get_size(&(info->super));
    return OPAL_SUCCESS;
}

bool opal_str_to_bool(char*);

#endif /* OPAL_INFO_H */

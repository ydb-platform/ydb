/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/dss/dss_internal.h"

int opal_dss_register(opal_dss_pack_fn_t pack_fn,
                      opal_dss_unpack_fn_t unpack_fn,
                      opal_dss_copy_fn_t copy_fn,
                      opal_dss_compare_fn_t compare_fn,
                      opal_dss_print_fn_t print_fn,
                      bool structured,
                      const char *name, opal_data_type_t *type)
{
    opal_dss_type_info_t *info, *ptr;
    int32_t i;

    /* Check for bozo cases */

    if (NULL == pack_fn || NULL == unpack_fn || NULL == copy_fn || NULL == compare_fn ||
        NULL == print_fn || NULL == name || NULL == type) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* check if this entry already exists - if so, error - we do NOT allow multiple type registrations */
    for (i=0; i < opal_pointer_array_get_size(&opal_dss_types); i++) {
        ptr = opal_pointer_array_get_item(&opal_dss_types, i);
        if (NULL != ptr) {
            /* check if the name exists */
            if (0 == strcmp(ptr->odti_name, name)) {
                return OPAL_ERR_DATA_TYPE_REDEF;
            }
            /* check if the specified type exists */
            if (*type > 0 && ptr->odti_type == *type) {
                return OPAL_ERR_DATA_TYPE_REDEF;
            }
        }
    }

    /* if type is given (i.e., *type > 0), then just use it.
     * otherwise, it is an error
     */
    if (0 >= *type) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Add a new entry to the table */
    info = (opal_dss_type_info_t*) OBJ_NEW(opal_dss_type_info_t);
    if (NULL == info) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    info->odti_type = *type;
    info->odti_name = strdup(name);
    info->odti_pack_fn = pack_fn;
    info->odti_unpack_fn = unpack_fn;
    info->odti_copy_fn = copy_fn;
    info->odti_compare_fn = compare_fn;
    info->odti_print_fn = print_fn;
    info->odti_structured = structured;

    return opal_pointer_array_set_item(&opal_dss_types, *type, info);
}

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2012 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <errno.h>

#include "opal/include/opal_stdint.h"
#include "opal/util/show_help.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/mca_base_vari.h"
#include "opal/mca/base/mca_base_pvar.h"
#include "opal/constants.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/runtime/opal.h"

static opal_pointer_array_t mca_base_var_groups;
static opal_hash_table_t mca_base_var_group_index_hash;
static int mca_base_var_group_count = 0;
static int mca_base_var_groups_timestamp = 0;
static bool mca_base_var_group_initialized = false;

static void mca_base_var_group_constructor (mca_base_var_group_t *group);
static void mca_base_var_group_destructor (mca_base_var_group_t *group);
OBJ_CLASS_INSTANCE(mca_base_var_group_t, opal_object_t,
                   mca_base_var_group_constructor,
                   mca_base_var_group_destructor);

int mca_base_var_group_init (void)
{
    int ret;

    if (!mca_base_var_group_initialized) {
        OBJ_CONSTRUCT(&mca_base_var_groups, opal_pointer_array_t);

        /* These values are arbitrary */
        ret = opal_pointer_array_init (&mca_base_var_groups, 128, 16384, 128);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }

        OBJ_CONSTRUCT(&mca_base_var_group_index_hash, opal_hash_table_t);
        ret = opal_hash_table_init (&mca_base_var_group_index_hash, 256);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }

        mca_base_var_group_initialized = true;
        mca_base_var_group_count = 0;
    }

    return OPAL_SUCCESS;
}

int mca_base_var_group_finalize (void)
{
    opal_object_t *object;
    int size, i;

    if (mca_base_var_group_initialized) {
        size = opal_pointer_array_get_size(&mca_base_var_groups);
        for (i = 0 ; i < size ; ++i) {
            object = opal_pointer_array_get_item (&mca_base_var_groups, i);
            if (NULL != object) {
                OBJ_RELEASE(object);
            }
        }
        OBJ_DESTRUCT(&mca_base_var_groups);
        OBJ_DESTRUCT(&mca_base_var_group_index_hash);
        mca_base_var_group_count = 0;
        mca_base_var_group_initialized = false;
    }

    return OPAL_SUCCESS;
}

int mca_base_var_group_get_internal (const int group_index, mca_base_var_group_t **group, bool invalidok)
{
    if (group_index < 0) {
        return OPAL_ERR_NOT_FOUND;
    }

    *group = (mca_base_var_group_t *) opal_pointer_array_get_item (&mca_base_var_groups,
                                                                   group_index);
    if (NULL == *group || (!invalidok && !(*group)->group_isvalid)) {
        *group = NULL;
        return OPAL_ERR_NOT_FOUND;
    }

    return OPAL_SUCCESS;
}

static int group_find_by_name (const char *full_name, int *index, bool invalidok)
{
    mca_base_var_group_t *group;
    void *tmp;
    int rc;

    rc = opal_hash_table_get_value_ptr (&mca_base_var_group_index_hash, full_name,
                                        strlen (full_name), &tmp);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    rc = mca_base_var_group_get_internal ((int)(uintptr_t) tmp, &group, invalidok);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    if (invalidok || group->group_isvalid) {
        *index = (int)(uintptr_t) tmp;
        return OPAL_SUCCESS;
    }

    return OPAL_ERR_NOT_FOUND;
}

static bool compare_strings (const char *str1, const char *str2) {
    if ((NULL != str1 && 0 == strcmp (str1, "*")) ||
        (NULL == str1 && NULL == str2)) {
        return true;
    }

    if (NULL != str1 && NULL != str2) {
        return 0 == strcmp (str1, str2);
    }

    return false;
}

static int group_find_linear (const char *project_name, const char *framework_name,
                              const char *component_name, bool invalidok)
{
    for (int i = 0 ; i < mca_base_var_group_count ; ++i) {
        mca_base_var_group_t *group;

        int rc = mca_base_var_group_get_internal (i, &group, invalidok);
        if (OPAL_SUCCESS != rc) {
            continue;
        }

        if (compare_strings (project_name, group->group_project) &&
            compare_strings (framework_name, group->group_framework) &&
            compare_strings (component_name, group->group_component)) {
            return i;
        }
    }

    return OPAL_ERR_NOT_FOUND;
}

static int group_find (const char *project_name, const char *framework_name,
                       const char *component_name, bool invalidok)
{
    char *full_name;
    int ret, index=0;

    if (!mca_base_var_initialized) {
        return OPAL_ERR_NOT_FOUND;
    }

    /* check for wildcards */
    if ((project_name && '*' == project_name[0]) || (framework_name && '*' == framework_name[0]) ||
        (component_name && '*' == component_name[0])) {
        return group_find_linear (project_name, framework_name, component_name, invalidok);
    }

    ret = mca_base_var_generate_full_name4(project_name, framework_name, component_name,
                                           NULL, &full_name);
    if (OPAL_SUCCESS != ret) {
        return OPAL_ERROR;
    }

    ret = group_find_by_name(full_name, &index, invalidok);
    free (full_name);

    return (0 > ret) ? ret : index;
}

static int group_register (const char *project_name, const char *framework_name,
                           const char *component_name, const char *description)
{
    mca_base_var_group_t *group;
    int group_id, parent_id = -1;
    int ret;

    if (NULL == project_name && NULL == framework_name && NULL == component_name) {
        /* don't create a group with no name (maybe we should create a generic group?) */
        return -1;
    }

    /* avoid groups of the form opal_opal, ompi_ompi, etc */
    if (NULL != project_name && NULL != framework_name &&
        (0 == strcmp (project_name, framework_name))) {
        project_name = NULL;
    }

    group_id = group_find (project_name, framework_name, component_name, true);
    if (0 <= group_id) {
        ret = mca_base_var_group_get_internal (group_id, &group, true);
        if (OPAL_SUCCESS != ret) {
            /* something went horribly wrong */
            assert (NULL != group);
            return ret;
        }
        group->group_isvalid = true;
        mca_base_var_groups_timestamp++;

        /* group already exists. return it's index */
        return group_id;
    }

    group = OBJ_NEW(mca_base_var_group_t);

    group->group_isvalid = true;

    if (NULL != project_name) {
        group->group_project = strdup (project_name);
        if (NULL == group->group_project) {
            OBJ_RELEASE(group);
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }
    if (NULL != framework_name) {
        group->group_framework = strdup (framework_name);
        if (NULL == group->group_framework) {
            OBJ_RELEASE(group);
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }
    if (NULL != component_name) {
        group->group_component = strdup (component_name);
        if (NULL == group->group_component) {
            OBJ_RELEASE(group);
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }
    if (NULL != description) {
        group->group_description = strdup (description);
        if (NULL == group->group_description) {
            OBJ_RELEASE(group);
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }

    if (NULL != framework_name && NULL != component_name) {
        if (component_name) {
            parent_id = group_register (project_name, framework_name, NULL, NULL);
        } else if (framework_name && project_name) {
            parent_id = group_register (project_name, NULL, NULL, NULL);
        }
    }

    /* build the group name */
    ret = mca_base_var_generate_full_name4 (NULL, project_name, framework_name, component_name,
                                            &group->group_full_name);
    if (OPAL_SUCCESS != ret) {
        OBJ_RELEASE(group);
        return ret;
    }

    group_id = opal_pointer_array_add (&mca_base_var_groups, group);
    if (0 > group_id) {
        OBJ_RELEASE(group);
        return OPAL_ERROR;
    }

    opal_hash_table_set_value_ptr (&mca_base_var_group_index_hash, group->group_full_name,
                                   strlen (group->group_full_name), (void *)(uintptr_t) group_id);

    mca_base_var_group_count++;
    mca_base_var_groups_timestamp++;

    if (0 <= parent_id) {
        mca_base_var_group_t *parent_group;

        (void) mca_base_var_group_get_internal(parent_id, &parent_group, false);
        opal_value_array_append_item (&parent_group->group_subgroups, &group_id);
    }

    return group_id;
}

int mca_base_var_group_register (const char *project_name, const char *framework_name,
                                 const char *component_name, const char *description)
{
    return group_register (project_name, framework_name, component_name, description);
}

int mca_base_var_group_component_register (const mca_base_component_t *component,
                                           const char *description)
{
    return group_register (component->mca_project_name, component->mca_type_name,
                           component->mca_component_name, description);
}


int mca_base_var_group_deregister (int group_index)
{
    mca_base_var_group_t *group;
    int size, ret;
    int *params, *subgroups;
    opal_object_t ** enums;

    ret = mca_base_var_group_get_internal (group_index, &group, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    group->group_isvalid = false;

    /* deregister all associated mca parameters */
    size = opal_value_array_get_size(&group->group_vars);
    params = OPAL_VALUE_ARRAY_GET_BASE(&group->group_vars, int);

    for (int i = 0 ; i < size ; ++i) {
        const mca_base_var_t *var;

        ret = mca_base_var_get (params[i], &var);
        if (OPAL_SUCCESS != ret || !(var->mbv_flags & MCA_BASE_VAR_FLAG_DWG)) {
            continue;
        }

        (void) mca_base_var_deregister (params[i]);
    }

    /* invalidate all associated mca performance variables */
    size = opal_value_array_get_size(&group->group_pvars);
    params = OPAL_VALUE_ARRAY_GET_BASE(&group->group_pvars, int);

    for (int i = 0 ; i < size ; ++i) {
        const mca_base_pvar_t *var;

        ret = mca_base_pvar_get (params[i], &var);
        if (OPAL_SUCCESS != ret || !(var->flags & MCA_BASE_PVAR_FLAG_IWG)) {
            continue;
        }

        (void) mca_base_pvar_mark_invalid (params[i]);
    }

    size = opal_value_array_get_size(&group->group_enums);
    enums = OPAL_VALUE_ARRAY_GET_BASE(&group->group_enums, opal_object_t *);
    for (int i = 0 ; i < size ; ++i) {
        OBJ_RELEASE (enums[i]);
    }

    size = opal_value_array_get_size(&group->group_subgroups);
    subgroups = OPAL_VALUE_ARRAY_GET_BASE(&group->group_subgroups, int);
    for (int i = 0 ; i < size ; ++i) {
        (void) mca_base_var_group_deregister (subgroups[i]);
    }
    /* ordering of variables and subgroups must be the same if the
     * group is re-registered */

    mca_base_var_groups_timestamp++;

    return OPAL_SUCCESS;
}

int mca_base_var_group_find (const char *project_name,
                             const char *framework_name,
                             const char *component_name)
{
    return group_find (project_name, framework_name, component_name, false);
}

int mca_base_var_group_find_by_name (const char *full_name, int *index)
{
    return group_find_by_name (full_name, index, false);
}

int mca_base_var_group_add_var (const int group_index, const int param_index)
{
    mca_base_var_group_t *group;
    int size, i, ret;
    int *params;

    ret = mca_base_var_group_get_internal (group_index, &group, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    size = opal_value_array_get_size(&group->group_vars);
    params = OPAL_VALUE_ARRAY_GET_BASE(&group->group_vars, int);
    for (i = 0 ; i < size ; ++i) {
        if (params[i] == param_index) {
            return i;
        }
    }

    if (OPAL_SUCCESS !=
        (ret = opal_value_array_append_item (&group->group_vars, &param_index))) {
        return ret;
    }

    mca_base_var_groups_timestamp++;

    /* return the group index */
    return (int) opal_value_array_get_size (&group->group_vars) - 1;
}

int mca_base_var_group_add_pvar (const int group_index, const int param_index)
{
    mca_base_var_group_t *group;
    int size, i, ret;
    int *params;

    ret = mca_base_var_group_get_internal (group_index, &group, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    size = opal_value_array_get_size(&group->group_pvars);
    params = OPAL_VALUE_ARRAY_GET_BASE(&group->group_pvars, int);
    for (i = 0 ; i < size ; ++i) {
        if (params[i] == param_index) {
            return i;
        }
    }

    if (OPAL_SUCCESS !=
        (ret = opal_value_array_append_item (&group->group_pvars, &param_index))) {
        return ret;
    }

    mca_base_var_groups_timestamp++;

    /* return the group index */
    return (int) opal_value_array_get_size (&group->group_pvars) - 1;
}

int mca_base_var_group_add_enum (const int group_index, const void * storage)
{
    mca_base_var_group_t *group;
    int size, i, ret;
    void **params;

    ret = mca_base_var_group_get_internal (group_index, &group, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    size = opal_value_array_get_size(&group->group_enums);
    params = OPAL_VALUE_ARRAY_GET_BASE(&group->group_enums, void *);
    for (i = 0 ; i < size ; ++i) {
        if (params[i] == storage) {
            return i;
        }
    }

    if (OPAL_SUCCESS !=
        (ret = opal_value_array_append_item (&group->group_enums, storage))) {
        return ret;
    }

    /* return the group index */
    return (int) opal_value_array_get_size (&group->group_enums) - 1;
}

int mca_base_var_group_get (const int group_index, const mca_base_var_group_t **group)
{
    return mca_base_var_group_get_internal (group_index, (mca_base_var_group_t **) group, false);
}

int mca_base_var_group_set_var_flag (const int group_index, int flags, bool set)
{
    mca_base_var_group_t *group;
    int size, i, ret;
    int *vars;

    ret = mca_base_var_group_get_internal (group_index, &group, false);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    /* set the flag on each valid variable */
    size = opal_value_array_get_size(&group->group_vars);
    vars = OPAL_VALUE_ARRAY_GET_BASE(&group->group_vars, int);

    for (i = 0 ; i < size ; ++i) {
        if (0 <= vars[i]) {
            (void) mca_base_var_set_flag (vars[i], flags, set);
        }
    }

    return OPAL_SUCCESS;
}


static void mca_base_var_group_constructor (mca_base_var_group_t *group)
{
    memset ((char *) group + sizeof (group->super), 0, sizeof (*group) - sizeof (group->super));

    OBJ_CONSTRUCT(&group->group_subgroups, opal_value_array_t);
    opal_value_array_init (&group->group_subgroups, sizeof (int));

    OBJ_CONSTRUCT(&group->group_vars, opal_value_array_t);
    opal_value_array_init (&group->group_vars, sizeof (int));

    OBJ_CONSTRUCT(&group->group_pvars, opal_value_array_t);
    opal_value_array_init (&group->group_pvars, sizeof (int));

    OBJ_CONSTRUCT(&group->group_enums, opal_value_array_t);
    opal_value_array_init (&group->group_enums, sizeof(void *));
}

static void mca_base_var_group_destructor (mca_base_var_group_t *group)
{
    free (group->group_full_name);
    group->group_full_name = NULL;

    free (group->group_description);
    group->group_description = NULL;

    free (group->group_project);
    group->group_project = NULL;

    free (group->group_framework);
    group->group_framework = NULL;

    free (group->group_component);
    group->group_component = NULL;

    OBJ_DESTRUCT(&group->group_subgroups);
    OBJ_DESTRUCT(&group->group_vars);
    OBJ_DESTRUCT(&group->group_pvars);
    OBJ_DESTRUCT(&group->group_enums);
}

int mca_base_var_group_get_count (void)
{
    return mca_base_var_group_count;
}

int mca_base_var_group_get_stamp (void)
{
    return mca_base_var_groups_timestamp;
}

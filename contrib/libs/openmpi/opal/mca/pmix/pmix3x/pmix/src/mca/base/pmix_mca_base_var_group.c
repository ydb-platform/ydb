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
 * Copyright (c) 2016-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

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

#include "src/include/pmix_stdint.h"
#include "src/util/show_help.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_vari.h"
#include "pmix_common.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"

static pmix_pointer_array_t pmix_mca_base_var_groups;
static pmix_hash_table_t pmix_mca_base_var_group_index_hash;
static int pmix_mca_base_var_group_count = 0;
static int pmix_mca_base_var_groups_timestamp = 0;
static bool pmix_mca_base_var_group_initialized = false;

static void pmix_mca_base_var_group_constructor (pmix_mca_base_var_group_t *group);
static void pmix_mca_base_var_group_destructor (pmix_mca_base_var_group_t *group);
PMIX_CLASS_INSTANCE(pmix_mca_base_var_group_t, pmix_object_t,
                    pmix_mca_base_var_group_constructor,
                    pmix_mca_base_var_group_destructor);

int pmix_mca_base_var_group_init (void)
{
    int ret;

    if (!pmix_mca_base_var_group_initialized) {
        PMIX_CONSTRUCT(&pmix_mca_base_var_groups, pmix_pointer_array_t);

        /* These values are arbitrary */
        ret = pmix_pointer_array_init (&pmix_mca_base_var_groups, 128, 16384, 128);
        if (PMIX_SUCCESS != ret) {
            return ret;
        }

        PMIX_CONSTRUCT(&pmix_mca_base_var_group_index_hash, pmix_hash_table_t);
        ret = pmix_hash_table_init (&pmix_mca_base_var_group_index_hash, 256);
        if (PMIX_SUCCESS != ret) {
            return ret;
        }

        pmix_mca_base_var_group_initialized = true;
        pmix_mca_base_var_group_count = 0;
    }

    return PMIX_SUCCESS;
}

int pmix_mca_base_var_group_finalize (void)
{
    pmix_object_t *object;
    int size, i;

    if (pmix_mca_base_var_group_initialized) {
        size = pmix_pointer_array_get_size(&pmix_mca_base_var_groups);
        for (i = 0 ; i < size ; ++i) {
            object = pmix_pointer_array_get_item (&pmix_mca_base_var_groups, i);
            if (NULL != object) {
                PMIX_RELEASE(object);
            }
        }
        PMIX_DESTRUCT(&pmix_mca_base_var_groups);
        PMIX_DESTRUCT(&pmix_mca_base_var_group_index_hash);
        pmix_mca_base_var_group_count = 0;
        pmix_mca_base_var_group_initialized = false;
    }

    return PMIX_SUCCESS;
}

int pmix_mca_base_var_group_get_internal (const int group_index, pmix_mca_base_var_group_t **group, bool invalidok)
{
    if (group_index < 0) {
        return PMIX_ERR_NOT_FOUND;
    }

    *group = (pmix_mca_base_var_group_t *) pmix_pointer_array_get_item (&pmix_mca_base_var_groups,
                                                                   group_index);
    if (NULL == *group || (!invalidok && !(*group)->group_isvalid)) {
        *group = NULL;
        return PMIX_ERR_NOT_FOUND;
    }

    return PMIX_SUCCESS;
}

static int group_find_by_name (const char *full_name, int *index, bool invalidok)
{
    pmix_mca_base_var_group_t *group;
    void *tmp;
    int rc;

    rc = pmix_hash_table_get_value_ptr (&pmix_mca_base_var_group_index_hash, full_name,
                                        strlen (full_name), &tmp);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }

    rc = pmix_mca_base_var_group_get_internal ((int)(uintptr_t) tmp, &group, invalidok);
    if (PMIX_SUCCESS != rc) {
        return rc;
    }

    if (invalidok || group->group_isvalid) {
        *index = (int)(uintptr_t) tmp;
        return PMIX_SUCCESS;
    }

    return PMIX_ERR_NOT_FOUND;
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
    for (int i = 0 ; i < pmix_mca_base_var_group_count ; ++i) {
        pmix_mca_base_var_group_t *group;

        int rc = pmix_mca_base_var_group_get_internal (i, &group, invalidok);
        if (PMIX_SUCCESS != rc) {
            continue;
        }

        if (compare_strings (project_name, group->group_project) &&
            compare_strings (framework_name, group->group_framework) &&
            compare_strings (component_name, group->group_component)) {
            return i;
        }
    }

    return PMIX_ERR_NOT_FOUND;
}

static int group_find (const char *project_name, const char *framework_name,
                       const char *component_name, bool invalidok)
{
    char *full_name;
    int ret, index=0;

    if (!pmix_mca_base_var_initialized) {
        return PMIX_ERR_NOT_FOUND;
    }

    /* check for wildcards */
    if ((project_name && '*' == project_name[0]) || (framework_name && '*' == framework_name[0]) ||
        (component_name && '*' == component_name[0])) {
        return group_find_linear (project_name, framework_name, component_name, invalidok);
    }

    ret = pmix_mca_base_var_generate_full_name4(project_name, framework_name, component_name,
                                           NULL, &full_name);
    if (PMIX_SUCCESS != ret) {
        return PMIX_ERROR;
    }

    ret = group_find_by_name(full_name, &index, invalidok);
    free (full_name);

    return (0 > ret) ? ret : index;
}

static int group_register (const char *project_name, const char *framework_name,
                           const char *component_name, const char *description)
{
    pmix_mca_base_var_group_t *group;
    int group_id, parent_id = -1;
    int ret;

    if (NULL == project_name && NULL == framework_name && NULL == component_name) {
        /* don't create a group with no name (maybe we should create a generic group?) */
        return -1;
    }

    /* avoid groups of the form pmix_pmix, etc */
    if (NULL != project_name && NULL != framework_name &&
        (0 == strcmp (project_name, framework_name))) {
        project_name = NULL;
    }

    group_id = group_find (project_name, framework_name, component_name, true);
    if (0 <= group_id) {
        ret = pmix_mca_base_var_group_get_internal (group_id, &group, true);
        if (PMIX_SUCCESS != ret) {
            /* something went horribly wrong */
            assert (NULL != group);
            return ret;
        }
        group->group_isvalid = true;
        pmix_mca_base_var_groups_timestamp++;

        /* group already exists. return it's index */
        return group_id;
    }

    group = PMIX_NEW(pmix_mca_base_var_group_t);

    group->group_isvalid = true;

    if (NULL != project_name) {
        group->group_project = strdup (project_name);
        if (NULL == group->group_project) {
            PMIX_RELEASE(group);
            return PMIX_ERR_OUT_OF_RESOURCE;
        }
    }
    if (NULL != framework_name) {
        group->group_framework = strdup (framework_name);
        if (NULL == group->group_framework) {
            PMIX_RELEASE(group);
            return PMIX_ERR_OUT_OF_RESOURCE;
        }
    }
    if (NULL != component_name) {
        group->group_component = strdup (component_name);
        if (NULL == group->group_component) {
            PMIX_RELEASE(group);
            return PMIX_ERR_OUT_OF_RESOURCE;
        }
    }
    if (NULL != description) {
        group->group_description = strdup (description);
        if (NULL == group->group_description) {
            PMIX_RELEASE(group);
            return PMIX_ERR_OUT_OF_RESOURCE;
        }
    }

    if (NULL != framework_name && NULL != component_name) {
        parent_id = group_register (project_name, framework_name, NULL, NULL);
    }

    /* build the group name */
    ret = pmix_mca_base_var_generate_full_name4 (NULL, project_name, framework_name, component_name,
                                            &group->group_full_name);
    if (PMIX_SUCCESS != ret) {
        PMIX_RELEASE(group);
        return ret;
    }

    group_id = pmix_pointer_array_add (&pmix_mca_base_var_groups, group);
    if (0 > group_id) {
        PMIX_RELEASE(group);
        return PMIX_ERROR;
    }

    pmix_hash_table_set_value_ptr (&pmix_mca_base_var_group_index_hash, group->group_full_name,
                                   strlen (group->group_full_name), (void *)(uintptr_t) group_id);

    pmix_mca_base_var_group_count++;
    pmix_mca_base_var_groups_timestamp++;

    if (0 <= parent_id) {
        pmix_mca_base_var_group_t *parent_group;

        (void) pmix_mca_base_var_group_get_internal(parent_id, &parent_group, false);
        pmix_value_array_append_item (&parent_group->group_subgroups, &group_id);
    }

    return group_id;
}

int pmix_mca_base_var_group_register (const char *project_name, const char *framework_name,
                                      const char *component_name, const char *description)
{
    return group_register (project_name, framework_name, component_name, description);
}

int pmix_mca_base_var_group_component_register (const pmix_mca_base_component_t *component,
                                                const char *description)
{
    /* 1.7 components do not store the project */
    return group_register (component->pmix_mca_project_name, component->pmix_mca_type_name,
                           component->pmix_mca_component_name, description);
}


int pmix_mca_base_var_group_deregister (int group_index)
{
    pmix_mca_base_var_group_t *group;
    int size, ret;
    int *params, *subgroups;

    ret = pmix_mca_base_var_group_get_internal (group_index, &group, false);
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    group->group_isvalid = false;

    /* deregister all associated mca parameters */
    size = pmix_value_array_get_size(&group->group_vars);
    params = PMIX_VALUE_ARRAY_GET_BASE(&group->group_vars, int);

    for (int i = 0 ; i < size ; ++i) {
        const pmix_mca_base_var_t *var;

        ret = pmix_mca_base_var_get (params[i], &var);
        if (PMIX_SUCCESS != ret || !(var->mbv_flags & PMIX_MCA_BASE_VAR_FLAG_DWG)) {
            continue;
        }

        (void) pmix_mca_base_var_deregister (params[i]);
    }

    size = pmix_value_array_get_size(&group->group_subgroups);
    subgroups = PMIX_VALUE_ARRAY_GET_BASE(&group->group_subgroups, int);
    for (int i = 0 ; i < size ; ++i) {
        (void) pmix_mca_base_var_group_deregister (subgroups[i]);
    }
    /* ordering of variables and subgroups must be the same if the
     * group is re-registered */

    pmix_mca_base_var_groups_timestamp++;

    return PMIX_SUCCESS;
}

int pmix_mca_base_var_group_find (const char *project_name,
                             const char *framework_name,
                             const char *component_name)
{
    return group_find (project_name, framework_name, component_name, false);
}

int pmix_mca_base_var_group_find_by_name (const char *full_name, int *index)
{
    return group_find_by_name (full_name, index, false);
}

int pmix_mca_base_var_group_add_var (const int group_index, const int param_index)
{
    pmix_mca_base_var_group_t *group;
    int size, i, ret;
    int *params;

    ret = pmix_mca_base_var_group_get_internal (group_index, &group, false);
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    size = pmix_value_array_get_size(&group->group_vars);
    params = PMIX_VALUE_ARRAY_GET_BASE(&group->group_vars, int);
    for (i = 0 ; i < size ; ++i) {
        if (params[i] == param_index) {
            return i;
        }
    }

    if (PMIX_SUCCESS !=
        (ret = pmix_value_array_append_item (&group->group_vars, &param_index))) {
        return ret;
    }

    pmix_mca_base_var_groups_timestamp++;

    /* return the group index */
    return (int) pmix_value_array_get_size (&group->group_vars) - 1;
}

int pmix_mca_base_var_group_get (const int group_index, const pmix_mca_base_var_group_t **group)
{
    return pmix_mca_base_var_group_get_internal (group_index, (pmix_mca_base_var_group_t **) group, false);
}

int pmix_mca_base_var_group_set_var_flag (const int group_index, int flags, bool set)
{
    pmix_mca_base_var_group_t *group;
    int size, i, ret;
    int *vars;

    ret = pmix_mca_base_var_group_get_internal (group_index, &group, false);
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    /* set the flag on each valid variable */
    size = pmix_value_array_get_size(&group->group_vars);
    vars = PMIX_VALUE_ARRAY_GET_BASE(&group->group_vars, int);

    for (i = 0 ; i < size ; ++i) {
        if (0 <= vars[i]) {
            (void) pmix_mca_base_var_set_flag (vars[i], flags, set);
        }
    }

    return PMIX_SUCCESS;
}


static void pmix_mca_base_var_group_constructor (pmix_mca_base_var_group_t *group)
{
    memset ((char *) group + sizeof (group->super), 0, sizeof (*group) - sizeof (group->super));

    PMIX_CONSTRUCT(&group->group_subgroups, pmix_value_array_t);
    pmix_value_array_init (&group->group_subgroups, sizeof (int));

    PMIX_CONSTRUCT(&group->group_vars, pmix_value_array_t);
    pmix_value_array_init (&group->group_vars, sizeof (int));

}

static void pmix_mca_base_var_group_destructor (pmix_mca_base_var_group_t *group)
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

    PMIX_DESTRUCT(&group->group_subgroups);
    PMIX_DESTRUCT(&group->group_vars);
}

int pmix_mca_base_var_group_get_count (void)
{
    return pmix_mca_base_var_group_count;
}

int pmix_mca_base_var_group_get_stamp (void)
{
    return pmix_mca_base_var_groups_timestamp;
}

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2008-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include <src/include/pmix_config.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "src/class/pmix_list.h"
#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_component_repository.h"
#include "src/mca/pdl/base/base.h"
#include "pmix_common.h"
#include "src/class/pmix_hash_table.h"
#include "src/util/basename.h"

#if PMIX_HAVE_PDL_SUPPORT

/*
 * Private types
 */
static void ri_constructor(pmix_mca_base_component_repository_item_t *ri);
static void ri_destructor(pmix_mca_base_component_repository_item_t *ri);
PMIX_CLASS_INSTANCE(pmix_mca_base_component_repository_item_t, pmix_list_item_t,
                    ri_constructor, ri_destructor);

#endif /* PMIX_HAVE_PDL_SUPPORT */

static void clf_constructor(pmix_object_t *obj)
{
    pmix_mca_base_failed_component_t *cli = (pmix_mca_base_failed_component_t *) obj;
    cli->comp = NULL;
    cli->error_msg = NULL;
}

static void clf_destructor(pmix_object_t *obj)
{
    pmix_mca_base_failed_component_t *cli = (pmix_mca_base_failed_component_t *) obj;
    cli->comp = NULL;
    if( NULL != cli->error_msg ) {
        free(cli->error_msg);
        cli->error_msg = NULL;
    }
}

PMIX_CLASS_INSTANCE(pmix_mca_base_failed_component_t, pmix_list_item_t,
                    clf_constructor, clf_destructor);


/*
 * Private variables
 */
static bool initialized = false;


#if PMIX_HAVE_PDL_SUPPORT

static pmix_hash_table_t pmix_mca_base_component_repository;

/* two-level macro for stringifying a number */
#define STRINGIFYX(x) #x
#define STRINGIFY(x) STRINGIFYX(x)

static int process_repository_item (const char *filename, void *data)
{
    char name[PMIX_MCA_BASE_MAX_COMPONENT_NAME_LEN + 1];
    char type[PMIX_MCA_BASE_MAX_TYPE_NAME_LEN + 1];
    pmix_mca_base_component_repository_item_t *ri;
    pmix_list_t *component_list;
    char *base;
    int ret;

    base = pmix_basename (filename);
    if (NULL == base) {
        return PMIX_ERROR;
    }

    /* check if the plugin has the appropriate prefix */
    if (0 != strncmp (base, "mca_", 4)) {
        free (base);
        return PMIX_SUCCESS;
    }

    /* read framework and component names. framework names may not include an _
     * but component names may */
    ret = sscanf(base, "mca_%" STRINGIFY(PMIX_MCA_BASE_MAX_TYPE_NAME_LEN) "[^_]_%"
                 STRINGIFY(PMIX_MCA_BASE_MAX_COMPONENT_NAME_LEN) "s", type, name);
    if (0 > ret) {
        /* does not patch the expected template. skip */
        free(base);
        return PMIX_SUCCESS;
    }

    /* lookup the associated framework list and create if it doesn't already exist */
    ret = pmix_hash_table_get_value_ptr(&pmix_mca_base_component_repository, type,
                                        strlen (type), (void **) &component_list);
    if (PMIX_SUCCESS != ret) {
        component_list = PMIX_NEW(pmix_list_t);
        if (NULL == component_list) {
            free (base);
            /* OOM. nothing to do but fail */
            return PMIX_ERR_OUT_OF_RESOURCE;
        }

        ret = pmix_hash_table_set_value_ptr(&pmix_mca_base_component_repository, type,
                                            strlen (type), (void *) component_list);
        if (PMIX_SUCCESS != ret) {
            free (base);
            PMIX_RELEASE(component_list);
            return ret;
        }
    }

    /* check for duplicate components */
    PMIX_LIST_FOREACH(ri, component_list, pmix_mca_base_component_repository_item_t) {
        if (0 == strcmp (ri->ri_name, name)) {
            /* already scanned this component */
            free (base);
            return PMIX_SUCCESS;
        }
    }

    ri = PMIX_NEW(pmix_mca_base_component_repository_item_t);
    if (NULL == ri) {
        free (base);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    ri->ri_base = base;

    ri->ri_path = strdup (filename);
    if (NULL == ri->ri_path) {
        PMIX_RELEASE(ri);
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    /* pmix_strncpy does not guarantee a \0 */
    ri->ri_type[PMIX_MCA_BASE_MAX_TYPE_NAME_LEN] = '\0';
    pmix_strncpy (ri->ri_type, type, PMIX_MCA_BASE_MAX_TYPE_NAME_LEN);

    ri->ri_name[PMIX_MCA_BASE_MAX_TYPE_NAME_LEN] = '\0';
    pmix_strncpy (ri->ri_name, name, PMIX_MCA_BASE_MAX_COMPONENT_NAME_LEN);

    pmix_list_append (component_list, &ri->super);

    return PMIX_SUCCESS;
}

static int file_exists(const char *filename, const char *ext)
{
    char *final;
    int ret;

    if (NULL == ext) {
        return access (filename, F_OK) == 0;
    }

    ret = asprintf(&final, "%s.%s", filename, ext);
    if (0 > ret || NULL == final) {
        return 0;
    }

    ret = access (final, F_OK);
    free(final);
    return (0 == ret);
}

#endif /* PMIX_HAVE_PDL_SUPPORT */

int pmix_mca_base_component_repository_add (const char *path)
{
#if PMIX_HAVE_PDL_SUPPORT
    char *path_to_use = NULL, *dir, *ctx;
    const char sep[] = {PMIX_ENV_SEP, '\0'};

    if (NULL == path) {
        /* nothing to do */
        return PMIX_SUCCESS;
    }

    path_to_use = strdup (path);

    dir = strtok_r (path_to_use, sep, &ctx);
    do {
        if ((0 == strcmp(dir, "USER_DEFAULT") || 0 == strcmp(dir, "USR_DEFAULT"))
            && NULL != pmix_mca_base_user_default_path) {
            dir = pmix_mca_base_user_default_path;
        } else if (0 == strcmp(dir, "SYS_DEFAULT") ||
                   0 == strcmp(dir, "SYSTEM_DEFAULT")) {
            dir = pmix_mca_base_system_default_path;
        }

        if (0 != pmix_pdl_foreachfile(dir, process_repository_item, NULL)) {
            break;
        }
    } while (NULL != (dir = strtok_r (NULL, sep, &ctx)));

    free (path_to_use);

#endif /* PMIX_HAVE_PDL_SUPPORT */

    return PMIX_SUCCESS;
}


/*
 * Initialize the repository
 */
int pmix_mca_base_component_repository_init(void)
{
  /* Setup internal structures */

  if (!initialized) {
#if PMIX_HAVE_PDL_SUPPORT

    /* Initialize the dl framework */
    int ret = pmix_mca_base_framework_open(&pmix_pdl_base_framework, 0);
    if (PMIX_SUCCESS != ret) {
        pmix_output(0, "%s %d:%s failed -- process will likely abort (open the dl framework returned %d instead of PMIX_SUCCESS)\n",
                    __FILE__, __LINE__, __func__, ret);
        return ret;
    }
    pmix_pdl_base_select();

    PMIX_CONSTRUCT(&pmix_mca_base_component_repository, pmix_hash_table_t);
    ret = pmix_hash_table_init (&pmix_mca_base_component_repository, 128);
    if (PMIX_SUCCESS != ret) {
        (void) pmix_mca_base_framework_close(&pmix_pdl_base_framework);
        return ret;
    }

    ret = pmix_mca_base_component_repository_add(pmix_mca_base_component_path);
    if (PMIX_SUCCESS != ret) {
        PMIX_DESTRUCT(&pmix_mca_base_component_repository);
        (void) pmix_mca_base_framework_close(&pmix_pdl_base_framework);
        return ret;
    }
#endif

    initialized = true;
  }

  /* All done */

  return PMIX_SUCCESS;
}

int pmix_mca_base_component_repository_get_components (pmix_mca_base_framework_t *framework,
                                                       pmix_list_t **framework_components)
{
    *framework_components = NULL;
#if PMIX_HAVE_PDL_SUPPORT
    return pmix_hash_table_get_value_ptr (&pmix_mca_base_component_repository, framework->framework_name,
                                          strlen (framework->framework_name), (void **) framework_components);
#endif
    return PMIX_ERR_NOT_FOUND;
}

#if PMIX_HAVE_PDL_SUPPORT
static void pmix_mca_base_component_repository_release_internal(pmix_mca_base_component_repository_item_t *ri) {
    int group_id;

    group_id = pmix_mca_base_var_group_find (NULL, ri->ri_type, ri->ri_name);
    if (0 <= group_id) {
        /* ensure all variables are deregistered before we dlclose the component */
        pmix_mca_base_var_group_deregister (group_id);
    }

    /* Close the component (and potentially unload it from memory */
    if (ri->ri_dlhandle) {
        pmix_pdl_close(ri->ri_dlhandle);
        ri->ri_dlhandle = NULL;
    }
}
#endif

#if PMIX_HAVE_PDL_SUPPORT
static pmix_mca_base_component_repository_item_t *find_component(const char *type, const char *name)
{
    pmix_mca_base_component_repository_item_t *ri;
    pmix_list_t *component_list;
    int ret;

    ret = pmix_hash_table_get_value_ptr (&pmix_mca_base_component_repository, type,
                                         strlen (type), (void **) &component_list);
    if (PMIX_SUCCESS != ret) {
        /* component does not exist in the repository */
        return NULL;
    }

    PMIX_LIST_FOREACH(ri, component_list, pmix_mca_base_component_repository_item_t) {
        if (0 == strcmp (ri->ri_name, name)) {
            return ri;
        }
    }

    return NULL;
}
#endif

void pmix_mca_base_component_repository_release(const pmix_mca_base_component_t *component)
{
#if PMIX_HAVE_PDL_SUPPORT
    pmix_mca_base_component_repository_item_t *ri;

    ri = find_component (component->pmix_mca_type_name, component->pmix_mca_component_name);
    if (NULL != ri && !(--ri->ri_refcnt)) {
        pmix_mca_base_component_repository_release_internal (ri);
    }
#endif
}

int pmix_mca_base_component_repository_retain_component(const char *type, const char *name)
{
#if PMIX_HAVE_PDL_SUPPORT
    pmix_mca_base_component_repository_item_t *ri = find_component(type, name);

    if (NULL != ri) {
        ++ri->ri_refcnt;
        return PMIX_SUCCESS;
    }

    return PMIX_ERR_NOT_FOUND;
#else
    return PMIX_ERR_NOT_SUPPORTED;
#endif
}

int pmix_mca_base_component_repository_open(pmix_mca_base_framework_t *framework,
                                            pmix_mca_base_component_repository_item_t *ri)
{
#if PMIX_HAVE_PDL_SUPPORT
    pmix_mca_base_component_t *component_struct;
    pmix_mca_base_component_list_item_t *mitem = NULL;
    char *struct_name = NULL;
    int vl, ret;

    pmix_output_verbose(PMIX_MCA_BASE_VERBOSE_INFO, 0, "pmix_mca_base_component_repository_open: examining dynamic "
                        "%s MCA component \"%s\" at path %s", ri->ri_type, ri->ri_name, ri->ri_path);

    vl = pmix_mca_base_component_show_load_errors ? PMIX_MCA_BASE_VERBOSE_ERROR : PMIX_MCA_BASE_VERBOSE_INFO;

    /* Ensure that this component is not already loaded (should only happen
       if it was statically loaded).  It's an error if it's already
       loaded because we're evaluating this file -- not this component.
       Hence, returning PMIX_ERR_PARAM indicates that the *file* failed
       to load, not the component. */

    PMIX_LIST_FOREACH(mitem, &framework->framework_components, pmix_mca_base_component_list_item_t) {
        if (0 == strcmp(mitem->cli_component->pmix_mca_component_name, ri->ri_name)) {
            pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_INFO, 0, "pmix_mca_base_component_repository_open: already loaded (ignored)");
            return PMIX_ERR_BAD_PARAM;
        }
    }

    /* silence coverity issue (invalid free) */
    mitem = NULL;

    if (NULL != ri->ri_dlhandle) {
        pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_INFO, 0, "pmix_mca_base_component_repository_open: already loaded. returning cached component");
        mitem = PMIX_NEW(pmix_mca_base_component_list_item_t);
        if (NULL == mitem) {
            return PMIX_ERR_OUT_OF_RESOURCE;
        }

        mitem->cli_component = ri->ri_component_struct;
        pmix_list_append (&framework->framework_components, &mitem->super);

        return PMIX_SUCCESS;
    }

    if (0 != strcmp (ri->ri_type, framework->framework_name)) {
        /* shouldn't happen. attempting to open a component belonging to
         * another framework. if this happens it is likely a MCA base
         * bug so assert */
        assert (0);
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* Now try to load the component */

    char *err_msg = NULL;
    if (PMIX_SUCCESS != pmix_pdl_open(ri->ri_path, true, false, &ri->ri_dlhandle, &err_msg)) {
        if (NULL == err_msg) {
            err_msg = "pmix_dl_open() error message was NULL!";
        }
        /* Because libltdl erroneously says "file not found" for any
           type of error -- which is especially misleading when the file
           is actually there but cannot be opened for some other reason
           (e.g., missing symbol) -- do some simple huersitics and if
           the file [probably] does exist, print a slightly better error
           message. */
        if (0 == strcasecmp("file not found", err_msg) &&
            (file_exists(ri->ri_path, "lo") ||
             file_exists(ri->ri_path, "so") ||
             file_exists(ri->ri_path, "dylib") ||
             file_exists(ri->ri_path, "dll"))) {
            err_msg = "perhaps a missing symbol, or compiled for a different version of Open MPI?";
        }
        pmix_output_verbose(vl, 0, "pmix_mca_base_component_repository_open: unable to open %s: %s (ignored)",
                            ri->ri_base, err_msg);

        if( pmix_mca_base_component_track_load_errors ) {
            pmix_mca_base_failed_component_t *f_comp = PMIX_NEW(pmix_mca_base_failed_component_t);
            f_comp->comp = ri;
            if (0 > asprintf(&(f_comp->error_msg), "%s", err_msg)) {
                PMIX_RELEASE(f_comp);
                return PMIX_ERR_BAD_PARAM;
            }
            pmix_list_append(&framework->framework_failed_components, &f_comp->super);
        }

        return PMIX_ERR_BAD_PARAM;
    }

    /* Successfully opened the component; now find the public struct.
       Malloc out enough space for it. */

    do {
        ret = asprintf (&struct_name, "mca_%s_%s_component", ri->ri_type, ri->ri_name);
        if (0 > ret) {
            ret = PMIX_ERR_OUT_OF_RESOURCE;
            break;
        }

        mitem = PMIX_NEW(pmix_mca_base_component_list_item_t);
        if (NULL == mitem) {
            ret = PMIX_ERR_OUT_OF_RESOURCE;
            break;
        }

        err_msg = NULL;
        ret = pmix_pdl_lookup(ri->ri_dlhandle, struct_name, (void**) &component_struct, &err_msg);
        if (PMIX_SUCCESS != ret || NULL == component_struct) {
            if (NULL == err_msg) {
                err_msg = "pmix_dl_loookup() error message was NULL!";
            }
            pmix_output_verbose(vl, 0, "pmix_mca_base_component_repository_open: \"%s\" does not appear to be a valid "
                                "%s MCA dynamic component (ignored): %s. ret %d", ri->ri_base, ri->ri_type, err_msg, ret);

            ret = PMIX_ERR_BAD_PARAM;
            break;
        }

        /* done with the structure name */
        free (struct_name);
        struct_name = NULL;

        /* We found the public struct.  Make sure its MCA major.minor
           version is the same as ours. TODO -- add checks for project version (from framework) */
        if (!(PMIX_MCA_BASE_VERSION_MAJOR == component_struct->pmix_mca_major_version &&
              PMIX_MCA_BASE_VERSION_MINOR == component_struct->pmix_mca_minor_version)) {
            pmix_output_verbose(vl, 0, "pmix_mca_base_component_repository_open: %s \"%s\" uses an MCA interface that is "
                                "not recognized (component MCA v%d.%d.%d != supported MCA v%d.%d.%d) -- ignored",
                                ri->ri_type, ri->ri_path, component_struct->pmix_mca_major_version,
                                component_struct->pmix_mca_minor_version, component_struct->pmix_mca_release_version,
                                PMIX_MCA_BASE_VERSION_MAJOR, PMIX_MCA_BASE_VERSION_MINOR, PMIX_MCA_BASE_VERSION_RELEASE);
            ret = PMIX_ERR_BAD_PARAM;
            break;
        }

        /* Also check that the component struct framework and component
           names match the expected names from the filename */
        if (0 != strcmp(component_struct->pmix_mca_type_name, ri->ri_type) ||
            0 != strcmp(component_struct->pmix_mca_component_name, ri->ri_name)) {
            pmix_output_verbose(vl, 0, "Component file data does not match filename: %s (%s / %s) != %s %s -- ignored",
                                ri->ri_path, ri->ri_type, ri->ri_name,
                                component_struct->pmix_mca_type_name,
                                component_struct->pmix_mca_component_name);
            ret = PMIX_ERR_BAD_PARAM;
            break;
        }

        /* Alles gut.  Save the component struct, and register this
           component to be closed later. */

        ri->ri_component_struct = mitem->cli_component = component_struct;
        ri->ri_refcnt = 1;
        pmix_list_append(&framework->framework_components, &mitem->super);

        pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_INFO, 0, "pmix_mca_base_component_repository_open: opened dynamic %s MCA "
                             "component \"%s\"", ri->ri_type, ri->ri_name);

        return PMIX_SUCCESS;
    } while (0);

    if (mitem) {
        PMIX_RELEASE(mitem);
    }

    if (struct_name) {
        free (struct_name);
    }

    pmix_pdl_close (ri->ri_dlhandle);
    ri->ri_dlhandle = NULL;

    return ret;
#else

    /* no dlopen support */
    return PMIX_ERR_NOT_SUPPORTED;
#endif
}

/*
 * Finalize the repository -- close everything that's still open.
 */
void pmix_mca_base_component_repository_finalize(void)
{
    if (!initialized) {
        return;
    }

    initialized = false;

#if PMIX_HAVE_PDL_SUPPORT
    pmix_list_t *component_list;
    void *node, *key;
    size_t key_size;
    int ret;

    ret = pmix_hash_table_get_first_key_ptr (&pmix_mca_base_component_repository, &key, &key_size,
                                             (void **) &component_list, &node);
    while (PMIX_SUCCESS == ret) {
        PMIX_LIST_RELEASE(component_list);
        ret = pmix_hash_table_get_next_key_ptr (&pmix_mca_base_component_repository, &key,
                                                &key_size, (void **) &component_list,
                                                node, &node);
    }

    (void) pmix_mca_base_framework_close(&pmix_pdl_base_framework);
    PMIX_DESTRUCT(&pmix_mca_base_component_repository);
#endif
}

#if PMIX_HAVE_PDL_SUPPORT

/*
 * Basic sentinel values, and construct the inner list
 */
static void ri_constructor (pmix_mca_base_component_repository_item_t *ri)
{
    memset(ri->ri_type, 0, sizeof(ri->ri_type));
    ri->ri_dlhandle = NULL;
    ri->ri_component_struct = NULL;
    ri->ri_path = NULL;
}


/*
 * Close a component
 */
static void ri_destructor (pmix_mca_base_component_repository_item_t *ri)
{
    /* dlclose the component if it is still open */
    pmix_mca_base_component_repository_release_internal (ri);

    /* It should be obvious, but I'll state it anyway because it bit me
       during debugging: after the dlclose(), the pmix_mca_base_component_t
       pointer is no longer valid because it has [potentially] been
       unloaded from memory.  So don't try to use it.  :-) */

    if (ri->ri_path) {
        free (ri->ri_path);
    }

    if (ri->ri_base) {
        free (ri->ri_base);
    }
}

#endif /* PMIX_HAVE_PDL_SUPPORT */

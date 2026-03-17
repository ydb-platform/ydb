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
 * Copyright (c) 2017 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/mca/dl/base/base.h"
#include "opal/constants.h"
#include "opal/class/opal_hash_table.h"
#include "opal/util/basename.h"

#if OPAL_HAVE_DL_SUPPORT

/*
 * Private types
 */
static void ri_constructor(mca_base_component_repository_item_t *ri);
static void ri_destructor(mca_base_component_repository_item_t *ri);
OBJ_CLASS_INSTANCE(mca_base_component_repository_item_t, opal_list_item_t,
                   ri_constructor, ri_destructor);

#endif /* OPAL_HAVE_DL_SUPPORT */

static void clf_constructor(opal_object_t *obj);
static void clf_destructor(opal_object_t *obj);

OBJ_CLASS_INSTANCE(mca_base_failed_component_t, opal_list_item_t,
                   clf_constructor, clf_destructor);


static void clf_constructor(opal_object_t *obj)
{
    mca_base_failed_component_t *cli = (mca_base_failed_component_t *) obj;
    cli->comp = NULL;
    cli->error_msg = NULL;
}

static void clf_destructor(opal_object_t *obj)
{
    mca_base_failed_component_t *cli = (mca_base_failed_component_t *) obj;
    cli->comp = NULL;
    if( NULL != cli->error_msg ) {
        free(cli->error_msg);
        cli->error_msg = NULL;
    }
}

/*
 * Private variables
 */
static bool initialized = false;


#if OPAL_HAVE_DL_SUPPORT

static opal_hash_table_t mca_base_component_repository;

/* two-level macro for stringifying a number */
#define STRINGIFYX(x) #x
#define STRINGIFY(x) STRINGIFYX(x)

static int process_repository_item (const char *filename, void *data)
{
    char name[MCA_BASE_MAX_COMPONENT_NAME_LEN + 1];
    char type[MCA_BASE_MAX_TYPE_NAME_LEN + 1];
    mca_base_component_repository_item_t *ri;
    opal_list_t *component_list;
    char *base;
    int ret;

    base = opal_basename (filename);
    if (NULL == base) {
        return OPAL_ERROR;
    }

    /* check if the plugin has the appropriate prefix */
    if (0 != strncmp (base, "mca_", 4)) {
        free (base);
        return OPAL_SUCCESS;
    }

    /* read framework and component names. framework names may not include an _
     * but component names may */
    ret = sscanf (base, "mca_%" STRINGIFY(MCA_BASE_MAX_TYPE_NAME_LEN) "[^_]_%"
                  STRINGIFY(MCA_BASE_MAX_COMPONENT_NAME_LEN) "s", type, name);
    if (0 > ret) {
        /* does not patch the expected template. skip */
        free(base);
        return OPAL_SUCCESS;
    }

    /* lookup the associated framework list and create if it doesn't already exist */
    ret = opal_hash_table_get_value_ptr (&mca_base_component_repository, type,
                                         strlen (type), (void **) &component_list);
    if (OPAL_SUCCESS != ret) {
        component_list = OBJ_NEW(opal_list_t);
        if (NULL == component_list) {
            free (base);
            /* OOM. nothing to do but fail */
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        ret = opal_hash_table_set_value_ptr (&mca_base_component_repository, type,
                                             strlen (type), (void *) component_list);
        if (OPAL_SUCCESS != ret) {
            free (base);
            OBJ_RELEASE(component_list);
            return ret;
        }
    }

    /* check for duplicate components */
    OPAL_LIST_FOREACH(ri, component_list, mca_base_component_repository_item_t) {
        if (0 == strcmp (ri->ri_name, name)) {
            /* already scanned this component */
            free (base);
            return OPAL_SUCCESS;
        }
    }

    ri = OBJ_NEW(mca_base_component_repository_item_t);
    if (NULL == ri) {
        free (base);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    ri->ri_base = base;

    ri->ri_path = strdup (filename);
    if (NULL == ri->ri_path) {
        OBJ_RELEASE(ri);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* strncpy does not guarantee a \0 */
    ri->ri_type[MCA_BASE_MAX_TYPE_NAME_LEN] = '\0';
    strncpy (ri->ri_type, type, MCA_BASE_MAX_TYPE_NAME_LEN);

    ri->ri_name[MCA_BASE_MAX_TYPE_NAME_LEN] = '\0';
    strncpy (ri->ri_name, name, MCA_BASE_MAX_COMPONENT_NAME_LEN);

    opal_list_append (component_list, &ri->super);

    return OPAL_SUCCESS;
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

#endif /* OPAL_HAVE_DL_SUPPORT */

int mca_base_component_repository_add (const char *path)
{
#if OPAL_HAVE_DL_SUPPORT
    char *path_to_use = NULL, *dir, *ctx;
    const char sep[] = {OPAL_ENV_SEP, '\0'};

    if (NULL == path) {
        /* nothing to do */
        return OPAL_SUCCESS;
    }

    path_to_use = strdup (path);

    dir = strtok_r (path_to_use, sep, &ctx);
    do {
        if ((0 == strcmp(dir, "USER_DEFAULT") || 0 == strcmp(dir, "USR_DEFAULT"))
            && NULL != mca_base_user_default_path) {
            dir = mca_base_user_default_path;
        } else if (0 == strcmp(dir, "SYS_DEFAULT") ||
                   0 == strcmp(dir, "SYSTEM_DEFAULT")) {
            dir = mca_base_system_default_path;
        }

        if (0 != opal_dl_foreachfile(dir, process_repository_item, NULL)) {
            break;
        }
    } while (NULL != (dir = strtok_r (NULL, sep, &ctx)));

    free (path_to_use);

#endif /* OPAL_HAVE_DL_SUPPORT */

    return OPAL_SUCCESS;
}


/*
 * Initialize the repository
 */
int mca_base_component_repository_init(void)
{
  /* Setup internal structures */

  if (!initialized) {
#if OPAL_HAVE_DL_SUPPORT

    /* Initialize the dl framework */
    int ret = mca_base_framework_open(&opal_dl_base_framework, 0);
    if (OPAL_SUCCESS != ret) {
        opal_output(0, "%s %d:%s failed -- process will likely abort (open the dl framework returned %d instead of OPAL_SUCCESS)\n",
                    __FILE__, __LINE__, __func__, ret);
        return ret;
    }
    opal_dl_base_select();

    OBJ_CONSTRUCT(&mca_base_component_repository, opal_hash_table_t);
    ret = opal_hash_table_init (&mca_base_component_repository, 128);
    if (OPAL_SUCCESS != ret) {
        (void) mca_base_framework_close (&opal_dl_base_framework);
        return ret;
    }

    ret = mca_base_component_repository_add (mca_base_component_path);
    if (OPAL_SUCCESS != ret) {
        OBJ_DESTRUCT(&mca_base_component_repository);
        (void) mca_base_framework_close (&opal_dl_base_framework);
        return ret;
    }
#endif

    initialized = true;
  }

  /* All done */

  return OPAL_SUCCESS;
}

int mca_base_component_repository_get_components (mca_base_framework_t *framework,
                                                  opal_list_t **framework_components)
{
    *framework_components = NULL;
#if OPAL_HAVE_DL_SUPPORT
    return opal_hash_table_get_value_ptr (&mca_base_component_repository, framework->framework_name,
                                          strlen (framework->framework_name), (void **) framework_components);
#endif
    return OPAL_ERR_NOT_FOUND;
}

#if OPAL_HAVE_DL_SUPPORT
static void mca_base_component_repository_release_internal (mca_base_component_repository_item_t *ri) {
    int group_id;

    group_id = mca_base_var_group_find (NULL, ri->ri_type, ri->ri_name);
    if (0 <= group_id) {
        /* ensure all variables are deregistered before we dlclose the component */
        mca_base_var_group_deregister (group_id);
    }

    /* Close the component (and potentially unload it from memory */
    if (ri->ri_dlhandle) {
        opal_dl_close(ri->ri_dlhandle);
        ri->ri_dlhandle = NULL;
    }
}
#endif

#if OPAL_HAVE_DL_SUPPORT
static mca_base_component_repository_item_t *find_component (const char *type, const char *name)
{
    mca_base_component_repository_item_t *ri;
    opal_list_t *component_list;
    int ret;

    ret = opal_hash_table_get_value_ptr (&mca_base_component_repository, type,
                                         strlen (type), (void **) &component_list);
    if (OPAL_SUCCESS != ret) {
        /* component does not exist in the repository */
        return NULL;
    }

    OPAL_LIST_FOREACH(ri, component_list, mca_base_component_repository_item_t) {
        if (0 == strcmp (ri->ri_name, name)) {
            return ri;
        }
    }

    return NULL;
}
#endif

void mca_base_component_repository_release(const mca_base_component_t *component)
{
#if OPAL_HAVE_DL_SUPPORT
    mca_base_component_repository_item_t *ri;

    ri = find_component (component->mca_type_name, component->mca_component_name);
    if (NULL != ri && !(--ri->ri_refcnt)) {
        mca_base_component_repository_release_internal (ri);
    }
#endif
}

int mca_base_component_repository_retain_component (const char *type, const char *name)
{
#if OPAL_HAVE_DL_SUPPORT
    mca_base_component_repository_item_t *ri = find_component(type, name);

    if (NULL != ri) {
        ++ri->ri_refcnt;
        return OPAL_SUCCESS;
    }

    return OPAL_ERR_NOT_FOUND;
#else
    return OPAL_ERR_NOT_SUPPORTED;
#endif
}

int mca_base_component_repository_open (mca_base_framework_t *framework,
                                        mca_base_component_repository_item_t *ri)
{
#if OPAL_HAVE_DL_SUPPORT
    mca_base_component_t *component_struct;
    mca_base_component_list_item_t *mitem = NULL;
    char *struct_name = NULL;
    int vl, ret;

    opal_output_verbose(MCA_BASE_VERBOSE_INFO, 0, "mca_base_component_repository_open: examining dynamic "
                        "%s MCA component \"%s\" at path %s", ri->ri_type, ri->ri_name, ri->ri_path);

    vl = mca_base_component_show_load_errors ? MCA_BASE_VERBOSE_ERROR : MCA_BASE_VERBOSE_INFO;

    /* Ensure that this component is not already loaded (should only happen
       if it was statically loaded).  It's an error if it's already
       loaded because we're evaluating this file -- not this component.
       Hence, returning OPAL_ERR_PARAM indicates that the *file* failed
       to load, not the component. */

    OPAL_LIST_FOREACH(mitem, &framework->framework_components, mca_base_component_list_item_t) {
        if (0 == strcmp(mitem->cli_component->mca_component_name, ri->ri_name)) {
            opal_output_verbose (MCA_BASE_VERBOSE_INFO, 0, "mca_base_component_repository_open: already loaded (ignored)");
            return OPAL_ERR_BAD_PARAM;
        }
    }

    /* silence coverity issue (invalid free) */
    mitem = NULL;

    if (NULL != ri->ri_dlhandle) {
        opal_output_verbose (MCA_BASE_VERBOSE_INFO, 0, "mca_base_component_repository_open: already loaded. returning cached component");
        mitem = OBJ_NEW(mca_base_component_list_item_t);
        if (NULL == mitem) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        mitem->cli_component = ri->ri_component_struct;
        opal_list_append (&framework->framework_components, &mitem->super);

        return OPAL_SUCCESS;
    }

    if (0 != strcmp (ri->ri_type, framework->framework_name)) {
        /* shouldn't happen. attempting to open a component belonging to
         * another framework. if this happens it is likely a MCA base
         * bug so assert */
        assert (0);
        return OPAL_ERR_NOT_SUPPORTED;
    }

    /* Now try to load the component */

    char *err_msg = NULL;
    if (OPAL_SUCCESS != opal_dl_open(ri->ri_path, true, false, &ri->ri_dlhandle, &err_msg)) {
        if (NULL == err_msg) {
            err_msg = "opal_dl_open() error message was NULL!";
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
        opal_output_verbose(vl, 0, "mca_base_component_repository_open: unable to open %s: %s (ignored)",
                            ri->ri_base, err_msg);

        if( mca_base_component_track_load_errors ) {
            mca_base_failed_component_t *f_comp = OBJ_NEW(mca_base_failed_component_t);
            f_comp->comp = ri;
            asprintf(&(f_comp->error_msg), "%s", err_msg);
            opal_list_append(&framework->framework_failed_components, &f_comp->super);
        }

        return OPAL_ERR_BAD_PARAM;
    }

    /* Successfully opened the component; now find the public struct.
       Malloc out enough space for it. */

    do {
        ret = asprintf (&struct_name, "mca_%s_%s_component", ri->ri_type, ri->ri_name);
        if (0 > ret) {
            ret = OPAL_ERR_OUT_OF_RESOURCE;
            break;
        }

        mitem = OBJ_NEW(mca_base_component_list_item_t);
        if (NULL == mitem) {
            ret = OPAL_ERR_OUT_OF_RESOURCE;
            break;
        }

        err_msg = NULL;
        ret = opal_dl_lookup(ri->ri_dlhandle, struct_name, (void**) &component_struct, &err_msg);
        if (OPAL_SUCCESS != ret || NULL == component_struct) {
            if (NULL == err_msg) {
                err_msg = "opal_dl_loookup() error message was NULL!";
            }
            opal_output_verbose(vl, 0, "mca_base_component_repository_open: \"%s\" does not appear to be a valid "
                                "%s MCA dynamic component (ignored): %s. ret %d", ri->ri_base, ri->ri_type, err_msg, ret);

            ret = OPAL_ERR_BAD_PARAM;
            break;
        }

        /* done with the structure name */
        free (struct_name);
        struct_name = NULL;

        /* We found the public struct.  Make sure its MCA major.minor
           version is the same as ours. TODO -- add checks for project version (from framework) */
        if (!(MCA_BASE_VERSION_MAJOR == component_struct->mca_major_version &&
              MCA_BASE_VERSION_MINOR == component_struct->mca_minor_version)) {
            opal_output_verbose(vl, 0, "mca_base_component_repository_open: %s \"%s\" uses an MCA interface that is "
                                "not recognized (component MCA v%d.%d.%d != supported MCA v%d.%d.%d) -- ignored",
                                ri->ri_type, ri->ri_path, component_struct->mca_major_version,
                                component_struct->mca_minor_version, component_struct->mca_release_version,
                                MCA_BASE_VERSION_MAJOR, MCA_BASE_VERSION_MINOR, MCA_BASE_VERSION_RELEASE);
            ret = OPAL_ERR_BAD_PARAM;
            break;
        }

        /* Also check that the component struct framework and component
           names match the expected names from the filename */
        if (0 != strcmp(component_struct->mca_type_name, ri->ri_type) ||
            0 != strcmp(component_struct->mca_component_name, ri->ri_name)) {
            opal_output_verbose(vl, 0, "Component file data does not match filename: %s (%s / %s) != %s %s -- ignored",
                                ri->ri_path, ri->ri_type, ri->ri_name,
                                component_struct->mca_type_name,
                                component_struct->mca_component_name);
            ret = OPAL_ERR_BAD_PARAM;
            break;
        }

        /* Alles gut.  Save the component struct, and register this
           component to be closed later. */

        ri->ri_component_struct = mitem->cli_component = component_struct;
        ri->ri_refcnt = 1;
        opal_list_append(&framework->framework_components, &mitem->super);

        opal_output_verbose (MCA_BASE_VERBOSE_INFO, 0, "mca_base_component_repository_open: opened dynamic %s MCA "
                             "component \"%s\"", ri->ri_type, ri->ri_name);

        return OPAL_SUCCESS;
    } while (0);

    if (mitem) {
        OBJ_RELEASE(mitem);
    }

    if (struct_name) {
        free (struct_name);
    }

    opal_dl_close (ri->ri_dlhandle);
    ri->ri_dlhandle = NULL;

    return ret;
#else

    /* no dlopen support */
    return OPAL_ERR_NOT_SUPPORTED;
#endif
}

/*
 * Finalize the repository -- close everything that's still open.
 */
void mca_base_component_repository_finalize(void)
{
    if (!initialized) {
        return;
    }

    initialized = false;

#if OPAL_HAVE_DL_SUPPORT
    opal_list_t *component_list;
    void *node, *key;
    size_t key_size;
    int ret;

    ret = opal_hash_table_get_first_key_ptr (&mca_base_component_repository, &key, &key_size,
                                             (void **) &component_list, &node);
    while (OPAL_SUCCESS == ret) {
        OPAL_LIST_RELEASE(component_list);
        ret = opal_hash_table_get_next_key_ptr (&mca_base_component_repository, &key,
                                                &key_size, (void **) &component_list,
                                                node, &node);
    }

    (void) mca_base_framework_close(&opal_dl_base_framework);
    OBJ_DESTRUCT(&mca_base_component_repository);
#endif
}

#if OPAL_HAVE_DL_SUPPORT

/*
 * Basic sentinel values, and construct the inner list
 */
static void ri_constructor (mca_base_component_repository_item_t *ri)
{
    memset(ri->ri_type, 0, sizeof(ri->ri_type));
    ri->ri_dlhandle = NULL;
    ri->ri_component_struct = NULL;
    ri->ri_path = NULL;
}


/*
 * Close a component
 */
static void ri_destructor (mca_base_component_repository_item_t *ri)
{
    /* dlclose the component if it is still open */
    mca_base_component_repository_release_internal (ri);

    /* It should be obvious, but I'll state it anyway because it bit me
       during debugging: after the dlclose(), the mca_base_component_t
       pointer is no longer valid because it has [potentially] been
       unloaded from memory.  So don't try to use it.  :-) */

    if (ri->ri_path) {
        free (ri->ri_path);
    }

    if (ri->ri_base) {
        free (ri->ri_base);
    }
}

#endif /* OPAL_HAVE_DL_SUPPORT */

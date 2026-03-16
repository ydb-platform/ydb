/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_DISABLE_ENABLE_MEM_DEBUG 1
#include "opal_config.h"
#include "opal/mca/base/base.h"
#include "opal/runtime/opal_params.h"
#include "opal/mca/base/mca_base_pvar.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/mca/allocator/base/base.h"

#include "opal/util/argv.h"

#include "mpool_hugepage.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif
#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif
#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif
#ifdef HAVE_MNTENT_H
#include <mntent.h>
#endif

#include <fcntl.h>

/*
 * Note that some OS's (e.g., NetBSD and Solaris) have statfs(), but
 * no struct statfs (!).  So check to make sure we have struct statfs
 * before allowing the use of statfs().
 */
#if defined(HAVE_STATFS) && (defined(HAVE_STRUCT_STATFS_F_FSTYPENAME) || \
                             defined(HAVE_STRUCT_STATFS_F_TYPE))
#define USE_STATFS 1
#endif


/*
 * Local functions
 */
static int mca_mpool_hugepage_open (void);
static int mca_mpool_hugepage_close (void);
static int mca_mpool_hugepage_register (void);
static int mca_mpool_hugepage_query (const char *hints, int *priority,
                                     mca_mpool_base_module_t **module);
static void mca_mpool_hugepage_find_hugepages (void);

static int mca_mpool_hugepage_priority;
static unsigned long mca_mpool_hugepage_page_size;

mca_mpool_hugepage_component_t mca_mpool_hugepage_component = {
    {
        /* First, the mca_base_component_t struct containing meta
           information about the component itself */

        .mpool_version ={
            MCA_MPOOL_BASE_VERSION_3_0_0,

            .mca_component_name = "hugepage",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),
            .mca_open_component = mca_mpool_hugepage_open,
            .mca_close_component = mca_mpool_hugepage_close,
            .mca_register_component_params = mca_mpool_hugepage_register,
        },
        .mpool_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .mpool_query = mca_mpool_hugepage_query,
    },
};

/**
  * component open/close/init function
  */

static int mca_mpool_hugepage_register(void)
{
    mca_mpool_hugepage_priority = 50;
    (void) mca_base_component_var_register (&mca_mpool_hugepage_component.super.mpool_version,
                                            "priority", "Default priority of the hugepage mpool component "
                                            "(default: 50)", MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                            OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_LOCAL,
                                            &mca_mpool_hugepage_priority);

    mca_mpool_hugepage_page_size = 1 << 21;
    (void) mca_base_component_var_register (&mca_mpool_hugepage_component.super.mpool_version,
                                            "page_size", "Default huge page size of the hugepage mpool component "
                                            "(default: 2M)", MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                            OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_LOCAL,
                                            &mca_mpool_hugepage_page_size);

    mca_mpool_hugepage_component.bytes_allocated = 0;
    (void) mca_base_component_pvar_register (&mca_mpool_hugepage_component.super.mpool_version,
                                             "bytes_allocated", "Number of bytes currently allocated in the mpool "
                                             "hugepage component", OPAL_INFO_LVL_3, MCA_BASE_PVAR_CLASS_SIZE,
                                             MCA_BASE_VAR_TYPE_UNSIGNED_LONG, NULL, MCA_BASE_VAR_BIND_NO_OBJECT,
                                             MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_CONTINUOUS,
                                             NULL, NULL, NULL, &mca_mpool_hugepage_component.bytes_allocated);

    return OPAL_SUCCESS;
}

static int mca_mpool_hugepage_open (void)
{
    mca_mpool_hugepage_module_t *hugepage_module;
    mca_mpool_hugepage_hugepage_t *hp;
    int module_index, rc;

    OBJ_CONSTRUCT(&mca_mpool_hugepage_component.huge_pages, opal_list_t);
    mca_mpool_hugepage_find_hugepages ();

    if (0 == opal_list_get_size (&mca_mpool_hugepage_component.huge_pages)) {
        return OPAL_SUCCESS;
    }

    mca_mpool_hugepage_component.modules = (mca_mpool_hugepage_module_t *)
        calloc (opal_list_get_size (&mca_mpool_hugepage_component.huge_pages),
                sizeof (mca_mpool_hugepage_module_t));
    if (NULL == mca_mpool_hugepage_component.modules) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    module_index = 0;
    OPAL_LIST_FOREACH(hp, &mca_mpool_hugepage_component.huge_pages, mca_mpool_hugepage_hugepage_t) {
        hugepage_module = mca_mpool_hugepage_component.modules + module_index;
        rc = mca_mpool_hugepage_module_init (hugepage_module, hp);
        if (OPAL_SUCCESS != rc) {
            continue;
        }
        module_index++;
    }

    mca_mpool_hugepage_component.module_count = module_index;

    return OPAL_SUCCESS;
}

static int mca_mpool_hugepage_close (void)
{
    OPAL_LIST_DESTRUCT(&mca_mpool_hugepage_component.huge_pages);

    for (int i = 0 ; i < mca_mpool_hugepage_component.module_count ; ++i) {
        mca_mpool_hugepage_module_t *module =  mca_mpool_hugepage_component.modules + i;
        module->super.mpool_finalize (&module->super);
    }

    free (mca_mpool_hugepage_component.modules);
    mca_mpool_hugepage_component.modules = NULL;

    return OPAL_SUCCESS;
}

#ifdef HAVE_MNTENT_H
static int page_compare (opal_list_item_t **a, opal_list_item_t **b) {
    mca_mpool_hugepage_hugepage_t *pagea = (mca_mpool_hugepage_hugepage_t *) *a;
    mca_mpool_hugepage_hugepage_t *pageb = (mca_mpool_hugepage_hugepage_t *) *b;
    if (pagea->page_size > pageb->page_size) {
        return 1;
    } else if (pagea->page_size < pageb->page_size) {
        return -1;
    }

    return 0;
}
#endif

static void mca_mpool_hugepage_find_hugepages (void) {
#ifdef HAVE_MNTENT_H
    mca_mpool_hugepage_hugepage_t *hp;
    FILE *fh;
    struct mntent *mntent;
    char *opts, *tok, *ctx;

    fh = setmntent ("/proc/mounts", "r");
    if (NULL == fh) {
        return;
    }

    while (NULL != (mntent = getmntent(fh))) {
        unsigned long page_size = 0;

        if (0 != strcmp(mntent->mnt_type, "hugetlbfs")) {
            continue;
        }

        opts = strdup(mntent->mnt_opts);
        if (NULL == opts) {
            break;
        }

        tok = strtok_r (opts, ",", &ctx);

        do {
            if (0 == strncmp (tok, "pagesize", 8)) {
                break;
            }
            tok = strtok_r (NULL, ",", &ctx);
        } while (tok);

        if (!tok) {
#if defined(USE_STATFS)
            struct statfs info;

            statfs (mntent->mnt_dir, &info);
#elif defined(HAVE_STATVFS)
            struct statvfs info;
            statvfs (mntent->mnt_dir, &info);
#endif
            page_size = info.f_bsize;
        } else {
            (void) sscanf (tok, "pagesize=%lu", &page_size);
        }
        free(opts);

        if (0 == page_size) {
            /* could not get page size */
            continue;
        }

        hp = OBJ_NEW(mca_mpool_hugepage_hugepage_t);
        if (NULL == hp) {
            break;
        }

        hp->path = strdup (mntent->mnt_dir);
        hp->page_size = page_size;
        
        if(0 == access (hp->path, R_OK | W_OK)){        
            opal_output_verbose (MCA_BASE_VERBOSE_INFO, opal_mpool_base_framework.framework_output,
                                 "found huge page with size = %lu, path = %s, mmap flags = 0x%x, adding to list",
                                 hp->page_size, hp->path, hp->mmap_flags);
            opal_list_append (&mca_mpool_hugepage_component.huge_pages, &hp->super);
        } else {
            opal_output_verbose (MCA_BASE_VERBOSE_INFO, opal_mpool_base_framework.framework_output,
                                 "found huge page with size = %lu, path = %s, mmap flags = 0x%x, with invalid " 
                                 "permissions, skipping", hp->page_size, hp->path, hp->mmap_flags);
        }        
    }

    opal_list_sort (&mca_mpool_hugepage_component.huge_pages, page_compare);

    endmntent (fh);
#endif
}

static int mca_mpool_hugepage_query (const char *hints, int *priority_out,
                                     mca_mpool_base_module_t **module)
{
    unsigned long page_size = 0;
    char **hints_array;
    int my_priority = mca_mpool_hugepage_priority;
    char *tmp;
    bool found = false;

    if (0 == mca_mpool_hugepage_component.module_count) {
        return OPAL_ERR_NOT_AVAILABLE;
    }

    if (hints) {
        hints_array = opal_argv_split (hints, ',');
        if (NULL == hints_array) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        for (int i = 0 ; hints_array[i] ; ++i) {
            char *key = hints_array[i];
            char *value = NULL;

            if (NULL != (tmp = strchr (key, '='))) {
                value = tmp + 1;
                *tmp = '\0';
            }

            if (0 == strcasecmp ("mpool", key)) {
                if (value && 0 == strcasecmp ("hugepage", value)) {
                    /* this mpool was requested by name */
                    my_priority = 100;
                    opal_output_verbose (MCA_BASE_VERBOSE_INFO, opal_mpool_base_framework.framework_output,
                                         "hugepage mpool matches hint: %s=%s", key, value);
                } else {
                    /* different mpool requested */
                    my_priority = 0;
                    opal_output_verbose (MCA_BASE_VERBOSE_INFO, opal_mpool_base_framework.framework_output,
                                         "hugepage mpool does not match hint: %s=%s", key, value);
                    opal_argv_free (hints_array);
                    return OPAL_ERR_NOT_FOUND;
                }
            }

            if (0 == strcasecmp ("page_size", key) && value) {
                page_size = strtoul (value, &tmp, 0);
                if (*tmp) {
                    switch (*tmp) {
                    case 'g':
                    case 'G':
                        page_size *= 1024;
                        /* fall through */
                    case 'm':
                    case 'M':
                        page_size *= 1024;
                        /* fall through */
                    case 'k':
                    case 'K':
                        page_size *= 1024;
                        break;
                    default:
                        page_size = -1;
                    }
                }
                opal_output_verbose (MCA_BASE_VERBOSE_INFO, opal_mpool_base_framework.framework_output,
                                     "hugepage mpool requested page size: %lu", page_size);
            }
        }

        opal_argv_free (hints_array);
    }

    if (0 == page_size) {
        /* use default huge page size */
        page_size = mca_mpool_hugepage_page_size;
        if (my_priority < 100) {
            /* take a priority hit if this mpool was not asked for by name */
            my_priority = 0;
        }
        opal_output_verbose (MCA_BASE_VERBOSE_WARN, opal_mpool_base_framework.framework_output,
                             "hugepage mpool did not match any hints: %s", hints);
    }

    for (int i = 0 ; i < mca_mpool_hugepage_component.module_count ; ++i) {
        mca_mpool_hugepage_module_t *hugepage_module = mca_mpool_hugepage_component.modules + i;

        if (hugepage_module->huge_page->page_size != page_size) {
            continue;
        }

        my_priority = (my_priority < 80) ? my_priority + 20 : 100;

        if (module) {
            *module = &hugepage_module->super;
        }

        opal_output_verbose (MCA_BASE_VERBOSE_INFO, opal_mpool_base_framework.framework_output,
                             "matches page size hint. page size: %lu, path: %s, mmap flags: "
                             "0x%x", page_size, hugepage_module->huge_page->path,
                             hugepage_module->huge_page->mmap_flags);
        found = true;
        break;
    }

    if (!found) {
        opal_output_verbose (MCA_BASE_VERBOSE_WARN, opal_mpool_base_framework.framework_output,
                             "could not find page matching page request: %lu", page_size);
        return OPAL_ERR_NOT_FOUND;
    }

    if (priority_out) {
        *priority_out = my_priority;
    }

    return OPAL_SUCCESS;
}

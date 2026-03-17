/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdlib.h>
#include <dlfcn.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pmix_common.h"
#include "src/mca/pdl/pdl.h"
#include "src/util/argv.h"
#include "src/util/error.h"

#include "pdl_pdlopen.h"


/*
 * Trivial helper function to avoid replicating code
 */
static void do_pdlopen(const char *fname, int flags,
                      void **handle, char **err_msg)
{
    assert(handle);

    *handle = dlopen(fname, flags);

    if (NULL != err_msg) {
        if (NULL != *handle) {
            *err_msg = NULL;
        } else {
            *err_msg = dlerror();
        }
    }
}


static int pdlopen_open(const char *fname, bool use_ext, bool private_namespace,
                       pmix_pdl_handle_t **handle, char **err_msg)
{
    int rc;

    assert(handle);

    *handle = NULL;

    /* Setup the dlopen flags */
    int flags = RTLD_LAZY;
    if (private_namespace) {
        flags |= RTLD_LOCAL;
    } else {
        flags |= RTLD_GLOBAL;
    }

    /* If the caller wants to use filename extensions, loop through
       them */
    void *local_handle = NULL;
    if (use_ext && NULL != fname) {
        int i;
        char *ext;

        for (i = 0, ext = mca_pdl_pdlopen_component.filename_suffixes[i];
             NULL != ext;
             ext = mca_pdl_pdlopen_component.filename_suffixes[++i]) {
            char *name;

            rc = asprintf(&name, "%s%s", fname, ext);
            if (0 > rc) {
                return PMIX_ERR_NOMEM;
            }
            if (NULL == name) {
                return PMIX_ERR_IN_ERRNO;
            }

            /* Does the file exist? */
            struct stat buf;
            if (stat(name, &buf) < 0) {
                free(name);
                if (NULL != err_msg) {
                    *err_msg = "File not found";
                }
                continue;
            }

            /* Yes, the file exists -- try to dlopen it.  If we can't
               dlopen it, bail. */
            do_pdlopen(name, flags, &local_handle, err_msg);
            free(name);
            break;
        }
    }

    /* Otherwise, the caller does not want to use filename extensions,
       so just use the single filename that the caller provided */
    else {
        do_pdlopen(fname, flags, &local_handle, err_msg);
    }

    if (NULL != local_handle) {
        *handle = calloc(1, sizeof(pmix_pdl_handle_t));
        (*handle)->dlopen_handle = local_handle;

#if PMIX_ENABLE_DEBUG
        if( NULL != fname ) {
            (*handle)->filename = strdup(fname);
        }
        else {
            (*handle)->filename = strdup("(null)");
        }
#endif
    }
    return (NULL != local_handle) ? PMIX_SUCCESS : PMIX_ERROR;
}


static int pdlopen_lookup(pmix_pdl_handle_t *handle, const char *symbol,
                         void **ptr, char **err_msg)
{
    assert(handle);
    assert(handle->dlopen_handle);
    assert(symbol);
    assert(ptr);

    *ptr = dlsym(handle->dlopen_handle, symbol);
    if (NULL != *ptr) {
        return PMIX_SUCCESS;
    }

    if (NULL != err_msg) {
        *err_msg = dlerror();
    }
    return PMIX_ERROR;
}


static int pdlopen_close(pmix_pdl_handle_t *handle)
{
    assert(handle);

    int ret;
    ret = dlclose(handle->dlopen_handle);

#if PMIX_ENABLE_DEBUG
    free(handle->filename);
#endif
    free(handle);

    return ret;
}

/*
 * Scan all the files in a directory (or path) and invoke a callback
 * on each one.
 */
static int pdlopen_foreachfile(const char *search_path,
                              int (*func)(const char *filename, void *data),
                              void *data)
{
    int ret;
    DIR *dp = NULL;
    char **dirs = NULL;
    char **good_files = NULL;

    dirs = pmix_argv_split(search_path, PMIX_ENV_SEP);
    for (int i = 0; NULL != dirs && NULL != dirs[i]; ++i) {

        dp = opendir(dirs[i]);
        if (NULL == dp) {
            ret = PMIX_ERR_IN_ERRNO;
            goto error;
        }

        struct dirent *de;
        while (NULL != (de = readdir(dp))) {

            /* Make the absolute path name */
            char *abs_name = NULL;
            ret = asprintf(&abs_name, "%s/%s", dirs[i], de->d_name);
            if (0 > ret) {
                goto error;
            }
            if (NULL == abs_name) {
                ret = PMIX_ERR_IN_ERRNO;
                goto error;
            }

            /* Stat the file */
            struct stat buf;
            if (stat(abs_name, &buf) < 0) {
                free(abs_name);
                ret = PMIX_ERR_IN_ERRNO;
                goto error;
            }

            /* Skip if not a file */
            if (!S_ISREG(buf.st_mode)) {
                free(abs_name);
                continue;
            }

            /* Find the suffix */
            char *ptr = strrchr(abs_name, '.');
            if (NULL != ptr) {

                /* Skip libtool files */
                if (strcmp(ptr, ".la") == 0 ||
                    strcmp(ptr, ".lo") == 0) {
                    free (abs_name);
                    continue;
                }

                *ptr = '\0';
            }

            /* Have we already found this file?  Or already found a
               file with the same basename (but different suffix)? */
            bool found = false;
            for (int j = 0; NULL != good_files &&
                     NULL != good_files[j]; ++j) {
                if (strcmp(good_files[j], abs_name) == 0) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                pmix_argv_append_nosize(&good_files, abs_name);
            }
            free(abs_name);
        }
        closedir(dp);
    }
    dp = NULL;

    /* Invoke the callback on all the found files */
    if (NULL != good_files) {
        for (int i = 0; NULL != good_files[i]; ++i) {
            ret = func(good_files[i], data);
            if (PMIX_SUCCESS != ret) {
                goto error;
            }
        }
    }

    ret = PMIX_SUCCESS;

 error:
    if (NULL != dp) {
        closedir(dp);
    }
    if (NULL != dirs) {
        pmix_argv_free(dirs);
    }
    if (NULL != good_files) {
        pmix_argv_free(good_files);
    }

    return ret;
}


/*
 * Module definition
 */
pmix_pdl_base_module_t pmix_pdl_pdlopen_module = {
    .open = pdlopen_open,
    .lookup = pdlopen_lookup,
    .close = pdlopen_close,
    .foreachfile = pdlopen_foreachfile
};

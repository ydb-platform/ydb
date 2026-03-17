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
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include <errno.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <stdlib.h>
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif /* HAVE_SYS_STAT_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif  /* HAVE_DIRENT_H */

#include "opal/util/output.h"
#include "opal/util/os_dirpath.h"
#include "opal/util/show_help.h"
#include "opal/util/argv.h"
#include "opal/util/os_path.h"
#include "opal/constants.h"

static const char path_sep[] = OPAL_PATH_SEP;

int opal_os_dirpath_create(const char *path, const mode_t mode)
{
    struct stat buf;
    char **parts, *tmp;
    int i, len;
    int ret;

    if (NULL == path) { /* protect ourselves from errors */
        return(OPAL_ERR_BAD_PARAM);
    }

    if (0 == (ret = stat(path, &buf))) { /* already exists */
        if (mode == (mode & buf.st_mode)) { /* has correct mode */
            return(OPAL_SUCCESS);
        }
        if (0 == (ret = chmod(path, (buf.st_mode | mode)))) { /* successfully change mode */
            return(OPAL_SUCCESS);
        }
        opal_show_help("help-opal-util.txt", "dir-mode", true,
                    path, mode, strerror(errno));
        return(OPAL_ERR_PERM); /* can't set correct mode */
    }

    /* quick -- try to make directory */
    if (0 == mkdir(path, mode)) {
        return(OPAL_SUCCESS);
    }

    /* didnt work, so now have to build our way down the tree */
    /* Split the requested path up into its individual parts */

    parts = opal_argv_split(path, path_sep[0]);

    /* Ensure to allocate enough space for tmp: the strlen of the
       incoming path + 1 (for \0) */

    tmp = (char*)malloc(strlen(path) + 1);
    tmp[0] = '\0';

    /* Iterate through all the subdirectory names in the path,
       building up a directory name.  Check to see if that dirname
       exists.  If it doesn't, create it. */

    len = opal_argv_count(parts);
    for (i = 0; i < len; ++i) {
        if (i == 0) {
            /* If in POSIX-land, ensure that we never end a directory
               name with path_sep */

            if ('/' == path[0]) {
                strcat(tmp, path_sep);
            }
            strcat(tmp, parts[i]);
        }

        /* If it's not the first part, ensure that there's a
           preceeding path_sep and then append this part */

        else {
            if (path_sep[0] != tmp[strlen(tmp) - 1]) {
                strcat(tmp, path_sep);
            }
            strcat(tmp, parts[i]);
        }

        /* Now that we have the name, try to create it */
        mkdir(tmp, mode);
        ret = errno;  // save the errno for an error msg, if needed
        if (0 != stat(tmp, &buf)) {
            opal_show_help("help-opal-util.txt", "mkdir-failed", true,
                        tmp, strerror(ret));
            opal_argv_free(parts);
            free(tmp);
            return OPAL_ERROR;
        } else if (i == (len-1) && (mode != (mode & buf.st_mode)) && (0 > chmod(tmp, (buf.st_mode | mode)))) {
            opal_show_help("help-opal-util.txt", "dir-mode", true,
                           tmp, mode, strerror(errno));
            opal_argv_free(parts);
            free(tmp);
            return(OPAL_ERR_PERM); /* can't set correct mode */
        }
    }

    /* All done */

    opal_argv_free(parts);
    free(tmp);
    return OPAL_SUCCESS;
}

/**
 * This function attempts to remove a directory along with all the
 * files in it.  If the recursive variable is non-zero, then it will
 * try to recursively remove all directories.  If provided, the
 * callback function is executed prior to the directory or file being
 * removed.  If the callback returns non-zero, then no removal is
 * done.
 */
int opal_os_dirpath_destroy(const char *path,
                            bool recursive,
                            opal_os_dirpath_destroy_callback_fn_t cbfunc)
{
    int rc, exit_status = OPAL_SUCCESS;
    bool is_dir = false;
    DIR *dp;
    struct dirent *ep;
    char *filenm;
    struct stat buf;

    if (NULL == path) {  /* protect against error */
        return OPAL_ERROR;
    }

    /*
     * Make sure we have access to the the base directory
     */
    if (OPAL_SUCCESS != (rc = opal_os_dirpath_access(path, 0))) {
        exit_status = rc;
        goto cleanup;
    }

    /* Open up the directory */
    dp = opendir(path);
    if (NULL == dp) {
        return OPAL_ERROR;
    }

    while (NULL != (ep = readdir(dp))) {
        /* skip:
         *  - . and ..
         */
        if ((0 == strcmp(ep->d_name, ".")) ||
            (0 == strcmp(ep->d_name, ".."))) {
            continue;
        }

        /* Check to see if it is a directory */
        is_dir = false;

        /* Create a pathname.  This is not always needed, but it makes
         * for cleaner code just to create it here.  Note that we are
         * allocating memory here, so we need to free it later on.
         */
        filenm = opal_os_path(false, path, ep->d_name, NULL);

        rc = stat(filenm, &buf);
        if (0 > rc) {
            /* Handle a race condition. filenm might have been deleted by an
             * other process running on the same node. That typically occurs
             * when one task is removing the job_session_dir and an other task
             * is still removing its proc_session_dir.
             */
            free(filenm);
            continue;
        }
        if (S_ISDIR(buf.st_mode)) {
            is_dir = true;
        }

        /*
         * If not recursively decending, then if we find a directory then fail
         * since we were not told to remove it.
         */
        if (is_dir && !recursive) {
            /* Set the error indicating that we found a directory,
             * but continue removing files
             */
            exit_status = OPAL_ERROR;
            free(filenm);
            continue;
        }

        /* Will the caller allow us to remove this file/directory? */
        if (NULL != cbfunc) {
            /*
             * Caller does not wish to remove this file/directory,
             * continue with the rest of the entries
             */
            if (!(cbfunc(path, ep->d_name))) {
                free(filenm);
                continue;
            }
        }
        /* Directories are recursively destroyed */
        if (is_dir) {
            rc = opal_os_dirpath_destroy(filenm, recursive, cbfunc);
            free(filenm);
            if (OPAL_SUCCESS != rc) {
                exit_status = rc;
                closedir(dp);
                goto cleanup;
            }
        } else {
            /* Files are removed right here */
            if (0 != (rc = unlink(filenm))) {
                exit_status = OPAL_ERROR;
            }
            free(filenm);
        }
    }

    /* Done with this directory */
    closedir(dp);

 cleanup:

    /*
     * If the directory is empty, them remove it
     */
    if(opal_os_dirpath_is_empty(path)) {
        rmdir(path);
    }

    return exit_status;
}

bool opal_os_dirpath_is_empty(const char *path ) {
    DIR *dp;
    struct dirent *ep;

    if (NULL != path) {  /* protect against error */
        dp = opendir(path);
        if (NULL != dp) {
            while ((ep = readdir(dp))) {
                        if ((0 != strcmp(ep->d_name, ".")) &&
                            (0 != strcmp(ep->d_name, ".."))) {
                            closedir(dp);
                            return false;
                        }
            }
            closedir(dp);
            return true;
        }
        return false;
    }

    return true;
}

int opal_os_dirpath_access(const char *path, const mode_t in_mode ) {
    struct stat buf;
    mode_t loc_mode = S_IRWXU;  /* looking for full rights */

    /*
     * If there was no mode specified, use the default mode
     */
    if (0 != in_mode) {
        loc_mode = in_mode;
    }

    if (0 == stat(path, &buf)) { /* exists - check access */
        if ((buf.st_mode & loc_mode) == loc_mode) { /* okay, I can work here */
            return(OPAL_SUCCESS);
        } else {
            /* Don't have access rights to the existing path */
            return(OPAL_ERROR);
        }
    } else {
        /* We could not find the path */
        return( OPAL_ERR_NOT_FOUND );
    }
}

/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2016      University of Houston. All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * @file
 */

#ifndef PMIX_PATH_H
#define PMIX_PATH_H

#include <src/include/pmix_config.h>

#include "pmix_common.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

BEGIN_C_DECLS

/**
 *  Locates a file with certain permissions
 *
 *  @param fname File name
 *  @param pathv Array of search directories
 *  @param mode  Permissions which must be satisfied (see access(2))
 *  @param envv  Pointer to string containing environment
 *
 *  @retval Full pathname of located file Success
 *  @retval NULL Failure
 *
 *  Environment variables can appear in the form $variable at the
 *  start of a prefix path and will be replaced by the environment
 *  value if it is defined; otherwise the whole prefix is ignored.
 *  Environment variables must be followed by a path delimiter or
 *  end-of-string.
 *
 * The caller is responsible for freeing the returned string.
 */
PMIX_EXPORT char *pmix_path_find(char *fname, char **pathv, int mode,
                                 char **envv) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;

/**
 *  Locates a file with certain permissions from a list of search
 *  paths
 *
 *  @param fname File name
 *  @param mode  Target permissions which must be satisfied (see access(2))
 *  @param envv  Pointer to environment list
 *  @param wrkdir Working directory
 *
 *  @retval Full pathname of located file Success
 *  @retval NULL Failure
 *
 *  Locates a file with certain permissions from the list of paths
 *  given by the $PATH environment variable.  Replaces "." in the
 *  path with the working dir.
 *
 * The caller is responsible for freeing the returned string.
 */
PMIX_EXPORT char *pmix_path_findv(char *fname, int mode,
                                  char **envv, char *wrkdir) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;
/**
 *  Detect if the requested path is absolute or relative.
 *
 *  @param path  File name
 *
 *  @retval true if the path is absolute
 *  @retval false otherwise
 *
 *  Detect if a path is absolute or relative. Handle Windows
 *  with special care as an absolute path on Windows starts
 *  with [A-Za-z]: or \\ instead of the usual / on UNIX.
 */
PMIX_EXPORT bool pmix_path_is_absolute( const char *path );

/**
 * Find the absolute path for an executable and return it.
 *
 *  @param app_name  Executable name
 *
 *  @retval The absolute path if the application can be reached,
 *  @retval NULL otherwise.
 *
 * Try to figure out the absolute path based on the application name
 * (usually argv[0]). If the path is already absolute return a copy, if
 * it start with . look into the current directory, if not dig into
 * the $PATH.
 * In case of error or if executable was not found (as an example if
 * the application did a cwd between the start and this call), the
 * function will return NULL. Otherwise, an newly allocated string
 * will be returned.
 */
PMIX_EXPORT char* pmix_find_absolute_path( char* app_name ) __pmix_attribute_warn_unused_result__;

/**
 * Forms a complete pathname and checks it for existance and
 * permissions
 *
 * @param fname File name
 * @param path Path prefix, if NULL then fname is an absolute path
 * @param mode Target permissions which must be satisfied (see access(2))
 *
 * @retval NULL Failure
 * @retval Full pathname of the located file on Success
 *
 * The caller is responsible for freeing the returned string.
 */
PMIX_EXPORT char *pmix_path_access(char *fname, char *path, int mode) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;


/**
 * @brief Figure out, whether fname is on network file system
 * and return fstype if known
 *
 * Try to figure out, whether the file name specified through fname is
 * on any network file system (currently NFS, Lustre, GPFS,  Panasas
 * and PVFS2 ).
 *
 * If the file is not created, the parent directory is checked.
 * This allows checking for NFS prior to opening the file.
 *
 * @fname[in]     File name to check
 * @fstype[out]   File system type if retval is true
 *
 * @retval true                If fname is on NFS, Lustre or Panasas
 * @retval false               otherwise
 */
PMIX_EXPORT bool pmix_path_nfs(char *fname, char **fstype) __pmix_attribute_warn_unused_result__;

/**
 * @brief Returns the disk usage of path.
 *
 * @param[in] path       Path to check
 * @out_avail[out]       Amount of free space available on path (if successful)
 *
 * @retval PMIX_SUCCESS  If the operation was successful
 * @retval PMIX_ERROR    otherwise
 */
PMIX_EXPORT int pmix_path_df(const char *path,
                             uint64_t *out_avail)__pmix_attribute_warn_unused_result__;

END_C_DECLS
#endif /* PMIX_PATH_H */

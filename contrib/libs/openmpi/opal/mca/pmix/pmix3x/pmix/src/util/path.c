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
 * Copyright (c) 2009-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SHLWAPI_H
#include <shlwapi.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif
#ifdef HAVE_SYS_STATFS_H
#include <sys/statfs.h>
#endif
#ifdef HAVE_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif
#ifdef HAVE_MNTENT_H
#include <mntent.h>
#endif
#ifdef HAVE_PATHS_H
#include <paths.h>
#endif

#ifdef _PATH_MOUNTED
#define MOUNTED_FILE _PATH_MOUNTED
#else
#define MOUNTED_FILE "/etc/mtab"
#endif


#include "src/include/pmix_stdint.h"
#include "src/util/output.h"
#include "src/util/path.h"
#include "src/util/os_path.h"
#include "src/util/argv.h"

/*
 * Sanity check to ensure we have either statfs or statvfs
 */
#if !defined(HAVE_STATFS) && !defined(HAVE_STATVFS)
#error Must have either statfs() or statvfs()
#endif

/*
 * Note that some OS's (e.g., NetBSD and Solaris) have statfs(), but
 * no struct statfs (!).  So check to make sure we have struct statfs
 * before allowing the use of statfs().
 */
#if defined(HAVE_STATFS) && \
    (defined(HAVE_STRUCT_STATFS_F_FSTYPENAME) || \
     defined(HAVE_STRUCT_STATFS_F_TYPE))
#define USE_STATFS 1
#endif

static void path_env_load(char *path, int *pargc, char ***pargv);
static char *list_env_get(char *var, char **list);

bool pmix_path_is_absolute( const char *path )
{
    if( PMIX_PATH_SEP[0] == *path ) {
        return true;
    }
    return false;
}

/**
 *  Locates a file with certain permissions
 */
char *pmix_path_find(char *fname, char **pathv, int mode, char **envv)
{
    char *fullpath;
    char *delimit;
    char *env;
    char *pfix;
    int i;

    /* If absolute path is given, return it without searching. */
    if( pmix_path_is_absolute(fname) ) {
        return pmix_path_access(fname, NULL, mode);
    }

    /* Initialize. */

    fullpath = NULL;
    i = 0;

    /* Consider each directory until the file is found.  Thus, the
       order of directories is important. */

    while (pathv[i] && NULL == fullpath) {

        /* Replace environment variable at the head of the string. */
        if ('$' == *pathv[i]) {
            delimit = strchr(pathv[i], PMIX_PATH_SEP[0]);
            if (delimit) {
                *delimit = '\0';
            }
            env = list_env_get(pathv[i]+1, envv);
            if (delimit) {
                *delimit = PMIX_PATH_SEP[0];
            }
            if (NULL != env) {
                if (!delimit) {
                    fullpath = pmix_path_access(fname, env, mode);
                } else {
                    pfix = (char*) malloc(strlen(env) + strlen(delimit) + 1);
                    if (NULL == pfix) {
                        return NULL;
                    }
                    strcpy(pfix, env);
                    strcat(pfix, delimit);
                    fullpath = pmix_path_access(fname, pfix, mode);
                    free(pfix);
                }
            }
        }
        else {
            fullpath = pmix_path_access(fname, pathv[i], mode);
        }
        i++;
    }
    return pmix_make_filename_os_friendly(fullpath);
}

/*
 * Locates a file with certain permissions from a list of search paths
 */
char *pmix_path_findv(char *fname, int mode, char **envv, char *wrkdir)
{
    char **dirv;
    char *fullpath;
    char *path;
    int dirc;
    int i;
    bool found_dot = false;

    /* Set the local search paths. */

    dirc = 0;
    dirv = NULL;

    if (NULL != (path = list_env_get("PATH", envv))) {
        path_env_load(path, &dirc, &dirv);
    }

    /* Replace the "." path by the working directory. */

    if (NULL != wrkdir) {
        for (i = 0; i < dirc; ++i) {
            if (0 == strcmp(dirv[i], ".")) {
                found_dot = true;
                free(dirv[i]);
                dirv[i] = strdup(wrkdir);
                if (NULL == dirv[i]){
                    return NULL;
                }
            }
        }
    }

    /* If we didn't find "." in the path and we have a wrkdir, append
       the wrkdir to the end of the path */

    if (!found_dot && NULL != wrkdir) {
        pmix_argv_append(&dirc, &dirv, wrkdir);
    }

    if(NULL == dirv)
        return NULL;
    fullpath = pmix_path_find(fname, dirv, mode, envv);
    pmix_argv_free(dirv);
    return fullpath;
}


/**
 *  Forms a complete pathname and checks it for existance and
 *  permissions
 *
 *  Accepts:
 *      -fname File name
 *      -path  Path prefix
 *      -mode  Target permissions which must be satisfied
 *
 *  Returns:
 *      -Full pathname of located file Success
 *      -NULL Failure
 */
char *pmix_path_access(char *fname, char *path, int mode)
{
    char *fullpath = NULL;
    struct stat buf;

    /* Allocate space for the full pathname. */
    if (NULL == path) {
        fullpath = pmix_os_path(false, fname, NULL);
    } else {
        fullpath = pmix_os_path(false, path, fname, NULL);
    }
    if (NULL == fullpath)
        return NULL;

    /* first check to see - is this a file or a directory? We
     * only want files
     */
    if (0 != stat(fullpath, &buf)) {
        /* couldn't stat the path - obviously, this also meets the
         * existence check, if that was requested
         */
        free(fullpath);
        return NULL;
    }

    if (!(S_IFREG & buf.st_mode) &&
        !(S_IFLNK & buf.st_mode)) {
        /* this isn't a regular file or a symbolic link, so
         * ignore it
         */
        free(fullpath);
        return NULL;
    }

    /* check the permissions */
    if ((X_OK & mode) && !(S_IXUSR & buf.st_mode)) {
        /* if they asked us to check executable permission,
         * and that isn't set, then return NULL
         */
        free(fullpath);
        return NULL;
    }
    if ((R_OK & mode) && !(S_IRUSR & buf.st_mode)) {
        /* if they asked us to check read permission,
         * and that isn't set, then return NULL
         */
        free(fullpath);
        return NULL;
    }
    if ((W_OK & mode) && !(S_IWUSR & buf.st_mode)) {
        /* if they asked us to check write permission,
         * and that isn't set, then return NULL
         */
        free(fullpath);
        return NULL;
    }

    /* must have met all criteria! */
    return fullpath;
}


/**
 *
 *  Loads argument array with $PATH env var.
 *
 *  Accepts
 *      -path String contiaing the $PATH
 *      -argc Pointer to argc
 *      -argv Pointer to list of argv
 */
static void path_env_load(char *path, int *pargc, char ***pargv)
{
    char *p;
    char saved;

    if (NULL == path) {
        *pargc = 0;
        return;
    }

    /* Loop through the paths (delimited by PATHENVSEP), adding each
       one to argv. */

    while ('\0' != *path) {

        /* Locate the delimiter. */

        for (p = path; *p && (*p != PMIX_ENV_SEP); ++p) {
            continue;
        }

        /* Add the path. */

        if (p != path) {
            saved = *p;
            *p = '\0';
            pmix_argv_append(pargc, pargv, path);
            *p = saved;
            path = p;
        }

        /* Skip past the delimiter, if present. */

        if (*path) {
            ++path;
        }
    }
}


/**
 *  Gets value of variable in list or environment. Looks in the list first
 *
 *  Accepts:
 *      -var  String variable
 *      -list Pointer to environment list
 *
 *  Returns:
 *      -List Pointer to environment list Success
 *      -NULL Failure
 */
static char *list_env_get(char *var, char **list)
{
    size_t n;

    if (NULL != list) {
        n = strlen(var);

        while (NULL != *list) {
            if ((0 == strncmp(var, *list, n)) && ('=' == (*list)[n])) {
                return (*list + n + 1);
            }
            ++list;
        }
    }
    return getenv(var);
}

/**
 * Try to figure out the absolute path based on the application name
 * (usually argv[0]). If the path is already absolute return a copy, if
 * it start with . look into the current directory, if not dig into
 * the $PATH.
 * In case of error or if executable was not found (as an example if
 * the application did a cwd between the start and this call), the
 * function will return NULL. Otherwise, an newly allocated string
 * will be returned.
 */
char* pmix_find_absolute_path( char* app_name )
{
    char* abs_app_name;
    char cwd[PMIX_PATH_MAX], *pcwd;

    if( pmix_path_is_absolute(app_name) ) { /* already absolute path */
        abs_app_name = app_name;
    } else if ( '.' == app_name[0] ||
               NULL != strchr(app_name, PMIX_PATH_SEP[0])) {
        /* the app is in the current directory or below it */
        pcwd = getcwd( cwd, PMIX_PATH_MAX );
        if( NULL == pcwd ) {
            /* too bad there is no way we can get the app absolute name */
            return NULL;
        }
        abs_app_name = pmix_os_path( false, pcwd, app_name, NULL );
    } else {
        /* Otherwise try to search for the application in the PATH ... */
        abs_app_name = pmix_path_findv( app_name, X_OK, NULL, NULL );
    }

    if( NULL != abs_app_name ) {
        char* resolved_path = (char*)malloc(PMIX_PATH_MAX);
        if (NULL == realpath( abs_app_name, resolved_path )) {
            free(resolved_path);
            free(abs_app_name);
            return NULL;
        }
        if( abs_app_name != app_name ) {
            free(abs_app_name);
        }
        return resolved_path;
    }
    return NULL;
}

/**
 * Read real FS type from /etc/mtab, needed to translate autofs fs type into real fs type
 * TODO: solaris? OSX?
 * Limitations: autofs on solaris/osx will be assumed as "nfs" type
 */

static char *pmix_check_mtab(char *dev_path)
{

#ifdef HAVE_MNTENT_H
    FILE * mtab = NULL;
    struct mntent * part = NULL;

    if ((mtab = setmntent(MOUNTED_FILE, "r")) != NULL) {
        while (NULL != (part = getmntent(mtab))) {
            if ((NULL != part->mnt_dir) &&
                (NULL != part->mnt_type) &&
                (0 == strcmp(part->mnt_dir, dev_path)))
            {
                endmntent(mtab);
                return strdup(part->mnt_type);
            }
        }
        endmntent(mtab);
    }
#endif
    return NULL;
}


/**
 * @brief Figure out, whether fname is on network file system
 *
 * Try to figure out, whether the file name specified through fname is
 * on any network file system (currently NFS, Lustre, Panasas and GPFS).
 *
 * If the file is not created, the parent directory is checked.
 * This allows checking for NFS prior to opening the file.
 *
 * @fname[in]          File name to check
 * @fstype[out]        File system type if retval is true
 *
 * @retval true                If fname is on NFS, Lustre, Panasas or GPFS
 * @retval false               otherwise
 *
 *
 * Linux:
 *   statfs(const char *path, struct statfs *buf);
 *          with fsid_t  f_fsid;  (in kernel struct{ int val[2] };)
 *          return 0 success, -1 on failure with errno set.
 *   statvfs (const char *path, struct statvfs *buf);
 *          with unsigned long  f_fsid;   -- returns wrong info
 *          return 0 success, -1 on failure with errno set.
 * Solaris:
 *   statvfs (const char *path, struct statvfs *buf);
 *          with f_basetype, contains a string of length FSTYPSZ
 *          return 0 success, -1 on failure with errno set.
 * FreeBSD:
 *   statfs(const char *path, struct statfs *buf);
 *          with f_fstypename, contains a string of length MFSNAMELEN
 *          return 0 success, -1 on failure with errno set.
 *          compliant with: 4.4BSD.
 * NetBSD:
 *   statvfs (const char *path, struct statvfs *buf);
 *          with f_fstypename, contains a string of length VFS_NAMELEN
 *          return 0 success, -1 on failure with errno set.
 * Mac OSX (10.6.2 through 10.9):
 *   statvfs(const char * restrict path, struct statvfs * restrict buf);
 *          with fsid    Not meaningful in this implementation.
 *          is just a wrapper around statfs()
 *   statfs(const char *path, struct statfs *buf);
 *          with f_fstypename, contains a string of length MFSTYPENAMELEN
 *          return 0 success, -1 on failure with errno set.
 */
#ifndef LL_SUPER_MAGIC
#define LL_SUPER_MAGIC                    0x0BD00BD0     /* Lustre magic number */
#endif
#ifndef NFS_SUPER_MAGIC
#define NFS_SUPER_MAGIC                   0x6969
#endif
#ifndef PAN_KERNEL_FS_CLIENT_SUPER_MAGIC
#define PAN_KERNEL_FS_CLIENT_SUPER_MAGIC  0xAAD7AAEA     /* Panasas FS */
#endif
#ifndef GPFS_SUPER_MAGIC
#define GPFS_SUPER_MAGIC  0x47504653    /* Thats GPFS in ASCII */
#endif
#ifndef AUTOFS_SUPER_MAGIC
#define AUTOFS_SUPER_MAGIC 0x0187
#endif
#ifndef PVFS2_SUPER_MAGIC
#define PVFS2_SUPER_MAGIC 0x20030528
#endif

#define MASK2        0xffff
#define MASK4    0xffffffff

bool pmix_path_nfs(char *fname, char **ret_fstype)
{
    int i;
    int fsrc = -1;
    int vfsrc = -1;
    int trials;
    char * file = strdup (fname);
#if defined(USE_STATFS)
    struct statfs fsbuf;
#endif
#if defined(HAVE_STATVFS)
    struct statvfs vfsbuf;
#endif
    /*
     * Be sure to update the test (test/util/pmix_path_nfs.c)
     * while adding a new Network/Cluster Filesystem here
     */
    static struct fs_types_t {
        unsigned long long f_fsid;
        unsigned long long f_mask;
        const char * f_fsname;
    } fs_types[] = {
        {LL_SUPER_MAGIC,                   MASK4, "lustre"},
        {NFS_SUPER_MAGIC,                  MASK2, "nfs"},
        {AUTOFS_SUPER_MAGIC,               MASK2, "autofs"},
        {PAN_KERNEL_FS_CLIENT_SUPER_MAGIC, MASK4, "panfs"},
        {GPFS_SUPER_MAGIC,                 MASK4, "gpfs"},
        {PVFS2_SUPER_MAGIC,                MASK4, "pvfs2"}
    };
#define FS_TYPES_NUM (int)(sizeof (fs_types)/sizeof (fs_types[0]))

    /*
     * First, get the OS-dependent struct stat(v)fs buf.  This may
     * return the ESTALE error on NFS, if the underlying file/path has
     * changed.
     */
again:
#if defined(USE_STATFS)
    trials = 5;
    do {
        fsrc = statfs(file, &fsbuf);
    } while (-1 == fsrc && ESTALE == errno && (0 < --trials));
#endif
#if defined(HAVE_STATVFS)
    trials = 5;
    do {
        vfsrc = statvfs(file, &vfsbuf);
    } while (-1 == vfsrc && ESTALE == errno && (0 < --trials));
#endif

    /* In case some error with the current filename, try the parent
       directory */
    if (-1 == fsrc && -1 == vfsrc) {
        char * last_sep;

        PMIX_OUTPUT_VERBOSE((10, 0, "pmix_path_nfs: stat(v)fs on file:%s failed errno:%d directory:%s\n",
                             fname, errno, file));
        if (EPERM == errno) {
            free(file);
            if ( NULL != ret_fstype ) {
                *ret_fstype = NULL;
            }
            return false;
        }

        last_sep = strrchr(file, PMIX_PATH_SEP[0]);
        /* Stop the search, when we have searched past root '/' */
        if (NULL == last_sep || (1 == strlen(last_sep) &&
            PMIX_PATH_SEP[0] == *last_sep)) {
            free (file);
            if ( NULL != ret_fstype ) {
                *ret_fstype=NULL;
            }
            return false;
        }
        *last_sep = '\0';

        goto again;
    }

    /* Next, extract the magic value */
    for (i = 0; i < FS_TYPES_NUM; i++) {
#if defined(USE_STATFS)
        /* These are uses of struct statfs */
#    if defined(HAVE_STRUCT_STATFS_F_FSTYPENAME)
        if (0 == fsrc &&
            0 == strncasecmp(fs_types[i].f_fsname, fsbuf.f_fstypename,
                             sizeof(fsbuf.f_fstypename))) {
            goto found;
        }
#    endif
#    if defined(HAVE_STRUCT_STATFS_F_TYPE)
        if (0 == fsrc &&
            fs_types[i].f_fsid == (fsbuf.f_type & fs_types[i].f_mask)) {
            goto found;
        }
#    endif
#endif

#if defined(HAVE_STATVFS)
        /* These are uses of struct statvfs */
#    if defined(HAVE_STRUCT_STATVFS_F_BASETYPE)
        if (0 == vfsrc &&
            0 == strncasecmp(fs_types[i].f_fsname, vfsbuf.f_basetype,
                             sizeof(vfsbuf.f_basetype))) {
            goto found;
        }
#    endif
#    if defined(HAVE_STRUCT_STATVFS_F_FSTYPENAME)
        if (0 == vfsrc &&
            0 == strncasecmp(fs_types[i].f_fsname, vfsbuf.f_fstypename,
                             sizeof(vfsbuf.f_fstypename))) {
            goto found;
        }
#    endif
#endif
    }

    free (file);
    if ( NULL != ret_fstype ) {
        *ret_fstype=NULL;
    }
    return false;

#if defined(HAVE_STRUCT_STATFS_F_FSTYPENAME) || \
    defined(HAVE_STRUCT_STATFS_F_TYPE) || \
    defined(HAVE_STRUCT_STATVFS_F_BASETYPE) || \
    defined(HAVE_STRUCT_STATVFS_F_FSTYPENAME)
  found:
#endif

    free (file);
    if (AUTOFS_SUPER_MAGIC == fs_types[i].f_fsid) {
        char *fs_type = pmix_check_mtab(fname);
        int x;
        if (NULL != fs_type) {
            for (x = 0; x < FS_TYPES_NUM; x++) {
                if (AUTOFS_SUPER_MAGIC == fs_types[x].f_fsid) {
                    continue;
                }
                if (0 == strcasecmp(fs_types[x].f_fsname, fs_type)) {
                    PMIX_OUTPUT_VERBOSE((10, 0, "pmix_path_nfs: file:%s on fs:%s\n", fname, fs_type));
                    free(fs_type);
                    if ( NULL != ret_fstype ) {
                        *ret_fstype = strdup(fs_types[x].f_fsname);
                    }
                    return true;
                }
            }
            free(fs_type);
            if ( NULL != ret_fstype ) {
                *ret_fstype=NULL;
            }
            return false;
        }
    }

    PMIX_OUTPUT_VERBOSE((10, 0, "pmix_path_nfs: file:%s on fs:%s\n",
                fname, fs_types[i].f_fsname));
    if ( NULL != ret_fstype ) {
        *ret_fstype = strdup (fs_types[i].f_fsname);
    }
    return true;

#undef FS_TYPES_NUM
}

int
pmix_path_df(const char *path,
             uint64_t *out_avail)
{
    int rc = -1;
    int trials = 5;
    int err = 0;
#if defined(USE_STATFS)
    struct statfs buf;
#elif defined(HAVE_STATVFS)
    struct statvfs buf;
#endif

    if (NULL == path || NULL == out_avail) {
        return PMIX_ERROR;
    }
    *out_avail = 0;

    do {
#if defined(USE_STATFS)
        rc = statfs(path, &buf);
#elif defined(HAVE_STATVFS)
        rc = statvfs(path, &buf);
#endif
        err = errno;
    } while (-1 == rc && ESTALE == err && (--trials > 0));

    if (-1 == rc) {
        PMIX_OUTPUT_VERBOSE((10, 2, "pmix_path_df: stat(v)fs on "
                             "path: %s failed with errno: %d (%s)\n",
                             path, err, strerror(err)));
        return PMIX_ERROR;
    }

    /* now set the amount of free space available on path */
                               /* sometimes buf.f_bavail is negative */
    *out_avail = buf.f_bsize * ((int)buf.f_bavail < 0 ? 0 : buf.f_bavail);

    PMIX_OUTPUT_VERBOSE((10, 2, "pmix_path_df: stat(v)fs states "
                         "path: %s has %"PRIu64 " B of free space.",
                         path, *out_avail));

    return PMIX_SUCCESS;
}

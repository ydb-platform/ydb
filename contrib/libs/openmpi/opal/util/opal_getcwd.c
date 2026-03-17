/*
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <string.h>

#include "opal/util/basename.h"
#include "opal/util/opal_getcwd.h"
#include "opal/constants.h"


/*
 * Use $PWD instead of getcwd() a) if $PWD exists and b) is a valid
 * synonym for the results from getcwd(). If both of these conditions
 * are not met, just fall back and use the results of getcwd().
 */
int opal_getcwd(char *buf, size_t size)
{
    char cwd[OPAL_PATH_MAX];
    char *pwd = getenv("PWD");
    struct stat a, b;
    char *shortened;

    /* Bozo checks (e.g., if someone accidentally passed -1 to the
       unsigned "size" param) */
    if (NULL == buf || size > INT_MAX) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Call getcwd() to get a baseline result */
    if (NULL == getcwd(cwd, sizeof(cwd))) {
        return OPAL_ERR_IN_ERRNO;
    }

#if !defined(HAVE_SYS_STAT_H)
    /* If we don't have stat(), then we can't tell if the $PWD and cwd
       are the same, so just fall back to getcwd(). */
    pwd = cwd;
#else
    if (NULL == pwd) {
        pwd = cwd;
    } else {
        /* If the two are not the same value, figure out if they are
           pointing to the same place */
        if (0 != strcmp(pwd, cwd)) {
            /* If we can't stat() what getcwd() gave us, give up */
            if (0 != stat(cwd, &a)) {
                return OPAL_ERR_IN_ERRNO;
            }
            /* If we can't stat() $PWD, then $PWD could just be stale
               -- so ignore it. */
            else if (0 != stat(pwd, &b)) {
                pwd = cwd;
            }
            /* Otherwise, we successfully stat()'ed them both, so
               compare.  If either the device or inode is not the
               same, then fallback to getcwd(). */
            else {
                if (a.st_dev != b.st_dev || a.st_ino != b.st_ino) {
                    pwd = cwd;
                }
            }
        }
    }
#endif

    /* If we got here, pwd is pointing to the result that we want to
       give.  Ensure the user's buffer is long enough.  If it is, copy
       in the value and be done. */
    if (strlen(pwd) > size) {
        /* if it isn't big enough, give them as much
         * of the basename as possible
         */
        shortened = opal_basename(pwd);
        strncpy(buf, shortened, size);
        free(shortened);
        /* ensure it is null terminated */
        buf[size-1] = '\0';
        /* indicate that it isn't the full path */
        return OPAL_ERR_TEMP_OUT_OF_RESOURCE;
    }
    strncpy(buf, pwd, size);
    return OPAL_SUCCESS;
}

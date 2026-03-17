/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <errno.h>

#include "opal/util/basename.h"
#include "opal/util/path.h"
#include "opal/util/opal_environ.h"

#include "orte/runtime/orte_globals.h"

#include "orte/util/context_fns.h"

int orte_util_check_context_cwd(orte_app_context_t *context,
                                bool want_chdir)
{
    bool good = true;
    const char *tmp;

    /* If we want to chdir and the chdir fails (for any reason -- such
       as if the dir doesn't exist, it isn't a dir, we don't have
       permissions, etc.), then set good to false. */
    if (want_chdir && 0 != chdir(context->cwd)) {
        good = false;
    }

    /* If either of the above failed, go into this block */
    if (!good) {
        /* See if the directory was a user-specified directory.  If it
        was, barf because they specifically asked for something we
        can't provide. */
        if (orte_get_attribute(&context->attributes, ORTE_APP_USER_CWD,
                               NULL, OPAL_BOOL)) {
            return ORTE_ERR_WDIR_NOT_FOUND;
        }

        /* If the user didn't specifically ask for it, then it
        was a system-supplied default directory, so it's ok
        to not go there.  Try to go to the $HOME directory
        instead. */
        tmp = opal_home_directory();
        if (NULL != tmp) {
            /* Try $HOME.  Same test as above. */
            good = true;
            if (want_chdir && 0 != chdir(tmp)) {
                good = false;
            }
            if (!good) {
                return ORTE_ERR_WDIR_NOT_FOUND;
            }

            /* Reset the pwd in this local copy of the
                context */
            if (NULL != context->cwd) free(context->cwd);
            context->cwd = strdup(tmp);
        }

        /* If we couldn't find $HOME, then just take whatever
            the default directory is -- assumedly there *is*
        one, or we wouldn't be running... */
    }

    /* All happy */
    return ORTE_SUCCESS;
}

int orte_util_check_context_app(orte_app_context_t *context, char **env)
{
    char *tmp;

    /* If the app is a naked filename, we need to do a path search for
        it.  orterun will send in whatever the user specified (e.g.,
        "orterun -np 2 uptime"), so in some cases, we need to search
        the path to verify that we can find it.  Here's the
        possibilities:

        1. The user specified an absolute pathname for the executable.
        We simply need to verify that it exists and we can run it.

        2. The user specified a relative pathname for the executable.
        Ditto with #1 -- based on the cwd, we need to verify that it
        exists and we can run it.

        3. The user specified a naked filename.  We need to search the
        path, find a match, and verify that we can run it.

        Note that in some cases, we won't be doing this work here --
        bproc, for example, does not use the fork pls for launching, so
        it does this same work over there. */

    tmp = opal_basename(context->app);
    if (strlen(tmp) == strlen(context->app)) {
        /* If this is a naked executable -- no relative or absolute
        pathname -- then search the PATH for it */
        free(tmp);
        tmp = opal_path_findv(context->app, X_OK, env, context->cwd);
        if (NULL == tmp) {
            return ORTE_ERR_EXE_NOT_FOUND;
        }
        free(context->app);
        context->app = tmp;
    } else {
        free(tmp);
        if (0 != access(context->app, X_OK)) {
            return ORTE_ERR_EXE_NOT_ACCESSIBLE;
        }
    }

    /* All was good */
    return ORTE_SUCCESS;
}

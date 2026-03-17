/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "opal/util/printf.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"
#include "opal/constants.h"

#define OPAL_DEFAULT_TMPDIR "/tmp"

/*
 * Merge two environ-like char arrays, ensuring that there are no
 * duplicate entires
 */
char **opal_environ_merge(char **minor, char **major)
{
    int i;
    char **ret = NULL;
    char *name, *value;

    /* Check for bozo cases */

    if (NULL == major) {
        if (NULL == minor) {
            return NULL;
        } else {
            return opal_argv_copy(minor);
        }
    }

    /* First, copy major */

    ret = opal_argv_copy(major);

    /* Do we have something in minor? */

    if (NULL == minor) {
        return ret;
    }

    /* Now go through minor and call opal_setenv(), but with overwrite
       as false */

    for (i = 0; NULL != minor[i]; ++i) {
        value = strchr(minor[i], '=');
        if (NULL == value) {
            opal_setenv(minor[i], NULL, false, &ret);
        } else {

            /* strdup minor[i] in case it's a constat string */

            name = strdup(minor[i]);
            value = name + (value - minor[i]);
            *value = '\0';
            opal_setenv(name, value + 1, false, &ret);
            free(name);
        }
    }

    /* All done */

    return ret;
}

/*
 * Portable version of setenv(), allowing editing of any environ-like
 * array
 */
int opal_setenv(const char *name, const char *value, bool overwrite,
                char ***env)
{
    int i;
    char *newvalue, *compare;
    size_t len;

    /* Make the new value */

    if (NULL == value) {
        value = "";
        asprintf(&newvalue, "%s=", name);
    } else {
        asprintf(&newvalue, "%s=%s", name, value);
    }
    if (NULL == newvalue) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* Check the bozo case */

    if( NULL == env ) {
        return OPAL_ERR_BAD_PARAM;
    } else if (NULL == *env) {
        i = 0;
        opal_argv_append(&i, env, newvalue);
        free(newvalue);
        return OPAL_SUCCESS;
    }

    /* If this is the "environ" array, use putenv */
    if( *env == environ ) {
        /* THIS IS POTENTIALLY A MEMORY LEAK!  But I am doing it
           so that we don't violate the law of least
           astonishment for OPAL developers (i.e., those that don't
           check the return code of opal_setenv() and notice that we
           returned an error if you passed in the real environ) */
#if defined (HAVE_SETENV)
        setenv(name, value, overwrite);
        /* setenv copies the value, so we can free it here */
        free(newvalue);
#else
        len = strlen(name);
        for (i = 0; (*env)[i] != NULL; ++i) {
            if (0 == strncmp((*env)[i], name, len)) {
                /* if we find the value in the environ, then
                 * we need to check the overwrite flag to determine
                 * the correct response */
                if (overwrite) {
                    /* since it was okay to overwrite, do so */
                    putenv(newvalue);
                    /* putenv does NOT copy the value, so we
                     * cannot free it here */
                    return OPAL_SUCCESS;
                }
                /* since overwrite was not allowed, we return
                 * an error as we cannot perform the requested action */
                free(newvalue);
                return OPAL_EXISTS;
            }
        }
        /* since the value wasn't found, we can add it */
        putenv(newvalue);
        /* putenv does NOT copy the value, so we
         * cannot free it here */
#endif
        return OPAL_SUCCESS;
    }

    /* Make something easy to compare to */

    asprintf(&compare, "%s=", name);
    if (NULL == compare) {
        free(newvalue);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    len = strlen(compare);

    /* Look for a duplicate that's already set in the env */

    for (i = 0; (*env)[i] != NULL; ++i) {
        if (0 == strncmp((*env)[i], compare, len)) {
            if (overwrite) {
                free((*env)[i]);
                (*env)[i] = newvalue;
                free(compare);
                return OPAL_SUCCESS;
            } else {
                free(compare);
                free(newvalue);
                return OPAL_EXISTS;
            }
        }
    }

    /* If we found no match, append this value */

    i = opal_argv_count(*env);
    opal_argv_append(&i, env, newvalue);

    /* All done */

    free(compare);
    free(newvalue);
    return OPAL_SUCCESS;
}


/*
 * Portable version of unsetenv(), allowing editing of any
 * environ-like array
 */
int opal_unsetenv(const char *name, char ***env)
{
    int i;
    char *compare;
    size_t len;
    bool found;

    /* Check for bozo case */

    if (NULL == *env) {
        return OPAL_SUCCESS;
    }

    /* Make something easy to compare to */

    asprintf(&compare, "%s=", name);
    if (NULL == compare) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    len = strlen(compare);

    /* Look for a duplicate that's already set in the env.  If we find
       it, free it, and then start shifting all elements down one in
       the array. */

    found = false;
    for (i = 0; (*env)[i] != NULL; ++i) {
        if (0 != strncmp((*env)[i], compare, len))
            continue;
        if (environ != *env) {
            free((*env)[i]);
        }
        for (; (*env)[i] != NULL; ++i)
            (*env)[i] = (*env)[i + 1];
        found = true;
        break;
    }
    free(compare);

    /* All done */

    return (found) ? OPAL_SUCCESS : OPAL_ERR_NOT_FOUND;
}

const char* opal_tmp_directory( void )
{
    const char* str;

    if( NULL == (str = getenv("TMPDIR")) )
        if( NULL == (str = getenv("TEMP")) )
            if( NULL == (str = getenv("TMP")) )
                str = OPAL_DEFAULT_TMPDIR;
    return str;
}

const char* opal_home_directory( void )
{
    char* home = getenv("HOME");

    return home;
}


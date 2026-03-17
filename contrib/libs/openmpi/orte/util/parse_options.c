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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "opal/util/argv.h"
#include "opal/util/output.h"

#include "orte/runtime/orte_globals.h"

#include "orte/util/parse_options.h"

void orte_util_parse_range_options(char *inp, char ***output)
{
    char **r1=NULL, **r2=NULL;
    int i, vint;
    int start, end, n;
    char nstr[32];
    char *input, *bang;
    bool bang_option=false;

    /* protect against null input */
    if (NULL == inp) {
        return;
    }

    /* protect the provided input */
    input = strdup(inp);

    /* check for the special '!' operator */
    if (NULL != (bang = strchr(input, '!'))) {
        bang_option = true;
        *bang = '\0';
    }

    /* split on commas */
    r1 = opal_argv_split(input, ',');
    /* for each resulting element, check for range */
    for (i=0; i < opal_argv_count(r1); i++) {
        r2 = opal_argv_split(r1[i], '-');
        if (1 < opal_argv_count(r2)) {
            /* given range - get start and end */
            start = strtol(r2[0], NULL, 10);
            end = strtol(r2[1], NULL, 10);
        } else {
            /* check for wildcard - have to do this here because
             * the -1 would have been caught in the split
             */
            vint = strtol(r1[i], NULL, 10);
            if (-1 == vint) {
                opal_argv_free(*output);
                *output = NULL;
                opal_argv_append_nosize(output, "-1");
                opal_argv_free(r2);
                goto cleanup;
            }
            start = strtol(r2[0], NULL, 10);
            end = start;
        }
        for (n = start; n <= end; n++) {
            snprintf(nstr, 32, "%d", n);
            opal_argv_append_nosize(output, nstr);
        }
        opal_argv_free(r2);
    }

cleanup:
    if (bang_option) {
        opal_argv_append_nosize(output, "BANG");
    }
    free(input);
    opal_argv_free(r1);

}

void orte_util_get_ranges(char *inp, char ***startpts, char ***endpts)
{
    char **r1=NULL, **r2=NULL;
    int i;
    char *input;

    /* protect against null input */
    if (NULL == inp) {
        return;
    }

    /* protect the provided input */
    input = strdup(inp);

    /* split on commas */
    r1 = opal_argv_split(input, ',');
    /* for each resulting element, check for range */
    for (i=0; i < opal_argv_count(r1); i++) {
        r2 = opal_argv_split(r1[i], '-');
        if (2 == opal_argv_count(r2)) {
            /* given range - get start and end */
            opal_argv_append_nosize(startpts, r2[0]);
            opal_argv_append_nosize(endpts, r2[1]);
        } else if (1 == opal_argv_count(r2)) {
            /* only one value provided, so it is both the start
             * and the end
             */
            opal_argv_append_nosize(startpts, r2[0]);
            opal_argv_append_nosize(endpts, r2[0]);
        } else {
            /* no idea how to parse this */
            opal_output(0, "%s Unknown parse error on string: %s(%s)",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), inp, r1[i]);
        }
        opal_argv_free(r2);
    }

    free(input);
    opal_argv_free(r1);

}

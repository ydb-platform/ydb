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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/util/numtostr.h"
#include "opal/util/printf.h"
#include <stdio.h>
#include <stdlib.h>


char*
opal_ltostr(long num)
{
    /* waste a little bit of space, but always have a big enough buffer */
    int buflen = sizeof(long) * 8;
    char *buf = NULL;
    int ret = 0;

    buf = (char*) malloc(sizeof(char) * buflen);
    if (NULL == buf) return NULL;

    ret = snprintf(buf, buflen, "%ld", num);
    if (ret < 0) {
        free(buf);
        return NULL;
    }

    return buf;
}


char*
opal_dtostr(double num)
{
    /* waste a little bit of space, but always have a big enough buffer */
    int buflen = sizeof(long) * 8;
    char *buf = NULL;
    int ret = 0;

    buf = (char*) malloc(sizeof(char) * buflen);
    if (NULL == buf) return NULL;

    ret = snprintf(buf, buflen, "%f", num);
    if (ret < 0) {
        free(buf);
        return NULL;
    }

    return buf;
}

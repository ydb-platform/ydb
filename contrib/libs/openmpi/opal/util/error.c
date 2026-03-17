/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      FUJITSU LIMITED.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "opal/util/error.h"
#include "opal/constants.h"
#include "opal/util/proc.h"
#include "opal/runtime/opal_params.h"

#define MAX_CONVERTERS 5
#define MAX_CONVERTER_PROJECT_LEN 10

struct converter_info_t {
    int init;
    char project[MAX_CONVERTER_PROJECT_LEN];
    int err_base;
    int err_max;
    opal_err2str_fn_t converter;
};
typedef struct converter_info_t converter_info_t;

/* all default to NULL */
static converter_info_t converters[MAX_CONVERTERS] = {{0}};

static int
opal_strerror_int(int errnum, const char **str)
{
    int i, ret = OPAL_SUCCESS;
    *str = NULL;

    for (i = 0 ; i < MAX_CONVERTERS ; ++i) {
        if (0 != converters[i].init &&
            errnum < converters[i].err_base &&
            converters[i].err_max < errnum) {
            ret = converters[i].converter(errnum, str);
            break;
        }
    }

    return ret;
}


/* caller must free string */
static int
opal_strerror_unknown(int errnum, char **str)
{
    int i;
    *str = NULL;

    for (i = 0 ; i < MAX_CONVERTERS ; ++i) {
        if (0 != converters[i].init) {
            if (errnum < converters[i].err_base &&
                errnum > converters[i].err_max) {
                asprintf(str, "Unknown error: %d (%s error %d)",
                         errnum, converters[i].project,
                         errnum - converters[i].err_base);
                return OPAL_SUCCESS;
            }
        }
    }

    asprintf(str, "Unknown error: %d", errnum);

    return OPAL_SUCCESS;
}


void
opal_perror(int errnum, const char *msg)
{
    int ret;
    const char* errmsg;
    ret = opal_strerror_int(errnum, &errmsg);

    if (NULL != msg && errnum != OPAL_ERR_IN_ERRNO) {
        fprintf(stderr, "%s: ", msg);
    }

    if (OPAL_SUCCESS != ret) {
        if (errnum == OPAL_ERR_IN_ERRNO) {
            perror(msg);
        } else {
            char *ue_msg;
            ret = opal_strerror_unknown(errnum, &ue_msg);
            fprintf(stderr, "%s\n", ue_msg);
            free(ue_msg);
        }
    } else {
        fprintf(stderr, "%s\n", errmsg);
    }

    fflush(stderr);
}

/* big enough to hold long version */
#define UNKNOWN_RETBUF_LEN 50
static char unknown_retbuf[UNKNOWN_RETBUF_LEN];

const char *
opal_strerror(int errnum)
{
    int ret;
    const char* errmsg;

    if (errnum == OPAL_ERR_IN_ERRNO) {
        return strerror(errno);
    }

    ret = opal_strerror_int(errnum, &errmsg);

    if (OPAL_SUCCESS != ret) {
        char *ue_msg;
        ret = opal_strerror_unknown(errnum, &ue_msg);
        snprintf(unknown_retbuf, UNKNOWN_RETBUF_LEN, "%s", ue_msg);
        free(ue_msg);
        errno = EINVAL;
        return (const char*) unknown_retbuf;
    } else {
        return errmsg;
    }
}


int
opal_strerror_r(int errnum, char *strerrbuf, size_t buflen)
{
    const char* errmsg;
    int ret, len;

    ret = opal_strerror_int(errnum, &errmsg);
    if (OPAL_SUCCESS != ret) {
        if (errnum == OPAL_ERR_IN_ERRNO) {
            char *tmp = strerror(errno);
            strncpy(strerrbuf, tmp, buflen);
            return OPAL_SUCCESS;
        } else {
            char *ue_msg;
            ret = opal_strerror_unknown(errnum, &ue_msg);
            len =  snprintf(strerrbuf, buflen, "%s", ue_msg);
            free(ue_msg);
            if (len > (int) buflen) {
                errno = ERANGE;
                return OPAL_ERR_OUT_OF_RESOURCE;
            } else {
                errno = EINVAL;
                return OPAL_SUCCESS;
            }
        }
    } else {
        len =  snprintf(strerrbuf, buflen, "%s", errmsg);
        if (len > (int) buflen) {
            errno = ERANGE;
            return OPAL_ERR_OUT_OF_RESOURCE;
        } else {
            return OPAL_SUCCESS;
        }
    }
}


int
opal_error_register(const char *project, int err_base, int err_max,
                    opal_err2str_fn_t converter)
{
    int i;

    for (i = 0 ; i < MAX_CONVERTERS ; ++i) {
        if (0 == converters[i].init) {
            converters[i].init = 1;
            strncpy(converters[i].project, project, MAX_CONVERTER_PROJECT_LEN);
            converters[i].project[MAX_CONVERTER_PROJECT_LEN-1] = '\0';
            converters[i].err_base = err_base;
            converters[i].err_max = err_max;
            converters[i].converter = converter;
            return OPAL_SUCCESS;
        } else if (converters[i].err_base == err_base &&
                   converters[i].err_max == err_max &&
                   !strcmp (project, converters[i].project)) {
            converters[i].converter = converter;
            return OPAL_SUCCESS;
        }
    }

    return OPAL_ERR_OUT_OF_RESOURCE;
}


void
opal_delay_abort(void)
{
    // Though snprintf and strlen are not guaranteed to be async-signal-safe
    // in POSIX, it is async-signal-safe on many implementations probably.

    if (0 != opal_abort_delay) {
        int delay = opal_abort_delay;
        pid_t pid = getpid();
        char msg[100 + OPAL_MAXHOSTNAMELEN];

        if (delay < 0) {
            snprintf(msg, sizeof(msg),
                     "[%s:%05d] Looping forever "
                     "(MCA parameter opal_abort_delay is < 0)\n",
                     opal_process_info.nodename, (int) pid);
            write(STDERR_FILENO, msg, strlen(msg));
            while (1) {
                sleep(5);
            }
        } else {
            snprintf(msg, sizeof(msg),
                     "[%s:%05d] Delaying for %d seconds before aborting\n",
                     opal_process_info.nodename, (int) pid, delay);
            write(STDERR_FILENO, msg, strlen(msg));
            do {
                sleep(1);
            } while (--delay > 0);
        }
    }
}

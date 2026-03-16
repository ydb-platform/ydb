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
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <time.h>

#include "opal/mca/base/mca_base_var.h"
#include "opal/util/alfg.h"
#include "opal/util/opal_environ.h"

#include "orte/constants.h"
#include "orte/types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/attr.h"

#include "orte/util/pre_condition_transports.h"

/* some network transports require a little bit of information to
 * "pre-condition" them - i.e., to setup their individual transport
 * connections so they can generate their endpoint addresses. This
 * function provides a means for doing so. The resulting info is placed
 * into the app_context's env array so it will automatically be pushed
 * into the environment of every MPI process when launched.
 */

static inline void orte_pre_condition_transports_use_rand(uint64_t* unique_key) {
    opal_rng_buff_t rng;
    opal_srand(&rng,(unsigned int)time(NULL));
    unique_key[0] = opal_rand(&rng);
    unique_key[1] = opal_rand(&rng);
}

char* orte_pre_condition_transports_print(uint64_t *unique_key)
{
    unsigned int *int_ptr;
    size_t i, j, string_key_len, written_len;
    char *string_key = NULL, *format = NULL;

    /* string is two 64 bit numbers printed in hex with a dash between
     * and zero padding.
     */
    string_key_len = (sizeof(uint64_t) * 2) * 2 + strlen("-") + 1;
    string_key = (char*) malloc(string_key_len);
    if (NULL == string_key) {
        return NULL;
    }

    string_key[0] = '\0';
    written_len = 0;

    /* get a format string based on the length of an unsigned int.  We
     * want to have zero padding for sizeof(unsigned int) * 2
     * characters -- when printing as a hex number, each byte is
     * represented by 2 hex characters.  Format will contain something
     * that looks like %08lx, where the number 8 might be a different
     * number if the system has a different sized long (8 would be for
     * sizeof(int) == 4)).
     */
    asprintf(&format, "%%0%dx", (int)(sizeof(unsigned int)) * 2);

    /* print the first number */
    int_ptr = (unsigned int*) &unique_key[0];
    for (i = 0 ; i < sizeof(uint64_t) / sizeof(unsigned int) ; ++i) {
        if (0 == int_ptr[i]) {
            /* inject some energy */
            for (j=0; j < sizeof(unsigned int); j++) {
                int_ptr[i] |= j << j;
            }
        }
        snprintf(string_key + written_len,
                 string_key_len - written_len,
                 format, int_ptr[i]);
        written_len = strlen(string_key);
    }

    /* print the middle dash */
    snprintf(string_key + written_len, string_key_len - written_len, "-");
    written_len = strlen(string_key);

    /* print the second number */
    int_ptr = (unsigned int*) &unique_key[1];
    for (i = 0 ; i < sizeof(uint64_t) / sizeof(unsigned int) ; ++i) {
        if (0 == int_ptr[i]) {
            /* inject some energy */
            for (j=0; j < sizeof(unsigned int); j++) {
                int_ptr[i] |= j << j;
            }
        }
        snprintf(string_key + written_len,
                 string_key_len - written_len,
                 format, int_ptr[i]);
        written_len = strlen(string_key);
    }
    free(format);

    return string_key;
}


int orte_pre_condition_transports(orte_job_t *jdata, char **key)
{
    uint64_t unique_key[2];
    int n;
    orte_app_context_t *app;
    char *string_key, *cs_env;
    int fd_rand;
    size_t bytes_read;
    struct stat buf;

    /* put the number here - or else create an appropriate string. this just needs to
     * eventually be a string variable
     */
    if(0 != stat("/dev/urandom", &buf)) {
        /* file doesn't exist! */
        orte_pre_condition_transports_use_rand(unique_key);
    }

    if(-1 == (fd_rand = open("/dev/urandom", O_RDONLY))) {
        orte_pre_condition_transports_use_rand(unique_key);
    } else {
        bytes_read = read(fd_rand, (char *) unique_key, 16);
        if(bytes_read != 16) {
            orte_pre_condition_transports_use_rand(unique_key);
        }
        close(fd_rand);
    }

    if (NULL == (string_key = orte_pre_condition_transports_print(unique_key))) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    /* record it in case this job executes a dynamic spawn */
    if (NULL != jdata) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_TRANSPORT_KEY, ORTE_ATTR_LOCAL, string_key, OPAL_STRING);

        if (OPAL_SUCCESS != mca_base_var_env_name ("orte_precondition_transports", &cs_env)) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            free(string_key);
            return ORTE_ERR_OUT_OF_RESOURCE;
        }

        for (n=0; n < jdata->apps->size; n++) {
            if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, n))) {
                continue;
            }
            opal_setenv(cs_env, string_key, true, &app->env);
        }
        free(cs_env);
        free(string_key);
    } else if (NULL != key) {
        *key = string_key;
    } else {
        free(string_key);
    }

    return ORTE_SUCCESS;
}

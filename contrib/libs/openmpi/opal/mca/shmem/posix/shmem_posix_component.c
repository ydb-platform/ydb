/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "opal_config.h"


#include <errno.h>
#include <string.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif /* HAVE_SYS_MMAN_H */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNISTD_H */
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif /* HAVE_NETDB_H */

#include "opal/constants.h"
#include "opal/util/show_help.h"
#include "opal/util/output.h"
#include "opal/mca/shmem/base/base.h"
#include "opal/mca/shmem/shmem.h"
#include "shmem_posix.h"
#include "shmem_posix_common_utils.h"

/* public string showing the shmem ompi_posix component version number */
const char *opal_shmem_posix_component_version_string =
    "OPAL posix shmem MCA component version " OPAL_VERSION;

/* local functions */
static int posix_register (void);
static int posix_open(void);
static int posix_query(mca_base_module_t **module, int *priority);
static int posix_runtime_query(mca_base_module_t **module,
                               int *priority,
                               const char *hint);

/* local variables */
static bool rt_successful = false;

/* instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
opal_shmem_posix_component_t mca_shmem_posix_component = {
    .super = {
        .base_version = {
            OPAL_SHMEM_BASE_VERSION_2_0_0,

            /* component name and version */
            .mca_component_name = "posix",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),

            .mca_open_component = posix_open,
            .mca_query_component = posix_query,
            .mca_register_component_params = posix_register
        },
        /* MCA v2.0.0 component meta data */
        .base_data = {
            /* the component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
        .runtime_query = posix_runtime_query,
    },
};


/* ////////////////////////////////////////////////////////////////////////// */
static int
posix_register(void)
{
    /* ////////////////////////////////////////////////////////////////////// */
    /* (default) priority - set lower than mmap's priority */
    mca_shmem_posix_component.priority = 40;
    (void) mca_base_component_var_register(&mca_shmem_posix_component.super.base_version,
                                           "priority", "Priority for the shmem posix "
                                           "component (default: 40)", MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_3,
                                           MCA_BASE_VAR_SCOPE_ALL_EQ,
                                           &mca_shmem_posix_component.priority);

    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */

static int
posix_open(void)
{
    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * this routine performs a test that indicates whether or not posix shared
 * memory can safely be used during this run.
 * note: that we want to run this test as few times as possible.
 *
 * @return OPAL_SUCCESS when posix can safely be used.
 */
static int
posix_runtime_query(mca_base_module_t **module,
                    int *priority,
                    const char *hint)
{
    char tmp_buff[OPAL_SHMEM_POSIX_FILE_LEN_MAX];
    int fd = -1;

    *priority = 0;
    *module = NULL;

    /* if hint isn't null, then someone else already figured out who is the
     * best runnable component is AND the caller is relaying that info so we
     * don't have to perform a run-time query.
     */
    if (NULL != hint) {
        OPAL_OUTPUT_VERBOSE(
            (70, opal_shmem_base_framework.framework_output,
             "shmem: posix: runtime_query: "
             "attempting to use runtime hint (%s)\n", hint)
        );
        /* was i selected? if so, then we are done.
         * otherwise, disqualify myself.
         */
        if (0 == strcasecmp(hint,
            mca_shmem_posix_component.super.base_version.mca_component_name)) {
            *priority = mca_shmem_posix_component.priority;
            *module = (mca_base_module_t *)&opal_shmem_posix_module.super;
            return OPAL_SUCCESS;
        }
        else {
            *priority = 0;
            *module = NULL;
            return OPAL_SUCCESS;
        }
    }
    /* if we are here, then perform a run-time query because we didn't get a
     * hint.  it's either up to us to figure it out, or the caller wants us to
     * re-run the runtime query.
     */
    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "shmem: posix: runtime_query: NO HINT PROVIDED:"
         "starting run-time test...\n")
    );
    /* shmem_posix_shm_open successfully shm_opened - we can use posix sm! */
    if (-1 != (fd = shmem_posix_shm_open(tmp_buff,
                                         OPAL_SHMEM_POSIX_FILE_LEN_MAX -1))) {
        /* free up allocated resources before we return */
        if (0 != shm_unlink(tmp_buff)) {
            int err = errno;
            char hn[OPAL_MAXHOSTNAMELEN];
            gethostname(hn, sizeof(hn));
            opal_show_help("help-opal-shmem-posix.txt", "sys call fail", 1,
                           hn, "shm_unlink(2)", "", strerror(err), err);
            /* something strange happened, so consider this a run-time test
             * failure even though shmem_posix_shm_open was successful */
        }
        /* all is well */
        else {
            *priority = mca_shmem_posix_component.priority;
            *module = (mca_base_module_t *)&opal_shmem_posix_module.super;
            rt_successful = true;
        }
    }

    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
posix_query(mca_base_module_t **module, int *priority)
{
    *priority = mca_shmem_posix_component.priority;
    *module = (mca_base_module_t *)&opal_shmem_posix_module.super;
    return OPAL_SUCCESS;
}


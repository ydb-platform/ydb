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
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif /* HAVE_SYS_MMAN_H */
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNISTD_H */

#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif /* HAVE_SYS_IPC_H */
#if HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif /* HAVE_SYS_SHM_H */
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif /* HAVE_SYS_STAT_H */

#include "opal/constants.h"
#include "opal/util/show_help.h"
#include "opal/util/output.h"
#include "opal/util/sys_limits.h"
#include "opal/mca/shmem/base/base.h"
#include "opal/mca/shmem/shmem.h"
#include "shmem_sysv.h"

/* public string showing the shmem ompi_sysv component version number */
const char *opal_shmem_sysv_component_version_string =
    "OPAL sysv shmem MCA component version " OPAL_VERSION;

/* local functions */
static int sysv_register (void);
static int sysv_open(void);
static int sysv_query(mca_base_module_t **module, int *priority);
static int sysv_runtime_query(mca_base_module_t **module,
                              int *priority,
                              const char *hint);

/* instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
opal_shmem_sysv_component_t mca_shmem_sysv_component = {
    .super = {
        /* common MCA component data */
        {
            OPAL_SHMEM_BASE_VERSION_2_0_0,

            /* component name and version */
            .mca_component_name = "sysv",
            MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                  OPAL_RELEASE_VERSION),

            .mca_open_component = sysv_open,
            .mca_query_component = sysv_query,
            .mca_register_component_params = sysv_register
        },
        /* MCA v2.0.0 component meta data */
        .base_data = {
            /* the component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
        .runtime_query = sysv_runtime_query,
    },
};

/* ////////////////////////////////////////////////////////////////////////// */
static int
sysv_register(void)
{
    /* ////////////////////////////////////////////////////////////////////// */
    /* (default) priority - set lower than mmap's priority */
    mca_shmem_sysv_component.priority = 30;
    (void) mca_base_component_var_register(&mca_shmem_sysv_component.super.base_version,
                                           "priority", "Priority for the shmem sysv "
                                           "component (default: 30)", MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_3,
                                           MCA_BASE_VAR_SCOPE_ALL_EQ,
                                           &mca_shmem_sysv_component.priority);

    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
sysv_open(void)
{
    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * this routine performs a test that indicates whether or not sysv shared
 * memory can safely be used during this run.
 * note: that we want to run this test as few times as possible.
 *
 * @return OPAL_SUCCESS when sysv can safely be used.
 */
static int
sysv_runtime_query(mca_base_module_t **module, int *priority, const char *hint)
{
    char c     = 'j';
    int shmid  = -1;
    char *a    = NULL;
    char *addr = NULL;
    struct shmid_ds tmp_buff;

    *priority = 0;
    *module = NULL;

    /* if hint isn't null, then someone else already figured out who is the
     * best runnable component is AND the caller is relaying that info so we
     * don't have to perform a run-time query.
     */
    if (NULL != hint) {
        OPAL_OUTPUT_VERBOSE(
            (70, opal_shmem_base_framework.framework_output,
             "shmem: sysv: runtime_query: "
             "attempting to use runtime hint (%s)\n", hint)
        );
        /* was i selected? if so, then we are done.
         * otherwise, disqualify myself.
         */
        if (0 == strcasecmp(hint,
            mca_shmem_sysv_component.super.base_version.mca_component_name)) {
            *priority = mca_shmem_sysv_component.priority;
            *module = (mca_base_module_t *)&opal_shmem_sysv_module.super;
            return OPAL_SUCCESS;
        }
        else {
            *priority = 0;
            *module = NULL;
            return OPAL_SUCCESS;
        }
    }

    /* if we are here, then let the run-time test games begin */
    OPAL_OUTPUT_VERBOSE(
        (70, opal_shmem_base_framework.framework_output,
         "shmem: sysv: runtime_query: NO HINT PROVIDED:"
         "starting run-time test...\n")
    );

    if (-1 == (shmid = shmget(IPC_PRIVATE, (size_t)(opal_getpagesize()),
                              IPC_CREAT | IPC_EXCL | S_IRWXU ))) {
        goto out;
    }
    else if ((void *)-1 == (addr = shmat(shmid, NULL, 0))) {
        goto out;
    }

    /* protect against lazy establishment - may not be needed, but can't hurt */
    a = addr;
    *a = c;

    if (-1 == shmctl(shmid, IPC_RMID, NULL)) {
        goto out;
    }
    else if (-1 == shmctl(shmid, IPC_STAT, &tmp_buff)) {
        goto out;
    }
    /* all is well - rainbows and butterflies */
    else {
        *priority = mca_shmem_sysv_component.priority;
        *module = (mca_base_module_t *)&opal_shmem_sysv_module.super;
    }

out:
    if (NULL != addr && (char *)-1 != addr) {
        shmdt(addr);
    }
    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
static int
sysv_query(mca_base_module_t **module, int *priority)
{
    *priority = mca_shmem_sysv_component.priority;
    *module = (mca_base_module_t *)&opal_shmem_sysv_module.super;
    return OPAL_SUCCESS;
}


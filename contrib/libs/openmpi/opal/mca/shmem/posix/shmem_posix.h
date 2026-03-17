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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_SHMEM_POSIX_EXPORT_H
#define MCA_SHMEM_POSIX_EXPORT_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/shmem/shmem.h"

/* max number of attempts to find an available shm_open file name. see
 * comments below for more details.
 */
#define OPAL_SHMEM_POSIX_MAX_ATTEMPTS 128

/* need the '/' for Solaris 10 and others, i'm sure */
#define OPAL_SHMEM_POSIX_FILE_NAME_PREFIX "/open_mpi."

/* posix sm file name length max.  on some systems shm_open's file name limit
 * is pretty low (32 chars, for instance).  16 is plenty for our needs, but
 * extra work on our end is needed to ensure things work properly. if a
 * system's limit is lower than OPAL_SHMEM_POSIX_FILE_LEN_MAX, then the
 * run-time test will catch that fact and posix sm will be disqualified. see
 * comments regarding this in shmem_posix_module.c.
 */
#define OPAL_SHMEM_POSIX_FILE_LEN_MAX 16

BEGIN_C_DECLS

/* globally exported variable to hold the posix component. */
typedef struct opal_shmem_posix_component_t {
    /* base component struct */
    opal_shmem_base_component_t super;
    /* priority for posix component */
    int priority;
} opal_shmem_posix_component_t;

OPAL_MODULE_DECLSPEC extern opal_shmem_posix_component_t
mca_shmem_posix_component;

typedef struct opal_shmem_posix_module_t {
    opal_shmem_base_module_t super;
} opal_shmem_posix_module_t;
extern opal_shmem_posix_module_t opal_shmem_posix_module;

END_C_DECLS

#endif /* MCA_SHMEM_POSIX_EXPORT_H */

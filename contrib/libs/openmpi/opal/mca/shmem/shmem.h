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
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * shmem (shared memory backing facility) framework component interface
 * definitions.
 *
 * usage example: see ompi/mca/common/sm
 *
 * The module has the following functions:
 *
 * - module_init
 * - segment_create
 * - ds_copy
 * - segment_attach
 * - segment_detach
 * - unlink
 * - module_finalize
 */

#ifndef OPAL_SHMEM_H
#define OPAL_SHMEM_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/mca/shmem/shmem_types.h"

BEGIN_C_DECLS

/* ////////////////////////////////////////////////////////////////////////// */
typedef int
(*mca_shmem_base_component_runtime_query_fn_t)(mca_base_module_t **module,
                                               int *priority,
                                               const char *hint);

/* structure for shmem components. */
struct opal_shmem_base_component_2_0_0_t {
    /* base MCA component */
    mca_base_component_t base_version;
    /* base MCA data */
    mca_base_component_data_t base_data;
    /* component runtime query */
    mca_shmem_base_component_runtime_query_fn_t runtime_query;
};

/* convenience typedefs */
typedef struct opal_shmem_base_component_2_0_0_t
opal_shmem_base_component_2_0_0_t;

typedef struct opal_shmem_base_component_2_0_0_t opal_shmem_base_component_t;

/* ////////////////////////////////////////////////////////////////////////// */
/* shmem API function pointers */

/**
 * module initialization function.
 * @return OPAL_SUCCESS on success.
 */
typedef int
(*opal_shmem_base_module_init_fn_t)(void);

/**
 * copy shmem data structure information pointed to by from to the structure
 * pointed to by to.
 *
 * @param from  source pointer (IN).
 *
 * @param to    destination pointer (OUT).
 *
 * @return OPAL_SUCCESS on success.
 */
typedef int
(*opal_shmem_base_ds_copy_fn_t)(const opal_shmem_ds_t *from,
                                opal_shmem_ds_t *to);

/**
 * create a new shared memory segment and initialize members in structure
 * pointed to by ds_buf.
 *
 * @param ds_buf               pointer to opal_shmem_ds_t typedef'd structure
 *                             defined in shmem_types.h (OUT).
 *
 * @param file_name file_name  unique string identifier that must be a valid,
 *                             writable path (IN).
 *
 * @param size                 size of the shared memory segment.
 *
 * @return OPAL_SUCCESS on success.
 */
typedef int
(*opal_shmem_base_module_segment_create_fn_t)(opal_shmem_ds_t *ds_buf,
                                              const char *file_name,
                                              size_t size);

/**
 * attach to an existing shared memory segment initialized by segment_create.
 *
 * @param ds_buf  pointer to initialized opal_shmem_ds_t typedef'd
 *                structure (IN/OUT).
 *
 * @return        base address of shared memory segment on success. returns
 *                NULL otherwise.
 */
typedef void *
(*opal_shmem_base_module_segment_attach_fn_t)(opal_shmem_ds_t *ds_buf);

/**
 * detach from an existing shared memory segment.
 *
 * @param ds_buf  pointer to initialized opal_shmem_ds_t typedef'd structure
 *                (IN/OUT).
 *
 * @return OPAL_SUCCESS on success.
 */
typedef int
(*opal_shmem_base_module_segment_detach_fn_t)(opal_shmem_ds_t *ds_buf);

/**
 * unlink an existing shared memory segment.
 *
 * @param ds_buf  pointer to initialized opal_shmem_ds_t typedef'd structure
 *                (IN/OUT).
 *
 * @return OPAL_SUCCESS on success.
 */
typedef int
(*opal_shmem_base_module_unlink_fn_t)(opal_shmem_ds_t *ds_buf);

/**
 * module finalize function.  invoked by the base on the selected
 * module when the shmem framework is being shut down.
 */
typedef int (*opal_shmem_base_module_finalize_fn_t)(void);

/**
 * structure for shmem modules
 */
struct opal_shmem_base_module_2_0_0_t {
    mca_base_module_t                           base;
    opal_shmem_base_module_init_fn_t            module_init;
    opal_shmem_base_module_segment_create_fn_t  segment_create;
    opal_shmem_base_ds_copy_fn_t                ds_copy;
    opal_shmem_base_module_segment_attach_fn_t  segment_attach;
    opal_shmem_base_module_segment_detach_fn_t  segment_detach;
    opal_shmem_base_module_unlink_fn_t          unlink;
    opal_shmem_base_module_finalize_fn_t        module_finalize;
};

/**
 * convenience typedefs
 */
typedef struct opal_shmem_base_module_2_0_0_t opal_shmem_base_module_2_0_0_t;
typedef struct opal_shmem_base_module_2_0_0_t opal_shmem_base_module_t;

/**
 * macro for use in components that are of type shmem
 * see: opal/mca/mca.h for more information
 */
#define OPAL_SHMEM_BASE_VERSION_2_0_0                                   \
    OPAL_MCA_BASE_VERSION_2_1_0("shmem", 2, 0, 0)

END_C_DECLS

#endif /* OPAL_SHMEM_H */

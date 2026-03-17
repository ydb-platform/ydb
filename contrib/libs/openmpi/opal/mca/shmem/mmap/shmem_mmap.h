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
 * Copyright (c) 2007-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_SHMEM_MMAP_EXPORT_H
#define MCA_SHMEM_MMAP_EXPORT_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/shmem/shmem.h"

BEGIN_C_DECLS

extern int opal_shmem_mmap_relocate_backing_file;
extern char *opal_shmem_mmap_backing_file_base_dir;
extern bool opal_shmem_mmap_nfs_warning;

/**
 * globally exported variable to hold the mmap component.
 */
typedef struct opal_shmem_mmap_component_t {
    /* base component struct */
    opal_shmem_base_component_t super;
    /* priority for mmap component */
    int priority;
} opal_shmem_mmap_component_t;

OPAL_MODULE_DECLSPEC extern opal_shmem_mmap_component_t
mca_shmem_mmap_component;

typedef struct opal_shmem_mmap_module_t {
    opal_shmem_base_module_t super;
} opal_shmem_mmap_module_t;
extern opal_shmem_mmap_module_t opal_shmem_mmap_module;

END_C_DECLS

#endif /* MCA_SHMEM_MMAP_EXPORT_H */

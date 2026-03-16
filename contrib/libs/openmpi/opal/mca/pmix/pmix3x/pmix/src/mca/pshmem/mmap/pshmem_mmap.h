/*
 * Copyright (c) 2015-2016 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_SM_MMAP_H
#define PMIX_SM_MMAP_H

#include <src/include/pmix_config.h>
#include <src/mca/pshmem/pshmem.h>

BEGIN_C_DECLS

PMIX_EXPORT extern pmix_pshmem_base_component_t mca_pshmem_mmap_component;
extern pmix_pshmem_base_module_t pmix_mmap_module;

END_C_DECLS

#endif /* PMIX_SM_MMAP_H */

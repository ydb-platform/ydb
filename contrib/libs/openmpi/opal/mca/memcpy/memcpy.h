/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 */

#ifndef OPAL_MCA_MEMCPY_MEMCPY_H
#define OPAL_MCA_MEMCPY_MEMCPY_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

/**
 * Structure for memcpy components.
 */
struct opal_memcpy_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t memcpyc_version;
    /** MCA base data */
    mca_base_component_data_t memcpyc_data;
};

/**
 * Convenience typedef
 */
typedef struct opal_memcpy_base_component_2_0_0_t opal_memcpy_base_component_2_0_0_t;

/*
 * Macro for use in components that are of type memcpy
 */
#define OPAL_MEMCPY_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("memcpy", 2, 0, 0)

#endif /* OPAL_MCA_MEMCPY_MEMCPY_H */

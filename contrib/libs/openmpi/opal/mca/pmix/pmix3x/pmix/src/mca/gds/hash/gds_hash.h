/*
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_GDS_HASH_H
#define PMIX_GDS_HASH_H

#include <src/include/pmix_config.h>


#include "src/mca/gds/gds.h"

BEGIN_C_DECLS

/* the component must be visible data for the linker to find it */
PMIX_EXPORT extern pmix_gds_base_component_t mca_gds_hash_component;
extern pmix_gds_base_module_t pmix_hash_module;

END_C_DECLS

#endif

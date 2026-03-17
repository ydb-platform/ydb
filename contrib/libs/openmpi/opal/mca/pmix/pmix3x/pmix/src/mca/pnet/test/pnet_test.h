/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_PNET_test_H
#define PMIX_PNET_test_H

#include <src/include/pmix_config.h>


#include "src/mca/pnet/pnet.h"

BEGIN_C_DECLS

typedef struct {
    pmix_pnet_base_component_t super;
    char **include;
    char **exclude;
} pmix_pnet_test_component_t;

/* the component must be visible data for the linker to find it */
PMIX_EXPORT extern pmix_pnet_test_component_t mca_pnet_test_component;
extern pmix_pnet_module_t pmix_test_module;

/* define a key for any blob we need to send in a launch msg */
#define PMIX_PNET_TEST_BLOB  "pmix.pnet.test.blob"

END_C_DECLS

#endif

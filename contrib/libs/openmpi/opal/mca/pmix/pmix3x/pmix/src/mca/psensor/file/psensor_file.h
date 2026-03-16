/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 *
 * Copyright (c) 2017      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * File movement sensor
 */
#ifndef PMIX_PSENSOR_FILE_H
#define PMIX_PSENSOR_FILE_H

#include <src/include/pmix_config.h>

#include "src/class/pmix_list.h"

#include "src/mca/psensor/psensor.h"

BEGIN_C_DECLS

typedef struct {
    pmix_psensor_base_component_t super;
    pmix_list_t trackers;
} pmix_psensor_file_component_t;

PMIX_EXPORT extern pmix_psensor_file_component_t mca_psensor_file_component;
extern pmix_psensor_base_module_t pmix_psensor_file_module;


END_C_DECLS

#endif

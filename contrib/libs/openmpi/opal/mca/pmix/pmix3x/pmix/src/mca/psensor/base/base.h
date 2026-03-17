/*
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 *
 * Copyright (c) 2017      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef PMIX_PSENSOR_BASE_H_
#define PMIX_PSENSOR_BASE_H_

#include <src/include/pmix_config.h>

#include "src/class/pmix_list.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/psensor/psensor.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_psensor_base_framework;

PMIX_EXPORT int pmix_psensor_base_select(void);

/* define a struct to hold framework-global values */
typedef struct {
    pmix_list_t actives;
    pmix_event_base_t *evbase;
} pmix_psensor_base_t;

typedef struct {
    pmix_list_item_t super;
    pmix_psensor_base_component_t *component;
    pmix_psensor_base_module_t *module;
    int priority;
} pmix_psensor_active_module_t;
PMIX_CLASS_DECLARATION(pmix_psensor_active_module_t);

PMIX_EXPORT extern pmix_psensor_base_t pmix_psensor_base;

PMIX_EXPORT pmix_status_t pmix_psensor_base_start(pmix_peer_t *requestor, pmix_status_t error,
                                                  const pmix_info_t *monitor,
                                                  const pmix_info_t directives[], size_t ndirs);

PMIX_EXPORT pmix_status_t pmix_psensor_base_stop(pmix_peer_t *requestor,
                                                 char *id);

END_C_DECLS
#endif

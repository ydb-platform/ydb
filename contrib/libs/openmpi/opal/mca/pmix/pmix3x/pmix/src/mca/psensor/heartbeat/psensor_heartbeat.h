/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 *
 * Copyright (c) 2017-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Heartbeat sensor
 */
#ifndef PMIX_PSENSOR_HEARTBEAT_H
#define PMIX_PSENSOR_HEARTBEAT_H

#include <src/include/pmix_config.h>
#include <src/include/types.h>

#include "src/class/pmix_list.h"
#include "src/include/pmix_globals.h"
#include "src/mca/psensor/psensor.h"

BEGIN_C_DECLS

typedef struct {
    pmix_psensor_base_component_t super;
    bool recv_active;
    pmix_list_t trackers;
} pmix_psensor_heartbeat_component_t;

PMIX_EXPORT extern pmix_psensor_heartbeat_component_t mca_psensor_heartbeat_component;
extern pmix_psensor_base_module_t pmix_psensor_heartbeat_module;

void pmix_psensor_heartbeat_recv_beats(struct pmix_peer_t *peer,
                                       pmix_ptl_hdr_t *hdr,
                                       pmix_buffer_t *buf, void *cbdata);

END_C_DECLS

#endif

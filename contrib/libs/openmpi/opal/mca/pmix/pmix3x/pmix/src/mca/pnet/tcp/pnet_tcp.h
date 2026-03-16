/*
 * Copyright (c) 2018-2019 Intel, Inc.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_PNET_OPA_H
#define PMIX_PNET_OPA_H

#include <src/include/pmix_config.h>


#include "src/mca/pnet/pnet.h"

BEGIN_C_DECLS

typedef struct {
    pmix_pnet_base_component_t super;
    char *static_ports;
    char *default_request;
    char *incparms;
    char *excparms;
    char **include;
    char **exclude;
} pmix_pnet_tcp_component_t;

/* the component must be visible data for the linker to find it */
PMIX_EXPORT extern pmix_pnet_tcp_component_t mca_pnet_tcp_component;
extern pmix_pnet_module_t pmix_tcp_module;

END_C_DECLS

#endif

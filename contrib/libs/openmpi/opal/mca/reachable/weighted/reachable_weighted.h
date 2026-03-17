/*
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_REACHABLE_WEIGHTED_H
#define MCA_REACHABLE_WEIGHTED_H

#include "opal_config.h"

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif

#include "opal/mca/reachable/reachable.h"
#include "opal/mca/mca.h"
#include "opal/mca/event/event.h"
#include "opal/util/proc.h"

#include "opal/mca/pmix/base/base.h"

BEGIN_C_DECLS

typedef struct {
    opal_reachable_base_component_t super;
} opal_reachable_weighted_component_t;

OPAL_DECLSPEC extern opal_reachable_weighted_component_t mca_reachable_weighted_component;

OPAL_DECLSPEC extern const opal_reachable_base_module_t opal_reachable_weighted_module;


END_C_DECLS

#endif /* MCA_REACHABLE_WEIGHTED_H */

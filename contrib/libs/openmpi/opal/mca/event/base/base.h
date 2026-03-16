/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_EVENT_BASE_H
#define OPAL_EVENT_BASE_H

#include "opal_config.h"

#include "opal/class/opal_pointer_array.h"
#include "opal/mca/base/base.h"
#include "opal/mca/event/event.h"

/*
 * Global functions for MCA overall event open and close
 */

BEGIN_C_DECLS

/**
 * Event framework
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_event_base_framework;

END_C_DECLS

#endif /* OPAL_BASE_EVENT_H */

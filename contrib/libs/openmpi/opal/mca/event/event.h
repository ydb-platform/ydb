/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 */

#ifndef OPAL_MCA_EVENT_H
#define OPAL_MCA_EVENT_H

#include "opal_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#include <stdint.h>
#include <stdarg.h>

#include "opal/class/opal_pointer_array.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

BEGIN_C_DECLS

/* set the number of event priority levels */
#define OPAL_EVENT_NUM_PRI   8

#define OPAL_EV_ERROR_PRI         0
#define OPAL_EV_MSG_HI_PRI        1
#define OPAL_EV_SYS_HI_PRI        2
#define OPAL_EV_MSG_LO_PRI        3
#define OPAL_EV_SYS_LO_PRI        4
#define OPAL_EV_INFO_HI_PRI       5
#define OPAL_EV_INFO_LO_PRI       6
#define OPAL_EV_LOWEST_PRI        7

#define OPAL_EVENT_SIGNAL(ev)	opal_event_get_signal(ev)

#define OPAL_TIMEOUT_DEFAULT	{1, 0}

/**
 * Structure for event components.
 */
struct opal_event_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;
};

/**
 * Convenience typedef
 */
typedef struct opal_event_base_component_2_0_0_t opal_event_base_component_2_0_0_t;
typedef struct opal_event_base_component_2_0_0_t opal_event_component_t;

/**
 * Macro for use in components that are of type event
 */
#define OPAL_EVENT_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("event", 2, 0, 0)

END_C_DECLS

/* include implementation to call */
#include MCA_event_IMPLEMENTATION_HEADER

#endif /* OPAL_EVENT_H_ */

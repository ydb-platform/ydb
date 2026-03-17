/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 *
 * Copyright (c) 2014-2015 Intel, Inc.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef MCA_NOTIFIER_BASE_H
#define MCA_NOTIFIER_BASE_H

/*
 * includes
 */
#include "orte_config.h"

#include "opal/class/opal_list.h"
#include "opal/mca/base/base.h"
#include "opal/mca/event/event.h"

#include "orte/mca/notifier/notifier.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_notifier_base_framework;

typedef struct {
    opal_event_base_t *ev_base;
    bool ev_base_active;
    opal_list_t modules;
    orte_notifier_severity_t severity_level;
    char *default_actions;
    char *emerg_actions;
    char *alert_actions;
    char *crit_actions;
    char *warn_actions;
    char *notice_actions;
    char *info_actions;
    char *debug_actions;
    char *error_actions;
} orte_notifier_base_t;

/*
 * Type for holding selected module / component pairs
 */
typedef struct {
    opal_list_item_t super;
    /* Component */
    orte_notifier_base_component_t *component;
    /* Module */
    orte_notifier_base_module_t *module;
} orte_notifier_active_module_t;
OBJ_CLASS_DECLARATION(orte_notifier_active_module_t);

ORTE_DECLSPEC extern orte_notifier_base_t orte_notifier_base;

/* select a component */
ORTE_DECLSPEC int orte_notifier_base_select(void);

/* base functions */
ORTE_DECLSPEC void orte_notifier_base_log(int sd, short args, void *cbdata);
ORTE_DECLSPEC void orte_notifier_base_event(int sd, short args, void *cbdata);
ORTE_DECLSPEC void orte_notifier_base_report(int sd, short args, void *cbdata);

/* severity to string */
ORTE_DECLSPEC const char* orte_notifier_base_sev2str(orte_notifier_severity_t severity);
END_C_DECLS
#endif

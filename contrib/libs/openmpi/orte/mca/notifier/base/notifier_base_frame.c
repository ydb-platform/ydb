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
 * Copyright (c) 2008-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>

#include "orte/mca/mca.h"
#include "opal/util/argv.h"
#include "opal/util/fd.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/runtime/opal_progress_threads.h"
#include "orte/mca/notifier/base/base.h"

/* default module to use for logging*/
#define ORTE_NOTIFIER_DEFAULT_MODULE "syslog"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/notifier/base/static-components.h"

/*
 * Global variables
 */
opal_list_t orte_notifier_base_components_available = {{0}};
int orte_notifier_debug_output = -1;

orte_notifier_base_t orte_notifier_base = {0};

static char *notifier_severity = NULL;
static bool use_progress_thread = false;

/**
 * Function for selecting a set of components from all those that are
 * available.
 *
 * Examples:
 * 1)
 * -mca notifier syslog,smtp
 *      --> syslog and smtp are selected for the loging
 */
static int orte_notifier_base_register(mca_base_register_flag_t flags)
{
    (void) mca_base_var_register("orte", "notifier", "base", "use_progress_thread",
                                 "Use a dedicated progress thread for notifications [default: false]",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &use_progress_thread);

    /* let the user define a base level of severity to report */
    (void) mca_base_var_register("orte", "notifier", "base", "severity_level",
                                 "Report all events at or above this severity [default: error]",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &notifier_severity);
    if (NULL == notifier_severity) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_ERROR;
    } else if (0 == strncasecmp(notifier_severity, "emerg", strlen("emerg"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_EMERG;
    } else if (0 == strncasecmp(notifier_severity, "alert", strlen("alert"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_ALERT;
    } else if (0 == strncasecmp(notifier_severity, "crit", strlen("crit"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_CRIT;
    } else if (0 == strncasecmp(notifier_severity, "warn", strlen("warn"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_WARN;
    } else if (0 == strncasecmp(notifier_severity, "notice", strlen("notice"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_NOTICE;
    } else if (0 == strncasecmp(notifier_severity, "info", strlen("info"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_INFO;
    } else if (0 == strncasecmp(notifier_severity, "debug", strlen("debug"))) {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_DEBUG;
    } else {
        orte_notifier_base.severity_level = ORTE_NOTIFIER_ERROR;
    }

    /* let the user define a base default actions */
    orte_notifier_base.default_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "default_actions",
                                 "Report all events to the default actions:NONE,syslog,smtp",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.default_actions);

    if (NULL == orte_notifier_base.default_actions) {
        orte_notifier_base.default_actions = strdup(ORTE_NOTIFIER_DEFAULT_MODULE);
    }
    /* let the user define a action for emergency events */
    orte_notifier_base.emerg_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "emerg_event_actions",
                                 "Report emergency events to the specified actions: example 'smtp'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.emerg_actions);

    /* let the user define a action for alert events */
    orte_notifier_base.alert_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "alert_event_actions",
                                 "Report alert events to the specified actions: example 'smtp'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.alert_actions);

    /* let the user define a action for critical events */
    orte_notifier_base.crit_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "crit_event_actions",
                                 "Report critical events to the specified actions: example 'syslog'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.crit_actions);

    /* let the user define a action for warning events */
    orte_notifier_base.warn_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "warn_event_actions",
                                 "Report warning events to the specified actions: example 'syslog'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.warn_actions);

    /* let the user define a action for notice events */
    orte_notifier_base.notice_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "notice_event_actions",
                                 "Report notice events to the specified actions: example 'syslog'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.notice_actions);

    /* let the user define a action for info events */
    orte_notifier_base.info_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "info_event_actions",
                                 "Report info events to the specified actions: example 'syslog'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.info_actions);

    /* let the user define a action for debug events */
    orte_notifier_base.debug_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "debug_event_actions",
                                 "Report debug events to the specified actions: example 'syslog'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.debug_actions);

    /* let the user define a action for error events */
    orte_notifier_base.error_actions = NULL;
    (void) mca_base_var_register("orte", "notifier", "base", "error_event_actions",
                                 "Report error events to the specified actions: example 'syslog'",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_notifier_base.error_actions);

    return ORTE_SUCCESS;
}

static int orte_notifier_base_close(void)
{
    orte_notifier_active_module_t *i_module;

    if (orte_notifier_base.ev_base_active) {
        orte_notifier_base.ev_base_active = false;
        opal_progress_thread_finalize("notifier");
    }

    OPAL_LIST_FOREACH(i_module, &orte_notifier_base.modules, orte_notifier_active_module_t) {
        if (NULL != i_module->module->finalize) {
            i_module->module->finalize();
        }
    }
    OPAL_LIST_DESTRUCT(&orte_notifier_base.modules);

    /* close all remaining available components */
    return mca_base_framework_components_close(&orte_notifier_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_notifier_base_open(mca_base_open_flag_t flags)
{
    int rc;

    /* construct the array of modules */
    OBJ_CONSTRUCT(&orte_notifier_base.modules, opal_list_t);

    /* if requested, create our own event base */
    if (use_progress_thread) {
        orte_notifier_base.ev_base_active = true;
        if (NULL == (orte_notifier_base.ev_base =
                     opal_progress_thread_init("notifier"))) {
            orte_notifier_base.ev_base_active = false;
            return ORTE_ERROR;
        }
    } else {
        orte_notifier_base.ev_base = orte_event_base;
    }

    /* Open up all available components */
    rc = mca_base_framework_components_open(&orte_notifier_base_framework,
                                            flags);
    orte_notifier_debug_output = orte_notifier_base_framework.framework_output;
    return rc;
}

MCA_BASE_FRAMEWORK_DECLARE(orte, notifier, "ORTE Notifier Framework",
                           orte_notifier_base_register,
                           orte_notifier_base_open, orte_notifier_base_close,
                           mca_notifier_base_static_components, 0);


OBJ_CLASS_INSTANCE (orte_notifier_active_module_t,
                    opal_list_item_t,
                    NULL, NULL);

static void req_cons (orte_notifier_request_t *r)
{
    r->jdata = NULL;
    r->msg = NULL;
    r->t = 0;
}
static void req_des(orte_notifier_request_t *r)
{
    if (NULL != r->jdata) {
        OBJ_RELEASE(r->jdata);
    }
}
OBJ_CLASS_INSTANCE (orte_notifier_request_t,
                    opal_object_t,
                    req_cons, req_des);

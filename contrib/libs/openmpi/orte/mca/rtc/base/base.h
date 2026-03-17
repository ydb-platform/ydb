/*
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 * rtc framework base functionality.
 */

#ifndef ORTE_MCA_RTC_BASE_H
#define ORTE_MCA_RTC_BASE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"

#include "opal/class/opal_list.h"
#include "orte/mca/mca.h"

#include "orte/mca/rtc/rtc.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_rtc_base_framework;
/* select a component */
ORTE_DECLSPEC    int orte_rtc_base_select(void);

/*
 * Global functions for MCA overall collective open and close
 */

/**
 * Struct to hold data global to the rtc framework
 */
typedef struct {
    /* list of selected modules */
    opal_list_t actives;
} orte_rtc_base_t;

/**
 * Global instance of rtc-wide framework data
 */
ORTE_DECLSPEC extern orte_rtc_base_t orte_rtc_base;

/**
 * Select an rtc component / module
 */
typedef struct {
    opal_list_item_t super;
    int pri;
    orte_rtc_base_module_t *module;
    mca_base_component_t *component;
} orte_rtc_base_selected_module_t;
OBJ_CLASS_DECLARATION(orte_rtc_base_selected_module_t);

ORTE_DECLSPEC void orte_rtc_base_assign(orte_job_t *jdata);
ORTE_DECLSPEC void orte_rtc_base_set(orte_job_t *jdata, orte_proc_t *proc,
                                     char ***env, int error_fd);
ORTE_DECLSPEC void orte_rtc_base_get_avail_vals(opal_list_t *vals);

/* Called from the child to send a warning show_help message up the
   pipe to the waiting parent. */
ORTE_DECLSPEC int orte_rtc_base_send_warn_show_help(int fd, const char *file,
                                                    const char *topic, ...);

/* Called from the child to send an error message up the pipe to the
   waiting parent. */
ORTE_DECLSPEC void orte_rtc_base_send_error_show_help(int fd, int exit_status,
                                                      const char *file,
                                                      const char *topic, ...);


END_C_DECLS

#endif

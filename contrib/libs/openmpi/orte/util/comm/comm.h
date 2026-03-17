/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file:
 *
 * Populates global structure with system-specific information.
 *
 * Notes: add limits.h, compute size of integer and other types via sizeof(type)*CHAR_BIT
 *
 */

#ifndef _ORTE_UTIL_COMM_H_
#define _ORTE_UTIL_COMM_H_

#include "orte_config.h"
#include "orte/types.h"

#include "orte/runtime/orte_globals.h"

BEGIN_C_DECLS

typedef uint8_t orte_comm_event_t;
#define ORTE_COMM_EVENT OPAL_UINT8

#define ORTE_COMM_EVENT_ALLOCATE    0x01
#define ORTE_COMM_EVENT_MAP         0x02
#define ORTE_COMM_EVENT_LAUNCH      0x04

ORTE_DECLSPEC int orte_util_comm_connect_tool(char *uri);

ORTE_DECLSPEC int orte_util_comm_report_event(orte_comm_event_t ev);

ORTE_DECLSPEC int orte_util_comm_query_job_info(const orte_process_name_t *hnp, orte_jobid_t job,
                                                int *num_jobs, orte_job_t ***job_info_array);

ORTE_DECLSPEC int orte_util_comm_query_node_info(const orte_process_name_t *hnp, char *node,
                                                 int *num_nodes, orte_node_t ***node_info_array);

ORTE_DECLSPEC int orte_util_comm_query_proc_info(const orte_process_name_t *hnp, orte_jobid_t job, orte_vpid_t vpid,
                                                 int *num_procs, orte_proc_t ***proc_info_array);

ORTE_DECLSPEC int orte_util_comm_spawn_job(const orte_process_name_t *hnp, orte_job_t *jdata);

ORTE_DECLSPEC int orte_util_comm_terminate_job(const orte_process_name_t *hnp, orte_jobid_t job);

ORTE_DECLSPEC int orte_util_comm_halt_vm(const orte_process_name_t *hnp);

END_C_DECLS
#endif

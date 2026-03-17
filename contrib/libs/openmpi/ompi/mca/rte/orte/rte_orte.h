/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * When this component is used, this file is included in the rest of
 * the OPAL/ORTE/OMPI code base via ompi/mca/rte/rte.h.  As such,
 * this header represents the public interface to this static component.
 */

#ifndef MCA_OMPI_RTE_ORTE_H
#define MCA_OMPI_RTE_ORTE_H

#include "ompi_config.h"
#include "ompi/constants.h"

struct opal_proc_t;

#include "opal/threads/threads.h"

#include "orte/types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/routed/routed.h"
#include "orte/runtime/orte_data_server.h"
#include "orte/runtime/runtime.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"

struct ompi_proc_t;
struct ompi_communicator_t;

BEGIN_C_DECLS

/* Process name objects and operations */
typedef orte_process_name_t ompi_process_name_t;
typedef orte_jobid_t ompi_jobid_t;
typedef orte_vpid_t ompi_vpid_t;
typedef orte_ns_cmp_bitmask_t ompi_rte_cmp_bitmask_t;
#define OMPI_PROC_MY_NAME ORTE_PROC_MY_NAME
#define OMPI_NAME_PRINT(a) ORTE_NAME_PRINT((const orte_process_name_t*)a)
#define ompi_rte_compare_name_fields(a, b, c) orte_util_compare_name_fields(a, (const orte_process_name_t*)(b), (const orte_process_name_t*)(c))
#define ompi_rte_convert_string_to_process_name(a,b) orte_util_convert_string_to_process_name(a,b)
#define ompi_rte_convert_process_name_to_string(a,b) orte_util_convert_process_name_to_string(a,b)
#define OMPI_NAME_WILDCARD  ORTE_NAME_WILDCARD
#define OMPI_NODE_RANK_INVALID ORTE_NODE_RANK_INVALID
#define OMPI_LOCAL_RANK_INVALID ORTE_LOCAL_RANK_INVALID
#define OMPI_RTE_CMP_JOBID  ORTE_NS_CMP_JOBID
#define OMPI_RTE_CMP_VPID   ORTE_NS_CMP_VPID
#define OMPI_RTE_CMP_ALL    ORTE_NS_CMP_ALL
#define OMPI_LOCAL_JOBID(jobid) ORTE_LOCAL_JOBID(jobid)
#define OMPI_JOB_FAMILY(jobid)  ORTE_JOB_FAMILY(jobid)
#define OMPI_CONSTRUCT_JOBID(family,local) ORTE_CONSTRUCT_JOBID(family,local)

/* This is the DSS tag to serialize a proc name */
#define OMPI_NAME ORTE_NAME
#define OMPI_PROCESS_NAME_HTON ORTE_PROCESS_NAME_HTON
#define OMPI_PROCESS_NAME_NTOH ORTE_PROCESS_NAME_NTOH

#if OPAL_ENABLE_DEBUG
static inline orte_process_name_t * OMPI_CAST_RTE_NAME(opal_process_name_t * name) {
    return (orte_process_name_t *)name;
}
#else
#define OMPI_CAST_RTE_NAME(a) ((orte_process_name_t*)(a))
#endif

/* Process info struct and values */
typedef orte_node_rank_t ompi_node_rank_t;
typedef orte_local_rank_t ompi_local_rank_t;
#define ompi_process_info orte_process_info
#define ompi_rte_proc_is_bound orte_proc_is_bound

/* Error handling objects and operations */
OMPI_DECLSPEC void __opal_attribute_noreturn__
  ompi_rte_abort(int error_code, char *fmt, ...);
#define ompi_rte_abort_peers(a, b, c) orte_errmgr.abort_peers(a, b, c)
#define OMPI_ERROR_LOG ORTE_ERROR_LOG

/* Init and finalize objects and operations */
#define ompi_rte_init(a, b) orte_init(a, b, ORTE_PROC_MPI)
#define ompi_rte_finalize() orte_finalize()
OMPI_DECLSPEC void ompi_rte_wait_for_debugger(void);

/* check dynamics support */
OMPI_DECLSPEC bool ompi_rte_connect_accept_support(const char *port);

#define ompi_proc_applied_binding orte_proc_applied_binding

END_C_DECLS

#endif /* MCA_OMPI_RTE_ORTE_H */

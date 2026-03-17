/*
 * Copyright (c) 2013      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2013      Inria.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_PROC_H
#define OPAL_PROC_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/types.h"
#include "opal/dss/dss.h"


#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#include <arpa/inet.h>
#endif

/**
 * This is a transparent handle proposed to the upper layer as a mean
 * to store whatever information it needs in order to efficiently
 * retrieve the RTE process naming scheme, and get access to the RTE
 * information associated with it. The only direct usage of this type
 * is to be copied from one structure to another, otherwise it should
 * only be used via the accessors defined below.
 */
#define OPAL_JOBID_T        OPAL_UINT32
#define OPAL_JOBID_MAX      UINT32_MAX-2
#define OPAL_JOBID_MIN      0
#define OPAL_JOBID_INVALID  (OPAL_JOBID_MAX + 2)
#define OPAL_JOBID_WILDCARD (OPAL_JOBID_MAX + 1)

#define OPAL_VPID_T         OPAL_UINT32
#define OPAL_VPID_MAX       UINT32_MAX-2
#define OPAL_VPID_MIN       0
#define OPAL_VPID_INVALID   (OPAL_VPID_MAX + 2)
#define OPAL_VPID_WILDCARD  (OPAL_VPID_MAX + 1)

#define OPAL_PROC_MY_NAME           (opal_proc_local_get()->proc_name)
#define OPAL_PROC_MY_HOSTNAME       (opal_proc_local_get()->proc_hostname)

#define OPAL_NAME_WILDCARD      (&opal_name_wildcard)
OPAL_DECLSPEC extern opal_process_name_t opal_name_wildcard;
#define OPAL_NAME_INVALID       (&opal_name_invalid)
OPAL_DECLSPEC extern opal_process_name_t opal_name_invalid;


#define OPAL_NAME_ARGS(n) \
    (unsigned long) ((NULL == n) ? (unsigned long)OPAL_JOBID_INVALID : (unsigned long)(n)->jobid), \
    (unsigned long) ((NULL == n) ? (unsigned long)OPAL_VPID_INVALID : (unsigned long)(n)->vpid) \

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT && !defined(WORDS_BIGENDIAN)
#define OPAL_PROCESS_NAME_NTOH(guid) opal_process_name_ntoh_intr(&(guid))
static inline __opal_attribute_always_inline__ void
opal_process_name_ntoh_intr(opal_process_name_t *name)
{
    name->jobid = ntohl(name->jobid);
    name->vpid = ntohl(name->vpid);
}
#define OPAL_PROCESS_NAME_HTON(guid) opal_process_name_hton_intr(&(guid))
static inline __opal_attribute_always_inline__ void
opal_process_name_hton_intr(opal_process_name_t *name)
{
    name->jobid = htonl(name->jobid);
    name->vpid = htonl(name->vpid);
}
#else
#define OPAL_PROCESS_NAME_NTOH(guid)
#define OPAL_PROCESS_NAME_HTON(guid)
#endif

typedef struct opal_proc_t {
    /** allow proc to be placed on a list */
    opal_list_item_t                super;
    /** this process' name */
    opal_process_name_t             proc_name;
    /** architecture of this process */
    uint32_t                        proc_arch;
    /** flags for this proc */
    opal_hwloc_locality_t           proc_flags;
    /** Base convertor for the proc described by this process */
    struct opal_convertor_t*        proc_convertor;
    /** A pointer to the name of this host - data is
     * actually stored outside of this framework.  */
    char*                           proc_hostname;
} opal_proc_t;
OBJ_CLASS_DECLARATION(opal_proc_t);

typedef struct {
    opal_list_item_t super;
    opal_process_name_t name;
} opal_namelist_t;
OBJ_CLASS_DECLARATION(opal_namelist_t);

typedef struct opal_process_info_t {
    char *nodename;                     /**< string name for this node */
    char *job_session_dir;              /**< Session directory for job */
    char *proc_session_dir;             /**< Session directory for the process */
    int32_t num_local_peers;            /**< number of procs from my job that share my node with me */
    int32_t my_local_rank;              /**< local rank on this node within my job */
    char *cpuset;                       /**< String-representation of bitmap where we are bound */
} opal_process_info_t;
OPAL_DECLSPEC extern opal_process_info_t opal_process_info;

OPAL_DECLSPEC extern opal_proc_t* opal_proc_local_get(void);
OPAL_DECLSPEC extern int opal_proc_local_set(opal_proc_t* proc);
OPAL_DECLSPEC extern void opal_proc_set_name(opal_process_name_t *name);

/**
 * Compare two processor name and return an integer greater than,
 * equal to, or less than 0, according as the proc_name of proc1
 * is greater than, equal to, or less than the proc_name of proc2.
 */
typedef int (*opal_compare_proc_fct_t)(const opal_process_name_t, const opal_process_name_t);
OPAL_DECLSPEC extern opal_compare_proc_fct_t opal_compare_proc;

/* Provide print functions that will be overwritten by the RTE layer */
OPAL_DECLSPEC extern char* (*opal_process_name_print)(const opal_process_name_t);
OPAL_DECLSPEC extern int (*opal_convert_string_to_process_name)(opal_process_name_t *name,
                                                                const char* name_string);
OPAL_DECLSPEC extern int (*opal_convert_process_name_to_string)(char** name_string,
                                                                const opal_process_name_t *name);
OPAL_DECLSPEC extern char* (*opal_vpid_print)(const opal_vpid_t);
OPAL_DECLSPEC extern char* (*opal_jobid_print)(const opal_jobid_t);
OPAL_DECLSPEC extern int (*opal_snprintf_jobid)(char* name_string, size_t size, opal_jobid_t jobid);
OPAL_DECLSPEC extern int (*opal_convert_string_to_jobid)(opal_jobid_t *jobid, const char *jobid_string);

/**
 * Lookup an opal_proc_t by name
 *
 * @param name (IN) name to lookup
 */
OPAL_DECLSPEC extern struct opal_proc_t *(*opal_proc_for_name) (const opal_process_name_t name);

#define OPAL_NAME_PRINT(OPAL_PN)    opal_process_name_print(OPAL_PN)
#define OPAL_JOBID_PRINT(OPAL_PN)   opal_jobid_print(OPAL_PN)
#define OPAL_VPID_PRINT(OPAL_PN)    opal_vpid_print(OPAL_PN)

/* provide a safe way to retrieve the hostname of a proc, including
 * our own. This is to be used by all BTLs so we don't retrieve hostnames
 * unless needed. The returned value MUST NOT be free'd as it is
 * owned by the proc_t */
OPAL_DECLSPEC char* opal_get_proc_hostname(const opal_proc_t *proc);

#endif  /* OPAL_PROC_H */

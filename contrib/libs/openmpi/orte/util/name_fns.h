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
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

#ifndef _ORTE_NAME_FNS_H_
#define _ORTE_NAME_FNS_H_

#include "orte_config.h"

#ifdef HAVE_STDINT_h
#include <stdint.h>
#endif

#include "orte/types.h"

#include "opal/class/opal_list.h"

BEGIN_C_DECLS

typedef uint8_t  orte_ns_cmp_bitmask_t;  /**< Bit mask for comparing process names */
#define ORTE_NS_CMP_NONE       0x00
#define ORTE_NS_CMP_JOBID      0x02
#define ORTE_NS_CMP_VPID       0x04
#define ORTE_NS_CMP_ALL        0x0f
#define ORTE_NS_CMP_WILD       0x10

/* useful define to print name args in output messages */
ORTE_DECLSPEC char* orte_util_print_name_args(const orte_process_name_t *name);
#define ORTE_NAME_PRINT(n) \
    orte_util_print_name_args(n)

ORTE_DECLSPEC char* orte_util_print_jobids(const orte_jobid_t job);
#define ORTE_JOBID_PRINT(n) \
    orte_util_print_jobids(n)

ORTE_DECLSPEC char* orte_util_print_vpids(const orte_vpid_t vpid);
#define ORTE_VPID_PRINT(n) \
    orte_util_print_vpids(n)

ORTE_DECLSPEC char* orte_util_print_job_family(const orte_jobid_t job);
#define ORTE_JOB_FAMILY_PRINT(n) \
    orte_util_print_job_family(n)

ORTE_DECLSPEC char* orte_util_print_local_jobid(const orte_jobid_t job);
#define ORTE_LOCAL_JOBID_PRINT(n) \
    orte_util_print_local_jobid(n)

ORTE_DECLSPEC char *orte_pretty_print_timing(int64_t secs, int64_t usecs);

/* a macro for identifying the job family - i.e., for
 * extracting the mpirun-specific id field of the jobid
 */
#define ORTE_JOB_FAMILY(n) \
    (((n) >> 16) & 0x0000ffff)

/* a macro for discovering the HNP name of a proc given its jobid */
#define ORTE_HNP_NAME_FROM_JOB(n, job)     \
    do {                                   \
        (n)->jobid = (job) & 0xffff0000;   \
        (n)->vpid = 0;                     \
    } while(0);

/* a macro for extracting the local jobid from the jobid - i.e.,
 * the non-mpirun-specific id field of the jobid
 */
#define ORTE_LOCAL_JOBID(n) \
    ( (n) & 0x0000ffff)

#define ORTE_CONSTRUCT_JOB_FAMILY(n) \
    ( ((n) << 16) & 0xffff0000)

#define ORTE_CONSTRUCT_LOCAL_JOBID(local, job) \
    ( ((local) & 0xffff0000) | ((job) & 0x0000ffff) )

#define ORTE_CONSTRUCT_JOBID(family, local) \
    ORTE_CONSTRUCT_LOCAL_JOBID(ORTE_CONSTRUCT_JOB_FAMILY(family), local)

/* a macro for identifying that a proc is a daemon */
#define ORTE_JOBID_IS_DAEMON(n)  \
    !((n) & 0x0000ffff)

/* a macro for obtaining the daemon jobid */
#define ORTE_DAEMON_JOBID(n) \
    ((n) & 0xffff0000)

/* List of names for general use */
struct orte_namelist_t {
    opal_list_item_t super;      /**< Allows this item to be placed on a list */
    orte_process_name_t name;   /**< Name of a process */
};
typedef struct orte_namelist_t orte_namelist_t;

ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_namelist_t);

ORTE_DECLSPEC int orte_util_snprintf_jobid(char *jobid_string, size_t size, const orte_jobid_t jobid);
ORTE_DECLSPEC int orte_util_convert_jobid_to_string(char **jobid_string, const orte_jobid_t jobid);
ORTE_DECLSPEC int orte_util_convert_string_to_jobid(orte_jobid_t *jobid, const char* jobidstring);
ORTE_DECLSPEC int orte_util_convert_vpid_to_string(char **vpid_string, const orte_vpid_t vpid);
ORTE_DECLSPEC int orte_util_convert_string_to_vpid(orte_vpid_t *vpid, const char* vpidstring);
ORTE_DECLSPEC int orte_util_convert_string_to_process_name(orte_process_name_t *name,
                                             const char* name_string);
ORTE_DECLSPEC int orte_util_convert_process_name_to_string(char** name_string,
                                             const orte_process_name_t *name);
ORTE_DECLSPEC int orte_util_create_process_name(orte_process_name_t **name,
                                  orte_jobid_t job,
                                  orte_vpid_t vpid);

ORTE_DECLSPEC int orte_util_compare_name_fields(orte_ns_cmp_bitmask_t fields,
                                  const orte_process_name_t* name1,
                                  const orte_process_name_t* name2);
/** This funtion returns a guaranteed unique hash value for the passed process name */
ORTE_DECLSPEC uint32_t orte_util_hash_vpid(orte_vpid_t vpid);
ORTE_DECLSPEC int orte_util_convert_string_to_sysinfo(char **cpu_type, char **cpu_model,
                                             const char* sysinfo_string);
ORTE_DECLSPEC int orte_util_convert_sysinfo_to_string(char** sysinfo_string,
						      const char *cpu_model, const char *cpu_type);

END_C_DECLS
#endif

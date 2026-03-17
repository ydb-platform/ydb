/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#ifndef ORTE_TYPES_H
#define ORTE_TYPES_H

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include "opal/dss/dss_types.h"
#include "opal/util/proc.h"

/**
 * Supported datatypes for messaging and storage operations.
 */

typedef int32_t orte_std_cntr_t;  /** standard counters used in ORTE */
#define ORTE_STD_CNTR_T         OPAL_INT32
#define ORTE_STD_CNTR_MAX       INT32_MAX
#define ORTE_STD_CNTR_MIN       INT32_MIN
#define ORTE_STD_CNTR_INVALID   -1

/** rank on node, used for both local and node rank. We
 * don't send these around on their own, so don't create
 * dedicated type support for them - we are defining them
 * here solely for readability in the code and so we have
 * one place where any future changes can be made
 */
typedef uint16_t orte_local_rank_t;
typedef uint16_t orte_node_rank_t;
#define ORTE_LOCAL_RANK         OPAL_UINT16
#define ORTE_NODE_RANK          OPAL_UINT16
#define ORTE_LOCAL_RANK_MAX     UINT16_MAX-1
#define ORTE_NODE_RANK_MAX      UINT16_MAX-1
#define ORTE_LOCAL_RANK_INVALID UINT16_MAX
#define ORTE_NODE_RANK_INVALID  UINT16_MAX

/* index for app_contexts */
typedef uint32_t orte_app_idx_t;
#define ORTE_APP_IDX        OPAL_UINT32
#define ORTE_APP_IDX_MAX    UINT32_MAX

/* general typedefs & structures */

/* Equate the ORTE jobid/vpid types and their associated values/structs
 * to their OPAL equivalents
 */

typedef opal_jobid_t orte_jobid_t;
#define ORTE_JOBID_T        OPAL_JOBID_T
#define ORTE_JOBID_MAX      OPAL_JOBID_MAX
#define ORTE_JOBID_MIN      OPAL_JOBID_MIN
typedef opal_vpid_t orte_vpid_t;
#define ORTE_VPID_T         OPAL_VPID_T
#define ORTE_VPID_MAX       OPAL_VPID_MAX
#define ORTE_VPID_MIN       OPAL_VPID_MIX

#define ORTE_PROCESS_NAME_HTON(n) OPAL_PROCESS_NAME_HTON(n)

#define ORTE_PROCESS_NAME_NTOH(n) OPAL_PROCESS_NAME_NTOH(n)

#define ORTE_NAME_ARGS(n) \
    (unsigned long) ((NULL == n) ? (unsigned long)ORTE_JOBID_INVALID : (unsigned long)(n)->jobid), \
    (unsigned long) ((NULL == n) ? (unsigned long)ORTE_VPID_INVALID : (unsigned long)(n)->vpid) \

/*
 * define invalid values
 */
#define ORTE_JOBID_INVALID          OPAL_JOBID_INVALID
#define ORTE_VPID_INVALID           OPAL_VPID_INVALID
#define ORTE_LOCAL_JOBID_INVALID    (ORTE_JOBID_INVALID & 0x0000FFFF)

/*
 * define wildcard values
 */
#define ORTE_JOBID_WILDCARD         OPAL_JOBID_WILDCARD
#define ORTE_VPID_WILDCARD          OPAL_VPID_WILDCARD
#define ORTE_LOCAL_JOBID_WILDCARD   (ORTE_JOBID_WILDCARD & 0x0000FFFF)

/*
 * define the process name structure
 */
typedef opal_process_name_t orte_process_name_t;


/**
 * handle differences in iovec
 */

#if defined(__APPLE__)
typedef char* orte_iov_base_ptr_t;
#else
typedef void* orte_iov_base_ptr_t;
#endif

/* ORTE attribute */
typedef uint16_t orte_attribute_key_t;
#define ORTE_ATTR_KEY_T   OPAL_UINT16
typedef struct {
    opal_list_item_t super;             /* required for this to be on lists */
    orte_attribute_key_t key;           /* key identifier */
    opal_data_type_t type;              /* the type of value stored */
    bool local;                         // whether or not to pack/send this value
    union {
        bool flag;
        uint8_t byte;
        char *string;
        size_t size;
        pid_t pid;
        int integer;
        int8_t int8;
        int16_t int16;
        int32_t int32;
        int64_t int64;
        unsigned int uint;
        uint8_t uint8;
        uint16_t uint16;
        uint32_t uint32;
        uint64_t uint64;
        opal_byte_object_t bo;
        opal_buffer_t buf;
        float fval;
        struct timeval tv;
        void *ptr;  // never packed or passed anywhere
        orte_vpid_t vpid;
        orte_jobid_t jobid;
        opal_process_name_t name;
        opal_envar_t envar;
    } data;
} orte_attribute_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(orte_attribute_t);


/* Name-related types - equate to OPAL equivalents*/
#define    ORTE_NAME                OPAL_NAME   /**< an orte_process_name_t */
#define    ORTE_VPID                OPAL_VPID   /**< a vpid */
#define    ORTE_JOBID               OPAL_JOBID  /**< a jobid */

/* General ORTE types - support handled within DSS */
#define    ORTE_STD_CNTR            (OPAL_DSS_ID_DYNAMIC + 1)  /**< standard counter type */
    /* State-related types */
#define    ORTE_NODE_STATE          (OPAL_DSS_ID_DYNAMIC + 2)  /**< node status flag */
#define    ORTE_PROC_STATE          (OPAL_DSS_ID_DYNAMIC + 3)  /**< process/resource status */
#define    ORTE_JOB_STATE           (OPAL_DSS_ID_DYNAMIC + 4)  /**< job status flag */
#define    ORTE_EXIT_CODE           (OPAL_DSS_ID_DYNAMIC + 5)  /**< process exit code */
    /* Data-passing types */
#define    ORTE_VALUE               (OPAL_DSS_ID_DYNAMIC + 6)  /**< registry return value */
    /* Resource types */
#define    ORTE_APP_CONTEXT         (OPAL_DSS_ID_DYNAMIC + 7) /**< argv and enviro arrays */
#define    ORTE_NODE_DESC           (OPAL_DSS_ID_DYNAMIC + 8) /**< describes capabilities of nodes */
#define    ORTE_SLOT_DESC           (OPAL_DSS_ID_DYNAMIC + 9) /**< describes slot allocations/reservations */
#define    ORTE_JOB                 (OPAL_DSS_ID_DYNAMIC + 10) /**< job information */
#define    ORTE_NODE                (OPAL_DSS_ID_DYNAMIC + 11) /**< node information */
#define    ORTE_PROC                (OPAL_DSS_ID_DYNAMIC + 12) /**< process information */
#define    ORTE_JOB_MAP             (OPAL_DSS_ID_DYNAMIC + 13) /**< map of process locations */

/* RML types */
#define    ORTE_RML_TAG             (OPAL_DSS_ID_DYNAMIC + 14) /**< tag for sending/receiving messages */
/* DAEMON command type */
#define    ORTE_DAEMON_CMD          (OPAL_DSS_ID_DYNAMIC + 15) /**< command flag for communicating with the daemon */

/* IOF types */
#define    ORTE_IOF_TAG             (OPAL_DSS_ID_DYNAMIC + 16)

/* Attribute */
#define    ORTE_ATTRIBUTE           (OPAL_DSS_ID_DYNAMIC + 17)
/* Grpcomm signature */
#define    ORTE_SIGNATURE           (OPAL_DSS_ID_DYNAMIC + 18)

/* provide a boundary for others to use */
#define    ORTE_DSS_ID_DYNAMIC      (OPAL_DSS_ID_DYNAMIC + 50)

#endif

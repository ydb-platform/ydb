/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2009-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * Contains the typedefs for the use of the rml
 */

#ifndef MCA_RML_TYPES_H_
#define MCA_RML_TYPES_H_

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include <limits.h>
#ifdef HAVE_SYS_UIO_H
/* for struct iovec */
#include <sys/uio.h>
#endif
#ifdef HAVE_NET_UIO_H
#include <net/uio.h>
#endif

#include "opal/dss/dss_types.h"
#include "opal/class/opal_list.h"

BEGIN_C_DECLS

/* Convenience def for readability */
#define ORTE_RML_PERSISTENT      true
#define ORTE_RML_NON_PERSISTENT  false


/**
 * Constant tag values for well-known services
 */

#define ORTE_RML_TAG_T    OPAL_UINT32

#define ORTE_RML_TAG_INVALID                 0
#define ORTE_RML_TAG_DAEMON                  1
#define ORTE_RML_TAG_IOF_HNP                 2
#define ORTE_RML_TAG_IOF_PROXY               3
#define ORTE_RML_TAG_XCAST_BARRIER           4
#define ORTE_RML_TAG_PLM                     5
#define ORTE_RML_TAG_LAUNCH_RESP             6
#define ORTE_RML_TAG_ERRMGR                  7
#define ORTE_RML_TAG_WIREUP                  8
#define ORTE_RML_TAG_RML_INFO_UPDATE         9
#define ORTE_RML_TAG_ORTED_CALLBACK         10
#define ORTE_RML_TAG_ROLLUP                 11
#define ORTE_RML_TAG_REPORT_REMOTE_LAUNCH   12

#define ORTE_RML_TAG_CKPT                   13

#define ORTE_RML_TAG_RML_ROUTE              14
#define ORTE_RML_TAG_XCAST                  15

#define ORTE_RML_TAG_UPDATE_ROUTE_ACK       19
#define ORTE_RML_TAG_SYNC                   20

/* For FileM Base */
#define ORTE_RML_TAG_FILEM_BASE             21
#define ORTE_RML_TAG_FILEM_BASE_RESP        22

/* For FileM RSH Component */
#define ORTE_RML_TAG_FILEM_RSH              23

/* For SnapC Framework */
#define ORTE_RML_TAG_SNAPC                  24
#define ORTE_RML_TAG_SNAPC_FULL             25

/* For tools */
#define ORTE_RML_TAG_TOOL                   26

/* support data store/lookup */
#define ORTE_RML_TAG_DATA_SERVER            27
#define ORTE_RML_TAG_DATA_CLIENT            28

/* timing related */
#define ORTE_RML_TAG_COLLECTIVE_TIMER       29

/* collectives */
#define ORTE_RML_TAG_COLLECTIVE             30
#define ORTE_RML_TAG_COLL_RELEASE           31
#define ORTE_RML_TAG_DAEMON_COLL            32
#define ORTE_RML_TAG_ALLGATHER_DIRECT       33
#define ORTE_RML_TAG_ALLGATHER_BRUCKS       34
#define ORTE_RML_TAG_ALLGATHER_RCD          35

/* show help */
#define ORTE_RML_TAG_SHOW_HELP              36

/* debugger release */
#define ORTE_RML_TAG_DEBUGGER_RELEASE       37

/* bootstrap */
#define ORTE_RML_TAG_BOOTSTRAP              38

/* report a missed msg */
#define ORTE_RML_TAG_MISSED_MSG             39

/* tag for receiving ack of abort msg */
#define ORTE_RML_TAG_ABORT                  40

/* tag for receiving heartbeats */
#define ORTE_RML_TAG_HEARTBEAT              41

/* Process Migration Tool Tag */
#define ORTE_RML_TAG_MIGRATE                42

/* For SStore Framework */
#define ORTE_RML_TAG_SSTORE                 43
#define ORTE_RML_TAG_SSTORE_INTERNAL        44

#define ORTE_RML_TAG_SUBSCRIBE              45


/* Notify of failed processes */
#define ORTE_RML_TAG_FAILURE_NOTICE         46

/* distributed file system */
#define ORTE_RML_TAG_DFS_CMD                47
#define ORTE_RML_TAG_DFS_DATA               48

/* sensor data */
#define ORTE_RML_TAG_SENSOR_DATA            49

/* direct modex support */
#define ORTE_RML_TAG_DIRECT_MODEX           50
#define ORTE_RML_TAG_DIRECT_MODEX_RESP      51

/* notifier support */
#define ORTE_RML_TAG_NOTIFIER_HNP           52
#define ORTE_RML_TAG_NOTIFY_COMPLETE        53

/*** QOS specific  RML TAGS ***/
#define ORTE_RML_TAG_OPEN_CHANNEL_REQ       54
#define ORTE_RML_TAG_OPEN_CHANNEL_RESP      55
#define ORTE_RML_TAG_MSG_ACK                56
#define ORTE_RML_TAG_CLOSE_CHANNEL_REQ      57
#define ORTE_RML_TAG_CLOSE_CHANNEL_ACCEPT   58

/* error notifications */
#define ORTE_RML_TAG_NOTIFICATION           59

/* stacktrace for debug */
#define ORTE_RML_TAG_STACK_TRACE            60

/* memory profile */
#define ORTE_RML_TAG_MEMPROFILE             61

/* topology report */
#define ORTE_RML_TAG_TOPOLOGY_REPORT        62

/* warmup connection - simply establishes the connection */
#define ORTE_RML_TAG_WARMUP_CONNECTION      63

/* node regex report */
#define ORTE_RML_TAG_NODE_REGEX_REPORT      64

#define ORTE_RML_TAG_MAX                   100


#define ORTE_RML_TAG_NTOH(t) ntohl(t)
#define ORTE_RML_TAG_HTON(t) htonl(t)

/*** length of the tag. change this when type of orte_rml_tag_t is changed ***/
/*** max valu in unit32_t is 0xFFFF_FFFF when converted to char this is 8  **
#define ORTE_RML_TAG_T_CHAR_LEN   8
#define ORTE_RML_TAG_T_SPRINT    "%8x" */

/**
 * Message matching tag
 *
 * Message matching tag.  Unlike MPI, there is no wildcard receive,
 * all messages must match exactly. Tag values less than
 * ORTE_RML_TAG_DYNAMIC are reserved and may only be referenced using
 * a defined constant.
 */
typedef uint32_t orte_rml_tag_t;

/* Conduit ID */
typedef uint16_t orte_rml_conduit_t;
#define ORTE_RML_CONDUIT_INVALID  0xff

/* define an object for reporting transports */
typedef struct {
    opal_list_item_t super;
    char *component;
    opal_list_t attributes;
    opal_list_t transports;
} orte_rml_pathway_t;
OBJ_CLASS_DECLARATION(orte_rml_pathway_t);

/* ******************************************************************** */


/*
 * RML proxy commands
 */
typedef uint8_t orte_rml_cmd_flag_t;
#define ORTE_RML_CMD    OPAL_UINT8
#define ORTE_RML_UPDATE_CMD    1


typedef enum {
    ORTE_RML_PEER_UNREACH,
    ORTE_RML_PEER_DISCONNECTED
} orte_rml_exception_t;


END_C_DECLS


#endif  /* RML_TYPES */

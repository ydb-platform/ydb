/* -*- C -*-
 *
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc. All rights
 *                         reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Buffer management types.
 */

#ifndef OPAL_DSS_TYPES_H_
#define OPAL_DSS_TYPES_H_

#include "opal_config.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif

#include "opal/class/opal_object.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_list.h"

typedef uint32_t opal_jobid_t;
typedef uint32_t opal_vpid_t;
typedef struct {
    opal_jobid_t jobid;
    opal_vpid_t vpid;
} opal_process_name_t;
#define OPAL_SIZEOF_PROCESS_NAME_T 8

BEGIN_C_DECLS

typedef uint8_t opal_data_type_t;  /** data type indicators */
#define OPAL_DATA_TYPE_T    OPAL_UINT8
#define OPAL_DSS_ID_MAX     UINT8_MAX
#define OPAL_DSS_ID_INVALID OPAL_DSS_ID_MAX

/* define a structure to hold generic byte objects */
typedef struct {
    int32_t size;
    uint8_t *bytes;
} opal_byte_object_t;

/* Type defines for packing and unpacking */
#define    OPAL_UNDEF               (opal_data_type_t)    0 /**< type hasn't been defined yet */
#define    OPAL_BYTE                (opal_data_type_t)    1 /**< a byte of data */
#define    OPAL_BOOL                (opal_data_type_t)    2 /**< boolean */
#define    OPAL_STRING              (opal_data_type_t)    3 /**< a NULL terminated string */
#define    OPAL_SIZE                (opal_data_type_t)    4 /**< the generic size_t */
#define    OPAL_PID                 (opal_data_type_t)    5 /**< process pid */
    /* all the integer flavors */
#define    OPAL_INT                 (opal_data_type_t)    6 /**< generic integer */
#define    OPAL_INT8                (opal_data_type_t)    7 /**< an 8-bit integer */
#define    OPAL_INT16               (opal_data_type_t)    8 /**< a 16-bit integer */
#define    OPAL_INT32               (opal_data_type_t)    9 /**< a 32-bit integer */
#define    OPAL_INT64               (opal_data_type_t)   10 /**< a 64-bit integer */
    /* all the unsigned integer flavors */
#define    OPAL_UINT                (opal_data_type_t)   11 /**< generic unsigned integer */
#define    OPAL_UINT8               (opal_data_type_t)   12 /**< an 8-bit unsigned integer */
#define    OPAL_UINT16              (opal_data_type_t)   13 /**< a 16-bit unsigned integer */
#define    OPAL_UINT32              (opal_data_type_t)   14 /**< a 32-bit unsigned integer */
#define    OPAL_UINT64              (opal_data_type_t)   15 /**< a 64-bit unsigned integer */
    /* floating point types */
#define    OPAL_FLOAT               (opal_data_type_t)   16
#define    OPAL_DOUBLE              (opal_data_type_t)   17
    /* system types */
#define    OPAL_TIMEVAL             (opal_data_type_t)   18
#define    OPAL_TIME                (opal_data_type_t)   19
    /* OPAL types */
#define    OPAL_BYTE_OBJECT         (opal_data_type_t)   20 /**< byte object structure */
#define    OPAL_DATA_TYPE           (opal_data_type_t)   21 /**< data type */
#define    OPAL_NULL                (opal_data_type_t)   22 /**< don't interpret data type */
#define    OPAL_PSTAT               (opal_data_type_t)   23 /**< process statistics */
#define    OPAL_NODE_STAT           (opal_data_type_t)   24 /**< node statistics */
#define    OPAL_HWLOC_TOPO          (opal_data_type_t)   25 /**< hwloc topology */
#define    OPAL_VALUE               (opal_data_type_t)   26 /**< opal value structure */
#define    OPAL_BUFFER              (opal_data_type_t)   27 /**< pack the remaining contents of a buffer as an object */
#define    OPAL_PTR                 (opal_data_type_t)   28 /**< pointer to void* */
#define    OPAL_NAME                (opal_data_type_t)   29
#define    OPAL_JOBID               (opal_data_type_t)   30
#define    OPAL_VPID                (opal_data_type_t)   31
#define    OPAL_STATUS              (opal_data_type_t)   32
#define    OPAL_PERSIST             (opal_data_type_t)   33 /**< corresponds to PMIx persistence type (uint8_t) */
#define    OPAL_SCOPE               (opal_data_type_t)   34 /**< corresponds to PMIx scope type (uint8_t) */
#define    OPAL_DATA_RANGE          (opal_data_type_t)   35 /**< corresponds to PMIx data range type (uint8_t) */
#define    OPAL_INFO_DIRECTIVES     (opal_data_type_t)   36 /**< corresponds to PMIx info directives type (uint32_t) */
#define    OPAL_PROC_STATE          (opal_data_type_t)   37 /**< corresponds to PMIx proc state type (uint8_t) */
#define    OPAL_PROC_INFO           (opal_data_type_t)   38 /**< corresponds to PMIx proc_info type */
#define    OPAL_ENVAR               (opal_data_type_t)   39 /**< corresponds to PMIx envar type */

    /* OPAL Dynamic */
#define    OPAL_DSS_ID_DYNAMIC      (opal_data_type_t)  100

/* define the results values for comparisons so we can change them in only one place */
#define OPAL_VALUE1_GREATER  +1
#define OPAL_VALUE2_GREATER  -1
#define OPAL_EQUAL            0

/* define some types so we can store the generic
 * values and still *know* how to convert it for PMIx */
typedef int opal_status_t;
typedef uint32_t opal_proc_state_t;

/* define an opal_proc_info_t for transferring info to/from PMIx */
typedef struct {
    opal_list_item_t super;
    opal_process_name_t name;
    char *hostname;
    char *executable_name;
    pid_t pid;
    opal_status_t exit_code;
    opal_proc_state_t state;
} opal_proc_info_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_proc_info_t);

/* defaine a struct for envar directives */
typedef struct {
    opal_list_item_t super;
    char *envar;
    char *value;
    char separator;
} opal_envar_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_envar_t);

/* Data value object */
typedef struct {
    opal_list_item_t super;             /* required for this to be on lists */
    char *key;                          /* key string */
    opal_data_type_t type;              /* the type of value stored */
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
        float fval;
        double dval;
        struct timeval tv;
        time_t time;
        opal_status_t status;
        opal_process_name_t name;
        opal_proc_info_t pinfo;
        void *ptr;  // never packed or passed anywhere
        opal_envar_t envar;
    } data;
} opal_value_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_value_t);

/* Process statistics object */
#define OPAL_PSTAT_MAX_STRING_LEN   32
typedef struct {
    opal_list_item_t super;                /* required for this to be on a list */
    /* process ident info */
    char node[OPAL_PSTAT_MAX_STRING_LEN];
    int32_t rank;
    pid_t pid;
    char cmd[OPAL_PSTAT_MAX_STRING_LEN];
    /* process stats */
    char state[2];
    struct timeval time;
    float percent_cpu;
    int32_t priority;
    int16_t num_threads;
    float pss;   /* in MBytes */
    float vsize;  /* in MBytes */
    float rss;  /* in MBytes */
    float peak_vsize;  /* in MBytes */
    int16_t processor;
    /* time at which sample was taken */
    struct timeval sample_time;
} opal_pstats_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_pstats_t);
typedef struct {
    opal_list_item_t super;
    char *disk;
    unsigned long num_reads_completed;
    unsigned long num_reads_merged;
    unsigned long num_sectors_read;
    unsigned long milliseconds_reading;
    unsigned long num_writes_completed;
    unsigned long num_writes_merged;
    unsigned long num_sectors_written;
    unsigned long milliseconds_writing;
    unsigned long num_ios_in_progress;
    unsigned long milliseconds_io;
    unsigned long weighted_milliseconds_io;
} opal_diskstats_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_diskstats_t);
typedef struct {
    opal_list_item_t super;
    char *net_interface;
    unsigned long num_bytes_recvd;
    unsigned long num_packets_recvd;
    unsigned long num_recv_errs;
    unsigned long num_bytes_sent;
    unsigned long num_packets_sent;
    unsigned long num_send_errs;
} opal_netstats_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_netstats_t);
typedef struct {
    opal_object_t super;
    /* node-level load averages */
    float la;
    float la5;
    float la15;
    /* memory usage */
    float total_mem;  /* in MBytes */
    float free_mem;  /* in MBytes */
    float buffers;  /* in MBytes */
    float cached;   /* in MBytes */
    float swap_cached;  /* in MBytes */
    float swap_total;   /* in MBytes */
    float swap_free;    /* in MBytes */
    float mapped;       /* in MBytes */
    /* time at which sample was taken */
    struct timeval sample_time;
    /* list of disk stats, one per disk */
    opal_list_t diskstats;
    /* list of net stats, one per interface */
    opal_list_t netstats;

} opal_node_stats_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_node_stats_t);

/* structured-unstructured data flags */
#define OPAL_DSS_STRUCTURED     true
#define OPAL_DSS_UNSTRUCTURED   false

/**
 * buffer type
 */
enum opal_dss_buffer_type_t {
    OPAL_DSS_BUFFER_NON_DESC   = 0x00,
    OPAL_DSS_BUFFER_FULLY_DESC = 0x01
};

typedef enum opal_dss_buffer_type_t opal_dss_buffer_type_t;

#define OPAL_DSS_BUFFER_TYPE_HTON(h);
#define OPAL_DSS_BUFFER_TYPE_NTOH(h);

/**
 * Structure for holding a buffer to be used with the RML or OOB
 * subsystems.
 */
struct opal_buffer_t {
    /** First member must be the object's parent */
    opal_object_t parent;
    /** type of buffer */
    opal_dss_buffer_type_t type;
    /** Start of my memory */
    char *base_ptr;
    /** Where the next data will be packed to (within the allocated
        memory starting at base_ptr) */
    char *pack_ptr;
    /** Where the next data will be unpacked from (within the
        allocated memory starting as base_ptr) */
    char *unpack_ptr;

    /** Number of bytes allocated (starting at base_ptr) */
    size_t bytes_allocated;
    /** Number of bytes used by the buffer (i.e., amount of data --
        including overhead -- packed in the buffer) */
    size_t bytes_used;
};
/**
 * Convenience typedef
 */
typedef struct opal_buffer_t opal_buffer_t;

/** formalize the declaration */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION (opal_buffer_t);

END_C_DECLS

#endif /* OPAL_DSS_TYPES_H */

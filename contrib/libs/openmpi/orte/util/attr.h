/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_ATTRS_H
#define ORTE_ATTRS_H

#include "orte_config.h"
#include "orte/types.h"

/*** FLAG FOR SETTING ATTRIBUTES - INDICATES IF THE
 *** ATTRIBUTE IS TO BE SHARED WITH REMOTE PROCS OR NOT
 */
#define ORTE_ATTR_LOCAL    true      // for local use only
#define ORTE_ATTR_GLOBAL   false     // include when sending this object

/* define the mininum value of the ORTE keys just in
 * case someone someday puts a layer underneath us */
#define ORTE_ATTR_KEY_BASE        0

/*** ATTRIBUTE FLAGS - never sent anywwhere ***/
typedef uint8_t orte_app_context_flags_t;
#define ORTE_APP_FLAG_USED_ON_NODE  0x01  // is being used on the local node


/* APP_CONTEXT ATTRIBUTE KEYS */
#define ORTE_APP_HOSTFILE            1    // string  - hostfile
#define ORTE_APP_ADD_HOSTFILE        2    // string  - hostfile to be added
#define ORTE_APP_DASH_HOST           3    // string  - hosts specified with -host option
#define ORTE_APP_ADD_HOST            4    // string  - hosts to be added
#define ORTE_APP_USER_CWD            5    // bool  - user specified cwd
#define ORTE_APP_SSNDIR_CWD          6    // bool  - use session dir as cwd
#define ORTE_APP_PRELOAD_BIN         7    // bool  - move binaries to remote nodes prior to exec
#define ORTE_APP_PRELOAD_FILES       8    // string  - files to be moved to remote nodes prior to exec
#define ORTE_APP_SSTORE_LOAD         9    // string
#define ORTE_APP_RECOV_DEF          10    // bool  - whether or not a recovery policy was defined
#define ORTE_APP_MAX_RESTARTS       11    // int32 - max number of times a process can be restarted
#define ORTE_APP_MIN_NODES          12    // int64 - min number of nodes required
#define ORTE_APP_MANDATORY          13    // bool - flag if nodes requested in -host are "mandatory" vs "optional"
#define ORTE_APP_MAX_PPN            14    // uint32 - maximum number of procs/node for this app
#define ORTE_APP_PREFIX_DIR         15    // string - prefix directory for this app, if override necessary
#define ORTE_APP_NO_CACHEDIR        16    // bool - flag that a cache dir is not to be specified for a Singularity container
#define ORTE_APP_SET_ENVAR          17    // opal_envar_t - set the given envar to the specified value
#define ORTE_APP_UNSET_ENVAR        18    // string - name of envar to unset, if present
#define ORTE_APP_PREPEND_ENVAR      19    // opal_envar_t - prepend the specified value to the given envar
#define ORTE_APP_APPEND_ENVAR       20    // opal_envar_t - append the specified value to the given envar
#define ORTE_APP_ADD_ENVAR          21    // opal_envar_t - add envar, do not override pre-existing one

#define ORTE_APP_MAX_KEY        100


/*** NODE FLAGS - never sent anywhere ***/
typedef uint8_t orte_node_flags_t;
#define ORTE_NODE_FLAG_DAEMON_LAUNCHED    0x01   // whether or not the daemon on this node has been launched
#define ORTE_NODE_FLAG_LOC_VERIFIED       0x02   // whether or not the location has been verified - used for
                                                 // environments where the daemon's final destination is uncertain
#define ORTE_NODE_FLAG_OVERSUBSCRIBED     0x04   // whether or not this node is oversubscribed
#define ORTE_NODE_FLAG_MAPPED             0x08   // whether we have been added to the current map
#define ORTE_NODE_FLAG_SLOTS_GIVEN        0x10   // the number of slots was specified - used only in non-managed environments
#define ORTE_NODE_NON_USABLE              0x20   // the node is hosting a tool and is NOT to be used for jobs


/*** NODE ATTRIBUTE KEYS - never sent anywhere ***/
#define ORTE_NODE_START_KEY    ORTE_APP_MAX_KEY

#define ORTE_NODE_USERNAME       (ORTE_NODE_START_KEY + 1)
#define ORTE_NODE_LAUNCH_ID      (ORTE_NODE_START_KEY + 2)   // int32 - Launch id needed by some systems to launch a proc on this node
#define ORTE_NODE_HOSTID         (ORTE_NODE_START_KEY + 3)   // orte_vpid_t - if this "node" is a coprocessor being hosted on a different node, then
                                                             // we need to know the id of our "host" to help any procs on us to determine locality
#define ORTE_NODE_ALIAS          (ORTE_NODE_START_KEY + 4)   // comma-separate list of alternate names for the node
#define ORTE_NODE_SERIAL_NUMBER  (ORTE_NODE_START_KEY + 5)   // string - serial number: used if node is a coprocessor
#define ORTE_NODE_PORT           (ORTE_NODE_START_KEY + 6)   // int32 - Alternate port to be passed to plm

#define ORTE_NODE_MAX_KEY        200

/*** JOB FLAGS - included in orte_job_t transmissions ***/
typedef uint16_t orte_job_flags_t;
#define ORTE_JOB_FLAGS_T  OPAL_UINT16
#define ORTE_JOB_FLAG_UPDATED            0x0001   // job has been updated and needs to be included in the pidmap message
#define ORTE_JOB_FLAG_RESTARTED          0x0004   // some procs in this job are being restarted
#define ORTE_JOB_FLAG_ABORTED            0x0008   // did this job abort?
#define ORTE_JOB_FLAG_DEBUGGER_DAEMON    0x0010   // job is launching debugger daemons
#define ORTE_JOB_FLAG_FORWARD_OUTPUT     0x0020   // forward output from the apps
#define ORTE_JOB_FLAG_DO_NOT_MONITOR     0x0040   // do not monitor apps for termination
#define ORTE_JOB_FLAG_FORWARD_COMM       0x0080   //
#define ORTE_JOB_FLAG_RECOVERABLE        0x0100   // job is recoverable
#define ORTE_JOB_FLAG_RESTART            0x0200   //
#define ORTE_JOB_FLAG_PROCS_MIGRATING    0x0400   // some procs in job are migrating from one node to another
#define ORTE_JOB_FLAG_OVERSUBSCRIBED     0x0800   // at least one node in the job is oversubscribed

/***   JOB ATTRIBUTE KEYS   ***/
#define ORTE_JOB_START_KEY   ORTE_NODE_MAX_KEY

#define ORTE_JOB_LAUNCH_MSG_SENT        (ORTE_JOB_START_KEY + 1)     // timeval - time launch message was sent
#define ORTE_JOB_LAUNCH_MSG_RECVD       (ORTE_JOB_START_KEY + 2)     // timeval - time launch message was recvd
#define ORTE_JOB_MAX_LAUNCH_MSG_RECVD   (ORTE_JOB_START_KEY + 3)     // timeval - max time for launch msg to be received
#define ORTE_JOB_FILE_MAPS              (ORTE_JOB_START_KEY + 4)     // opal_buffer_t - file maps associates with this job
#define ORTE_JOB_CKPT_STATE             (ORTE_JOB_START_KEY + 5)     // size_t - ckpt state
#define ORTE_JOB_SNAPSHOT_REF           (ORTE_JOB_START_KEY + 6)     // string - snapshot reference
#define ORTE_JOB_SNAPSHOT_LOC           (ORTE_JOB_START_KEY + 7)     // string - snapshot location
#define ORTE_JOB_SNAPC_INIT_BAR         (ORTE_JOB_START_KEY + 8)     // orte_grpcomm_coll_id_t - collective id
#define ORTE_JOB_SNAPC_FINI_BAR         (ORTE_JOB_START_KEY + 9)     // orte_grpcomm_coll_id_t - collective id
#define ORTE_JOB_NUM_NONZERO_EXIT       (ORTE_JOB_START_KEY + 10)    // int32 - number of procs with non-zero exit codes
#define ORTE_JOB_FAILURE_TIMER_EVENT    (ORTE_JOB_START_KEY + 11)    // opal_ptr (orte_timer_t*) - timer event for failure detect/response if fails to launch
#define ORTE_JOB_ABORTED_PROC           (ORTE_JOB_START_KEY + 12)    // opal_ptr (orte_proc_t*) - proc that caused abort to happen
#define ORTE_JOB_MAPPER                 (ORTE_JOB_START_KEY + 13)    // bool - job consists of MapReduce mappers
#define ORTE_JOB_REDUCER                (ORTE_JOB_START_KEY + 14)    // bool - job consists of MapReduce reducers
#define ORTE_JOB_COMBINER               (ORTE_JOB_START_KEY + 15)    // bool - job consists of MapReduce combiners
#define ORTE_JOB_INDEX_ARGV             (ORTE_JOB_START_KEY + 16)    // bool - automatically index argvs
#define ORTE_JOB_NO_VM                  (ORTE_JOB_START_KEY + 17)    // bool - do not use VM launch
#define ORTE_JOB_SPIN_FOR_DEBUG         (ORTE_JOB_START_KEY + 18)    // bool - job consists of continuously operating apps
#define ORTE_JOB_CONTINUOUS_OP          (ORTE_JOB_START_KEY + 19)    // bool - recovery policy defined for job
#define ORTE_JOB_RECOVER_DEFINED        (ORTE_JOB_START_KEY + 20)    // bool - recovery policy has been defined
#define ORTE_JOB_NON_ORTE_JOB           (ORTE_JOB_START_KEY + 22)    // bool - non-orte job
#define ORTE_JOB_STDOUT_TARGET          (ORTE_JOB_START_KEY + 23)    // orte_jobid_t - job that is to receive the stdout (on its stdin) from this one
#define ORTE_JOB_POWER                  (ORTE_JOB_START_KEY + 24)    // string - power setting for nodes in job
#define ORTE_JOB_MAX_FREQ               (ORTE_JOB_START_KEY + 25)    // string - max freq setting for nodes in job
#define ORTE_JOB_MIN_FREQ               (ORTE_JOB_START_KEY + 26)    // string - min freq setting for nodes in job
#define ORTE_JOB_GOVERNOR               (ORTE_JOB_START_KEY + 27)    // string - governor used for nodes in job
#define ORTE_JOB_FAIL_NOTIFIED          (ORTE_JOB_START_KEY + 28)    // bool - abnormal term of proc within job has been reported
#define ORTE_JOB_TERM_NOTIFIED          (ORTE_JOB_START_KEY + 29)    // bool - normal term of job has been reported
#define ORTE_JOB_PEER_MODX_ID           (ORTE_JOB_START_KEY + 30)    // orte_grpcomm_coll_id_t - collective id
#define ORTE_JOB_INIT_BAR_ID            (ORTE_JOB_START_KEY + 31)    // orte_grpcomm_coll_id_t - collective id
#define ORTE_JOB_FINI_BAR_ID            (ORTE_JOB_START_KEY + 32)    // orte_grpcomm_coll_id_t - collective id
#define ORTE_JOB_FWDIO_TO_TOOL          (ORTE_JOB_START_KEY + 33)    // Forward IO for this job to the tool requesting its spawn
#define ORTE_JOB_PHYSICAL_CPUIDS        (ORTE_JOB_START_KEY + 34)    // bool - Hostfile contains physical jobids in cpuset
#define ORTE_JOB_LAUNCHED_DAEMONS       (ORTE_JOB_START_KEY + 35)    // bool - Job caused new daemons to be spawned
#define ORTE_JOB_REPORT_BINDINGS        (ORTE_JOB_START_KEY + 36)    // bool - Report process bindings
#define ORTE_JOB_CPU_LIST               (ORTE_JOB_START_KEY + 37)    // string - cpus to which procs are to be bound
#define ORTE_JOB_NOTIFICATIONS          (ORTE_JOB_START_KEY + 38)    // string - comma-separated list of desired notifications+methods
#define ORTE_JOB_ROOM_NUM               (ORTE_JOB_START_KEY + 39)    // int - number of remote request's hotel room
#define ORTE_JOB_LAUNCH_PROXY           (ORTE_JOB_START_KEY + 40)    // opal_process_name_t - name of spawn requestor
#define ORTE_JOB_NSPACE_REGISTERED      (ORTE_JOB_START_KEY + 41)    // bool - job has been registered with embedded PMIx server
#define ORTE_JOB_FIXED_DVM              (ORTE_JOB_START_KEY + 42)    // bool - do not change the size of the DVM for this job
#define ORTE_JOB_DVM_JOB                (ORTE_JOB_START_KEY + 43)    // bool - job is using a DVM
#define ORTE_JOB_CANCELLED              (ORTE_JOB_START_KEY + 44)    // bool - job was cancelled
#define ORTE_JOB_OUTPUT_TO_FILE         (ORTE_JOB_START_KEY + 45)    // string - name of directory to which stdout/err is to be directed
#define ORTE_JOB_MERGE_STDERR_STDOUT    (ORTE_JOB_START_KEY + 46)    // bool - merge stderr into stdout stream
#define ORTE_JOB_TAG_OUTPUT             (ORTE_JOB_START_KEY + 47)    // bool - tag stdout/stderr
#define ORTE_JOB_TIMESTAMP_OUTPUT       (ORTE_JOB_START_KEY + 48)    // bool - timestamp stdout/stderr
#define ORTE_JOB_MULTI_DAEMON_SIM       (ORTE_JOB_START_KEY + 49)    // bool - multiple daemons/node to simulate large cluster
#define ORTE_JOB_NOTIFY_COMPLETION      (ORTE_JOB_START_KEY + 50)    // bool - notify parent proc when spawned job terminates
#define ORTE_JOB_TRANSPORT_KEY          (ORTE_JOB_START_KEY + 51)    // string - transport keys assigned to this job
#define ORTE_JOB_INFO_CACHE             (ORTE_JOB_START_KEY + 52)    // opal_list_t - list of opal_value_t to be included in job_info
#define ORTE_JOB_FULLY_DESCRIBED        (ORTE_JOB_START_KEY + 53)    // bool - job is fully described in launch msg
#define ORTE_JOB_SILENT_TERMINATION     (ORTE_JOB_START_KEY + 54)    // bool - do not generate an event notification when job
                                                                     //        normally terminates
#define ORTE_JOB_SET_ENVAR              (ORTE_JOB_START_KEY + 55)    // opal_envar_t - set the given envar to the specified value
#define ORTE_JOB_UNSET_ENVAR            (ORTE_JOB_START_KEY + 56)    // string - name of envar to unset, if present
#define ORTE_JOB_PREPEND_ENVAR          (ORTE_JOB_START_KEY + 57)    // opal_envar_t - prepend the specified value to the given envar
#define ORTE_JOB_APPEND_ENVAR           (ORTE_JOB_START_KEY + 58)    // opal_envar_t - append the specified value to the given envar
#define ORTE_JOB_ADD_ENVAR              (ORTE_JOB_START_KEY + 59)    // opal_envar_t - add envar, do not override pre-existing one
#define ORTE_JOB_APP_SETUP_DATA         (ORTE_JOB_START_KEY + 60)    // opal_byte_object_t - blob containing app setup data

#define ORTE_JOB_MAX_KEY   300


/*** PROC FLAGS - never sent anywhere ***/
typedef uint16_t orte_proc_flags_t;
#define ORTE_PROC_FLAG_ALIVE         0x0001  // proc has been launched and has not yet terminated
#define ORTE_PROC_FLAG_ABORT         0x0002  // proc called abort
#define ORTE_PROC_FLAG_UPDATED       0x0004  // proc has been updated and need to be included in the next pidmap message
#define ORTE_PROC_FLAG_LOCAL         0x0008  // indicate that this proc is local
#define ORTE_PROC_FLAG_REPORTED      0x0010  // indicate proc has reported in
#define ORTE_PROC_FLAG_REG           0x0020  // proc has registered
#define ORTE_PROC_FLAG_HAS_DEREG     0x0040  // proc has deregistered
#define ORTE_PROC_FLAG_AS_MPI        0x0080  // proc is MPI process
#define ORTE_PROC_FLAG_IOF_COMPLETE  0x0100  // IOF has completed
#define ORTE_PROC_FLAG_WAITPID       0x0200  // waitpid fired
#define ORTE_PROC_FLAG_RECORDED      0x0400  // termination has been recorded
#define ORTE_PROC_FLAG_DATA_IN_SM    0x0800  // modex data has been stored in the local shared memory region
#define ORTE_PROC_FLAG_DATA_RECVD    0x1000  // modex data for this proc has been received
#define ORTE_PROC_FLAG_SM_ACCESS     0x2000  // indicate if process can read modex data from shared memory region
#define ORTE_PROC_FLAG_TOOL          0x4000  // proc is a tool and doesn't count against allocations

/***   PROCESS ATTRIBUTE KEYS   ***/
#define ORTE_PROC_START_KEY   ORTE_JOB_MAX_KEY

#define ORTE_PROC_NOBARRIER       (ORTE_PROC_START_KEY +  1)           // bool  - indicates proc should not barrier in orte_init
#define ORTE_PROC_CPU_BITMAP      (ORTE_PROC_START_KEY +  2)           // string - string representation of cpu bindings
#define ORTE_PROC_HWLOC_LOCALE    (ORTE_PROC_START_KEY +  3)           // opal_ptr (hwloc_obj_t) = pointer to object where proc was mapped
#define ORTE_PROC_HWLOC_BOUND     (ORTE_PROC_START_KEY +  4)           // opal_ptr (hwloc_obj_t) = pointer to object where proc was bound
#define ORTE_PROC_PRIOR_NODE      (ORTE_PROC_START_KEY +  5)           // void* - pointer to orte_node_t where this proc last executed
#define ORTE_PROC_NRESTARTS       (ORTE_PROC_START_KEY +  6)           // int32 - number of times this process has been restarted
#define ORTE_PROC_RESTART_TIME    (ORTE_PROC_START_KEY +  7)           // timeval - time of last restart
#define ORTE_PROC_FAST_FAILS      (ORTE_PROC_START_KEY +  8)           // int32 - number of failures in "fast" window
#define ORTE_PROC_CKPT_STATE      (ORTE_PROC_START_KEY +  9)           // size_t - ckpt state
#define ORTE_PROC_SNAPSHOT_REF    (ORTE_PROC_START_KEY + 10)           // string - snapshot reference
#define ORTE_PROC_SNAPSHOT_LOC    (ORTE_PROC_START_KEY + 11)           // string - snapshot location
#define ORTE_PROC_NODENAME        (ORTE_PROC_START_KEY + 12)           // string - node where proc is located, used only by tools
#define ORTE_PROC_CGROUP          (ORTE_PROC_START_KEY + 13)           // string - name of cgroup this proc shall be assigned to
#define ORTE_PROC_NBEATS          (ORTE_PROC_START_KEY + 14)           // int32 - number of heartbeats in current window

#define ORTE_PROC_MAX_KEY   400

/*** RML ATTRIBUTE keys ***/
#define ORTE_RML_START_KEY  ORTE_PROC_MAX_KEY
#define ORTE_RML_TRANSPORT_TYPE         (ORTE_RML_START_KEY +  1)   // string - null terminated string containing transport type
#define ORTE_RML_PROTOCOL_TYPE          (ORTE_RML_START_KEY +  2)   // string - protocol type (e.g., as returned by fi_info)
#define ORTE_RML_CONDUIT_ID             (ORTE_RML_START_KEY +  3)   // orte_rml_conduit_t - conduit_id for this transport
#define ORTE_RML_INCLUDE_COMP_ATTRIB    (ORTE_RML_START_KEY +  4)   // string - comma delimited list of RML component names to be considered
#define ORTE_RML_EXCLUDE_COMP_ATTRIB    (ORTE_RML_START_KEY +  5)   // string - comma delimited list of RML component names to be excluded
#define ORTE_RML_TRANSPORT_ATTRIB       (ORTE_RML_START_KEY +  6)   // string - comma delimited list of transport types to be considered (e.g., "fabric,ethernet")
#define ORTE_RML_QUALIFIER_ATTRIB       (ORTE_RML_START_KEY +  7)   // string - comma delimited list of qualifiers (e.g., routed=direct,bandwidth=xxx)
#define ORTE_RML_PROVIDER_ATTRIB        (ORTE_RML_START_KEY +  8)   // string - comma delimited list of provider names to be considered
#define ORTE_RML_PROTOCOL_ATTRIB        (ORTE_RML_START_KEY +  9)   // string - comma delimited list of protocols to be considered (e.g., tcp,udp)
#define ORTE_RML_ROUTED_ATTRIB          (ORTE_RML_START_KEY + 10)   // string - comma delimited list of routed modules to be considered

#define ORTE_ATTR_KEY_MAX  1000


/*** FLAG OPS ***/
#define ORTE_FLAG_SET(p, f)         ((p)->flags |= (f))
#define ORTE_FLAG_UNSET(p, f)       ((p)->flags &= ~(f))
#define ORTE_FLAG_TEST(p, f)        ((p)->flags & (f))

ORTE_DECLSPEC const char *orte_attr_key_to_str(orte_attribute_key_t key);

/* Retrieve the named attribute from a list */
ORTE_DECLSPEC bool orte_get_attribute(opal_list_t *attributes, orte_attribute_key_t key,
                                      void **data, opal_data_type_t type);

/* Set the named attribute in a list, overwriting any prior entry */
ORTE_DECLSPEC int orte_set_attribute(opal_list_t *attributes, orte_attribute_key_t key,
                                     bool local, void *data, opal_data_type_t type);

/* Remove the named attribute from a list */
ORTE_DECLSPEC void orte_remove_attribute(opal_list_t *attributes, orte_attribute_key_t key);

ORTE_DECLSPEC orte_attribute_t* orte_fetch_attribute(opal_list_t *attributes,
                                                     orte_attribute_t *prev,
                                                     orte_attribute_key_t key);

ORTE_DECLSPEC int orte_add_attribute(opal_list_t *attributes,
                                     orte_attribute_key_t key, bool local,
                                     void *data, opal_data_type_t type);

ORTE_DECLSPEC int orte_prepend_attribute(opal_list_t *attributes,
                                         orte_attribute_key_t key, bool local,
                                         void *data, opal_data_type_t type);

ORTE_DECLSPEC int orte_attr_load(orte_attribute_t *kv,
                                 void *data, opal_data_type_t type);

ORTE_DECLSPEC int orte_attr_unload(orte_attribute_t *kv,
                                   void **data, opal_data_type_t type);

/*
 * Register a handler for converting attr keys to strings
 *
 * Handlers will be invoked by orte_attr_key_to_str to return the appropriate value.
 */
typedef char* (*orte_attr2str_fn_t)(orte_attribute_key_t key);

ORTE_DECLSPEC int orte_attr_register(const char *project,
                                     orte_attribute_key_t key_base,
                                     orte_attribute_key_t key_max,
                                     orte_attr2str_fn_t converter);

#endif

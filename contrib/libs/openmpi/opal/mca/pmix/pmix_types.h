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

#ifndef OPAL_PMIX_TYPES_H
#define OPAL_PMIX_TYPES_H

#include "opal_config.h"

#include "opal/dss/dss_types.h"
#include "opal/util/proc.h"

BEGIN_C_DECLS

/* define a value for requests for job-level data
 * where the info itself isn't associated with any
 * specific rank, or when a request involves
 * a rank that isn't known - e.g., when someone requests
 * info thru one of the legacy interfaces where the rank
 * is typically encoded into the key itself since there is
 * no rank parameter in the API itself */
#define OPAL_PMIX_RANK_UNDEF     UINT32_MAX
/* define a value to indicate that the user wants the
 * data for the given key from every rank that posted
 * that key */
#define OPAL_PMIX_RANK_WILDCARD  UINT32_MAX-1

/* other special rank values will be used to define
 * groups of ranks for use in collectives */
#define OPAL_PMIX_RANK_LOCAL_NODE    UINT32_MAX-2        // all ranks on local node


/* define a set of "standard" attributes that can
 * be queried. Implementations (and users) are free to extend as
 * desired, so the get functions need to be capable
 * of handling the "not found" condition. Note that these
 * are attributes of the system and the job as opposed to
 * values the application (or underlying MPI library)
 * might choose to expose - i.e., they are values provided
 * by the resource manager as opposed to the application. Thus,
 * these keys are RESERVED */
#define OPAL_PMIX_ATTR_UNDEF      NULL

#define OPAL_PMIX_SERVER_TOOL_SUPPORT           "pmix.srvr.tool"        // (bool) The host RM wants to declare itself as willing to
                                                                        //        accept tool connection requests
#define OPAL_PMIX_SERVER_REMOTE_CONNECTIONS     "pmix.srvr.remote"      // (bool) Allow connections from remote tools (do not use loopback device)
#define OPAL_PMIX_SERVER_SYSTEM_SUPPORT         "pmix.srvr.sys"         // (bool) The host RM wants to declare itself as being the local
                                                                        //        system server for PMIx connection requests
#define OPAL_PMIX_SERVER_TMPDIR                 "pmix.srvr.tmpdir"      // (char*) temp directory where PMIx server will place
                                                                        //        client rendezvous points
#define OPAL_PMIX_SYSTEM_TMPDIR                 "pmix.sys.tmpdir"       // (char*) temp directory where PMIx server will place
                                                                        //        tool rendezvous points
#define OPAL_PMIX_REGISTER_NODATA               "pmix.reg.nodata"       // (bool) Registration is for nspace only, do not copy job data
#define OPAL_PMIX_SERVER_ENABLE_MONITORING      "pmix.srv.monitor"      // (bool) Enable PMIx internal monitoring by server
#define OPAL_PMIX_SERVER_NSPACE                 "pmix.srv.nspace"       // (char*) Name of the nspace to use for this server
#define OPAL_PMIX_SERVER_RANK                   "pmix.srv.rank"         // (uint32_t) Rank of this server

/* tool-related attributes */
#define OPAL_PMIX_TOOL_NSPACE                   "pmix.tool.nspace"      // (char*) Name of the nspace to use for this tool
#define OPAL_PMIX_TOOL_RANK                     "pmix.tool.rank"        // (uint32_t) Rank of this tool
#define OPAL_PMIX_SERVER_PIDINFO                "pmix.srvr.pidinfo"     // (pid_t) pid of the target server for a tool
#define OPAL_PMIX_CONNECT_TO_SYSTEM             "pmix.cnct.sys"         // (bool) The requestor requires that a connection be made only to
                                                                        //        a local system-level PMIx server
#define OPAL_PMIX_CONNECT_SYSTEM_FIRST          "pmix.cnct.sys.first"   // (bool) Preferentially look for a system-level PMIx server first
#define OPAL_PMIX_SERVER_URI                    "pmix.srvr.uri"         // (char*) URI of server to be contacted
#define OPAL_PMIX_SERVER_HOSTNAME               "pmix.srvr.host"        // (char*) node where target server is located
#define OPAL_PMIX_CONNECT_MAX_RETRIES           "pmix.tool.mretries"    // (uint32_t) maximum number of times to try to connect to server
#define OPAL_PMIX_CONNECT_RETRY_DELAY           "pmix.tool.retry"       // (uint32_t) time in seconds between connection attempts
#define OPAL_PMIX_TOOL_DO_NOT_CONNECT           "pmix.tool.nocon"       // (bool) the tool wants to use internal PMIx support, but does
                                                                        //        not want to connect to a PMIx server


/* identification attributes */
#define OPAL_PMIX_USERID                        "pmix.euid"             // (uint32_t) effective user id
#define OPAL_PMIX_GRPID                         "pmix.egid"             // (uint32_t) effective group id
#define OPAL_PMIX_PROGRAMMING_MODEL             "pmix.pgm.model"        // (char*) programming model being initialized (e.g., "MPI" or "OpenMP")
#define OPAL_PMIX_MODEL_LIBRARY_NAME            "pmix.mdl.name"         // (char*) programming model implementation ID (e.g., "OpenMPI" or "MPICH")
#define OPAL_PMIX_MODEL_LIBRARY_VERSION         "pmix.mld.vrs"          // (char*) programming model version string (e.g., "2.1.1")
#define OPAL_PMIX_THREADING_MODEL               "pmix.threads"          // (char*) threading model used (e.g., "pthreads")
#define OPAL_PMIX_REQUESTOR_IS_TOOL             "pmix.req.tool"         // (bool) requesting process is a tool
#define OPAL_PMIX_REQUESTOR_IS_CLIENT           "pmix.req.client"       // (bool) requesting process is a client process

/* attributes for the rendezvous socket  */
#define OPAL_PMIX_USOCK_DISABLE                 "pmix.usock.disable"    // (bool) disable legacy usock support
#define OPAL_PMIX_SOCKET_MODE                   "pmix.sockmode"         // (uint32_t) POSIX mode_t (9 bits valid)
#define OPAL_PMIX_SINGLE_LISTENER               "pmix.sing.listnr"      // (bool) use only one rendezvous socket, letting priorities and/or
                                                                        //        MCA param select the active transport

/* attributes for TCP connections */
#define OPAL_PMIX_TCP_URI                       "pmix.tcp.uri"          // (char*) URI of server to connect to
#define OPAL_PMIX_TCP_IF_INCLUDE                "pmix.tcp.ifinclude"    // (char*) comma-delimited list of devices and/or CIDR notation
#define OPAL_PMIX_TCP_IF_EXCLUDE                "pmix.tcp.ifexclude"    // (char*) comma-delimited list of devices and/or CIDR notation
#define OPAL_PMIX_TCP_IPV4_PORT                 "pmix.tcp.ipv4"         // (int) IPv4 port to be used
#define OPAL_PMIX_TCP_IPV6_PORT                 "pmix.tcp.ipv6"         // (int) IPv6 port to be used
#define OPAL_PMIX_TCP_DISABLE_IPV4              "pmix.tcp.disipv4"      // (bool) true to disable IPv4 family
#define OPAL_PMIX_TCP_DISABLE_IPV6              "pmix.tcp.disipv6"      // (bool) true to disable IPv6 family


/* general proc-level attributes */
#define OPAL_PMIX_CPUSET                        "pmix.cpuset"           // (char*) hwloc bitmap applied to proc upon launch
#define OPAL_PMIX_CREDENTIAL                    "pmix.cred"             // (char*) security credential assigned to proc
#define OPAL_PMIX_SPAWNED                       "pmix.spawned"          // (bool) true if this proc resulted from a call to PMIx_Spawn
#define OPAL_PMIX_ARCH                          "opal.pmix.arch"        // (uint32_t) datatype architecture flag
                                                                        // not set at job startup, so cannot have the pmix prefix

/* scratch directory locations for use by applications */
#define OPAL_PMIX_TMPDIR                        "pmix.tmpdir"           // (char*) top-level tmp dir assigned to session
#define OPAL_PMIX_NSDIR                         "pmix.nsdir"            // (char*) sub-tmpdir assigned to namespace
#define OPAL_PMIX_PROCDIR                       "pmix.pdir"             // (char*) sub-nsdir assigned to proc
#define OPAL_PMIX_TDIR_RMCLEAN                  "pmix.tdir.rmclean"     // (bool)  Resource Manager will clean session directories


/* information about relative ranks as assigned by the RM */
#define OPAL_PMIX_CLUSTER_ID                    "pmix.clid"             // (char*) a string name for the cluster this proc is executing on
#define OPAL_PMIX_PROCID                        "pmix.procid"           // (opal_process_name_t) process identifier
#define OPAL_PMIX_NSPACE                        "pmix.nspace"           // (char*) nspace of a job
#define OPAL_PMIX_JOBID                         "pmix.jobid"            // (uint32_t) jobid assigned by scheduler
#define OPAL_PMIX_APPNUM                        "pmix.appnum"           // (uint32_t) app number within the job
#define OPAL_PMIX_RANK                          "pmix.rank"             // (uint32_t) process rank within the job
#define OPAL_PMIX_GLOBAL_RANK                   "pmix.grank"            // (uint32_t) rank spanning across all jobs in this session
#define OPAL_PMIX_UNIV_RANK                     "pmix.grank"            // (uint32_t) synonym for global_rank
#define OPAL_PMIX_APP_RANK                      "pmix.apprank"          // (uint32_t) rank within this app
#define OPAL_PMIX_NPROC_OFFSET                  "pmix.offset"           // (uint32_t) starting global rank of this job
#define OPAL_PMIX_LOCAL_RANK                    "pmix.lrank"            // (uint16_t) rank on this node within this job
#define OPAL_PMIX_NODE_RANK                     "pmix.nrank"            // (uint16_t) rank on this node spanning all jobs
#define OPAL_PMIX_LOCALLDR                      "pmix.lldr"             // (uint64_t) opal_identifier of lowest rank on this node within this job
#define OPAL_PMIX_APPLDR                        "pmix.aldr"             // (uint32_t) lowest rank in this app within this job
#define OPAL_PMIX_PROC_PID                      "pmix.ppid"             // (pid_t) pid of specified proc
#define OPAL_PMIX_SESSION_ID                    "pmix.session.id"       // (uint32_t) session identifier
#define OPAL_PMIX_NODE_LIST                     "pmix.nlist"            // (char*) comma-delimited list of nodes running procs for the specified nspace
#define OPAL_PMIX_ALLOCATED_NODELIST            "pmix.alist"            // (char*) comma-delimited list of all nodes in this allocation regardless of
                                                                        //           whether or not they currently host procs.
#define OPAL_PMIX_HOSTNAME                      "pmix.hname"            // (char*) name of the host the specified proc is on
#define OPAL_PMIX_NODEID                        "pmix.nodeid"           // (uint32_t) node identifier
#define OPAL_PMIX_LOCAL_PEERS                   "pmix.lpeers"           // (char*) comma-delimited string of ranks on this node within the specified nspace
#define OPAL_PMIX_LOCAL_PROCS                   "pmix.lprocs"           // (opal_list_t*) list of opal_namelist_t of procs on the specified node
#define OPAL_PMIX_LOCAL_CPUSETS                 "pmix.lcpus"            // (char*) colon-delimited cpusets of local peers within the specified nspace
#define OPAL_PMIX_PROC_URI                      "opal.puri"             // (char*) URI containing contact info for proc - NOTE: this is published by procs and
                                                                        //            thus cannot be prefixed with "pmix"
#define OPAL_PMIX_LOCALITY                      "pmix.loc"              // (uint16_t) relative locality of two procs


/* Memory info */
#define OPAL_PMIX_AVAIL_PHYS_MEMORY             "pmix.pmem"             // (uint64_t) total available physical memory on this node
#define OPAL_PMIX_DAEMON_MEMORY                 "pmix.dmn.mem"          // (float) Mbytes of memory currently used by daemon
#define OPAL_PMIX_CLIENT_AVG_MEMORY             "pmix.cl.mem.avg"       // (float) Average Mbytes of memory used by client processes


/* size info */
#define OPAL_PMIX_UNIV_SIZE                     "pmix.univ.size"        // (uint32_t) #procs in this nspace
#define OPAL_PMIX_JOB_SIZE                      "pmix.job.size"         // (uint32_t) #procs in this job
#define OPAL_PMIX_JOB_NUM_APPS                  "pmix.job.napps"        // (uint32_t) #apps in this job
#define OPAL_PMIX_APP_SIZE                      "pmix.app.size"         // (uint32_t) #procs in this app
#define OPAL_PMIX_LOCAL_SIZE                    "pmix.local.size"       // (uint32_t) #procs in this job on this node
#define OPAL_PMIX_NODE_SIZE                     "pmix.node.size"        // (uint32_t) #procs across all jobs on this node
#define OPAL_PMIX_MAX_PROCS                     "pmix.max.size"         // (uint32_t) max #procs for this job
#define OPAL_PMIX_NUM_NODES                     "pmix.num.nodes"        // (uint32_t) #nodes in this nspace


/* topology info */
#define OPAL_PMIX_NET_TOPO                      "pmix.ntopo"            // (char*) xml-representation of network topology
#define OPAL_PMIX_LOCAL_TOPO                    "pmix.ltopo"            // (char*) xml-representation of local node topology
#define OPAL_PMIX_NODE_LIST                     "pmix.nlist"            // (char*) comma-delimited list of nodes running procs for this job
#define OPAL_PMIX_TOPOLOGY                      "pmix.topo"             // (hwloc_topology_t) pointer to the PMIx client's internal topology object
#define OPAL_PMIX_TOPOLOGY_SIGNATURE            "pmix.toposig"          // (char*) topology signature string
#define OPAL_PMIX_LOCALITY_STRING               "pmix.locstr"           // (char*) string describing a proc's location
#define OPAL_PMIX_HWLOC_SHMEM_ADDR              "pmix.hwlocaddr"        // (size_t) address of HWLOC shared memory segment
#define OPAL_PMIX_HWLOC_SHMEM_SIZE              "pmix.hwlocsize"        // (size_t) size of HWLOC shared memory segment
#define OPAL_PMIX_HWLOC_SHMEM_FILE              "pmix.hwlocfile"        // (char*) path to HWLOC shared memory file
#define OPAL_PMIX_HWLOC_XML_V1                  "pmix.hwlocxml1"        // (char*) XML representation of local topology using HWLOC v1.x format
#define OPAL_PMIX_HWLOC_XML_V2                  "pmix.hwlocxml2"        // (char*) XML representation of local topology using HWLOC v2.x format


/* request-related info */
#define OPAL_PMIX_COLLECT_DATA                  "pmix.collect"          // (bool) collect data and return it at the end of the operation
#define OPAL_PMIX_TIMEOUT                       "pmix.timeout"          // (int) time in sec before specified operation should time out
#define OPAL_PMIX_IMMEDIATE                     "pmix.immediate"        // (bool) specified operation should immediately return an error if requested
                                                                        //        data cannot be found - do not request it from the host RM
#define OPAL_PMIX_WAIT                          "pmix.wait"             // (int) caller requests that the server wait until at least the specified
                                                                        //       #values are found (0 => all and is the default)
#define OPAL_PMIX_COLLECTIVE_ALGO               "pmix.calgo"            // (char*) comma-delimited list of algorithms to use for collective
#define OPAL_PMIX_COLLECTIVE_ALGO_REQD          "pmix.calreqd"          // (bool) if true, indicates that the requested choice of algo is mandatory
#define OPAL_PMIX_NOTIFY_COMPLETION             "pmix.notecomp"         // (bool) notify parent process upon termination of child job
#define OPAL_PMIX_RANGE                         "pmix.range"            // (int) opal_pmix_data_range_t value for calls to publish/lookup/unpublish
#define OPAL_PMIX_PERSISTENCE                   "pmix.persist"          // (int) opal_pmix_persistence_t value for calls to publish
#define OPAL_PMIX_DATA_SCOPE                    "pmix.scope"            // (pmix_scope_t) scope of the data to be found in a PMIx_Get call
#define OPAL_PMIX_OPTIONAL                      "pmix.optional"         // (bool) look only in the immediate data store for the requested value - do
                                                                        //        not request data from the server if not found
#define OPAL_PMIX_EMBED_BARRIER                 "pmix.embed.barrier"    // (bool) execute a blocking fence operation before executing the
                                                                        //        specified operation
#define OPAL_PMIX_JOB_TERM_STATUS               "pmix.job.term.status"  // (int) status returned upon job termination
#define OPAL_PMIX_PROC_STATE_STATUS             "pmix.proc.state"       // (int) process state



/* attribute used by host server to pass data to the server convenience library - the
 * data will then be parsed and provided to the local clients */
#define OPAL_PMIX_PROC_DATA                     "pmix.pdata"            // (pmix_value_array_t) starts with rank, then contains more data
#define OPAL_PMIX_NODE_MAP                      "pmix.nmap"             // (char*) regex of nodes containing procs for this job
#define OPAL_PMIX_PROC_MAP                      "pmix.pmap"             // (char*) regex describing procs on each node within this job


/* attributes used internally to communicate data from the server to the client */
#define OPAL_PMIX_PROC_BLOB                     "pmix.pblob"            // (pmix_byte_object_t) packed blob of process data
#define OPAL_PMIX_MAP_BLOB                      "pmix.mblob"            // (pmix_byte_object_t) packed blob of process location


/* error handler registration  and notification info keys */
#define OPAL_PMIX_EVENT_HDLR_NAME               "pmix.evname"           // (char*) string name identifying this handler
#define OPAL_PMIX_EVENT_JOB_LEVEL               "pmix.evjob"            // (bool) register for job-specific events only
#define OPAL_PMIX_EVENT_ENVIRO_LEVEL            "pmix.evenv"            // (bool) register for environment events only
#define OPAL_PMIX_EVENT_ORDER_PREPEND           "pmix.evprepend"        // (bool) prepend this handler to the precedence list
#define OPAL_PMIX_EVENT_CUSTOM_RANGE            "pmix.evrange"          // (pmix_proc_t*) array of pmix_proc_t defining range of event notification
#define OPAL_PMIX_EVENT_AFFECTED_PROC           "pmix.evproc"           // (pmix_proc_t) single proc that was affected
#define OPAL_PMIX_EVENT_AFFECTED_PROCS          "pmix.evaffected"       // (pmix_proc_t*) array of pmix_proc_t defining affected procs
#define OPAL_PMIX_EVENT_NON_DEFAULT             "pmix.evnondef"         // (bool) event is not to be delivered to default event handlers
#define OPAL_PMIX_EVENT_RETURN_OBJECT           "pmix.evobject"         // (void*) object to be returned whenever the registered cbfunc is invoked
                                                                        //     NOTE: the object will _only_ be returned to the process that
                                                                        //           registered it
#define OPAL_PMIX_EVENT_DO_NOT_CACHE            "pmix.evnocache"        // (bool) instruct the PMIx server not to cache the event
#define OPAL_PMIX_EVENT_SILENT_TERMINATION      "pmix.evsilentterm"     // (bool) do not generate an event when this job normally terminates


/* fault tolerance-related events */
#define OPAL_PMIX_EVENT_TERMINATE_SESSION       "pmix.evterm.sess"      // (bool) RM intends to terminate session
#define OPAL_PMIX_EVENT_TERMINATE_JOB           "pmix.evterm.job"       // (bool) RM intends to terminate this job
#define OPAL_PMIX_EVENT_TERMINATE_NODE          "pmix.evterm.node"      // (bool) RM intends to terminate all procs on this node
#define OPAL_PMIX_EVENT_TERMINATE_PROC          "pmix.evterm.proc"      // (bool) RM intends to terminate just this process
#define OPAL_PMIX_EVENT_ACTION_TIMEOUT          "pmix.evtimeout"        // (int) time in sec before RM will execute error response


/* attributes used to describe "spawn" attributes */
#define OPAL_PMIX_PERSONALITY                   "pmix.pers"             // (char*) name of personality to use
#define OPAL_PMIX_HOST                          "pmix.host"             // (char*) comma-delimited list of hosts to use for spawned procs
#define OPAL_PMIX_HOSTFILE                      "pmix.hostfile"         // (char*) hostfile to use for spawned procs
#define OPAL_PMIX_ADD_HOST                      "pmix.addhost"          // (char*) comma-delimited list of hosts to add to allocation
#define OPAL_PMIX_ADD_HOSTFILE                  "pmix.addhostfile"      // (char*) hostfile to add to existing allocation
#define OPAL_PMIX_PREFIX                        "pmix.prefix"           // (char*) prefix to use for starting spawned procs
#define OPAL_PMIX_WDIR                          "pmix.wdir"             // (char*) working directory for spawned procs
#define OPAL_PMIX_MAPPER                        "pmix.mapper"           // (char*) mapper to use for placing spawned procs
#define OPAL_PMIX_DISPLAY_MAP                   "pmix.dispmap"          // (bool) display process map upon spawn
#define OPAL_PMIX_PPR                           "pmix.ppr"              // (char*) #procs to spawn on each identified resource
#define OPAL_PMIX_MAPBY                         "pmix.mapby"            // (char*) mapping policy
#define OPAL_PMIX_RANKBY                        "pmix.rankby"           // (char*) ranking policy
#define OPAL_PMIX_BINDTO                        "pmix.bindto"           // (char*) binding policy
#define OPAL_PMIX_PRELOAD_BIN                   "pmix.preloadbin"       // (bool) preload binaries
#define OPAL_PMIX_PRELOAD_FILES                 "pmix.preloadfiles"     // (char*) comma-delimited list of files to pre-position
#define OPAL_PMIX_NON_PMI                       "pmix.nonpmi"           // (bool) spawned procs will not call PMIx_Init
#define OPAL_PMIX_STDIN_TGT                     "pmix.stdin"            // (uint32_t) spawned proc rank that is to receive stdin
#define OPAL_PMIX_DEBUGGER_DAEMONS              "pmix.debugger"         // (bool) spawned app consists of debugger daemons
#define OPAL_PMIX_COSPAWN_APP                   "pmix.cospawn"          // (bool) designated app is to be spawned as a disconnected
                                                                        //        job - i.e., not part of the "comm_world" of the job
#define OPAL_PMIX_SET_SESSION_CWD               "pmix.ssncwd"           // (bool) set the application's current working directory to
                                                                        //        the session working directory assigned by the RM
#define OPAL_PMIX_TAG_OUTPUT                    "pmix.tagout"           // (bool) tag application output with the ID of the source
#define OPAL_PMIX_TIMESTAMP_OUTPUT              "pmix.tsout"            // (bool) timestamp output from applications
#define OPAL_PMIX_MERGE_STDERR_STDOUT           "pmix.mergeerrout"      // (bool) merge stdout and stderr streams from application procs
#define OPAL_PMIX_OUTPUT_TO_FILE                "pmix.outfile"          // (char*) output application output to given file
#define OPAL_PMIX_INDEX_ARGV                    "pmix.indxargv"         // (bool) mark the argv with the rank of the proc
#define OPAL_PMIX_CPUS_PER_PROC                 "pmix.cpuperproc"       // (uint32_t) #cpus to assign to each rank
#define OPAL_PMIX_NO_PROCS_ON_HEAD              "pmix.nolocal"          // (bool) do not place procs on the head node
#define OPAL_PMIX_NO_OVERSUBSCRIBE              "pmix.noover"           // (bool) do not oversubscribe the cpus
#define OPAL_PMIX_REPORT_BINDINGS               "pmix.repbind"          // (bool) report bindings of the individual procs
#define OPAL_PMIX_CPU_LIST                      "pmix.cpulist"          // (char*) list of cpus to use for this job
#define OPAL_PMIX_JOB_RECOVERABLE               "pmix.recover"          // (bool) application supports recoverable operations
#define OPAL_PMIX_JOB_CONTINUOUS                "pmix.continuous"       // (bool) application is continuous, all failed procs should
                                                                        //        be immediately restarted
#define OPAL_PMIX_MAX_RESTARTS                  "pmix.maxrestarts"      // (uint32_t) max number of times to restart a job


/* environmental variable operation attributes */
#define OPAL_PMIX_SET_ENVAR                     "pmix.envar.set"        // (pmix_envar_t*) set the envar to the given value,
                                                                        //                 overwriting any pre-existing one
#define OPAL_PMIX_ADD_ENVAR                     "pmix.envar.add"        // (pmix_envar_t*) add envar, but do not overwrite any existing one
#define OPAL_PMIX_UNSET_ENVAR                   "pmix.envar.unset"      // (char*) unset the envar, if present
#define OPAL_PMIX_PREPEND_ENVAR                 "pmix.envar.prepnd"     // (pmix_envar_t*) prepend the given value to the
                                                                        //                 specified envar using the separator
                                                                        //                 character, creating the envar if it doesn't already exist
#define OPAL_PMIX_APPEND_ENVAR                  "pmix.envar.appnd"      // (pmix_envar_t*) append the given value to the specified
                                                                        //                 envar using the separator character,
                                                                        //                 creating the envar if it doesn't already exist

/* query attributes */
#define OPAL_PMIX_QUERY_NAMESPACES              "pmix.qry.ns"           // (char*) request a comma-delimited list of active nspaces
#define OPAL_PMIX_QUERY_JOB_STATUS              "pmix.qry.jst"          // (pmix_status_t) status of a specified currently executing job
#define OPAL_PMIX_QUERY_QUEUE_LIST              "pmix.qry.qlst"         // (char*) request a comma-delimited list of scheduler queues
#define OPAL_PMIX_QUERY_QUEUE_STATUS            "pmix.qry.qst"          // (TBD) status of a specified scheduler queue
#define OPAL_PMIX_QUERY_PROC_TABLE              "pmix.qry.ptable"       // (char*) input nspace of job whose info is being requested
                                                                        //     returns (pmix_data_array_t) an array of pmix_proc_info_t
#define OPAL_PMIX_QUERY_LOCAL_PROC_TABLE        "pmix.qry.lptable"      // (char*) input nspace of job whose info is being requested
                                                                        //     returns (pmix_data_array_t) an array of pmix_proc_info_t for
                                                                        //     procs in job on same node
#define OPAL_PMIX_QUERY_AUTHORIZATIONS          "pmix.qry.auths"        // return operations tool is authorized to perform"
#define OPAL_PMIX_QUERY_SPAWN_SUPPORT           "pmix.qry.spawn"        // return a comma-delimited list of supported spawn attributes
#define OPAL_PMIX_QUERY_DEBUG_SUPPORT           "pmix.qry.debug"        // return a comma-delimited list of supported debug attributes
#define OPAL_PMIX_QUERY_MEMORY_USAGE            "pmix.qry.mem"          // return info on memory usage for the procs indicated in the qualifiers
#define OPAL_PMIX_QUERY_LOCAL_ONLY              "pmix.qry.local"        // constrain the query to local information only
#define OPAL_PMIX_QUERY_REPORT_AVG              "pmix.qry.avg"          // report average values
#define OPAL_PMIX_QUERY_REPORT_MINMAX           "pmix.qry.minmax"       // report minimum and maximum value
#define OPAL_PMIX_QUERY_ALLOC_STATUS            "pmix.query.alloc"      // (char*) string identifier of the allocation whose status
                                                                        //         is being requested
#define OPAL_PMIX_TIME_REMAINING                "pmix.time.remaining"   // (char*) query number of seconds (uint32_t) remaining in allocation
                                                                        //         for the specified nspace


/* log attributes */
#define OPAL_PMIX_LOG_STDERR                    "pmix.log.stderr"       // (char*) log string to stderr
#define OPAL_PMIX_LOG_STDOUT                    "pmix.log.stdout"       // (char*) log string to stdout
#define OPAL_PMIX_LOG_SYSLOG                    "pmix.log.syslog"       // (char*) log data to syslog - defaults to ERROR priority unless
#define OPAL_PMIX_LOG_MSG                       "pmix.log.msg"          // (pmix_byte_object_t) message blob to be sent somewhere
#define OPAL_PMIX_LOG_EMAIL                     "pmix.log.email"        // (pmix_data_array_t) log via email based on pmix_info_t containing directives
#define OPAL_PMIX_LOG_EMAIL_ADDR                "pmix.log.emaddr"       // (char*) comma-delimited list of email addresses that are to recv msg
#define OPAL_PMIX_LOG_EMAIL_SUBJECT             "pmix.log.emsub"        // (char*) subject line for email
#define OPAL_PMIX_LOG_EMAIL_MSG                 "pmix.log.emmsg"        // (char*) msg to be included in email


/* debugger attributes */
#define OPAL_PMIX_DEBUG_STOP_ON_EXEC            "pmix.dbg.exec"         // (bool) job is being spawned under debugger - instruct it to pause on start
#define OPAL_PMIX_DEBUG_STOP_IN_INIT            "pmix.dbg.init"         // (bool) instruct job to stop during PMIx init
#define OPAL_PMIX_DEBUG_WAIT_FOR_NOTIFY         "pmix.dbg.notify"       // (bool) block at desired point until receiving debugger release notification
#define OPAL_PMIX_DEBUG_JOB                     "pmix.dbg.job"          // (char*) nspace of the job to be debugged - the RM/PMIx server are
#define OPAL_PMIX_DEBUG_WAITING_FOR_NOTIFY      "pmix.dbg.waiting"      // (bool) job to be debugged is waiting for a release
#define OPAL_PMIX_DEBUG_JOB_DIRECTIVES          "pmix.dbg.jdirs"        // (opal_list_t*) list of job-level directives
#define OPAL_PMIX_DEBUG_APP_DIRECTIVES          "pmix.dbg.adirs"        // (opal_list_t*) list of app-level directives


/* Resource Manager identification */
#define OPAL_PMIX_RM_NAME                       "pmix.rm.name"          // (char*) string name of the resource manager
#define OPAL_PMIX_RM_VERSION                    "pmix.rm.version"       // (char*) RM version string


/* attributes relating to allocations */
#define OPAL_PMIX_ALLOC_ID                      "pmix.alloc.id"         // (char*) provide a string identifier for this allocation request
                                                                        //         which can later be used to query status of the request
#define OPAL_PMIX_ALLOC_NUM_NODES               "pmix.alloc.nnodes"     // (uint64_t) number of nodes
#define OPAL_PMIX_ALLOC_NODE_LIST               "pmix.alloc.nlist"      // (char*) regex of specific nodes
#define OPAL_PMIX_ALLOC_NUM_CPUS                "pmix.alloc.ncpus"      // (uint64_t) number of cpus
#define OPAL_PMIX_ALLOC_NUM_CPU_LIST            "pmix.alloc.ncpulist"   // (char*) regex of #cpus for each node
#define OPAL_PMIX_ALLOC_CPU_LIST                "pmix.alloc.cpulist"    // (char*) regex of specific cpus indicating the cpus involved.
#define OPAL_PMIX_ALLOC_MEM_SIZE                "pmix.alloc.msize"      // (float) number of Mbytes
#define OPAL_PMIX_ALLOC_NETWORK                 "pmix.alloc.net"        // (array) array of pmix_info_t describing network resources. If not
                                                                        //         given as part of an info struct that identifies the
                                                                        //         impacted nodes, then the description will be applied
                                                                        //         across all nodes in the requestor's allocation
#define OPAL_PMIX_ALLOC_NETWORK_ID              "pmix.alloc.netid"      // (char*) name of network
#define OPAL_PMIX_ALLOC_BANDWIDTH               "pmix.alloc.bw"         // (float) Mbits/sec
#define OPAL_PMIX_ALLOC_NETWORK_QOS             "pmix.alloc.netqos"     // (char*) quality of service level
#define OPAL_PMIX_ALLOC_TIME                    "pmix.alloc.time"       // (uint32_t) time in seconds


/* job control attributes */
#define OPAL_PMIX_JOB_CTRL_ID                   "pmix.jctrl.id"         // (char*) provide a string identifier for this request
#define OPAL_PMIX_JOB_CTRL_PAUSE                "pmix.jctrl.pause"      // (bool) pause the specified processes
#define OPAL_PMIX_JOB_CTRL_RESUME               "pmix.jctrl.resume"     // (bool) "un-pause" the specified processes
#define OPAL_PMIX_JOB_CTRL_CANCEL               "pmix.jctrl.cancel"     // (char*) cancel the specified request
                                                                        //         (NULL => cancel all requests from this requestor)
#define OPAL_PMIX_JOB_CTRL_KILL                 "pmix.jctrl.kill"       // (bool) forcibly terminate the specified processes and cleanup
#define OPAL_PMIX_JOB_CTRL_RESTART              "pmix.jctrl.restart"    // (char*) restart the specified processes using the given checkpoint ID
#define OPAL_PMIX_JOB_CTRL_CHECKPOINT           "pmix.jctrl.ckpt"       // (char*) checkpoint the specified processes and assign the given ID to it
#define OPAL_PMIX_JOB_CTRL_CHECKPOINT_EVENT     "pmix.jctrl.ckptev"     // (bool) use event notification to trigger process checkpoint
#define OPAL_PMIX_JOB_CTRL_CHECKPOINT_SIGNAL    "pmix.jctrl.ckptsig"    // (int) use the given signal to trigger process checkpoint
#define OPAL_PMIX_JOB_CTRL_CHECKPOINT_TIMEOUT   "pmix.jctrl.ckptsig"    // (int) time in seconds to wait for checkpoint to complete
#define OPAL_PMIX_JOB_CTRL_SIGNAL               "pmix.jctrl.sig"        // (int) send given signal to specified processes
#define OPAL_PMIX_JOB_CTRL_PROVISION            "pmix.jctrl.pvn"        // (char*) regex identifying nodes that are to be provisioned
#define OPAL_PMIX_JOB_CTRL_PROVISION_IMAGE      "pmix.jctrl.pvnimg"     // (char*) name of the image that is to be provisioned
#define OPAL_PMIX_JOB_CTRL_PREEMPTIBLE          "pmix.jctrl.preempt"    // (bool) job can be pre-empted
#define OPAL_PMIX_JOB_CTRL_TERMINATE            "pmix.jctrl.term"       // (bool) politely terminate the specified procs
#define OPAL_PMIX_REGISTER_CLEANUP              "pmix.reg.cleanup"      // (char*) comma-delimited list of files/directories to
                                                                        //         be removed upon process termination
#define OPAL_PMIX_CLEANUP_RECURSIVE             "pmix.clnup.recurse"    // (bool) recursively cleanup all subdirectories under the
                                                                        //        specified one(s)
#define OPAL_PMIX_CLEANUP_EMPTY                 "pmix.clnup.empty"      // (bool) only remove empty subdirectories
#define OPAL_PMIX_CLEANUP_IGNORE                "pmix.clnup.ignore"     // (char*) comma-delimited list of filenames that are not
                                                                        //         to be removed
#define OPAL_PMIX_CLEANUP_LEAVE_TOPDIR          "pmix.clnup.lvtop"      // (bool) when recursively cleaning subdirs, do not remove
                                                                        //        the top-level directory (the one given in the
                                                                        //        cleanup request)


/* monitoring attributes */
#define OPAL_PMIX_MONITOR_HEARTBEAT             "pmix.monitor.mbeat"    // (void) register to have the server monitor the requestor for heartbeats
#define OPAL_PMIX_SEND_HEARTBEAT                "pmix.monitor.beat"     // (void) send heartbeat to local server
#define OPAL_PMIX_MONITOR_HEARTBEAT_TIME        "pmix.monitor.btime"    // (uint32_t) time in seconds before declaring heartbeat missed
#define OPAL_PMIX_MONITOR_HEARTBEAT_DROPS       "pmix.monitor.bdrop"    // (uint32_t) number of heartbeats that can be missed before taking
                                                                        //            specified action
#define OPAL_PMIX_MONITOR_FILE                  "pmix.monitor.fmon"     // (char*) register to monitor file for signs of life
#define OPAL_PMIX_MONITOR_FILE_SIZE             "pmix.monitor.fsize"    // (bool) monitor size of given file is growing to determine app is running
#define OPAL_PMIX_MONITOR_FILE_ACCESS           "pmix.monitor.faccess"  // (char*) monitor time since last access of given file to determine app is running
#define OPAL_PMIX_MONITOR_FILE_MODIFY           "pmix.monitor.fmod"     // (char*) monitor time since last modified of given file to determine app is running
#define OPAL_PMIX_MONITOR_FILE_CHECK_TIME       "pmix.monitor.ftime"    // (uint32_t) time in seconds between checking file
#define OPAL_PMIX_MONITOR_FILE_DROPS            "pmix.monitor.fdrop"    // (uint32_t) number of file checks that can be missed before taking
                                                                        //            specified action

/* security attributes */
#define OPAL_PMIX_CRED_TYPE                     "pmix.sec.ctype"        // (char*) when passed in PMIx_Get_credential, a prioritized,
                                                                        // comma-delimited list of desired credential types for use
                                                                        // in environments where multiple authentication mechanisms
                                                                        // may be available. When returned in a callback function, a
                                                                        // string identifier of the credential type

/* IO Forwarding Attributes */
#define OPAL_PMIX_IOF_CACHE_SIZE                "pmix.iof.csize"        // (uint32_t) requested size of the server cache in bytes for each specified channel.
                                                                        //            By default, the server is allowed (but not required) to drop
                                                                        //            all bytes received beyond the max size
#define OPAL_PMIX_IOF_DROP_OLDEST               "pmix.iof.old"          // (bool) in an overflow situation, drop the oldest bytes to make room in the cache
#define OPAL_PMIX_IOF_DROP_NEWEST               "pmix.iof.new"          // (bool) in an overflow situation, drop any new bytes received until room becomes
                                                                        //        available in the cache (default)
#define OPAL_PMIX_IOF_BUFFERING_SIZE            "pmix.iof.bsize"        // (uint32_t) basically controls grouping of IO on the specified channel(s) to
                                                                        //            avoid being called every time a bit of IO arrives. The library
                                                                        //            will execute the callback whenever the specified number of bytes
                                                                        //            becomes available. Any remaining buffered data will be "flushed"
                                                                        //            upon call to deregister the respective channel
#define OPAL_PMIX_IOF_BUFFERING_TIME            "pmix.iof.btime"        // (uint32_t) max time in seconds to buffer IO before delivering it. Used in conjunction
                                                                        //            with buffering size, this prevents IO from being held indefinitely
                                                                        //            while waiting for another payload to arrive
#define OPAL_PMIX_IOF_COMPLETE                  "pmix.iof.cmp"          // (bool) indicates whether or not the specified IO channel has been closed
                                                                        //        by the source
#define OPAL_PMIX_IOF_PUSH_STDIN                "pmix.iof.stdin"        // (bool) Used by a tool to request that the PMIx library collect
                                                                        //        the tool's stdin and forward it to the procs specified in
                                                                        //        the PMIx_IOF_push call

/* Attributes for controlling contents of application setup data */
#define OPAL_PMIX_SETUP_APP_ENVARS              "pmix.setup.env"        // (bool) harvest and include relevant envars
#define OPAL_PMIX_SETUP_APP_NONENVARS           "pmix.setup.nenv"       // (bool) include all non-envar data
#define OPAL_PMIX_SETUP_APP_ALL                 "pmix.setup.all"        // (bool) include all relevant data


/* define a scope for data "put" by PMI per the following:
 *
 * OPAL_PMI_LOCAL - the data is intended only for other application
 *                  processes on the same node. Data marked in this way
 *                  will not be included in data packages sent to remote requestors
 * OPAL_PMI_REMOTE - the data is intended solely for applications processes on
 *                   remote nodes. Data marked in this way will not be shared with
 *                   other processes on the same node
 * OPAL_PMI_GLOBAL - the data is to be shared with all other requesting processes,
 *                   regardless of location
 */
#define OPAL_PMIX_SCOPE PMIX_UINT
typedef enum {
    OPAL_PMIX_SCOPE_UNDEF = 0,
    OPAL_PMIX_LOCAL,           // share to procs also on this node
    OPAL_PMIX_REMOTE,          // share with procs not on this node
    OPAL_PMIX_GLOBAL
} opal_pmix_scope_t;

/* define a range for data "published" by PMI */
#define OPAL_PMIX_DATA_RANGE OPAL_UINT8
typedef uint8_t opal_pmix_data_range_t;
#define OPAL_PMIX_RANGE_UNDEF        0
#define OPAL_PMIX_RANGE_RM           1   // data is intended for the host resource manager
#define OPAL_PMIX_RANGE_LOCAL        2   // available on local node only
#define OPAL_PMIX_RANGE_NAMESPACE    3   // data is available to procs in the same nspace only
#define OPAL_PMIX_RANGE_SESSION      4   // data available to all procs in session
#define OPAL_PMIX_RANGE_GLOBAL       5   // data available to all procs
#define OPAL_PMIX_RANGE_CUSTOM       6   // range is specified in a pmix_info_t
#define OPAL_PMIX_RANGE_PROC_LOCAL   7   // restrict range to the local proc

/* define a "persistence" policy for data published by clients */
typedef enum {
    OPAL_PMIX_PERSIST_INDEF = 0,   // retain until specifically deleted
    OPAL_PMIX_PERSIST_FIRST_READ,  // delete upon first access
    OPAL_PMIX_PERSIST_PROC,        // retain until publishing process terminates
    OPAL_PMIX_PERSIST_APP,         // retain until application terminates
    OPAL_PMIX_PERSIST_SESSION      // retain until session/allocation terminates
} opal_pmix_persistence_t;


/* define allocation request flags */
typedef enum {
    OPAL_PMIX_ALLOC_UNDEF = 0,
    OPAL_PMIX_ALLOC_NEW,
    OPAL_PMIX_ALLOC_EXTEND,
    OPAL_PMIX_ALLOC_RELEASE,
    OPAL_PMIX_ALLOC_REAQCUIRE
} opal_pmix_alloc_directive_t;

/* define a set of bit-mask flags for specifying IO
 * forwarding channels. These can be OR'd together
 * to reference multiple channels */
typedef uint16_t opal_pmix_iof_channel_t;
#define OPAL_PMIX_FWD_STDIN_CHANNEL      0x01
#define OPAL_PMIX_FWD_STDOUT_CHANNEL     0x02
#define OPAL_PMIX_FWD_STDERR_CHANNEL     0x04
#define OPAL_PMIX_FWD_STDDIAG_CHANNEL    0x08


/****    PMIX INFO STRUCT    ****/

/* NOTE: the pmix_info_t is essentially equivalent to the opal_value_t
 * Hence, we do not define an opal_value_t */


/****    PMIX LOOKUP RETURN STRUCT    ****/
typedef struct {
    opal_list_item_t super;
    opal_process_name_t proc;
    opal_value_t value;
} opal_pmix_pdata_t;
OBJ_CLASS_DECLARATION(opal_pmix_pdata_t);


/****    PMIX APP STRUCT    ****/
typedef struct {
    opal_list_item_t super;
    char *cmd;
    char **argv;
    char **env;
    char *cwd;
    int maxprocs;
    opal_list_t info;
} opal_pmix_app_t;
/* utility macros for working with pmix_app_t structs */
OBJ_CLASS_DECLARATION(opal_pmix_app_t);


/****    PMIX MODEX STRUCT    ****/
typedef struct {
    opal_object_t super;
    opal_process_name_t proc;
    uint8_t *blob;
    size_t size;
} opal_pmix_modex_data_t;
OBJ_CLASS_DECLARATION(opal_pmix_modex_data_t);

/****    PMIX QUERY STRUCT    ****/
typedef struct {
    opal_list_item_t super;
    char **keys;
    opal_list_t qualifiers;  // list of opal_value_t
} opal_pmix_query_t;
OBJ_CLASS_DECLARATION(opal_pmix_query_t);

/****    CALLBACK FUNCTIONS FOR NON-BLOCKING OPERATIONS    ****/

typedef void (*opal_pmix_release_cbfunc_t)(void *cbdata);

/* define a callback function that is solely used by servers, and
 * not clients, to return modex data in response to "fence" and "get"
 * operations. The returned blob contains the data collected from each
 * server participating in the operation. */
typedef void (*opal_pmix_modex_cbfunc_t)(int status,
                                         const char *data, size_t ndata, void *cbdata,
                                         opal_pmix_release_cbfunc_t relcbfunc, void *relcbdata);

/* define a callback function for calls to spawn_nb - the function
 * will be called upon completion of the spawn command. The status
 * will indicate whether or not the spawn succeeded. The jobid
 * of the spawned processes will be returned, along with any provided
 * callback data. */
typedef void (*opal_pmix_spawn_cbfunc_t)(int status, opal_jobid_t jobid, void *cbdata);

/* define a callback for common operations that simply return
 * a status. Examples include the non-blocking versions of
 * Fence, Connect, and Disconnect */
typedef void (*opal_pmix_op_cbfunc_t)(int status, void *cbdata);

/* define a callback function for calls to lookup_nb - the
 * function will be called upon completion of the command with the
 * status indicating the success of failure of the request. Any
 * retrieved data will be returned in a list of opal_pmix_pdata_t's.
 * The nspace/rank of the process that provided each data element is
 * also returned.
 *
 * Note that these structures will be released upon return from
 * the callback function, so the receiver must copy/protect the
 * data prior to returning if it needs to be retained */

typedef void (*opal_pmix_lookup_cbfunc_t)(int status,
                                          opal_list_t *data,
                                          void *cbdata);

/* define a callback function by which event handlers can notify
 * us that they have completed their action, and pass along any
 * further information for subsequent handlers */
typedef void (*opal_pmix_notification_complete_fn_t)(int status, opal_list_t *results,
                                                     opal_pmix_op_cbfunc_t cbfunc, void *thiscbdata,
                                                     void *notification_cbdata);

/* define a callback function for the evhandler. Upon receipt of an
 * event notification, the active module will execute the specified notification
 * callback function, providing:
 *
 * status - the error that occurred
 * source - identity of the proc that generated the event
 * info - any additional info provided regarding the error.
 * results - any info from prior event handlers
 * cbfunc - callback function to execute when the evhandler is
 *          finished with the provided data so it can be released
 * cbdata - pointer to be returned in cbfunc
 *
 * Note that different resource managers may provide differing levels
 * of support for event notification to application processes. Thus, the
 * info list may be NULL or may contain detailed information of the event.
 * It is the responsibility of the application to parse any provided info array
 * for defined key-values if it so desires.
 *
 * Possible uses of the opal_value_t list include:
 *
 * - for the RM to alert the process as to planned actions, such as
 *   to abort the session, in response to the reported event
 *
 * - provide a timeout for alternative action to occur, such as for
 *   the application to request an alternate response to the event
 *
 * For example, the RM might alert the application to the failure of
 * a node that resulted in termination of several processes, and indicate
 * that the overall session will be aborted unless the application
 * requests an alternative behavior in the next 5 seconds. The application
 * then has time to respond with a checkpoint request, or a request to
 * recover from the failure by obtaining replacement nodes and restarting
 * from some earlier checkpoint.
 *
 * Support for these options is left to the discretion of the host RM. Info
 * keys are included in the common definions above, but also may be augmented
 * on a per-RM basis.
 *
 * On the server side, the notification function is used to inform the host
 * server of a detected error in the PMIx subsystem and/or client */
typedef void (*opal_pmix_notification_fn_t)(int status,
                                            const opal_process_name_t *source,
                                            opal_list_t *info, opal_list_t *results,
                                            opal_pmix_notification_complete_fn_t cbfunc,
                                            void *cbdata);

/* define a callback function for calls to register_evhandler. The
 * status indicates if the request was successful or not, evhandler_ref is
 * a size_t reference assigned to the evhandler by PMIX, this reference
 * must be used to deregister the err handler. A ptr to the original
 * cbdata is returned. */
typedef void (*opal_pmix_evhandler_reg_cbfunc_t)(int status,
                                                 size_t evhandler_ref,
                                                 void *cbdata);

/* define a callback function for calls to get_nb. The status
 * indicates if the requested data was found or not - a pointer to the
 * opal_value_t structure containing the found data is returned. The
 * pointer will be NULL if the requested data was not found. */
typedef void (*opal_pmix_value_cbfunc_t)(int status,
                                         opal_value_t *kv, void *cbdata);


/* define a callback function for calls to PMIx_Query. The status
 * indicates if requested data was found or not - a list of
 * opal_value_t will contain the key/value pairs. */
typedef void (*opal_pmix_info_cbfunc_t)(int status,
                                        opal_list_t *info,
                                        void *cbdata,
                                        opal_pmix_release_cbfunc_t release_fn,
                                        void *release_cbdata);

/* Callback function for incoming tool connections - the host
 * RTE shall provide a jobid/rank for the connecting tool. We
 * assume that a rank=0 will be the normal assignment, but allow
 * for the future possibility of a parallel set of tools
 * connecting, and thus each proc requiring a rank */
typedef void (*opal_pmix_tool_connection_cbfunc_t)(int status,
                                                   opal_process_name_t proc,
                                                   void *cbdata);


END_C_DECLS

#endif

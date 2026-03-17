/* include/pmix_common.h.  Generated from pmix_common.h.in by configure.  */
/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2019 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2018 Mellanox Technologies, Inc.
 *                         All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer listed
 *   in this license in the documentation and/or other materials
 *   provided with the distribution.
 *
 * - Neither the name of the copyright holders nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * The copyright holders provide no reassurances that the source code
 * provided does not infringe any patent, copyright, or any other
 * intellectual property rights of third parties.  The copyright holders
 * disclaim any liability to any recipient for claims brought against
 * recipient by any third party for infringement of that parties
 * intellectual property rights.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIx_COMMON_H
#define PMIx_COMMON_H

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h> /* for struct timeval */
#include <unistd.h> /* for uid_t and gid_t */
#include <sys/types.h> /* for uid_t and gid_t */

/* Whether C compiler supports -fvisibility */
#define PMIX_HAVE_VISIBILITY 0

#if PMIX_HAVE_VISIBILITY == 1
#define PMIX_EXPORT __attribute__((__visibility__("default")))
#else
#define PMIX_EXPORT
#endif


#include <pmix_rename.h>
#include <pmix_version.h>

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

/****  PMIX CONSTANTS    ****/

/* define maximum value and key sizes */
#define PMIX_MAX_NSLEN     255
#define PMIX_MAX_KEYLEN    511

/* define abstract types for namespaces and keys */
typedef char pmix_nspace_t[PMIX_MAX_NSLEN+1];
typedef char pmix_key_t[PMIX_MAX_KEYLEN+1];

/* define a type for rank values */
typedef uint32_t pmix_rank_t;

/* define a value for requests for job-level data
 * where the info itself isn't associated with any
 * specific rank, or when a request involves
 * a rank that isn't known - e.g., when someone requests
 * info thru one of the legacy interfaces where the rank
 * is typically encoded into the key itself since there is
 * no rank parameter in the API itself */
#define PMIX_RANK_UNDEF     UINT32_MAX
/* define a value to indicate that the user wants the
 * data for the given key from every rank that posted
 * that key */
#define PMIX_RANK_WILDCARD  UINT32_MAX-1
/* other special rank values will be used to define
 * groups of ranks for use in collectives */
#define PMIX_RANK_LOCAL_NODE    UINT32_MAX-2        // all ranks on local node
/* define an invalid value */
#define PMIX_RANK_INVALID   UINT32_MAX-3
/* define a boundary for valid ranks */
#define PMIX_RANK_VALID         UINT32_MAX-50


/****  PMIX ENVIRONMENTAL PARAMETERS  ****/
/* There are a few environmental parameters used by PMIx for
 * various operations. While there is no "definition" of them
 * as values, we do record them here for informational purposes.
 *
 * PMIX_LAUNCHER_PAUSE_FOR_TOOL - if set to non-zero value, instructs
 * launchers (e.g., "prun") to stop prior to spawning the application until
 * a tool can connect with further instructions. This envar will be
 * set by the tool and is _not_ intended for the direct use of users.
 *
 * PMIX_LAUNCHER_RENDEZVOUS_FILE - if set, contains the full pathname
 * of a file the launcher is to write that contains its connection info.
 * Works in addition to anything else the launcher may output.
 */

/* define a set of "standard" PMIx attributes that can
 * be queried. Implementations (and users) are free to extend as
 * desired, so the get functions need to be capable
 * of handling the "not found" condition. Note that these
 * are attributes of the system and the job as opposed to
 * values the application (or underlying MPI library)
 * might choose to expose - i.e., they are values provided
 * by the resource manager as opposed to the application. Thus,
 * these keys are RESERVED */
#define PMIX_ATTR_UNDEF      NULL

/* initialization attributes */
#define PMIX_EVENT_BASE                     "pmix.evbase"           // (struct event_base *) pointer to libevent event_base to use in place
                                                                    //                       of the internal progress thread
#define PMIX_SERVER_TOOL_SUPPORT            "pmix.srvr.tool"        // (bool) The host RM wants to declare itself as willing to
                                                                    //        accept tool connection requests
#define PMIX_SERVER_REMOTE_CONNECTIONS      "pmix.srvr.remote"      // (bool) Allow connections from remote tools (do not use loopback device)
#define PMIX_SERVER_SYSTEM_SUPPORT          "pmix.srvr.sys"         // (bool) The host RM wants to declare itself as being the local
                                                                    //        system server for PMIx connection requests
#define PMIX_SERVER_TMPDIR                  "pmix.srvr.tmpdir"      // (char*) temp directory where PMIx server will place
                                                                    //        client rendezvous points and contact info
#define PMIX_SYSTEM_TMPDIR                  "pmix.sys.tmpdir"       // (char*) temp directory for this system, where PMIx
                                                                    //        server will place tool rendezvous points and contact info
#define PMIX_SERVER_ENABLE_MONITORING       "pmix.srv.monitor"      // (bool) Enable PMIx internal monitoring by server
#define PMIX_SERVER_NSPACE                  "pmix.srv.nspace"       // (char*) Name of the nspace to use for this server
#define PMIX_SERVER_RANK                    "pmix.srv.rank"         // (pmix_rank_t) Rank of this server
#define PMIX_SERVER_GATEWAY                 "pmix.srv.gway"         // (bool) Server is acting as a gateway for PMIx requests
                                                                    //        that cannot be serviced on backend nodes
                                                                    //        (e.g., logging to email)

/* tool-related attributes */
#define PMIX_TOOL_NSPACE                    "pmix.tool.nspace"      // (char*) Name of the nspace to use for this tool
#define PMIX_TOOL_RANK                      "pmix.tool.rank"        // (uint32_t) Rank of this tool
#define PMIX_SERVER_PIDINFO                 "pmix.srvr.pidinfo"     // (pid_t) pid of the target server for a tool
#define PMIX_CONNECT_TO_SYSTEM              "pmix.cnct.sys"         // (bool) The requestor requires that a connection be made only to
                                                                    //        a local system-level PMIx server
#define PMIX_CONNECT_SYSTEM_FIRST           "pmix.cnct.sys.first"   // (bool) Preferentially look for a system-level PMIx server first
#define PMIX_SERVER_URI                     "pmix.srvr.uri"         // (char*) URI of server to be contacted
#define PMIX_SERVER_HOSTNAME                "pmix.srvr.host"        // (char*) node where target server is located
#define PMIX_CONNECT_MAX_RETRIES            "pmix.tool.mretries"    // (uint32_t) maximum number of times to try to connect to server
#define PMIX_CONNECT_RETRY_DELAY            "pmix.tool.retry"       // (uint32_t) time in seconds between connection attempts
#define PMIX_TOOL_DO_NOT_CONNECT            "pmix.tool.nocon"       // (bool) the tool wants to use internal PMIx support, but does
                                                                    //        not want to connect to a PMIx server
                                                                    //        from the specified processes to this tool
#define PMIX_RECONNECT_SERVER               "pmix.cnct.recon"       // (bool) tool is requesting to change server connections
#define PMIX_LAUNCHER                       "pmix.tool.launcher"    // (bool) tool is a launcher and needs rendezvous files created
#define PMIX_LAUNCHER_RENDEZVOUS_FILE       "pmix.tool.lncrnd"      // (char*) Pathname of file where connection info is to be stored

/* identification attributes */
#define PMIX_USERID                         "pmix.euid"             // (uint32_t) effective user id
#define PMIX_GRPID                          "pmix.egid"             // (uint32_t) effective group id
#define PMIX_DSTPATH                        "pmix.dstpath"          // (char*) path to dstore files
#define PMIX_VERSION_INFO                   "pmix.version"          // (char*) PMIx version of contactor
#define PMIX_REQUESTOR_IS_TOOL              "pmix.req.tool"         // (bool) requesting process is a tool
#define PMIX_REQUESTOR_IS_CLIENT            "pmix.req.client"       // (bool) requesting process is a client process

/* model attributes */
#define PMIX_PROGRAMMING_MODEL              "pmix.pgm.model"        // (char*) programming model being initialized (e.g., "MPI" or "OpenMP")
#define PMIX_MODEL_LIBRARY_NAME             "pmix.mdl.name"         // (char*) programming model implementation ID (e.g., "OpenMPI" or "MPICH")
#define PMIX_MODEL_LIBRARY_VERSION          "pmix.mld.vrs"          // (char*) programming model version string (e.g., "2.1.1")
#define PMIX_THREADING_MODEL                "pmix.threads"          // (char*) threading model used (e.g., "pthreads")
#define PMIX_MODEL_NUM_THREADS              "pmix.mdl.nthrds"       // (uint64_t) number of active threads being used by the model
#define PMIX_MODEL_NUM_CPUS                 "pmix.mdl.ncpu"         // (uint64_t) number of cpus being used by the model
#define PMIX_MODEL_CPU_TYPE                 "pmix.mdl.cputype"      // (char*) granularity - "hwthread", "core", etc.
#define PMIX_MODEL_PHASE_NAME               "pmix.mdl.phase"        // (char*) user-assigned name for a phase in the application execution - e.g.,
                                                                    //         "cfd reduction"
#define PMIX_MODEL_PHASE_TYPE               "pmix.mdl.ptype"        // (char*) type of phase being executed - e.g., "matrix multiply"
#define PMIX_MODEL_AFFINITY_POLICY          "pmix.mdl.tap"          // (char*) thread affinity policy - e.g.:
                                                                    //           "master" (thread co-located with master thread),
                                                                    //           "close" (thread located on cpu close to master thread)
                                                                    //           "spread" (threads load-balanced across available cpus)

/* attributes for the USOCK rendezvous socket  */
#define PMIX_USOCK_DISABLE                  "pmix.usock.disable"    // (bool) disable legacy usock support
#define PMIX_SOCKET_MODE                    "pmix.sockmode"         // (uint32_t) POSIX mode_t (9 bits valid)
#define PMIX_SINGLE_LISTENER                "pmix.sing.listnr"      // (bool) use only one rendezvous socket, letting priorities and/or
                                                                    //        MCA param select the active transport

/* attributes for TCP connections */
#define PMIX_TCP_REPORT_URI                 "pmix.tcp.repuri"       // (char*) output URI - '-' => stdout, '+' => stderr, or filename
#define PMIX_TCP_URI                        "pmix.tcp.uri"          // (char*) URI of server to connect to, or file:<name of file containing it>
#define PMIX_TCP_IF_INCLUDE                 "pmix.tcp.ifinclude"    // (char*) comma-delimited list of devices and/or CIDR notation
#define PMIX_TCP_IF_EXCLUDE                 "pmix.tcp.ifexclude"    // (char*) comma-delimited list of devices and/or CIDR notation
#define PMIX_TCP_IPV4_PORT                  "pmix.tcp.ipv4"         // (int) IPv4 port to be used
#define PMIX_TCP_IPV6_PORT                  "pmix.tcp.ipv6"         // (int) IPv6 port to be used
#define PMIX_TCP_DISABLE_IPV4               "pmix.tcp.disipv4"      // (bool) true to disable IPv4 family
#define PMIX_TCP_DISABLE_IPV6               "pmix.tcp.disipv6"      // (bool) true to disable IPv6 family


/* attributes for GDS */
#define PMIX_GDS_MODULE                     "pmix.gds.mod"          // (char*) comma-delimited string of desired modules


/* general proc-level attributes */
#define PMIX_CPUSET                         "pmix.cpuset"           // (char*) hwloc bitmap applied to proc upon launch
#define PMIX_CREDENTIAL                     "pmix.cred"             // (char*) security credential assigned to proc
#define PMIX_SPAWNED                        "pmix.spawned"          // (bool) true if this proc resulted from a call to PMIx_Spawn
#define PMIX_ARCH                           "pmix.arch"             // (uint32_t) datatype architecture flag

/* scratch directory locations for use by applications */
#define PMIX_TMPDIR                         "pmix.tmpdir"           // (char*) top-level tmp dir assigned to session
#define PMIX_NSDIR                          "pmix.nsdir"            // (char*) sub-tmpdir assigned to namespace
#define PMIX_PROCDIR                        "pmix.pdir"             // (char*) sub-nsdir assigned to proc
#define PMIX_TDIR_RMCLEAN                   "pmix.tdir.rmclean"     // (bool)  Resource Manager will clean session directories

/* information about relative ranks as assigned by the RM */
#define PMIX_CLUSTER_ID                     "pmix.clid"             // (char*) a string name for the cluster this proc is executing on
#define PMIX_PROCID                         "pmix.procid"           // (pmix_proc_t*) process identifier
#define PMIX_NSPACE                         "pmix.nspace"           // (char*) nspace of a job
#define PMIX_JOBID                          "pmix.jobid"            // (char*) jobid assigned by scheduler
#define PMIX_APPNUM                         "pmix.appnum"           // (uint32_t) app number within the job
#define PMIX_RANK                           "pmix.rank"             // (pmix_rank_t) process rank within the job
#define PMIX_GLOBAL_RANK                    "pmix.grank"            // (pmix_rank_t) rank spanning across all jobs in this session
#define PMIX_APP_RANK                       "pmix.apprank"          // (pmix_rank_t) rank within this app
#define PMIX_NPROC_OFFSET                   "pmix.offset"           // (pmix_rank_t) starting global rank of this job
#define PMIX_LOCAL_RANK                     "pmix.lrank"            // (uint16_t) rank on this node within this job
#define PMIX_NODE_RANK                      "pmix.nrank"            // (uint16_t) rank on this node spanning all jobs
#define PMIX_LOCALLDR                       "pmix.lldr"             // (pmix_rank_t) lowest rank on this node within this job
#define PMIX_APPLDR                         "pmix.aldr"             // (pmix_rank_t) lowest rank in this app within this job
#define PMIX_PROC_PID                       "pmix.ppid"             // (pid_t) pid of specified proc
#define PMIX_SESSION_ID                     "pmix.session.id"       // (uint32_t) session identifier

#define PMIX_NODE_LIST                      "pmix.nlist"            // (char*) comma-delimited list of nodes running procs for the specified nspace
#define PMIX_ALLOCATED_NODELIST             "pmix.alist"            // (char*) comma-delimited list of all nodes in this allocation regardless of
                                                                    //         whether or not they currently host procs.
#define PMIX_HOSTNAME                       "pmix.hname"            // (char*) name of the host the specified proc is on
#define PMIX_NODEID                         "pmix.nodeid"           // (uint32_t) node identifier where the specified proc is located
#define PMIX_LOCAL_PEERS                    "pmix.lpeers"           // (char*) comma-delimited string of ranks on this node within the specified nspace
#define PMIX_LOCAL_PROCS                    "pmix.lprocs"           // (pmix_proc_t array) array of pmix_proc_t of procs on the specified node
#define PMIX_LOCAL_CPUSETS                  "pmix.lcpus"            // (char*) colon-delimited cpusets of local peers within the specified nspace
#define PMIX_PROC_URI                       "pmix.puri"             // (char*) URI containing contact info for proc
#define PMIX_LOCALITY                       "pmix.loc"              // (uint16_t) relative locality of two procs
#define PMIX_PARENT_ID                      "pmix.parent"           // (pmix_proc_t*) identifier of the process that called PMIx_Spawn
                                                                    //                to launch this proc's application
#define PMIX_EXIT_CODE                      "pmix.exit.code"        // (int) exit code returned when proc terminated


/* size info */
#define PMIX_UNIV_SIZE                      "pmix.univ.size"        // (uint32_t) #procs in this nspace
#define PMIX_JOB_SIZE                       "pmix.job.size"         // (uint32_t) #procs in this job
#define PMIX_JOB_NUM_APPS                   "pmix.job.napps"        // (uint32_t) #apps in this job
#define PMIX_APP_SIZE                       "pmix.app.size"         // (uint32_t) #procs in this application
#define PMIX_LOCAL_SIZE                     "pmix.local.size"       // (uint32_t) #procs in this job on this node
#define PMIX_NODE_SIZE                      "pmix.node.size"        // (uint32_t) #procs across all jobs on this node
#define PMIX_MAX_PROCS                      "pmix.max.size"         // (uint32_t) max #procs for this job
#define PMIX_NUM_NODES                      "pmix.num.nodes"        // (uint32_t) #nodes in this nspace


/* Memory info */
#define PMIX_AVAIL_PHYS_MEMORY              "pmix.pmem"             // (uint64_t) total available physical memory on this node
#define PMIX_DAEMON_MEMORY                  "pmix.dmn.mem"          // (float) Mbytes of memory currently used by daemon
#define PMIX_CLIENT_AVG_MEMORY              "pmix.cl.mem.avg"       // (float) Average Mbytes of memory used by client processes


/* topology info */
#define PMIX_NET_TOPO                       "pmix.ntopo"            // (char*) xml-representation of network topology
#define PMIX_LOCAL_TOPO                     "pmix.ltopo"            // (char*) xml-representation of local node topology
#define PMIX_TOPOLOGY                       "pmix.topo"             // (hwloc_topology_t) pointer to the PMIx client's internal topology object
#define PMIX_TOPOLOGY_XML                   "pmix.topo.xml"         // (char*) XML-based description of topology
#define PMIX_TOPOLOGY_FILE                  "pmix.topo.file"        // (char*) full path to file containing XML topology description
#define PMIX_TOPOLOGY_SIGNATURE             "pmix.toposig"          // (char*) topology signature string
#define PMIX_LOCALITY_STRING                "pmix.locstr"           // (char*) string describing a proc's location
#define PMIX_HWLOC_SHMEM_ADDR               "pmix.hwlocaddr"        // (size_t) address of HWLOC shared memory segment
#define PMIX_HWLOC_SHMEM_SIZE               "pmix.hwlocsize"        // (size_t) size of HWLOC shared memory segment
#define PMIX_HWLOC_SHMEM_FILE               "pmix.hwlocfile"        // (char*) path to HWLOC shared memory file
#define PMIX_HWLOC_XML_V1                   "pmix.hwlocxml1"        // (char*) XML representation of local topology using HWLOC v1.x format
#define PMIX_HWLOC_XML_V2                   "pmix.hwlocxml2"        // (char*) XML representation of local topology using HWLOC v2.x format
#define PMIX_HWLOC_SHARE_TOPO               "pmix.hwlocsh"          // (bool) Share the HWLOC topology via shared memory
#define PMIX_HWLOC_HOLE_KIND                "pmix.hwlocholek"       // (char*) Kind of VM "hole" HWLOC should use for shared memory


/* request-related info */
#define PMIX_COLLECT_DATA                   "pmix.collect"          // (bool) collect data and return it at the end of the operation
#define PMIX_TIMEOUT                        "pmix.timeout"          // (int) time in sec before specified operation should time out (0 => infinite)
#define PMIX_IMMEDIATE                      "pmix.immediate"        // (bool) specified operation should immediately return an error from the PMIx
                                                                    //        server if requested data cannot be found - do not request it from
                                                                    //        the host RM
#define PMIX_WAIT                           "pmix.wait"             // (int) caller requests that the server wait until at least the specified
                                                                    //       #values are found (0 => all and is the default)
#define PMIX_COLLECTIVE_ALGO                "pmix.calgo"            // (char*) comma-delimited list of algorithms to use for collective
#define PMIX_COLLECTIVE_ALGO_REQD           "pmix.calreqd"          // (bool) if true, indicates that the requested choice of algo is mandatory
#define PMIX_NOTIFY_COMPLETION              "pmix.notecomp"         // (bool) notify parent process upon termination of child job
#define PMIX_RANGE                          "pmix.range"            // (pmix_data_range_t) value for calls to publish/lookup/unpublish or for
                                                                    //        monitoring event notifications
#define PMIX_PERSISTENCE                    "pmix.persist"          // (pmix_persistence_t) value for calls to publish
#define PMIX_DATA_SCOPE                     "pmix.scope"            // (pmix_scope_t) scope of the data to be found in a PMIx_Get call
#define PMIX_OPTIONAL                       "pmix.optional"         // (bool) look only in the client's local data store for the requested value - do
                                                                    //        not request data from the server if not found
#define PMIX_EMBED_BARRIER                  "pmix.embed.barrier"    // (bool) execute a blocking fence operation before executing the
                                                                    //        specified operation
#define PMIX_JOB_TERM_STATUS                "pmix.job.term.status"  // (pmix_status_t) status returned upon job termination
#define PMIX_PROC_STATE_STATUS              "pmix.proc.state"       // (pmix_proc_state_t) process state


/* attributes used by host server to pass data to the server convenience library - the
 * data will then be parsed and provided to the local clients */
#define PMIX_REGISTER_NODATA                "pmix.reg.nodata"       // (bool) Registration is for nspace only, do not copy job data
#define PMIX_PROC_DATA                      "pmix.pdata"            // (pmix_data_array_t*) starts with rank, then contains more data
#define PMIX_NODE_MAP                       "pmix.nmap"             // (char*) regex of nodes containing procs for this job
#define PMIX_PROC_MAP                       "pmix.pmap"             // (char*) regex describing procs on each node within this job
#define PMIX_ANL_MAP                        "pmix.anlmap"           // (char*) process mapping in ANL notation (used in PMI-1/PMI-2)
#define PMIX_APP_MAP_TYPE                   "pmix.apmap.type"       // (char*) type of mapping used to layout the application (e.g., cyclic)
#define PMIX_APP_MAP_REGEX                  "pmix.apmap.regex"      // (char*) regex describing the result of the mapping


/* attributes used internally to communicate data from the server to the client */
#define PMIX_PROC_BLOB                      "pmix.pblob"            // (pmix_byte_object_t) packed blob of process data
#define PMIX_MAP_BLOB                       "pmix.mblob"            // (pmix_byte_object_t) packed blob of process location


/* event handler registration and notification info keys */
#define PMIX_EVENT_HDLR_NAME                "pmix.evname"           // (char*) string name identifying this handler
#define PMIX_EVENT_HDLR_FIRST               "pmix.evfirst"          // (bool) invoke this event handler before any other handlers
#define PMIX_EVENT_HDLR_LAST                "pmix.evlast"           // (bool) invoke this event handler after all other handlers have been called
#define PMIX_EVENT_HDLR_FIRST_IN_CATEGORY   "pmix.evfirstcat"       // (bool) invoke this event handler before any other handlers in this category
#define PMIX_EVENT_HDLR_LAST_IN_CATEGORY    "pmix.evlastcat"        // (bool) invoke this event handler after all other handlers in this category have been called
#define PMIX_EVENT_HDLR_BEFORE              "pmix.evbefore"         // (char*) put this event handler immediately before the one specified in the (char*) value
#define PMIX_EVENT_HDLR_AFTER               "pmix.evafter"          // (char*) put this event handler immediately after the one specified in the (char*) value
#define PMIX_EVENT_HDLR_PREPEND             "pmix.evprepend"        // (bool) prepend this handler to the precedence list within its category
#define PMIX_EVENT_HDLR_APPEND              "pmix.evappend"         // (bool) append this handler to the precedence list within its category
#define PMIX_EVENT_CUSTOM_RANGE             "pmix.evrange"          // (pmix_data_array_t*) array of pmix_proc_t defining range of event notification
#define PMIX_EVENT_AFFECTED_PROC            "pmix.evproc"           // (pmix_proc_t*) single proc that was affected
#define PMIX_EVENT_AFFECTED_PROCS           "pmix.evaffected"       // (pmix_data_array_t*) array of pmix_proc_t defining affected procs
#define PMIX_EVENT_NON_DEFAULT              "pmix.evnondef"         // (bool) event is not to be delivered to default event handlers
#define PMIX_EVENT_RETURN_OBJECT            "pmix.evobject"         // (void*) object to be returned whenever the registered cbfunc is invoked
                                                                    //     NOTE: the object will _only_ be returned to the process that
                                                                    //           registered it
#define PMIX_EVENT_DO_NOT_CACHE             "pmix.evnocache"        // (bool) instruct the PMIx server not to cache the event
#define PMIX_EVENT_SILENT_TERMINATION       "pmix.evsilentterm"     // (bool) do not generate an event when this job normally terminates
#define PMIX_EVENT_PROXY                    "pmix.evproxy"          // (pmix_proc_t*) PMIx server that sourced the event
#define PMIX_EVENT_TEXT_MESSAGE             "pmix.evtext"           // (char*) text message suitable for output by recipient - e.g., describing
                                                                    //         the cause of the event

/* fault tolerance-related events */
#define PMIX_EVENT_TERMINATE_SESSION        "pmix.evterm.sess"      // (bool) RM intends to terminate session
#define PMIX_EVENT_TERMINATE_JOB            "pmix.evterm.job"       // (bool) RM intends to terminate this job
#define PMIX_EVENT_TERMINATE_NODE           "pmix.evterm.node"      // (bool) RM intends to terminate all procs on this node
#define PMIX_EVENT_TERMINATE_PROC           "pmix.evterm.proc"      // (bool) RM intends to terminate just this process
#define PMIX_EVENT_ACTION_TIMEOUT           "pmix.evtimeout"        // (int) time in sec before RM will execute error response
#define PMIX_EVENT_NO_TERMINATION           "pmix.evnoterm"         // (bool) indicates that the handler has satisfactorily handled
                                                                    //        the event and believes termination of the application is not required
#define PMIX_EVENT_WANT_TERMINATION         "pmix.evterm"           // (bool) indicates that the handler has determined that the
                                                                    //        application should be terminated


/* attributes used to describe "spawn" directives */
#define PMIX_PERSONALITY                    "pmix.pers"             // (char*) name of personality to use
#define PMIX_HOST                           "pmix.host"             // (char*) comma-delimited list of hosts to use for spawned procs
#define PMIX_HOSTFILE                       "pmix.hostfile"         // (char*) hostfile to use for spawned procs
#define PMIX_ADD_HOST                       "pmix.addhost"          // (char*) comma-delimited list of hosts to add to allocation
#define PMIX_ADD_HOSTFILE                   "pmix.addhostfile"      // (char*) hostfile to add to existing allocation
#define PMIX_PREFIX                         "pmix.prefix"           // (char*) prefix to use for starting spawned procs
#define PMIX_WDIR                           "pmix.wdir"             // (char*) working directory for spawned procs
#define PMIX_MAPPER                         "pmix.mapper"           // (char*) mapper to use for placing spawned procs
#define PMIX_DISPLAY_MAP                    "pmix.dispmap"          // (bool) display process map upon spawn
#define PMIX_PPR                            "pmix.ppr"              // (char*) #procs to spawn on each identified resource
#define PMIX_MAPBY                          "pmix.mapby"            // (char*) mapping policy
#define PMIX_RANKBY                         "pmix.rankby"           // (char*) ranking policy
#define PMIX_BINDTO                         "pmix.bindto"           // (char*) binding policy
#define PMIX_PRELOAD_BIN                    "pmix.preloadbin"       // (bool) preload binaries
#define PMIX_PRELOAD_FILES                  "pmix.preloadfiles"     // (char*) comma-delimited list of files to pre-position
#define PMIX_NON_PMI                        "pmix.nonpmi"           // (bool) spawned procs will not call PMIx_Init
#define PMIX_STDIN_TGT                      "pmix.stdin"            // (pmix_proc_t*) proc that is to receive stdin
                                                                    //                (PMIX_RANK_WILDCARD = all in given nspace)
#define PMIX_DEBUGGER_DAEMONS               "pmix.debugger"         // (bool) spawned app consists of debugger daemons
#define PMIX_COSPAWN_APP                    "pmix.cospawn"          // (bool) designated app is to be spawned as a disconnected
                                                                    //        job - i.e., not part of the "comm_world" of the job
#define PMIX_SET_SESSION_CWD                "pmix.ssncwd"           // (bool) set the application's current working directory to
                                                                    //        the session working directory assigned by the RM
#define PMIX_TAG_OUTPUT                     "pmix.tagout"           // (bool) tag application output with the ID of the source
#define PMIX_TIMESTAMP_OUTPUT               "pmix.tsout"            // (bool) timestamp output from applications
#define PMIX_MERGE_STDERR_STDOUT            "pmix.mergeerrout"      // (bool) merge stdout and stderr streams from application procs
#define PMIX_OUTPUT_TO_FILE                 "pmix.outfile"          // (char*) output application output to given file
#define PMIX_INDEX_ARGV                     "pmix.indxargv"         // (bool) mark the argv with the rank of the proc
#define PMIX_CPUS_PER_PROC                  "pmix.cpuperproc"       // (uint32_t) #cpus to assign to each rank
#define PMIX_NO_PROCS_ON_HEAD               "pmix.nolocal"          // (bool) do not place procs on the head node
#define PMIX_NO_OVERSUBSCRIBE               "pmix.noover"           // (bool) do not oversubscribe the cpus
#define PMIX_REPORT_BINDINGS                "pmix.repbind"          // (bool) report bindings of the individual procs
#define PMIX_CPU_LIST                       "pmix.cpulist"          // (char*) list of cpus to use for this job
#define PMIX_JOB_RECOVERABLE                "pmix.recover"          // (bool) application supports recoverable operations
#define PMIX_JOB_CONTINUOUS                 "pmix.continuous"       // (bool) application is continuous, all failed procs should
                                                                        //        be immediately restarted
#define PMIX_MAX_RESTARTS                   "pmix.maxrestarts"      // (uint32_t) max number of times to restart a job
#define PMIX_FWD_STDIN                      "pmix.fwd.stdin"        // (bool) forward the stdin from this process to the target processes
#define PMIX_FWD_STDOUT                     "pmix.fwd.stdout"       // (bool) forward stdout from the spawned processes to this process (typically used by a tool)
#define PMIX_FWD_STDERR                     "pmix.fwd.stderr"       // (bool) forward stderr from the spawned processes to this process (typically used by a tool)
#define PMIX_FWD_STDDIAG                    "pmix.fwd.stddiag"      // (bool) if a diagnostic channel exists, forward any output on it
                                                                    //        from the spawned processes to this process (typically used by a tool)
#define PMIX_SPAWN_TOOL                     "pmix.spwn.tool"        // (bool) job being spawned is a tool
#define PMIX_CMD_LINE                       "pmix.cmd.line"         // (char*) command line executing in the specified nspace

/* query attributes */
#define PMIX_QUERY_REFRESH_CACHE            "pmix.qry.rfsh"         // (bool) retrieve updated information from server
                                                                    //        to update local cache
#define PMIX_QUERY_NAMESPACES               "pmix.qry.ns"           // (char*) return a comma-delimited list of active namespaces
#define PMIX_QUERY_NAMESPACE_INFO           "pmix.qry.nsinfo"       // (pmix_data_array_t) request an array of active nspace information - each
                                                                    //        element will contain an array including the namespace plus the
                                                                    //        command line of the application executing within it
#define PMIX_QUERY_JOB_STATUS               "pmix.qry.jst"          // (pmix_status_t) status of a specified currently executing job
#define PMIX_QUERY_QUEUE_LIST               "pmix.qry.qlst"         // (char*) request a comma-delimited list of scheduler queues
#define PMIX_QUERY_QUEUE_STATUS             "pmix.qry.qst"          // (TBD) status of a specified scheduler queue
#define PMIX_QUERY_PROC_TABLE               "pmix.qry.ptable"       // (char*) input nspace of job whose info is being requested
                                                                    //         returns (pmix_data_array_t*) an array of pmix_proc_info_t
#define PMIX_QUERY_LOCAL_PROC_TABLE         "pmix.qry.lptable"      // (char*) input nspace of job whose info is being requested
                                                                    //         returns (pmix_data_array_t*) an array of pmix_proc_info_t for
                                                                    //         procs in job on same node
#define PMIX_QUERY_AUTHORIZATIONS           "pmix.qry.auths"        // (bool) return operations tool is authorized to perform
#define PMIX_QUERY_SPAWN_SUPPORT            "pmix.qry.spawn"        // (bool) return a comma-delimited list of supported spawn attributes
#define PMIX_QUERY_DEBUG_SUPPORT            "pmix.qry.debug"        // (bool) return a comma-delimited list of supported debug attributes
#define PMIX_QUERY_MEMORY_USAGE             "pmix.qry.mem"          // (bool) return info on memory usage for the procs indicated in the qualifiers
#define PMIX_QUERY_LOCAL_ONLY               "pmix.qry.local"        // (bool) constrain the query to local information only
#define PMIX_QUERY_REPORT_AVG               "pmix.qry.avg"          // (bool) report average values
#define PMIX_QUERY_REPORT_MINMAX            "pmix.qry.minmax"       // (bool) report minimum and maximum value
#define PMIX_QUERY_ALLOC_STATUS             "pmix.query.alloc"      // (char*) string identifier of the allocation whose status
                                                                    //         is being requested
#define PMIX_TIME_REMAINING                 "pmix.time.remaining"   // (char*) query number of seconds (uint32_t) remaining in allocation
                                                                    //         for the specified nspace

/* information retrieval attributes */
#define PMIX_SESSION_INFO                   "pmix.ssn.info"         // (bool) Return information about the specified session. If information
                                                                    //        about a session other than the one containing the requesting
                                                                    //        process is desired, then the attribute array must contain a
                                                                    //        PMIX_SESSION_ID attribute identifying the desired target.
#define PMIX_JOB_INFO                       "pmix.job.info"         // (bool) Return information about the specified job or namespace. If
                                                                    //        information about a job or namespace other than the one containing
                                                                    //        the requesting process is desired, then the attribute array must
                                                                    //        contain a PMIX_JOBID or PMIX_NSPACE attribute identifying the
                                                                    //        desired target. Similarly, if information is requested about a
                                                                    //        job or namespace in a session other than the one containing the
                                                                    //        requesting process, then an attribute identifying the target
                                                                    //        session must be provided.
#define PMIX_APP_INFO                       "pmix.app.info"         // (bool) Return information about the specified application. If information
                                                                    //        about an application other than the one containing the requesting
                                                                    //        process is desired, then the attribute array must contain a
                                                                    //        PMIX_APPNUM attribute identifying the desired target. Similarly,
                                                                    //        if information is requested about an application in a job or session
                                                                    //        other than the one containing the requesting process, then attributes
                                                                    //        identifying the target job and/or session must be provided.
#define PMIX_NODE_INFO                      "pmix.node.info"        // (bool) Return information about the specified node. If information about a
                                                                    //        node other than the one containing the requesting process is desired,
                                                                    //        then the attribute array must contain either the PMIX_NODEID or
                                                                    //        PMIX_HOSTNAME attribute identifying the desired target.

/* information storage attributes */
#define PMIX_SESSION_INFO_ARRAY             "pmix.ssn.arr"          // (pmix_data_array_t) Provide an array of pmix_info_t containing
                                                                    //        session-level information. The PMIX_SESSION_ID attribute is required
                                                                    //        to be included in the array.
#define PMIX_JOB_INFO_ARRAY                 "pmix.job.arr"          // (pmix_data_array_t) Provide an array of pmix_info_t containing job-level
                                                                    //        information. Information is registered one job (aka namespace) at a time
                                                                    //        via the PMIx_server_register_nspace API. Thus, there is no requirement that
                                                                    //        the array contain either the PMIX_NSPACE or PMIX_JOBID attributes, though
                                                                    //        either or both of them may be included.
#define PMIX_APP_INFO_ARRAY                 "pmix.app.arr"          // (pmix_data_array_t) Provide an array of pmix_info_t containing app-level
                                                                    //        information. The PMIX_NSPACE or PMIX_JOBID attributes of the job containing
                                                                    //        the appplication, plus its PMIX_APPNUM attribute, are required to be
                                                                    //        included in the array.
#define PMIX_NODE_INFO_ARRAY                "pmix.node.arr"         // (pmix_data_array_t) Provide an array of pmix_info_t containing node-level
                                                                    //        information. At a minimum, either the PMIX_NODEID or PMIX_HOSTNAME
                                                                    //        attribute is required to be included in the array, though both may be
                                                                    //        included.

/* log attributes */
#define PMIX_LOG_SOURCE                     "pmix.log.source"       // (pmix_proc_t*) ID of source of the log request
#define PMIX_LOG_STDERR                     "pmix.log.stderr"       // (char*) log string to stderr
#define PMIX_LOG_STDOUT                     "pmix.log.stdout"       // (char*) log string to stdout
#define PMIX_LOG_SYSLOG                     "pmix.log.syslog"       // (char*) log message to syslog - defaults to ERROR priority. Will log
                                                                    //         to global syslog if available, otherwise to local syslog
#define PMIX_LOG_LOCAL_SYSLOG               "pmix.log.lsys"         // (char*) log msg to local syslog - defaults to ERROR priority
#define PMIX_LOG_GLOBAL_SYSLOG              "pmix.log.gsys"         // (char*) forward data to system "master" and log msg to that syslog
#define PMIX_LOG_SYSLOG_PRI                 "pmix.log.syspri"       // (int) syslog priority level

#define PMIX_LOG_TIMESTAMP                  "pmix.log.tstmp"        // (time_t) timestamp for log report
#define PMIX_LOG_GENERATE_TIMESTAMP         "pmix.log.gtstmp"       // (bool) generate timestamp for log
#define PMIX_LOG_TAG_OUTPUT                 "pmix.log.tag"          // (bool) label the output stream with the channel name (e.g., "stdout")
#define PMIX_LOG_TIMESTAMP_OUTPUT           "pmix.log.tsout"        // (bool) print timestamp in output string
#define PMIX_LOG_XML_OUTPUT                 "pmix.log.xml"          // (bool) print the output stream in xml format
#define PMIX_LOG_ONCE                       "pmix.log.once"         // (bool) only log this once with whichever channel can first support it
#define PMIX_LOG_MSG                        "pmix.log.msg"          // (pmix_byte_object_t) message blob to be sent somewhere

#define PMIX_LOG_EMAIL                      "pmix.log.email"        // (pmix_data_array_t*) log via email based on array of pmix_info_t
                                                                    //         containing directives
#define PMIX_LOG_EMAIL_ADDR                 "pmix.log.emaddr"       // (char*) comma-delimited list of email addresses that are to recv msg
#define PMIX_LOG_EMAIL_SENDER_ADDR          "pmix.log.emfaddr"      // (char*) return email address of sender
#define PMIX_LOG_EMAIL_SUBJECT              "pmix.log.emsub"        // (char*) subject line for email
#define PMIX_LOG_EMAIL_MSG                  "pmix.log.emmsg"        // (char*) msg to be included in email
#define PMIX_LOG_EMAIL_SERVER               "pmix.log.esrvr"        // (char*) hostname (or IP addr) of estmp server
#define PMIX_LOG_EMAIL_SRVR_PORT            "pmix.log.esrvrprt"     // (int32_t) port the email server is listening to

#define PMIX_LOG_GLOBAL_DATASTORE           "pmix.log.gstore"       // (bool)
#define PMIX_LOG_JOB_RECORD                 "pmix.log.jrec"         // (bool) log the provided information to the RM's job record


/* debugger attributes */
#define PMIX_DEBUG_STOP_ON_EXEC             "pmix.dbg.exec"         // (bool) job is being spawned under debugger - instruct it to pause on start
#define PMIX_DEBUG_STOP_IN_INIT             "pmix.dbg.init"         // (bool) instruct job to stop during PMIx init
#define PMIX_DEBUG_WAIT_FOR_NOTIFY          "pmix.dbg.notify"       // (bool) block at desired point until receiving debugger release notification
#define PMIX_DEBUG_JOB                      "pmix.dbg.job"          // (char*) nspace of the job assigned to this debugger to be debugged. Note
                                                                    //         that id's, pids, and other info on the procs is available
                                                                    //         via a query for the nspace's local or global proctable
#define PMIX_DEBUG_WAITING_FOR_NOTIFY       "pmix.dbg.waiting"      // (bool) job to be debugged is waiting for a release
#define PMIX_DEBUG_JOB_DIRECTIVES           "pmix.dbg.jdirs"        // (pmix_data_array_t*) array of job-level directives
#define PMIX_DEBUG_APP_DIRECTIVES           "pmix.dbg.adirs"        // (pmix_data_array_t*) array of app-level directives


/* Resource Manager identification */
#define PMIX_RM_NAME                        "pmix.rm.name"          // (char*) string name of the resource manager
#define PMIX_RM_VERSION                     "pmix.rm.version"       // (char*) RM version string

/* environmental variable operation attributes */
#define PMIX_SET_ENVAR                      "pmix.envar.set"          // (pmix_envar_t*) set the envar to the given value,
                                                                      //                 overwriting any pre-existing one
#define PMIX_ADD_ENVAR                      "pmix.envar.add"          // (pmix_envar_t*) add envar, but do not overwrite any existing one
#define PMIX_UNSET_ENVAR                    "pmix.envar.unset"        // (char*) unset the envar, if present
#define PMIX_PREPEND_ENVAR                  "pmix.envar.prepnd"       // (pmix_envar_t*) prepend the given value to the
                                                                      //                 specified envar using the separator
                                                                      //                 character, creating the envar if it doesn't already exist
#define PMIX_APPEND_ENVAR                   "pmix.envar.appnd"        // (pmix_envar_t*) append the given value to the specified
                                                                      //                 envar using the separator character,
                                                                      //                 creating the envar if it doesn't already exist

/* attributes relating to allocations */
#define PMIX_ALLOC_ID                       "pmix.alloc.id"         // (char*) provide a string identifier for this allocation request
                                                                    //         which can later be used to query status of the request
#define PMIX_ALLOC_NUM_NODES                "pmix.alloc.nnodes"     // (uint64_t) number of nodes
#define PMIX_ALLOC_NODE_LIST                "pmix.alloc.nlist"      // (char*) regex of specific nodes
#define PMIX_ALLOC_NUM_CPUS                 "pmix.alloc.ncpus"      // (uint64_t) number of cpus
#define PMIX_ALLOC_NUM_CPU_LIST             "pmix.alloc.ncpulist"   // (char*) regex of #cpus for each node
#define PMIX_ALLOC_CPU_LIST                 "pmix.alloc.cpulist"    // (char*) regex of specific cpus indicating the cpus involved.
#define PMIX_ALLOC_MEM_SIZE                 "pmix.alloc.msize"      // (float) number of Mbytes
#define PMIX_ALLOC_NETWORK                  "pmix.alloc.net"        // (pmix_data_array_t*) Array of pmix_info_t describing
                                                                    //         network resource request. This must include at least:
                                                                    //           * PMIX_ALLOC_NETWORK_ID
                                                                    //           * PMIX_ALLOC_NETWORK_TYPE
                                                                    //           * PMIX_ALLOC_NETWORK_ENDPTS
                                                                    //         plus whatever other descriptors are desired
#define PMIX_ALLOC_NETWORK_ID               "pmix.alloc.netid"      // (char*) key to be used when accessing this requested network allocation. The
                                                                    //         allocation will be returned/stored as a pmix_data_array_t of
                                                                    //         pmix_info_t indexed by this key and containing at least one
                                                                    //         entry with the same key and the allocated resource description.
                                                                    //         The type of the included value depends upon the network
                                                                    //         support. For example, a TCP allocation might consist of a
                                                                    //         comma-delimited string of socket ranges such as
                                                                    //         "32000-32100,33005,38123-38146". Additional entries will consist
                                                                    //         of any provided resource request directives, along with their
                                                                    //         assigned values. Examples include:
                                                                    //           * PMIX_ALLOC_NETWORK_TYPE - the type of resources provided
                                                                    //           * PMIX_ALLOC_NETWORK_PLANE - if applicable, what plane the
                                                                    //               resources were assigned from
                                                                    //           * PMIX_ALLOC_NETWORK_QOS - the assigned QoS
                                                                    //           * PMIX_ALLOC_BANDWIDTH - the allocated bandwidth
                                                                    //           * PMIX_ALLOC_NETWORK_SEC_KEY - a security key for the requested
                                                                    //               network allocation
                                                                    //         NOTE: the assigned values may differ from those requested,
                                                                    //         especially if the "required" flag was not set in the request
#define PMIX_ALLOC_BANDWIDTH                "pmix.alloc.bw"         // (float) Mbits/sec
#define PMIX_ALLOC_NETWORK_QOS              "pmix.alloc.netqos"     // (char*) quality of service level
#define PMIX_ALLOC_TIME                     "pmix.alloc.time"       // (uint32_t) time in seconds that the allocation shall remain valid
#define PMIX_ALLOC_NETWORK_TYPE             "pmix.alloc.nettype"    // (char*) type of desired transport (e.g., tcp, udp)
#define PMIX_ALLOC_NETWORK_PLANE            "pmix.alloc.netplane"   // (char*) id string for the NIC (aka plane) to be used for this allocation
                                                                    //         (e.g., CIDR for Ethernet)
#define PMIX_ALLOC_NETWORK_ENDPTS           "pmix.alloc.endpts"     // (size_t) number of endpoints to allocate per process
#define PMIX_ALLOC_NETWORK_ENDPTS_NODE      "pmix.alloc.endpts.nd"  // (size_t) number of endpoints to allocate per node
#define PMIX_ALLOC_NETWORK_SEC_KEY          "pmix.alloc.nsec"       // (pmix_byte_object_t) network security key


/* job control attributes */
#define PMIX_JOB_CTRL_ID                    "pmix.jctrl.id"         // (char*) provide a string identifier for this request
#define PMIX_JOB_CTRL_PAUSE                 "pmix.jctrl.pause"      // (bool) pause the specified processes
#define PMIX_JOB_CTRL_RESUME                "pmix.jctrl.resume"     // (bool) "un-pause" the specified processes
#define PMIX_JOB_CTRL_CANCEL                "pmix.jctrl.cancel"     // (char*) cancel the specified request
                                                                    //         (NULL => cancel all requests from this requestor)
#define PMIX_JOB_CTRL_KILL                  "pmix.jctrl.kill"       // (bool) forcibly terminate the specified processes and cleanup
#define PMIX_JOB_CTRL_RESTART               "pmix.jctrl.restart"    // (char*) restart the specified processes using the given checkpoint ID
#define PMIX_JOB_CTRL_CHECKPOINT            "pmix.jctrl.ckpt"       // (char*) checkpoint the specified processes and assign the given ID to it
#define PMIX_JOB_CTRL_CHECKPOINT_EVENT      "pmix.jctrl.ckptev"     // (bool) use event notification to trigger process checkpoint
#define PMIX_JOB_CTRL_CHECKPOINT_SIGNAL     "pmix.jctrl.ckptsig"    // (int) use the given signal to trigger process checkpoint
#define PMIX_JOB_CTRL_CHECKPOINT_TIMEOUT    "pmix.jctrl.ckptsig"    // (int) time in seconds to wait for checkpoint to complete
#define PMIX_JOB_CTRL_CHECKPOINT_METHOD     "pmix.jctrl.ckmethod"   // (pmix_data_array_t) array of pmix_info_t declaring each
                                                                    //      method and value supported by this application
#define PMIX_JOB_CTRL_SIGNAL                "pmix.jctrl.sig"        // (int) send given signal to specified processes
#define PMIX_JOB_CTRL_PROVISION             "pmix.jctrl.pvn"        // (char*) regex identifying nodes that are to be provisioned
#define PMIX_JOB_CTRL_PROVISION_IMAGE       "pmix.jctrl.pvnimg"     // (char*) name of the image that is to be provisioned
#define PMIX_JOB_CTRL_PREEMPTIBLE           "pmix.jctrl.preempt"    // (bool) job can be pre-empted
#define PMIX_JOB_CTRL_TERMINATE             "pmix.jctrl.term"       // (bool) politely terminate the specified procs
#define PMIX_REGISTER_CLEANUP               "pmix.reg.cleanup"      // (char*) comma-delimited list of files to
                                                                    //         be removed upon process termination
#define PMIX_REGISTER_CLEANUP_DIR           "pmix.reg.cleanupdir"   // (char*) comma-delimited list of directories to
                                                                    //         be removed upon process termination
#define PMIX_CLEANUP_RECURSIVE              "pmix.clnup.recurse"    // (bool) recursively cleanup all subdirectories under the
                                                                    //        specified one(s)
#define PMIX_CLEANUP_EMPTY                  "pmix.clnup.empty"      // (bool) only remove empty subdirectories
#define PMIX_CLEANUP_IGNORE                 "pmix.clnup.ignore"     // (char*) comma-delimited list of filenames that are not
                                                                    //         to be removed
#define PMIX_CLEANUP_LEAVE_TOPDIR           "pmix.clnup.lvtop"      // (bool) when recursively cleaning subdirs, do not remove
                                                                    //        the top-level directory (the one given in the
                                                                    //        cleanup request)

/* monitoring attributes */
#define PMIX_MONITOR_ID                     "pmix.monitor.id"       // (char*) provide a string identifier for this request
#define PMIX_MONITOR_CANCEL                 "pmix.monitor.cancel"   // (char*) identifier to be canceled (NULL = cancel all
                                                                    //         monitoring for this process)
#define PMIX_MONITOR_APP_CONTROL            "pmix.monitor.appctrl"  // (bool) the application desires to control the response to
                                                                    //        a monitoring event
#define PMIX_MONITOR_HEARTBEAT              "pmix.monitor.mbeat"    // (void) register to have the server monitor the requestor for heartbeats
#define PMIX_SEND_HEARTBEAT                 "pmix.monitor.beat"     // (void) send heartbeat to local server
#define PMIX_MONITOR_HEARTBEAT_TIME         "pmix.monitor.btime"    // (uint32_t) time in seconds before declaring heartbeat missed
#define PMIX_MONITOR_HEARTBEAT_DROPS        "pmix.monitor.bdrop"    // (uint32_t) number of heartbeats that can be missed before
                                                                    //            generating the event
#define PMIX_MONITOR_FILE                   "pmix.monitor.fmon"     // (char*) register to monitor file for signs of life
#define PMIX_MONITOR_FILE_SIZE              "pmix.monitor.fsize"    // (bool) monitor size of given file is growing to determine app is running
#define PMIX_MONITOR_FILE_ACCESS            "pmix.monitor.faccess"  // (char*) monitor time since last access of given file to determine app is running
#define PMIX_MONITOR_FILE_MODIFY            "pmix.monitor.fmod"     // (char*) monitor time since last modified of given file to determine app is running
#define PMIX_MONITOR_FILE_CHECK_TIME        "pmix.monitor.ftime"    // (uint32_t) time in seconds between checking file
#define PMIX_MONITOR_FILE_DROPS             "pmix.monitor.fdrop"    // (uint32_t) number of file checks that can be missed before
                                                                    //            generating the event

/* security attributes */
#define PMIX_CRED_TYPE                      "pmix.sec.ctype"        // (char*) when passed in PMIx_Get_credential, a prioritized,
                                                                    // comma-delimited list of desired credential types for use
                                                                    // in environments where multiple authentication mechanisms
                                                                    // may be available. When returned in a callback function, a
                                                                    // string identifier of the credential type
#define PMIX_CRYPTO_KEY                     "pmix.sec.key"          // (pmix_byte_object_t) blob containing crypto key


/* IO Forwarding Attributes */
#define PMIX_IOF_CACHE_SIZE                 "pmix.iof.csize"        // (uint32_t) requested size of the server cache in bytes for each specified channel.
                                                                    //            By default, the server is allowed (but not required) to drop
                                                                    //            all bytes received beyond the max size
#define PMIX_IOF_DROP_OLDEST                "pmix.iof.old"          // (bool) in an overflow situation, drop the oldest bytes to make room in the cache
#define PMIX_IOF_DROP_NEWEST                "pmix.iof.new"          // (bool) in an overflow situation, drop any new bytes received until room becomes
                                                                    //        available in the cache (default)
#define PMIX_IOF_BUFFERING_SIZE             "pmix.iof.bsize"        // (uint32_t) basically controls grouping of IO on the specified channel(s) to
                                                                    //            avoid being called every time a bit of IO arrives. The library
                                                                    //            will execute the callback whenever the specified number of bytes
                                                                    //            becomes available. Any remaining buffered data will be "flushed"
                                                                    //            upon call to deregister the respective channel
#define PMIX_IOF_BUFFERING_TIME             "pmix.iof.btime"        // (uint32_t) max time in seconds to buffer IO before delivering it. Used in conjunction
                                                                    //            with buffering size, this prevents IO from being held indefinitely
                                                                    //            while waiting for another payload to arrive
#define PMIX_IOF_COMPLETE                   "pmix.iof.cmp"          // (bool) indicates whether or not the specified IO channel has been closed
                                                                    //        by the source
#define PMIX_IOF_PUSH_STDIN                 "pmix.iof.stdin"        // (bool) Used by a tool to request that the PMIx library collect
                                                                    //        the tool's stdin and forward it to the procs specified in
                                                                    //        the PMIx_IOF_push call
#define PMIX_IOF_TAG_OUTPUT                 "pmix.iof.tag"          // (bool) Tag output with the channel it comes from
#define PMIX_IOF_TIMESTAMP_OUTPUT           "pmix.iof.ts"           // (bool) Timestamp output
#define PMIX_IOF_XML_OUTPUT                 "pmix.iof.xml"          // (bool) Format output in XML


/* Attributes for controlling contents of application setup data */
#define PMIX_SETUP_APP_ENVARS               "pmix.setup.env"        // (bool) harvest and include relevant envars
#define PMIX_SETUP_APP_NONENVARS            "pmix.setup.nenv"       // (bool) include all non-envar data
#define PMIX_SETUP_APP_ALL                  "pmix.setup.all"        // (bool) include all relevant data


/****    PROCESS STATE DEFINITIONS    ****/
typedef uint8_t pmix_proc_state_t;
#define PMIX_PROC_STATE_UNDEF                    0  /* undefined process state */
#define PMIX_PROC_STATE_PREPPED                  1  /* process is ready to be launched */
#define PMIX_PROC_STATE_LAUNCH_UNDERWAY          2  /* launch process underway */
#define PMIX_PROC_STATE_RESTART                  3  /* the proc is ready for restart */
#define PMIX_PROC_STATE_TERMINATE                4  /* process is marked for termination */
#define PMIX_PROC_STATE_RUNNING                  5  /* daemon has locally fork'd process */
#define PMIX_PROC_STATE_CONNECTED                6  /* proc connected to PMIx server */
/*
* Define a "boundary" so users can easily and quickly determine
* if a proc is still running or not - any value less than
* this one means that the proc has not terminated
*/
#define PMIX_PROC_STATE_UNTERMINATED            15

#define PMIX_PROC_STATE_TERMINATED              20  /* process has terminated and is no longer running */
/* Define a boundary so users can easily and quickly determine
* if a proc abnormally terminated - leave a little room
* for future expansion
*/
#define PMIX_PROC_STATE_ERROR                   50
/* Define specific error code values */
#define PMIX_PROC_STATE_KILLED_BY_CMD           (PMIX_PROC_STATE_ERROR +  1)  /* process was killed by cmd */
#define PMIX_PROC_STATE_ABORTED                 (PMIX_PROC_STATE_ERROR +  2)  /* process aborted */
#define PMIX_PROC_STATE_FAILED_TO_START         (PMIX_PROC_STATE_ERROR +  3)  /* process failed to start */
#define PMIX_PROC_STATE_ABORTED_BY_SIG          (PMIX_PROC_STATE_ERROR +  4)  /* process aborted by signal */
#define PMIX_PROC_STATE_TERM_WO_SYNC            (PMIX_PROC_STATE_ERROR +  5)  /* process exit'd w/o calling PMIx_Finalize */
#define PMIX_PROC_STATE_COMM_FAILED             (PMIX_PROC_STATE_ERROR +  6)  /* process communication has failed */
#define PMIX_PROC_STATE_SENSOR_BOUND_EXCEEDED   (PMIX_PROC_STATE_ERROR +  7)  /* process exceeded a sensor limit */
#define PMIX_PROC_STATE_CALLED_ABORT            (PMIX_PROC_STATE_ERROR +  8)  /* process called "PMIx_Abort" */
#define PMIX_PROC_STATE_HEARTBEAT_FAILED        (PMIX_PROC_STATE_ERROR +  9)  /* process failed to send heartbeat w/in time limit */
#define PMIX_PROC_STATE_MIGRATING               (PMIX_PROC_STATE_ERROR + 10)  /* process failed and is waiting for resources before restarting */
#define PMIX_PROC_STATE_CANNOT_RESTART          (PMIX_PROC_STATE_ERROR + 11)  /* process failed and cannot be restarted */
#define PMIX_PROC_STATE_TERM_NON_ZERO           (PMIX_PROC_STATE_ERROR + 12)  /* process exited with a non-zero status, indicating abnormal */
#define PMIX_PROC_STATE_FAILED_TO_LAUNCH        (PMIX_PROC_STATE_ERROR + 13)  /* unable to launch process */


/****    PMIX ERROR CONSTANTS    ****/
/* PMIx errors are always negative, with 0 reserved for success */
#define PMIX_ERR_BASE                   0

typedef int pmix_status_t;

/* v1.x error values - must be fixed in place for backward
 * compatability. Note that some number of these have been
 * deprecated and may not be returned by v2.x and above
 * clients or servers. However, they must always be
 * at least defined to ensure older codes will compile */
#define PMIX_SUCCESS                                 0
#define PMIX_ERROR                                  -1          // general error
#define PMIX_ERR_SILENT                             -2          // internal-only
/* debugger release flag */
#define PMIX_ERR_DEBUGGER_RELEASE                   -3
/* fault tolerance */
#define PMIX_ERR_PROC_RESTART                       -4
#define PMIX_ERR_PROC_CHECKPOINT                    -5
#define PMIX_ERR_PROC_MIGRATE                       -6
/* abort */
#define PMIX_ERR_PROC_ABORTED                       -7
#define PMIX_ERR_PROC_REQUESTED_ABORT               -8
#define PMIX_ERR_PROC_ABORTING                      -9
/* communication failures */
#define PMIX_ERR_SERVER_FAILED_REQUEST              -10
#define PMIX_EXISTS                                 -11
#define PMIX_ERR_INVALID_CRED                       -12         // internal-only
#define PMIX_ERR_HANDSHAKE_FAILED                   -13         // internal-only
#define PMIX_ERR_READY_FOR_HANDSHAKE                -14         // internal-only
#define PMIX_ERR_WOULD_BLOCK                        -15
#define PMIX_ERR_UNKNOWN_DATA_TYPE                  -16         // internal-only
#define PMIX_ERR_PROC_ENTRY_NOT_FOUND               -17         // internal-only
#define PMIX_ERR_TYPE_MISMATCH                      -18         // internal-only
#define PMIX_ERR_UNPACK_INADEQUATE_SPACE            -19         // internal-only
#define PMIX_ERR_UNPACK_FAILURE                     -20         // internal-only
#define PMIX_ERR_PACK_FAILURE                       -21         // internal-only
#define PMIX_ERR_PACK_MISMATCH                      -22         // internal-only
#define PMIX_ERR_NO_PERMISSIONS                     -23
#define PMIX_ERR_TIMEOUT                            -24
#define PMIX_ERR_UNREACH                            -25
#define PMIX_ERR_IN_ERRNO                           -26         // internal-only
#define PMIX_ERR_BAD_PARAM                          -27
#define PMIX_ERR_RESOURCE_BUSY                      -28         // internal-only
#define PMIX_ERR_OUT_OF_RESOURCE                    -29
#define PMIX_ERR_DATA_VALUE_NOT_FOUND               -30
#define PMIX_ERR_INIT                               -31
#define PMIX_ERR_NOMEM                              -32         // internal-only
#define PMIX_ERR_INVALID_ARG                        -33         // internal-only
#define PMIX_ERR_INVALID_KEY                        -34         // internal-only
#define PMIX_ERR_INVALID_KEY_LENGTH                 -35         // internal-only
#define PMIX_ERR_INVALID_VAL                        -36         // internal-only
#define PMIX_ERR_INVALID_VAL_LENGTH                 -37         // internal-only
#define PMIX_ERR_INVALID_LENGTH                     -38         // internal-only
#define PMIX_ERR_INVALID_NUM_ARGS                   -39         // internal-only
#define PMIX_ERR_INVALID_ARGS                       -40         // internal-only
#define PMIX_ERR_INVALID_NUM_PARSED                 -41         // internal-only
#define PMIX_ERR_INVALID_KEYVALP                    -42         // internal-only
#define PMIX_ERR_INVALID_SIZE                       -43
#define PMIX_ERR_INVALID_NAMESPACE                  -44
#define PMIX_ERR_SERVER_NOT_AVAIL                   -45         // internal-only
#define PMIX_ERR_NOT_FOUND                          -46
#define PMIX_ERR_NOT_SUPPORTED                      -47
#define PMIX_ERR_NOT_IMPLEMENTED                    -48
#define PMIX_ERR_COMM_FAILURE                       -49
#define PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER     -50         // internal-only
#define PMIX_ERR_CONFLICTING_CLEANUP_DIRECTIVES     -51

/* define a starting point for v2.x error values */
#define PMIX_ERR_V2X_BASE                   -100

/* v2.x communication errors */
#define PMIX_ERR_LOST_CONNECTION_TO_SERVER          -101
#define PMIX_ERR_LOST_PEER_CONNECTION               -102
#define PMIX_ERR_LOST_CONNECTION_TO_CLIENT          -103
/* used by the query system */
#define PMIX_QUERY_PARTIAL_SUCCESS                  -104
/* request responses */
#define PMIX_NOTIFY_ALLOC_COMPLETE                  -105
/* job control */
#define PMIX_JCTRL_CHECKPOINT                       -106    // monitored by client to trigger checkpoint operation
#define PMIX_JCTRL_CHECKPOINT_COMPLETE              -107    // sent by client and monitored by server to notify that requested
                                                                            //     checkpoint operation has completed
#define PMIX_JCTRL_PREEMPT_ALERT                    -108    // monitored by client to detect RM intends to preempt

/* monitoring */
#define PMIX_MONITOR_HEARTBEAT_ALERT                -109
#define PMIX_MONITOR_FILE_ALERT                     -110
#define PMIX_PROC_TERMINATED                        -111
#define PMIX_ERR_INVALID_TERMINATION                -112

/* operational */
#define PMIX_ERR_EVENT_REGISTRATION                 -144
#define PMIX_ERR_JOB_TERMINATED                     -145
#define PMIX_ERR_UPDATE_ENDPOINTS                   -146
#define PMIX_MODEL_DECLARED                         -147
#define PMIX_GDS_ACTION_COMPLETE                    -148
#define PMIX_PROC_HAS_CONNECTED                     -149
#define PMIX_CONNECT_REQUESTED                      -150
#define PMIX_MODEL_RESOURCES                        -151     // model resource usage has changed
#define PMIX_OPENMP_PARALLEL_ENTERED                -152     // an OpenMP parallel region has been entered
#define PMIX_OPENMP_PARALLEL_EXITED                 -153     // an OpenMP parallel region has completed
#define PMIX_LAUNCH_DIRECTIVE                       -154
#define PMIX_LAUNCHER_READY                         -155
#define PMIX_OPERATION_IN_PROGRESS                  -156
#define PMIX_OPERATION_SUCCEEDED                    -157
#define PMIX_ERR_INVALID_OPERATION                  -158

/* system failures */
#define PMIX_ERR_NODE_DOWN                          -231
#define PMIX_ERR_NODE_OFFLINE                       -232
#define PMIX_ERR_SYS_OTHER                          -330

/* define a macro for identifying system event values */
#define PMIX_SYSTEM_EVENT(a)   \
    ((a) <= PMIX_ERR_NODE_DOWN && PMIX_ERR_SYS_OTHER <= (a))

/* used by event handlers */
#define PMIX_EVENT_NO_ACTION_TAKEN                  -331
#define PMIX_EVENT_PARTIAL_ACTION_TAKEN             -332
#define PMIX_EVENT_ACTION_DEFERRED                  -333
#define PMIX_EVENT_ACTION_COMPLETE                  -334

/* define a starting point for PMIx internal error codes
 * that are never exposed outside the library */
#define PMIX_INTERNAL_ERR_BASE                      -1330

/* define a starting point for user-level defined error
 * constants - negative values larger than this are guaranteed
 * not to conflict with PMIx values. Definitions should always
 * be based on the PMIX_EXTERNAL_ERR_BASE constant and -not- a
 * specific value as the value of the constant may change */
#define PMIX_EXTERNAL_ERR_BASE           PMIX_INTERNAL_ERR_BASE-2000

/****    PMIX DATA TYPES    ****/
typedef uint16_t pmix_data_type_t;
#define PMIX_UNDEF               0
#define PMIX_BOOL                1  // converted to/from native true/false to uint8 for pack/unpack
#define PMIX_BYTE                2  // a byte of data
#define PMIX_STRING              3  // NULL-terminated string
#define PMIX_SIZE                4  // size_t
#define PMIX_PID                 5  // OS-pid
#define PMIX_INT                 6
#define PMIX_INT8                7
#define PMIX_INT16               8
#define PMIX_INT32               9
#define PMIX_INT64              10
#define PMIX_UINT               11
#define PMIX_UINT8              12
#define PMIX_UINT16             13
#define PMIX_UINT32             14
#define PMIX_UINT64             15
#define PMIX_FLOAT              16
#define PMIX_DOUBLE             17
#define PMIX_TIMEVAL            18
#define PMIX_TIME               19
#define PMIX_STATUS             20  // needs to be tracked separately from integer for those times
                                    // when we are embedded and it needs to be converted to the
                                    // host error definitions
#define PMIX_VALUE              21
#define PMIX_PROC               22
#define PMIX_APP                23
#define PMIX_INFO               24
#define PMIX_PDATA              25
#define PMIX_BUFFER             26
#define PMIX_BYTE_OBJECT        27
#define PMIX_KVAL               28
// Hole left by deprecation/removal of PMIX_MODEX
#define PMIX_PERSIST            30
#define PMIX_POINTER            31
#define PMIX_SCOPE              32
#define PMIX_DATA_RANGE         33
#define PMIX_COMMAND            34
#define PMIX_INFO_DIRECTIVES    35
#define PMIX_DATA_TYPE          36
#define PMIX_PROC_STATE         37
#define PMIX_PROC_INFO          38
#define PMIX_DATA_ARRAY         39
#define PMIX_PROC_RANK          40
#define PMIX_QUERY              41
#define PMIX_COMPRESSED_STRING  42  // string compressed with zlib
#define PMIX_ALLOC_DIRECTIVE    43
// Hole left by deprecation/removal of PMIX_INFO_ARRAY
#define PMIX_IOF_CHANNEL        45
#define PMIX_ENVAR              46
/********************/

/* define a boundary for implementers so they can add their own data types */
#define PMIX_DATA_TYPE_MAX     500


/* define a scope for data "put" by PMIx per the following:
 *
 * PMI_LOCAL - the data is intended only for other application
 *             processes on the same node. Data marked in this way
 *             will not be included in data packages sent to remote requestors
 * PMI_REMOTE - the data is intended solely for applications processes on
 *              remote nodes. Data marked in this way will not be shared with
 *              other processes on the same node
 * PMI_GLOBAL - the data is to be shared with all other requesting processes,
 *              regardless of location
 */
typedef uint8_t pmix_scope_t;
#define PMIX_SCOPE_UNDEF    0
#define PMIX_LOCAL          1   // share to procs also on this node
#define PMIX_REMOTE         2   // share with procs not on this node
#define PMIX_GLOBAL         3   // share with all procs (local + remote)
#define PMIX_INTERNAL       4   // store data in the internal tables

/* define a range for data "published" by PMIx
 */
typedef uint8_t pmix_data_range_t;
#define PMIX_RANGE_UNDEF        0
#define PMIX_RANGE_RM           1   // data is intended for the host resource manager
#define PMIX_RANGE_LOCAL        2   // available on local node only
#define PMIX_RANGE_NAMESPACE    3   // data is available to procs in the same nspace only
#define PMIX_RANGE_SESSION      4   // data available to all procs in session
#define PMIX_RANGE_GLOBAL       5   // data available to all procs
#define PMIX_RANGE_CUSTOM       6   // range is specified in a pmix_info_t
#define PMIX_RANGE_PROC_LOCAL   7   // restrict range to the local proc
#define PMIX_RANGE_INVALID   UINT8_MAX

/* define a "persistence" policy for data published by clients */
typedef uint8_t pmix_persistence_t;
#define PMIX_PERSIST_INDEF          0   // retain until specifically deleted
#define PMIX_PERSIST_FIRST_READ     1   // delete upon first access
#define PMIX_PERSIST_PROC           2   // retain until publishing process terminates
#define PMIX_PERSIST_APP            3   // retain until application terminates
#define PMIX_PERSIST_SESSION        4   // retain until session/allocation terminates
#define PMIX_PERSIST_INVALID   UINT8_MAX

/* define a set of bit-mask flags for specifying behavior of
 * command directives via pmix_info_t arrays */
typedef uint32_t pmix_info_directives_t;
#define PMIX_INFO_REQD          0x00000001
#define PMIX_INFO_ARRAY_END     0x00000002      // mark the end of an array created by PMIX_INFO_CREATE
/* the top 16-bits are reserved for internal use by
 * implementers - these may be changed inside the
 * PMIx library */
#define PMIX_INFO_DIR_RESERVED 0xffff0000

/* define a set of directives for allocation requests */
typedef uint8_t pmix_alloc_directive_t;
#define PMIX_ALLOC_NEW          1  // new allocation is being requested. The resulting allocation will be
                                   // disjoint (i.e., not connected in a job sense) from the requesting allocation
#define PMIX_ALLOC_EXTEND       2  // extend the existing allocation, either in time or as additional resources
#define PMIX_ALLOC_RELEASE      3  // release part of the existing allocation. Attributes in the accompanying
                                   // pmix\_info\_t array may be used to specify permanent release of the
                                   // identified resources, or "lending" of those resources for some period
                                   // of time.
#define PMIX_ALLOC_REAQUIRE     4  // reacquire resources that were previously "lent" back to the scheduler

/* define a value boundary beyond which implementers are free
 * to define their own directive values */
#define PMIX_ALLOC_EXTERNAL     128


/* define a set of bit-mask flags for specifying IO
 * forwarding channels. These can be OR'd together
 * to reference multiple channels */
typedef uint16_t pmix_iof_channel_t;
#define PMIX_FWD_NO_CHANNELS        0x0000
#define PMIX_FWD_STDIN_CHANNEL      0x0001
#define PMIX_FWD_STDOUT_CHANNEL     0x0002
#define PMIX_FWD_STDERR_CHANNEL     0x0004
#define PMIX_FWD_STDDIAG_CHANNEL    0x0008
#define PMIX_FWD_ALL_CHANNELS       0x00ff

/* declare a convenience macro for checking keys */
#define PMIX_CHECK_KEY(a, b) \
    (0 == strncmp((a)->key, (b), PMIX_MAX_KEYLEN))

#define PMIX_LOAD_KEY(a, b) \
    do {                                            \
        memset((a), 0, PMIX_MAX_KEYLEN+1);          \
        pmix_strncpy((a), (b), PMIX_MAX_KEYLEN);    \
    }while(0)

/* define a convenience macro for loading nspaces */
#define PMIX_LOAD_NSPACE(a, b)                      \
    do {                                            \
        memset((a), 0, PMIX_MAX_NSLEN+1);           \
        pmix_strncpy((a), (b), PMIX_MAX_NSLEN);     \
    }while(0)

/* define a convenience macro for checking nspaces */
#define PMIX_CHECK_NSPACE(a, b) \
    (0 == strncmp((a), (b), PMIX_MAX_NSLEN))

/* define a convenience macro for loading names */
#define PMIX_LOAD_PROCID(a, b, c)               \
    do {                                        \
        PMIX_LOAD_NSPACE((a)->nspace, (b));     \
        (a)->rank = (c);                        \
    }while(0)

/* define a convenience macro for checking names */
#define PMIX_CHECK_PROCID(a, b) \
    (PMIX_CHECK_NSPACE((a)->nspace, (b)->nspace) && ((a)->rank == (b)->rank || (PMIX_RANK_WILDCARD == (a)->rank || PMIX_RANK_WILDCARD == (b)->rank)))


/****    PMIX BYTE OBJECT    ****/
typedef struct pmix_byte_object {
    char *bytes;
    size_t size;
} pmix_byte_object_t;

#define PMIX_BYTE_OBJECT_CREATE(m, n)   \
    do {                                \
        (m) = (pmix_byte_object_t*)malloc((n) * sizeof(pmix_byte_object_t));   \
        if (NULL != (m)) {                                                     \
            memset((m), 0, (n)*sizeof(pmix_byte_object_t));                    \
        }                                                                      \
    } while(0)

#define PMIX_BYTE_OBJECT_CONSTRUCT(m)   \
    do {                                \
        (m)->bytes = NULL;              \
        (m)->size = 0;                  \
    } while(0)

#define PMIX_BYTE_OBJECT_DESTRUCT(m)    \
    do {                                \
        if (NULL != (m)->bytes) {       \
            free((m)->bytes);           \
        }                               \
    } while(0)

#define PMIX_BYTE_OBJECT_FREE(m, n)             \
    do {                                        \
        size_t _bon;                            \
        if (NULL != (m)) {                      \
            for (_bon=0; _bon < n; _bon++) {    \
                if (NULL != (m)[_bon].bytes) {  \
                    free((m)[_bon].bytes);      \
                }                               \
            }                                   \
            free((m));                          \
            (m) = NULL;                         \
        }                                       \
    } while(0)

#define PMIX_BYTE_OBJECT_LOAD(b, d, s)      \
    do {                                    \
        (b)->bytes = (d);                   \
        (d) = NULL;                         \
        (b)->size = (s);                    \
        (s) = 0;                            \
    } while(0)


/****    PMIX ENVAR STRUCT   ****/
/* Provide a structure for specifying environment variable modifications
 * Standard environment variables (e.g., PATH, LD_LIBRARY_PATH, and LD_PRELOAD)
 * take multiple arguments separated by delimiters. Unfortunately, the delimiters
 * depend upon the variable itself - some use semi-colons, some colons, etc. Thus,
 * the operation requires not only the name of the variable to be modified and
 * the value to be inserted, but also the separator to be used when composing
 * the aggregate value
 */
typedef struct {
    char *envar;
    char *value;
    char separator;
} pmix_envar_t;

#define PMIX_ENVAR_CREATE(m, n)                                     \
    do {                                                            \
        (m) = (pmix_envar_t*)calloc((n) , sizeof(pmix_envar_t));    \
    } while (0)
#define PMIX_ENVAR_FREE(m, n)                       \
    do {                                            \
        size_t _ek;                                 \
        if (NULL != (m)) {                          \
            for (_ek=0; _ek < (n); _ek++) {         \
               PMIX_ENVAR_DESTRUCT(&(m)[_ek]);      \
            }                                       \
            free((m));                              \
        }                                           \
    } while (0)
#define PMIX_ENVAR_CONSTRUCT(m)        \
    do {                               \
        (m)->envar = NULL;             \
        (m)->value = NULL;             \
        (m)->separator = '\0';         \
    } while(0)
#define PMIX_ENVAR_DESTRUCT(m)         \
    do {                               \
        if (NULL != (m)->envar) {      \
            free((m)->envar);          \
            (m)->envar = NULL;         \
        }                              \
        if (NULL != (m)->value) {      \
            free((m)->value);          \
            (m)->value = NULL;         \
        }                              \
    } while(0)
#define PMIX_ENVAR_LOAD(m, e, v, s)    \
    do {                               \
        if (NULL != (e)) {             \
            (m)->envar = strdup(e);    \
        }                              \
        if (NULL != (v)) {             \
            (m)->value = strdup(v);    \
        }                              \
        (m)->separator = (s);          \
    } while(0)


/****    PMIX DATA BUFFER    ****/
typedef struct pmix_data_buffer {
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
} pmix_data_buffer_t;
#define PMIX_DATA_BUFFER_CREATE(m)                                          \
    do {                                                                    \
        (m) = (pmix_data_buffer_t*)calloc(1, sizeof(pmix_data_buffer_t));   \
    } while (0)
#define PMIX_DATA_BUFFER_RELEASE(m)             \
    do {                                        \
        if (NULL != (m)->base_ptr) {            \
            free((m)->base_ptr);                \
        }                                       \
        free((m));                              \
        (m) = NULL;                             \
    } while (0)
#define PMIX_DATA_BUFFER_CONSTRUCT(m)       \
    memset((m), 0, sizeof(pmix_data_buffer_t))
#define PMIX_DATA_BUFFER_DESTRUCT(m)        \
    do {                                    \
        if (NULL != (m)->base_ptr) {        \
            free((m)->base_ptr);            \
            (m)->base_ptr = NULL;           \
        }                                   \
        (m)->pack_ptr = NULL;               \
        (m)->unpack_ptr = NULL;             \
        (m)->bytes_allocated = 0;           \
        (m)->bytes_used = 0;                \
    } while (0)
#define PMIX_DATA_BUFFER_LOAD(b, d, s)          \
    do {                                        \
        (b)->base_ptr = (char*)(d);             \
        (b)->pack_ptr = (b)->base_ptr + (s);    \
        (b)->unpack_ptr = (b)->base_ptr;        \
        (b)->bytes_allocated = (s);             \
        (b)->bytes_used = (s);                  \
    } while(0)

#define PMIX_DATA_BUFFER_UNLOAD(b, d, s)    \
    do {                                    \
        (d) = (b)->base_ptr;                \
        (s) = (b)->bytes_used;              \
        (b)->base_ptr = NULL;               \
    } while(0)

/****    PMIX PROC OBJECT    ****/
typedef struct pmix_proc {
    pmix_nspace_t nspace;
    pmix_rank_t rank;
} pmix_proc_t;
#define PMIX_PROC_CREATE(m, n)                                  \
    do {                                                        \
        (m) = (pmix_proc_t*)calloc((n) , sizeof(pmix_proc_t));  \
    } while (0)

#define PMIX_PROC_RELEASE(m)    \
    do {                        \
        free((m));              \
        (m) = NULL;             \
    } while (0)

#define PMIX_PROC_CONSTRUCT(m)                  \
    do {                                        \
        memset((m), 0, sizeof(pmix_proc_t));    \
    } while (0)

#define PMIX_PROC_DESTRUCT(m)

#define PMIX_PROC_FREE(m, n)                    \
    do {                                        \
        if (NULL != (m)) {                      \
            free((m));                          \
            (m) = NULL;                         \
        }                                       \
    } while (0)

#define PMIX_PROC_LOAD(m, n, r)                             \
    do {                                                    \
        PMIX_PROC_CONSTRUCT((m));                           \
        pmix_strncpy((m)->nspace, (n), PMIX_MAX_NSLEN);    \
        (m)->rank = (r);                                    \
    } while(0)

#define PMIX_MULTICLUSTER_NSPACE_CONSTRUCT(t, c, n)                         \
    do {                                                                    \
        size_t _len;                                                        \
        memset((t), 0, PMIX_MAX_NSLEN+1);                                   \
        _len = strlen((c));                                                 \
        if ((_len + strlen((n))) < PMIX_MAX_NSLEN) {                        \
            pmix_strncpy((t), (c), PMIX_MAX_NSLEN);                         \
            (t)[_len] = ':';                                                \
            pmix_strncpy(&(t)[_len+1], (n), PMIX_MAX_NSLEN - _len);         \
        }                                                                   \
    } while(0)

#define PMIX_MULTICLUSTER_NSPACE_PARSE(t, c, n)             \
    do {                                                    \
        size_t _n, _j;                                      \
        for (_n=0; '\0' != (t)[_n] && ':' != (t)[_n] &&     \
             _n <= PMIX_MAX_NSLEN; _n++) {                  \
            (c)[_n] = (t)[_n];                              \
        }                                                   \
        _n++;                                               \
        for (_j=0; _n <= PMIX_MAX_NSLEN &&                  \
             '\0' != (t)[_n]; _n++, _j++) {                 \
            (n)[_j] = (t)[_n];                              \
        }                                                   \
    } while(0)


/****    PMIX PROC INFO STRUCT    ****/
typedef struct pmix_proc_info {
    pmix_proc_t proc;
    char *hostname;
    char *executable_name;
    pid_t pid;
    int exit_code;
    pmix_proc_state_t state;
} pmix_proc_info_t;
#define PMIX_PROC_INFO_CREATE(m, n)                                         \
    do {                                                                    \
        (m) = (pmix_proc_info_t*)calloc((n) , sizeof(pmix_proc_info_t));    \
    } while (0)

#define PMIX_PROC_INFO_RELEASE(m)      \
    do {                               \
        PMIX_PROC_INFO_FREE((m), 1);   \
    } while (0)

#define PMIX_PROC_INFO_CONSTRUCT(m)                 \
    do {                                            \
        memset((m), 0, sizeof(pmix_proc_info_t));   \
    } while (0)

#define PMIX_PROC_INFO_DESTRUCT(m)              \
    do {                                        \
        if (NULL != (m)->hostname) {            \
            free((m)->hostname);                \
            (m)->hostname = NULL;               \
        }                                       \
        if (NULL != (m)->executable_name) {     \
            free((m)->executable_name);         \
            (m)->executable_name = NULL;        \
        }                                       \
    } while(0)

#define PMIX_PROC_INFO_FREE(m, n)                   \
    do {                                            \
        size_t _k;                                  \
        if (NULL != (m)) {                          \
            for (_k=0; _k < (n); _k++) {            \
                PMIX_PROC_INFO_DESTRUCT(&(m)[_k]);  \
            }                                       \
            free((m));                              \
        }                                           \
    } while (0)


/****    PMIX DATA ARRAY STRUCT    ****/

typedef struct pmix_data_array {
    pmix_data_type_t type;
    size_t size;
    void *array;
} pmix_data_array_t;

/**** THE PMIX_DATA_ARRAY SUPPORT MACROS ARE DEFINED ****/
/**** DOWN BELOW (NEAR THE BOTTOM OF THE FILE) TO    ****/
/**** AVOID CIRCULAR DEPENDENCIES                    ****/


/****    PMIX VALUE STRUCT    ****/

/* NOTE: operations can supply a collection of values under
 * a single key by passing a pmix_value_t containing a
 * data array of type PMIX_INFO, with each array element
 * containing its own pmix_info_t object */

typedef struct pmix_value {
    pmix_data_type_t type;
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
        float fval;
        double dval;
        struct timeval tv;
        time_t time;
        pmix_status_t status;
        pmix_rank_t rank;
        pmix_proc_t *proc;
        pmix_byte_object_t bo;
        pmix_persistence_t persist;
        pmix_scope_t scope;
        pmix_data_range_t range;
        pmix_proc_state_t state;
        pmix_proc_info_t *pinfo;
        pmix_data_array_t *darray;
        void *ptr;
        pmix_alloc_directive_t adir;
        pmix_envar_t envar;
    } data;
} pmix_value_t;
/* allocate and initialize a specified number of value structs */
#define PMIX_VALUE_CREATE(m, n)                                 \
    do {                                                        \
        int _ii;                                                \
        pmix_value_t *_v;                                       \
        (m) = (pmix_value_t*)calloc((n), sizeof(pmix_value_t)); \
        _v = (pmix_value_t*)(m);                                \
        if (NULL != (m)) {                                      \
            for (_ii=0; _ii < (int)(n); _ii++) {                \
                _v[_ii].type = PMIX_UNDEF;                     \
            }                                                   \
        }                                                       \
    } while (0)

/* release a single pmix_value_t struct, including its data */
#define PMIX_VALUE_RELEASE(m)       \
    do {                            \
        PMIX_VALUE_DESTRUCT((m));   \
        free((m));                  \
        (m) = NULL;                 \
    } while (0)

/* initialize a single value struct */
#define PMIX_VALUE_CONSTRUCT(m)                 \
    do {                                        \
        memset((m), 0, sizeof(pmix_value_t));   \
        (m)->type = PMIX_UNDEF;                 \
    } while (0)

/* release the memory in the value struct data field */
#define PMIX_VALUE_DESTRUCT(m) pmix_value_destruct(m)

#define PMIX_VALUE_FREE(m, n)                           \
    do {                                                \
        size_t _vv;                                     \
        if (NULL != (m)) {                              \
            for (_vv=0; _vv < (n); _vv++) {             \
                PMIX_VALUE_DESTRUCT(&((m)[_vv]));       \
            }                                           \
            free((m));                                  \
            (m) = NULL;                                 \
        }                                               \
    } while (0)

#define PMIX_VALUE_LOAD(v, d, t) \
    pmix_value_load((v), (d), (t))

#define PMIX_VALUE_UNLOAD(r, k, d, s)      \
    (r) = pmix_value_unload((k), (d), (s))

#define PMIX_VALUE_XFER(r, v, s)                                \
    do {                                                        \
        if (NULL == (v)) {                                      \
            (v) = (pmix_value_t*)malloc(sizeof(pmix_value_t));  \
            if (NULL == (v)) {                                  \
                (r) = PMIX_ERR_NOMEM;                           \
            } else {                                            \
                (r) = pmix_value_xfer((v), (s));                \
            }                                                   \
        } else {                                                \
            (r) = pmix_value_xfer((v), (s));                    \
        }                                                       \
    } while(0)

#define PMIX_VALUE_GET_NUMBER(s, m, n, t)               \
    do {                                                \
        (s) = PMIX_SUCCESS;                             \
        if (PMIX_SIZE == (m)->type) {                   \
            (n) = (t)((m)->data.size);                  \
        } else if (PMIX_INT == (m)->type) {             \
            (n) = (t)((m)->data.integer);               \
        } else if (PMIX_INT8 == (m)->type) {            \
            (n) = (t)((m)->data.int8);                  \
        } else if (PMIX_INT16 == (m)->type) {           \
            (n) = (t)((m)->data.int16);                 \
        } else if (PMIX_INT32 == (m)->type) {           \
            (n) = (t)((m)->data.int32);                 \
        } else if (PMIX_INT64 == (m)->type) {           \
            (n) = (t)((m)->data.int64);                 \
        } else if (PMIX_UINT == (m)->type) {            \
            (n) = (t)((m)->data.uint);                  \
        } else if (PMIX_UINT8 == (m)->type) {           \
            (n) = (t)((m)->data.uint8);                 \
        } else if (PMIX_UINT16 == (m)->type) {          \
            (n) = (t)((m)->data.uint16);                \
        } else if (PMIX_UINT32 == (m)->type) {          \
            (n) = (t)((m)->data.uint32);                \
        } else if (PMIX_UINT64 == (m)->type) {          \
            (n) = (t)((m)->data.uint64);                \
        } else if (PMIX_FLOAT == (m)->type) {           \
            (n) = (t)((m)->data.fval);                  \
        } else if (PMIX_DOUBLE == (m)->type) {          \
            (n) = (t)((m)->data.dval);                  \
        } else if (PMIX_PID == (m)->type) {             \
            (n) = (t)((m)->data.pid);                   \
        } else {                                        \
            (s) = PMIX_ERR_BAD_PARAM;                   \
        }                                               \
    } while(0)

#define PMIX_VALUE_COMPRESSED_STRING_UNPACK(s)                              \
    do {                                                                    \
        char *tmp;                                                          \
        /* if this is a compressed string, then uncompress it */            \
        if (PMIX_COMPRESSED_STRING == (s)->type) {                          \
            pmix_util_uncompress_string(&tmp, (uint8_t*)(s)->data.bo.bytes, \
                (s)->data.bo.size);                                         \
            if (NULL == tmp) {                                              \
                PMIX_ERROR_LOG(PMIX_ERR_NOMEM);                             \
                rc = PMIX_ERR_NOMEM;                                        \
                PMIX_VALUE_RELEASE(s);                                      \
                val = NULL;                                                 \
            } else {                                                        \
                PMIX_VALUE_DESTRUCT(s);                                     \
                (s)->data.string = tmp;                                     \
                (s)->type = PMIX_STRING;                                    \
            }                                                               \
        }                                                                   \
    } while(0)

/****    PMIX INFO STRUCT    ****/
typedef struct pmix_info {
    pmix_key_t key;
    pmix_info_directives_t flags;   // bit-mask of flags
    pmix_value_t value;
} pmix_info_t;

/* utility macros for working with pmix_info_t structs */
#define PMIX_INFO_CREATE(m, n)                                  \
    do {                                                        \
        pmix_info_t *_i;                                        \
        (m) = (pmix_info_t*)calloc((n), sizeof(pmix_info_t));   \
        _i = (pmix_info_t*)(m);                                 \
        _i[(n)-1].flags = PMIX_INFO_ARRAY_END;                  \
    } while (0)

#define PMIX_INFO_CONSTRUCT(m)                  \
    do {                                        \
        memset((m), 0, sizeof(pmix_info_t));    \
        (m)->value.type = PMIX_UNDEF;           \
    } while (0)

#define PMIX_INFO_DESTRUCT(m) \
    do {                                        \
        PMIX_VALUE_DESTRUCT(&(m)->value);       \
    } while (0)

#define PMIX_INFO_FREE(m, n)                        \
    do {                                            \
        size_t _is;                                 \
        if (NULL != (m)) {                          \
            for (_is=0; _is < (n); _is++) {         \
                PMIX_INFO_DESTRUCT(&((m)[_is]));    \
            }                                       \
            free((m));                              \
            (m) = NULL;                             \
        }                                           \
    } while (0)

#define PMIX_INFO_LOAD(m, k, v, t)                          \
    do {                                                    \
        if (NULL != (k)) {                                  \
            pmix_strncpy((m)->key, (k), PMIX_MAX_KEYLEN);   \
        }                                                   \
        (m)->flags = 0;                                     \
        pmix_value_load(&((m)->value), (v), (t));           \
    } while (0)
#define PMIX_INFO_XFER(d, s)                                        \
    do {                                                            \
        if (NULL != (s)->key) {                                     \
            pmix_strncpy((d)->key, (s)->key, PMIX_MAX_KEYLEN);      \
        }                                                           \
        (d)->flags = (s)->flags;                                    \
        pmix_value_xfer(&(d)->value, (pmix_value_t*)&(s)->value);   \
    } while(0)


/* macros for setting and unsetting the "reqd" flag
 * in a pmix_info_t */
#define PMIX_INFO_REQUIRED(m)       \
    (m)->flags |= PMIX_INFO_REQD
#define PMIX_INFO_OPTIONAL(m)       \
    (m)->flags &= ~PMIX_INFO_REQD

/* macros for testing the "reqd" flag in a pmix_info_t */
#define PMIX_INFO_IS_REQUIRED(m)    \
    (m)->flags & PMIX_INFO_REQD
#define PMIX_INFO_IS_OPTIONAL(m)    \
    !((m)->flags & PMIX_INFO_REQD)

/* macro for testing end of the array */
#define PMIX_INFO_IS_END(m)         \
    (m)->flags & PMIX_INFO_ARRAY_END

/* define a special macro for checking if a boolean
 * info is true - when info structs are provided, a
 * type of PMIX_UNDEF is taken to imply a boolean "true"
 * as the presence of the key defaults to indicating
 * "true" */
#define PMIX_INFO_TRUE(m)   \
    (PMIX_UNDEF == (m)->value.type || (PMIX_BOOL == (m)->value.type && (m)->value.data.flag)) ? true : false


/****    PMIX LOOKUP RETURN STRUCT    ****/
typedef struct pmix_pdata {
    pmix_proc_t proc;
    pmix_key_t key;
    pmix_value_t value;
} pmix_pdata_t;

/* utility macros for working with pmix_pdata_t structs */
#define PMIX_PDATA_CREATE(m, n)                                 \
    do {                                                        \
        (m) = (pmix_pdata_t*)calloc((n), sizeof(pmix_pdata_t)); \
    } while (0)

#define PMIX_PDATA_RELEASE(m)                   \
    do {                                        \
        PMIX_VALUE_DESTRUCT(&(m)->value);       \
        free((m));                              \
        (m) = NULL;                             \
    } while (0)

#define PMIX_PDATA_CONSTRUCT(m)                 \
    do {                                        \
        memset((m), 0, sizeof(pmix_pdata_t));   \
        (m)->value.type = PMIX_UNDEF;           \
    } while (0)

#define PMIX_PDATA_DESTRUCT(m)                  \
    do {                                        \
        PMIX_VALUE_DESTRUCT(&(m)->value);       \
    } while (0)

#define PMIX_PDATA_FREE(m, n)                           \
    do {                                                \
        size_t _ps;                                     \
        pmix_pdata_t *_pdf = (pmix_pdata_t*)(m);        \
        if (NULL != _pdf) {                             \
            for (_ps=0; _ps < (n); _ps++) {             \
                PMIX_PDATA_DESTRUCT(&(_pdf[_ps]));      \
            }                                           \
            free((m));                                  \
            (m) = NULL;                                 \
        }                                               \
    } while (0)

#define PMIX_PDATA_LOAD(m, p, k, v, t)                                      \
    do {                                                                    \
        if (NULL != (m)) {                                                  \
            memset((m), 0, sizeof(pmix_pdata_t));                           \
            pmix_strncpy((m)->proc.nspace, (p)->nspace, PMIX_MAX_NSLEN);    \
            (m)->proc.rank = (p)->rank;                                     \
            pmix_strncpy((m)->key, (k), PMIX_MAX_KEYLEN);                   \
            pmix_value_load(&((m)->value), (v), (t));                       \
        }                                                                   \
    } while (0)

#define PMIX_PDATA_XFER(d, s)                                                   \
    do {                                                                        \
        if (NULL != (d)) {                                                      \
            memset((d), 0, sizeof(pmix_pdata_t));                               \
            pmix_strncpy((d)->proc.nspace, (s)->proc.nspace, PMIX_MAX_NSLEN);   \
            (d)->proc.rank = (s)->proc.rank;                                    \
            pmix_strncpy((d)->key, (s)->key, PMIX_MAX_KEYLEN);                  \
            pmix_value_xfer(&((d)->value), &((s)->value));                      \
        }                                                                       \
    } while (0)


/****    PMIX APP STRUCT    ****/
typedef struct pmix_app {
    char *cmd;
    char **argv;
    char **env;
    char *cwd;
    int maxprocs;
    pmix_info_t *info;
    size_t ninfo;
} pmix_app_t;
/* utility macros for working with pmix_app_t structs */
#define PMIX_APP_CREATE(m, n)                                   \
    do {                                                        \
        (m) = (pmix_app_t*)calloc((n), sizeof(pmix_app_t));     \
    } while (0)

#define PMIX_APP_INFO_CREATE(m, n)                  \
    do {                                            \
        (m)->ninfo = (n);                           \
        PMIX_INFO_CREATE((m)->info, (m)->ninfo);    \
    } while(0)

#define PMIX_APP_RELEASE(m)                     \
    do {                                        \
        PMIX_APP_DESTRUCT((m));                 \
        free((m));                              \
        (m) = NULL;                             \
    } while (0)

#define PMIX_APP_CONSTRUCT(m)                   \
    do {                                        \
        memset((m), 0, sizeof(pmix_app_t));     \
    } while (0)

#define PMIX_APP_DESTRUCT(m)                                    \
    do {                                                        \
        size_t _aii;                                            \
        if (NULL != (m)->cmd) {                                 \
            free((m)->cmd);                                     \
            (m)->cmd = NULL;                                    \
        }                                                       \
        if (NULL != (m)->argv) {                                \
            for (_aii=0; NULL != (m)->argv[_aii]; _aii++) {     \
                free((m)->argv[_aii]);                          \
            }                                                   \
            free((m)->argv);                                    \
            (m)->argv = NULL;                                   \
        }                                                       \
        if (NULL != (m)->env) {                                 \
            for (_aii=0; NULL != (m)->env[_aii]; _aii++) {      \
                free((m)->env[_aii]);                           \
            }                                                   \
            free((m)->env);                                     \
            (m)->env = NULL;                                    \
        }                                                       \
        if (NULL != (m)->cwd) {                                 \
            free((m)->cwd);                                     \
            (m)->cwd = NULL;                                    \
        }                                                       \
        if (NULL != (m)->info) {                                \
            PMIX_INFO_FREE((m)->info, (m)->ninfo);              \
            (m)->info = NULL;                                   \
            (m)->ninfo = 0;                                     \
        }                                                       \
    } while (0)

#define PMIX_APP_FREE(m, n)                     \
    do {                                        \
        size_t _as;                             \
        if (NULL != (m)) {                      \
            for (_as=0; _as < (n); _as++) {     \
                PMIX_APP_DESTRUCT(&((m)[_as])); \
            }                                   \
            free((m));                          \
            (m) = NULL;                         \
        }                                       \
    } while (0)


/****    PMIX QUERY STRUCT    ****/
typedef struct pmix_query {
    char **keys;
    pmix_info_t *qualifiers;
    size_t nqual;
} pmix_query_t;
/* utility macros for working with pmix_query_t structs */
#define PMIX_QUERY_CREATE(m, n)                                     \
    do {                                                            \
        (m) = (pmix_query_t*)calloc((n) , sizeof(pmix_query_t));    \
    } while (0)

#define PMIX_QUERY_QUALIFIERS_CREATE(m, n)                  \
    do {                                                    \
        (m)->nqual = (n);                                   \
        PMIX_INFO_CREATE((m)->qualifiers, (m)->nqual);      \
    } while(0)

#define PMIX_QUERY_RELEASE(m)       \
    do {                            \
        PMIX_QUERY_DESTRUCT((m));   \
        free((m));                  \
        (m) = NULL;                 \
    } while (0)

#define PMIX_QUERY_CONSTRUCT(m)                 \
    do {                                        \
        memset((m), 0, sizeof(pmix_query_t));   \
    } while (0)

#define PMIX_QUERY_DESTRUCT(m)                                  \
    do {                                                        \
        size_t _qi;                                             \
        if (NULL != (m)->keys) {                                \
            for (_qi=0; NULL != (m)->keys[_qi]; _qi++) {        \
                free((m)->keys[_qi]);                           \
            }                                                   \
            free((m)->keys);                                    \
            (m)->keys = NULL;                                   \
        }                                                       \
        if (NULL != (m)->qualifiers) {                          \
            PMIX_INFO_FREE((m)->qualifiers, (m)->nqual);        \
            (m)->qualifiers = NULL;                             \
            (m)->nqual = 0;                                     \
        }                                                       \
    } while (0)

#define PMIX_QUERY_FREE(m, n)                       \
    do {                                            \
        size_t _qs;                                 \
        if (NULL != (m)) {                          \
            for (_qs=0; _qs < (n); _qs++) {         \
                PMIX_QUERY_DESTRUCT(&((m)[_qs]));   \
            }                                       \
            free((m));                              \
            (m) = NULL;                             \
        }                                           \
    } while (0)


/****    GENERIC HELPER MACROS    ****/

/* Append a string (by value) to an new or existing NULL-terminated
 * argv array.
 *
 * @param argv Pointer to an argv array.
 * @param str Pointer to the string to append.
 *
 * @retval PMIX_SUCCESS On success
 * @retval PMIX_ERROR On failure
 *
 * This function adds a string to an argv array of strings by value;
 * it is permissable to pass a string on the stack as the str
 * argument to this function.
 *
 * To add the first entry to an argv array, call this function with
 * (*argv == NULL).  This function will allocate an array of length
 * 2; the first entry will point to a copy of the string passed in
 * arg, the second entry will be set to NULL.
 *
 * If (*argv != NULL), it will be realloc'ed to be 1 (char*) larger,
 * and the next-to-last entry will point to a copy of the string
 * passed in arg.  The last entry will be set to NULL.
 *
 * Just to reinforce what was stated above: the string is copied by
 * value into the argv array; there is no need to keep the original
 * string (i.e., the arg parameter) after invoking this function.
 */
#define PMIX_ARGV_APPEND(r, a, b) \
    (r) = pmix_argv_append_nosize(&(a), (b))

/* Prepend a string to a new or existing NULL-terminated
 * argv array - same as above only prepend
 */
#define PMIX_ARGV_PREPEND(r, a, b) \
    (r) = pmix_argv_prepend_nosize(a, b)

/* Append to an argv-style array, but only if the provided argument
 * doesn't already exist somewhere in the array. Ignore the size of the array.
 *
 * @param argv Pointer to an argv array.
 * @param str Pointer to the string to append.
 * @param bool Whether or not to overwrite a matching value if found
 *
 * @retval PMIX_SUCCESS On success
 * @retval PMIX_ERROR On failure
 *
 * This function is identical to the pmix_argv_append_nosize() function
 * except that it only appends the provided argument if it does not already
 * exist in the provided array, or overwrites it if it is.
 */
#define PMIX_ARGV_APPEND_UNIQUE(r, a, b, c) \
    (r) = pmix_argv_append_unique_nosize(a, b, c)

/* Free a NULL-terminated argv array.
 *
 * @param argv Argv array to free.
 *
 * This function frees an argv array and all of the strings that it
 * contains.  Since the argv parameter is passed by value, it is not
 * set to NULL in the caller's scope upon return.
 *
 * It is safe to invoke this function with a NULL pointer.  It is
 * not safe to invoke this function with a non-NULL-terminated argv
 * array.
 */
#define PMIX_ARGV_FREE(a)  pmix_argv_free(a)

/*
 * Split a string into a NULL-terminated argv array. Do not include empty
 * strings in result array.
 *
 * @param src_string Input string.
 * @param delimiter Delimiter character.
 *
 * @retval argv pointer to new argv array on success
 * @retval NULL on error
 *
 * All strings are insertted into the argv array by value; the
 * newly-allocated array makes no references to the src_string
 * argument (i.e., it can be freed after calling this function
 * without invalidating the output argv).
 */
#define PMIX_ARGV_SPLIT(a, b, c) \
    (a) = pmix_argv_split(b, c)

/*
 * Return the length of a NULL-terminated argv array.
 *
 * @param argv The input argv array.
 *
 * @retval 0 If NULL is passed as argv.
 * @retval count Number of entries in the argv array.
 *
 * The argv array must be NULL-terminated.
 */
#define PMIX_ARGV_COUNT(r, a) \
    (r) = pmix_argv_count(a)

/*
 * Join all the elements of an argv array into a single
 * newly-allocated string.
 *
 * @param argv The input argv array.
 * @param delimiter Delimiter character placed between each argv string.
 *
 * @retval new_string Output string on success.
 * @retval NULL On failure.
 *
 * Similar to the Perl join function, this function takes an input
 * argv and joins them into into a single string separated by the
 * delimiter character.
 *
 * It is the callers responsibility to free the returned string.
 */
#define PMIX_ARGV_JOIN(a, b, c) \
    (a) = pmix_argv_join(b, c)

/*
 * Copy a NULL-terminated argv array.
 *
 * @param argv The input argv array.
 *
 * @retval argv Copied argv array on success.
 * @retval NULL On failure.
 *
 * Copy an argv array, including copying all off its strings.
 * Specifically, the output argv will be an array of the same length
 * as the input argv, and strcmp(argv_in[i], argv_out[i]) will be 0.
 */
#define PMIX_ARGV_COPY(a, b) \
    (a) = pmix_argv_copy(b)

/*
 * Set an environmenal paramter in an env array
 *
 * @retval r Return pmix_status_t status
 *
 * @param a Name of the environmental param
 *
 * @param b String value of the environmental param
 *
 * @param c Address of the NULL-terminated env array
 */
#define PMIX_SETENV(r, a, b, c) \
    (r) = pmix_setenv((a), (b), true, (c))


/****    CALLBACK FUNCTIONS FOR NON-BLOCKING OPERATIONS    ****/

typedef void (*pmix_release_cbfunc_t)(void *cbdata);

/* define a callback function that is solely used by servers, and
 * not clients, to return modex data in response to "fence" and "get"
 * operations. The returned blob contains the data collected from each
 * server participating in the operation.
 *
 * As the data is "owned" by the host server, provide a secondary
 * callback function to notify the host server that we are done
 * with the data so it can be released */
typedef void (*pmix_modex_cbfunc_t)(pmix_status_t status,
                                    const char *data, size_t ndata,
                                    void *cbdata,
                                    pmix_release_cbfunc_t release_fn,
                                    void *release_cbdata);

/* define a callback function for calls to PMIx_Spawn_nb - the function
 * will be called upon completion of the spawn command. The status
 * will indicate whether or not the spawn succeeded. The nspace
 * of the spawned processes will be returned, along with any provided
 * callback data. Note that the returned nspace value will be
 * released by the library upon return from the callback function, so
 * the receiver must copy it if it needs to be retained */
typedef void (*pmix_spawn_cbfunc_t)(pmix_status_t status,
                                    pmix_nspace_t nspace, void *cbdata);

/* define a callback for common operations that simply return
 * a status. Examples include the non-blocking versions of
 * Fence, Connect, and Disconnect */
typedef void (*pmix_op_cbfunc_t)(pmix_status_t status, void *cbdata);

/* define a callback function for calls to PMIx_Lookup_nb - the
 * function will be called upon completion of the command with the
 * status indicating the success of failure of the request. Any
 * retrieved data will be returned in an array of pmix_pdata_t structs.
 * The nspace/rank of the process that provided each data element is
 * also returned.
 *
 * Note that these structures will be released upon return from
 * the callback function, so the receiver must copy/protect the
 * data prior to returning if it needs to be retained */

typedef void (*pmix_lookup_cbfunc_t)(pmix_status_t status,
                                     pmix_pdata_t data[], size_t ndata,
                                     void *cbdata);

/* define a callback by which an event handler can notify the PMIx library
 * that it has completed its response to the notification. The handler
 * is _required_ to execute this callback so the library can determine
 * if additional handlers need to be called. The handler shall return
 * PMIX_SUCCESS if no further action is required. The return status
 * of each event handler and any returned pmix_info_t structures
 * will be added to the array of pmix_info_t passed to any subsequent
 * event handlers to help guide their operation.
 *
 * If non-NULL, the provided callback function will be called to allow
 * the event handler to release the provided info array.
 */
typedef void (*pmix_event_notification_cbfunc_fn_t)(pmix_status_t status,
                                                    pmix_info_t *results, size_t nresults,
                                                    pmix_op_cbfunc_t cbfunc, void *thiscbdata,
                                                    void *notification_cbdata);

/* define a callback function for the event handler. Upon receipt of an
 * event notification, PMIx will execute the specified notification
 * callback function, providing:
 *
 * evhdlr_registration_id - the returned registration number of
 *                          the event handler being called
 * status - the event that occurred
 * source - the nspace and rank of the process that generated
 *          the event. If the source is the resource manager,
 *          then the nspace will be empty and the rank will
 *          be PMIX_RANK_UNDEF
 * info - any additional info provided regarding the event.
 * ninfo - the number of info objects in the info array
 * results - any provided results from event handlers called
 *           prior to this one.
 * nresults - number of info objects in the results array
 * cbfunc - the function to be called upon completion of the handler
 * cbdata - pointer to be returned in the completion cbfunc
 *
 * Note that different resource managers may provide differing levels
 * of support for event notification to application processes. Thus, the
 * info array may be NULL or may contain detailed information of the event.
 * It is the responsibility of the application to parse any provided info array
 * for defined key-values if it so desires.
 *
 * Possible uses of the pmix_info_t object include:
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
 * server of a detected event in the PMIx subsystem and/or client
 */
typedef void (*pmix_notification_fn_t)(size_t evhdlr_registration_id,
                                       pmix_status_t status,
                                       const pmix_proc_t *source,
                                       pmix_info_t info[], size_t ninfo,
                                       pmix_info_t *results, size_t nresults,
                                       pmix_event_notification_cbfunc_fn_t cbfunc,
                                       void *cbdata);

/* define a callback function for calls to register handlers, e.g., event
 * notification and IOF requests
 *
 * status - PMIX_SUCCESS or an appropriate error constant
 *
 * refid - reference identifier assigned to the handler by PMIx,
 *         used to deregister the handler
 *
 * cbdata - object provided to the registration call
 */
typedef void (*pmix_hdlr_reg_cbfunc_t)(pmix_status_t status,
                                       size_t refid,
                                       void *cbdata);
/* maintain backward compatibility with v2 definition - change of name */
typedef void (*pmix_evhdlr_reg_cbfunc_t)(pmix_status_t status,
                                         size_t evhdlr_ref,
                                         void *cbdata);

/* define a callback function for calls to PMIx_Get_nb. The status
 * indicates if the requested data was found or not - a pointer to the
 * pmix_value_t structure containing the found data is returned. The
 * pointer will be NULL if the requested data was not found. */
typedef void (*pmix_value_cbfunc_t)(pmix_status_t status,
                                    pmix_value_t *kv, void *cbdata);

/* define a callback function for calls to PMIx_Query. The status
 * indicates if requested data was found or not - an array of
 * pmix_info_t will contain the key/value pairs. */
typedef void (*pmix_info_cbfunc_t)(pmix_status_t status,
                                   pmix_info_t *info, size_t ninfo,
                                   void *cbdata,
                                   pmix_release_cbfunc_t release_fn,
                                   void *release_cbdata);

/* Define a callback function to return a requested security credential.
 * Returned values include:
 *
 * status - PMIX_SUCCESS if a credential could be assigned as requested, or
 *          else an appropriate error code indicating the problem
 *
 * credential - pointer to an allocated pmix_byte_object_t containing the
 *              credential (as a opaque blob) and its size. Ownership of
 *              the credential is transferred to the receiving function - thus,
 *              responsibility for releasing the memory lies outside the
 *              PMIx library.
 *
 * info - an array of pmix_info_t structures provided by the system to pass
 *        any additional information about the credential - e.g., the identity
 *        of the issuing agent. The info array is owned by the PMIx library
 *        and is not to be released or altered by the receiving party. Note that
 *        this array is not related to the pmix_info_t structures possibly
 *        provided in the call to PMIx_Get_credential.
 *
 *        Information provided by the issuing agent can subsequently be used
 *        by the application for a variety of purposes. Examples include:
 *            - checking identified authorizations to determine what
 *              requests/operations are feasible as a means to steering
 *              workflows
 *            - compare the credential type to that of the local SMS for
 *              compatibility
 *
 * ninfo - number of elements in the info array
 *
 * cbdata - the caller's provided void* object
 *
 * NOTE: the credential is opaque and therefore understandable only by
 *       a service compatible with the issuer.
 */
typedef void (*pmix_credential_cbfunc_t)(pmix_status_t status,
                                         pmix_byte_object_t *credential,
                                         pmix_info_t info[], size_t ninfo,
                                         void *cbdata);


/* Define a validation callback function to indicate if a provided
 * credential is valid, and any corresponding information regarding
 * authorizations and other security matters
 * Returned values include:
 *
 * status - PMIX_SUCCESS if the provided credential is valid. An appropriate
 *          error code indicating the issue if the credential is rejected.
 *
 * info - an array of pmix_info_t structures provided by the system to pass
 *        any additional information about the authentication - e.g., the
 *        effective userid and group id of the certificate holder, and any
 *        related authorizations. The info array is owned by the PMIx library
 *        and is not to be released or altered by the receiving party. Note that
 *        this array is not related to the pmix_info_t structures possibly
 *        provided in the call to PMIx_Validate_credential.
 *
 *        The precise contents of the array will depend on the host SMS and
 *        its associated security system. At the minimum, it is expected (but
 *        not required) that the array will contain entries for the PMIX_USERID
 *        and PMIX_GROUPID of the client described in the credential.
 *
 * ninfo - number of elements in the info array
 *
 * cbdata - the caller's provided void* object
 */
typedef void (*pmix_validation_cbfunc_t)(pmix_status_t status,
                                         pmix_info_t info[], size_t ninfo,
                                         void *cbdata);


/****    COMMON SUPPORT FUNCTIONS    ****/
/* Register an event handler to report events. Three types of events
 * can be reported:
 *
 * (a) those that occur within the client library, but are not
 *     reportable via the API itself (e.g., loss of connection to
 *     the server). These events typically occur during behind-the-scenes
 *     non-blocking operations.
 *
 * (b) job-related events such as the failure of another process in
 *     the job or in any connected job, impending failure of hardware
 *     within the job's usage footprint, etc.
 *
 * (c) system notifications that are made available by the local
 *     administrators
 *
 * By default, only events that directly affect the process and/or
 * any process to which it is connected (via the PMIx_Connect call)
 * will be reported. Options to modify that behavior can be provided
 * in the info array
 *
 * Both the client application and the resource manager can register
 * err handlers for specific events. PMIx client/server calls the registered
 * err handler upon receiving event notify notification (via PMIx_Notify_event)
 * from the other end (Resource Manager/Client application).
 *
 * Multiple err handlers can be registered for different events. PMIX returns
 * an integer reference to each register handler in the callback fn. The caller
 * must retain the reference in order to deregister the evhdlr.
 * Modification of the notification behavior can be accomplished by
 * deregistering the current evhdlr, and then registering it
 * using a new set of info values.
 *
 * See pmix_common.h for a description of the notification function */
PMIX_EXPORT void PMIx_Register_event_handler(pmix_status_t codes[], size_t ncodes,
                                             pmix_info_t info[], size_t ninfo,
                                             pmix_notification_fn_t evhdlr,
                                             pmix_hdlr_reg_cbfunc_t cbfunc,
                                             void *cbdata);

/* Deregister an event handler
 * evhdlr_ref is the reference returned by PMIx from the call to
 * PMIx_Register_event_handler. If non-NULL, the provided cbfunc
 * will be called to confirm removal of the designated handler */
PMIX_EXPORT void PMIx_Deregister_event_handler(size_t evhdlr_ref,
                                               pmix_op_cbfunc_t cbfunc,
                                               void *cbdata);

/* Report an event for notification via any
 * registered evhdlr.
 *
 * This function allows the host server to direct the server
 * convenience library to notify all registered local procs of
 * an event. The event can be local, or anywhere in the cluster.
 * The status indicates the event being reported.
 *
 * The client application can also call this function to notify the
 * resource manager and/or other processes of an event it encountered.
 * It can also be used to asynchronously notify other parts of its
 * own internal process - e.g., for one library to notify another
 * when initialized inside the process.
 *
 * status - status code indicating the event being reported
 *
 * source - the process that generated the event
 *
 * range - the range in which the event is to be reported. For example,
 *         a value of PMIX_RANGE_LOCAL would instruct the system
 *         to only notify procs on the same local node as the
 *         event generator.
 *
 * info - an array of pmix_info_t structures provided by the event
 *        generator to pass any additional information about the
 *        event. This can include an array of pmix_proc_t structs
 *        describing the processes impacted by the event, the nature
 *        of the event and its severity, etc. The precise contents
 *        of the array will depend on the event generator.
 *
 * ninfo - number of elements in the info array
 *
 * cbfunc - callback function to be called upon completion of the
 *          notify_event function's actions. Note that any messages
 *          will have been queued, but may not have been transmitted
 *          by this time. Note that the caller is required to maintain
 *          the input data until the callback function has been executed!
 *
 * cbdata - the caller's provided void* object
 */
PMIX_EXPORT pmix_status_t PMIx_Notify_event(pmix_status_t status,
                                            const pmix_proc_t *source,
                                            pmix_data_range_t range,
                                            const pmix_info_t info[], size_t ninfo,
                                            pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Provide a string representation for several types of value. Note
 * that the provided string is statically defined and must NOT be
 * free'd. Supported value types:
 *
 * - pmix_status_t (PMIX_STATUS)
 * - pmix_scope_t   (PMIX_SCOPE)
 * - pmix_persistence_t  (PMIX_PERSIST)
 * - pmix_data_range_t   (PMIX_DATA_RANGE)
 * - pmix_info_directives_t   (PMIX_INFO_DIRECTIVES)
 * - pmix_data_type_t   (PMIX_DATA_TYPE)
 * - pmix_alloc_directive_t  (PMIX_ALLOC_DIRECTIVE)
 * - pmix_iof_channel_t  (PMIX_IOF_CHANNEL)
 */
PMIX_EXPORT const char* PMIx_Error_string(pmix_status_t status);
PMIX_EXPORT const char* PMIx_Proc_state_string(pmix_proc_state_t state);
PMIX_EXPORT const char* PMIx_Scope_string(pmix_scope_t scope);
PMIX_EXPORT const char* PMIx_Persistence_string(pmix_persistence_t persist);
PMIX_EXPORT const char* PMIx_Data_range_string(pmix_data_range_t range);
PMIX_EXPORT const char* PMIx_Info_directives_string(pmix_info_directives_t directives);
PMIX_EXPORT const char* PMIx_Data_type_string(pmix_data_type_t type);
PMIX_EXPORT const char* PMIx_Alloc_directive_string(pmix_alloc_directive_t directive);
PMIX_EXPORT const char* PMIx_IOF_channel_string(pmix_iof_channel_t channel);

/* Get the PMIx version string. Note that the provided string is
 * statically defined and must NOT be free'd  */
PMIX_EXPORT const char* PMIx_Get_version(void);

/* Store some data locally for retrieval by other areas of the
 * proc. This is data that has only internal scope - it will
 * never be "pushed" externally */
PMIX_EXPORT pmix_status_t PMIx_Store_internal(const pmix_proc_t *proc,
                                              const pmix_key_t key, pmix_value_t *val);

/**
 * Top-level interface function to pack one or more values into a
 * buffer.
 *
 * The pack function packs one or more values of a specified type into
 * the specified buffer.  The buffer must have already been
 * initialized via the PMIX_DATA_BUFFER_CREATE or PMIX_DATA_BUFFER_CONSTRUCT
 * call - otherwise, the pack_value function will return an error.
 * Providing an unsupported type flag will likewise be reported as an error.
 *
 * Note that any data to be packed that is not hard type cast (i.e.,
 * not type cast to a specific size) may lose precision when unpacked
 * by a non-homogeneous recipient.  The PACK function will do its best to deal
 * with heterogeneity issues between the packer and unpacker in such
 * cases. Sending a number larger than can be handled by the recipient
 * will return an error code (generated upon unpacking) -
 * the error cannot be detected during packing.
 *
 * The identity of the intended recipient of the packed buffer (i.e., the
 * process that will be unpacking it) is used solely to resolve any data type
 * differences between PMIx versions. The recipient must, therefore, be
 * known to the user prior to calling the pack function so that the
 * PMIx library is aware of the version the recipient is using.
 *
 * @param *target Pointer to a pmix_proc_t structure containing the
 * nspace/rank of the process that will be unpacking the final buffer.
 * A NULL value may be used to indicate that the target is based on
 * the same PMIx version as the caller.
 *
 * @param *buffer A pointer to the buffer into which the value is to
 * be packed.
 *
 * @param *src A void* pointer to the data that is to be packed. Note
 * that strings are to be passed as (char **) - i.e., the caller must
 * pass the address of the pointer to the string as the void*. This
 * allows PMIx to use a single pack function, but still allow
 * the caller to pass multiple strings in a single call.
 *
 * @param num_values An int32_t indicating the number of values that are
 * to be packed, beginning at the location pointed to by src. A string
 * value is counted as a single value regardless of length. The values
 * must be contiguous in memory. Arrays of pointers (e.g., string
 * arrays) should be contiguous, although (obviously) the data pointed
 * to need not be contiguous across array entries.
 *
 * @param type The type of the data to be packed - must be one of the
 * PMIX defined data types.
 *
 * @retval PMIX_SUCCESS The data was packed as requested.
 *
 * @retval PMIX_ERROR(s) An appropriate PMIX error code indicating the
 * problem encountered. This error code should be handled
 * appropriately.
 *
 * @code
 * pmix_data_buffer_t *buffer;
 * int32_t src;
 *
 * PMIX_DATA_BUFFER_CREATE(buffer);
 * status_code = PMIx_Data_pack(buffer, &src, 1, PMIX_INT32);
 * @endcode
 */
PMIX_EXPORT pmix_status_t PMIx_Data_pack(const pmix_proc_t *target,
                                         pmix_data_buffer_t *buffer,
                                         void *src, int32_t num_vals,
                                         pmix_data_type_t type);

/**
 * Unpack values from a buffer.
 *
 * The unpack function unpacks the next value (or values) of a
 * specified type from the specified buffer.
 *
 * The buffer must have already been initialized via an PMIX_DATA_BUFFER_CREATE or
 * PMIX_DATA_BUFFER_CONSTRUCT call (and assumedly filled with some data) -
 * otherwise, the unpack_value function will return an
 * error. Providing an unsupported type flag will likewise be reported
 * as an error, as will specifying a data type that DOES NOT match the
 * type of the next item in the buffer. An attempt to read beyond the
 * end of the stored data held in the buffer will also return an
 * error.
 *
 * NOTE: it is possible for the buffer to be corrupted and that
 * PMIx will *think* there is a proper variable type at the
 * beginning of an unpack region - but that the value is bogus (e.g., just
 * a byte field in a string array that so happens to have a value that
 * matches the specified data type flag). Therefore, the data type error check
 * is NOT completely safe. This is true for ALL unpack functions.
 *
 *
 * Unpacking values is a "nondestructive" process - i.e., the values are
 * not removed from the buffer. It is therefore possible for the caller
 * to re-unpack a value from the same buffer by resetting the unpack_ptr.
 *
 * Warning: The caller is responsible for providing adequate memory
 * storage for the requested data. As noted below, the user
 * must provide a parameter indicating the maximum number of values that
 * can be unpacked into the allocated memory. If more values exist in the
 * buffer than can fit into the memory storage, then the function will unpack
 * what it can fit into that location and return an error code indicating
 * that the buffer was only partially unpacked.
 *
 * Note that any data that was not hard type cast (i.e., not type cast
 * to a specific size) when packed may lose precision when unpacked by
 * a non-homogeneous recipient.  PMIx will do its best to deal with
 * heterogeneity issues between the packer and unpacker in such
 * cases. Sending a number larger than can be handled by the recipient
 * will return an error code generated upon unpacking - these errors
 * cannot be detected during packing.
 *
 * The identity of the source of the packed buffer (i.e., the
 * process that packed it) is used solely to resolve any data type
 * differences between PMIx versions. The source must, therefore, be
 * known to the user prior to calling the unpack function so that the
 * PMIx library is aware of the version the source used.
 *
 * @param *source Pointer to a pmix_proc_t structure containing the
 * nspace/rank of the process that packed the provided buffer.
 * A NULL value may be used to indicate that the source is based on
 * the same PMIx version as the caller.
 *
 * @param *buffer A pointer to the buffer from which the value will be
 * extracted.
 *
 * @param *dest A void* pointer to the memory location into which the
 * data is to be stored. Note that these values will be stored
 * contiguously in memory. For strings, this pointer must be to (char
 * **) to provide a means of supporting multiple string
 * operations. The unpack function will allocate memory for each
 * string in the array - the caller must only provide adequate memory
 * for the array of pointers.
 *
 * @param type The type of the data to be unpacked - must be one of
 * the BFROP defined data types.
 *
 * @retval *max_num_values The number of values actually unpacked. In
 * most cases, this should match the maximum number provided in the
 * parameters - but in no case will it exceed the value of this
 * parameter.  Note that if you unpack fewer values than are actually
 * available, the buffer will be in an unpackable state - the function will
 * return an error code to warn of this condition.
 *
 * @note The unpack function will return the actual number of values
 * unpacked in this location.
 *
 * @retval PMIX_SUCCESS The next item in the buffer was successfully
 * unpacked.
 *
 * @retval PMIX_ERROR(s) The unpack function returns an error code
 * under one of several conditions: (a) the number of values in the
 * item exceeds the max num provided by the caller; (b) the type of
 * the next item in the buffer does not match the type specified by
 * the caller; or (c) the unpack failed due to either an error in the
 * buffer or an attempt to read past the end of the buffer.
 *
 * @code
 * pmix_data_buffer_t *buffer;
 * int32_t dest;
 * char **string_array;
 * int32_t num_values;
 *
 * num_values = 1;
 * status_code = PMIx_Data_unpack(buffer, (void*)&dest, &num_values, PMIX_INT32);
 *
 * num_values = 5;
 * string_array = malloc(num_values*sizeof(char *));
 * status_code = PMIx_Data_unpack(buffer, (void*)(string_array), &num_values, PMIX_STRING);
 *
 * @endcode
 */
PMIX_EXPORT pmix_status_t PMIx_Data_unpack(const pmix_proc_t *source,
                                           pmix_data_buffer_t *buffer, void *dest,
                                           int32_t *max_num_values,
                                           pmix_data_type_t type);

/**
 * Copy a data value from one location to another.
 *
 * Since registered data types can be complex structures, the system
 * needs some way to know how to copy the data from one location to
 * another (e.g., for storage in the registry). This function, which
 * can call other copy functions to build up complex data types, defines
 * the method for making a copy of the specified data type.
 *
 * @param **dest The address of a pointer into which the
 * address of the resulting data is to be stored.
 *
 * @param *src A pointer to the memory location from which the
 * data is to be copied.
 *
 * @param type The type of the data to be copied - must be one of
 * the PMIx defined data types.
 *
 * @retval PMIX_SUCCESS The value was successfully copied.
 *
 * @retval PMIX_ERROR(s) An appropriate error code.
 *
 */
PMIX_EXPORT pmix_status_t PMIx_Data_copy(void **dest, void *src,
                                         pmix_data_type_t type);

/**
 * Print a data value.
 *
 * Since registered data types can be complex structures, the system
 * needs some way to know how to print them (i.e., convert them to a string
 * representation). Provided for debug purposes.
 *
 * @retval PMIX_SUCCESS The value was successfully printed.
 *
 * @retval PMIX_ERROR(s) An appropriate error code.
 */
PMIX_EXPORT pmix_status_t PMIx_Data_print(char **output, char *prefix,
                                          void *src, pmix_data_type_t type);

/**
 * Copy a payload from one buffer to another
 *
 * This function will append a copy of the payload in one buffer into
 * another buffer.
 * NOTE: This is NOT a destructive procedure - the
 * source buffer's payload will remain intact, as will any pre-existing
 * payload in the destination's buffer.
 */
PMIX_EXPORT pmix_status_t PMIx_Data_copy_payload(pmix_data_buffer_t *dest,
                                                 pmix_data_buffer_t *src);


static inline void pmix_darray_destruct(pmix_data_array_t *m);

static inline void pmix_value_destruct(pmix_value_t * m)
{
    if (PMIX_STRING == (m)->type) {
        if (NULL != (m)->data.string) {
            free((m)->data.string);
            (m)->data.string = NULL;
        }
    } else if ((PMIX_BYTE_OBJECT == (m)->type) ||
               (PMIX_COMPRESSED_STRING == (m)->type)) {
        if (NULL != (m)->data.bo.bytes) {
            free((m)->data.bo.bytes);
            (m)->data.bo.bytes = NULL;
            (m)->data.bo.size = 0;
        }
    } else if (PMIX_DATA_ARRAY == (m)->type) {
        if (NULL != (m)->data.darray) {
            pmix_darray_destruct((m)->data.darray);
            free((m)->data.darray);
            (m)->data.darray = NULL;
        }
    } else if (PMIX_ENVAR == (m)->type) {
        PMIX_ENVAR_DESTRUCT(&(m)->data.envar);
    } else if (PMIX_PROC == (m)->type) {
        PMIX_PROC_RELEASE((m)->data.proc);
    }
}

static inline void pmix_darray_destruct(pmix_data_array_t *m)
{
    if (NULL != m) {
        if (PMIX_INFO == m->type) {
            pmix_info_t *_info = (pmix_info_t*)m->array;
            PMIX_INFO_FREE(_info, m->size);
        } else if (PMIX_PROC == m->type) {
            pmix_proc_t *_p = (pmix_proc_t*)m->array;
            PMIX_PROC_FREE(_p, m->size);
        } else if (PMIX_PROC_INFO == m->type) {
            pmix_proc_info_t *_pi = (pmix_proc_info_t*)m->array;
            PMIX_PROC_INFO_FREE(_pi, m->size);
        } else if (PMIX_ENVAR == m->type) {
            pmix_envar_t *_e = (pmix_envar_t*)m->array;
            PMIX_ENVAR_FREE(_e, m->size);
        } else if (PMIX_VALUE == m->type) {
            pmix_value_t *_v = (pmix_value_t*)m->array;
            PMIX_VALUE_FREE(_v, m->size);
        } else if (PMIX_PDATA == m->type) {
            pmix_pdata_t *_pd = (pmix_pdata_t*)m->array;
            PMIX_PDATA_FREE(_pd, m->size);
        } else if (PMIX_QUERY == m->type) {
            pmix_query_t *_q = (pmix_query_t*)m->array;
            PMIX_QUERY_FREE(_q, m->size);
        } else if (PMIX_APP == m->type) {
            pmix_app_t *_a = (pmix_app_t*)m->array;
            PMIX_APP_FREE(_a, m->size);
        } else if (PMIX_BYTE_OBJECT == m->type) {
            pmix_byte_object_t *_b = (pmix_byte_object_t*)m->array;
            PMIX_BYTE_OBJECT_FREE(_b, m->size);
        } else if (PMIX_STRING == m->type) {
            char **_s = (char**)m->array;
            size_t _si;
            for (_si=0; _si < m->size; _si++) {
                free(_s[_si]);
            }
            free(m->array);
            m->array = NULL;
        } else {
            free(m->array);
        }
    }
}

#define PMIX_DATA_ARRAY_CONSTRUCT(m, n, t)                          \
    do {                                                            \
        (m)->type = (t);                                            \
        (m)->size = (n);                                            \
        if (0 < (n)) {                                              \
            if (PMIX_INFO == (t)) {                                 \
                PMIX_INFO_CREATE((m)->array, (n));                  \
            } else if (PMIX_PROC == (t)) {                          \
                PMIX_PROC_CREATE((m)->array, (n));                  \
            } else if (PMIX_PROC_INFO == (t)) {                     \
                PMIX_PROC_INFO_CREATE((m)->array, (n));             \
            } else if (PMIX_ENVAR == (t)) {                         \
                PMIX_ENVAR_CREATE((m)->array, (n));                 \
            } else if (PMIX_VALUE == (t)) {                         \
                PMIX_VALUE_CREATE((m)->array, (n));                 \
            } else if (PMIX_PDATA == (t)) {                         \
                PMIX_PDATA_CREATE((m)->array, (n));                 \
            } else if (PMIX_QUERY == (t)) {                         \
                PMIX_QUERY_CREATE((m)->array, (n));                 \
            } else if (PMIX_APP == (t)) {                           \
                PMIX_APP_CREATE((m)->array, (n));                   \
            } else if (PMIX_BYTE_OBJECT == (t)) {                   \
                PMIX_BYTE_OBJECT_CREATE((m)->array, (n));           \
            } else if (PMIX_ALLOC_DIRECTIVE == (t) ||               \
                       PMIX_PROC_STATE == (t) ||                    \
                       PMIX_PERSIST == (t) ||                       \
                       PMIX_SCOPE == (t) ||                         \
                       PMIX_DATA_RANGE == (t) ||                    \
                       PMIX_BYTE == (t) ||                          \
                       PMIX_INT8 == (t) ||                          \
                       PMIX_UINT8 == (t)) {                         \
                (m)->array = calloc((n), sizeof(int8_t));           \
            } else if (PMIX_STRING == (t)) {                        \
                (m)->array = calloc((n), sizeof(char*));            \
            } else if (PMIX_SIZE == (t)) {                          \
                (m)->array = calloc((n), sizeof(size_t));           \
            } else if (PMIX_PID == (t)) {                           \
                (m)->array = calloc((n), sizeof(pid_t));            \
            } else if (PMIX_INT == (t) ||                           \
                       PMIX_UINT == (t) ||                          \
                       PMIX_STATUS == (t)) {                        \
                (m)->array = calloc((n), sizeof(int));              \
            } else if (PMIX_IOF_CHANNEL == (t) ||                   \
                       PMIX_DATA_TYPE == (t) ||                     \
                       PMIX_INT16 == (t) ||                         \
                       PMIX_UINT16 == (t)) {                        \
                (m)->array = calloc((n), sizeof(int16_t));          \
            } else if (PMIX_PROC_RANK == (t) ||                     \
                       PMIX_INFO_DIRECTIVES == (t) ||               \
                       PMIX_INT32 == (t) ||                         \
                       PMIX_UINT32 == (t)) {                        \
                (m)->array = calloc((n), sizeof(int32_t));          \
            } else if (PMIX_INT64 == (t) ||                         \
                       PMIX_UINT64 == (t)) {                        \
                (m)->array = calloc((n), sizeof(int64_t));          \
            } else if (PMIX_FLOAT == (t)) {                         \
                (m)->array = calloc((n), sizeof(float));            \
            } else if (PMIX_DOUBLE == (t)) {                        \
                (m)->array = calloc((n), sizeof(double));           \
            } else if (PMIX_TIMEVAL == (t)) {                       \
                (m)->array = calloc((n), sizeof(struct timeval));   \
            } else if (PMIX_TIME == (t)) {                          \
                (m)->array = calloc((n), sizeof(time_t));           \
            }                                                       \
        } else {                                                    \
            (m)->array = NULL;                                      \
        }                                                           \
    } while(0)
#define PMIX_DATA_ARRAY_CREATE(m, n, t)                                   \
    do {                                                                  \
        (m) = (pmix_data_array_t*)calloc(1, sizeof(pmix_data_array_t));   \
        PMIX_DATA_ARRAY_CONSTRUCT((m), (n), (t));                         \
    } while(0)

#define PMIX_DATA_ARRAY_DESTRUCT(m) pmix_darray_destruct(m)

#define PMIX_DATA_ARRAY_FREE(m)             \
    do {                                    \
        if (NULL != (m)) {                  \
            PMIX_DATA_ARRAY_DESTRUCT(m);    \
            free((m));                      \
            (m) = NULL;                     \
        }                                   \
    } while(0)


/**
 * Provide a safe version of strncpy that doesn't generate
 * a ton of spurious warnings. Note that not every environment
 * provides nice string functions, and we aren't concerned about
 * max performance here
 *
 * @param dest Destination string.
 * @param src Source string.
 * @param len Size of the dest array - 1
 *
 */
static inline void pmix_strncpy(char *dest, const char *src, size_t len)
{
    size_t i, k;
    char *new_dest = dest;

    /* use an algorithm that also protects against
     * non-NULL-terminated src strings */
    for (i=0, k=0; i <= len; ++i, ++src, ++new_dest) {
        ++k;
        *new_dest = *src;
        if ('\0' == *src) {
            break;
        }
    }
    dest[k-1] = '\0';
}

#include <pmix_extend.h>

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif

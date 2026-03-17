/* opal/include/opal_config.h.  Generated from opal_config.h.in by configure.  */
/* opal/include/opal_config.h.in.  Generated from configure.ac by autoheader.  */

/* -*- c -*-
 *
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * Function: - OS, CPU and compiler dependent configuration
 */

#ifndef OPAL_CONFIG_H
#define OPAL_CONFIG_H

#include "opal_config_top.h"



/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* rdmacm without IB_AF addressing support */
/* #undef BTL_OPENIB_RDMACM_IB_ADDR */

/* defined to 1 if cray wlm available, 0 otherwise */
/* #undef CRAY_WLM_DETECT */

/* Version of event */
#define EVENT_EXTERNAL_EVENT_VERSION external

/* Define to 1 if you have the <aio.h> header file. */
#define HAVE_AIO_H 1

/* Define to 1 if you have the <alloca.h> header file. */
#define HAVE_ALLOCA_H 1

/* Define to 1 if you have the <alps/apInfo.h> header file. */
/* #undef HAVE_ALPS_APINFO_H */

/* Define to 1 if you have the <arpa/inet.h> header file. */
#define HAVE_ARPA_INET_H 1

/* Define to 1 if you have the `asprintf' function. */
#define HAVE_ASPRINTF 1

/* Define to 1 if the system has the type `CACHE_DESCRIPTOR'. */
/* #undef HAVE_CACHE_DESCRIPTOR */

/* Define to 1 if the system has the type `CACHE_RELATIONSHIP'. */
/* #undef HAVE_CACHE_RELATIONSHIP */

/* Define to 1 if you have the `clz' function. */
/* #undef HAVE_CLZ */

/* Define to 1 if you have the `clzl' function. */
/* #undef HAVE_CLZL */

/* Define to 1 if you have the <CL/cl_ext.h> header file. */
/* #undef HAVE_CL_CL_EXT_H */

/* Define to 1 if you have the <complex.h> header file. */
#define HAVE_COMPLEX_H 1

/* Define to 1 if you have the `cpuset_setaffinity' function. */
/* #undef HAVE_CPUSET_SETAFFINITY */

/* Define to 1 if you have the `cpuset_setid' function. */
/* #undef HAVE_CPUSET_SETID */

/* Define to 1 if you have the <crt_externs.h> header file. */
/* #undef HAVE_CRT_EXTERNS_H */

/* Define to 1 if you have the <ctype.h> header file. */
#define HAVE_CTYPE_H 1

/* Define to 1 if we have -lcuda */
/* #undef HAVE_CUDA */

/* Define to 1 if you have the <cuda.h> header file. */
/* #undef HAVE_CUDA_H */

/* Define to 1 if you have the <cuda_runtime_api.h> header file. */
/* #undef HAVE_CUDA_RUNTIME_API_H */

/* Define to 1 if you have the `dbm_open' function. */
/* #undef HAVE_DBM_OPEN */

/* Define to 1 if you have the `dbopen' function. */
/* #undef HAVE_DBOPEN */

/* Define to 1 if you have the <db.h> header file. */
/* #undef HAVE_DB_H */

/* Define to 1 if you have the declaration of `AF_INET6', and to 0 if you
   don't. */
#define HAVE_DECL_AF_INET6 1

/* Define to 1 if you have the declaration of `AF_UNSPEC', and to 0 if you
   don't. */
#define HAVE_DECL_AF_UNSPEC 1

/* Define to 1 if you have the declaration of `CTL_HW', and to 0 if you don't.
   */
#define HAVE_DECL_CTL_HW 0

/* Define to 1 if you have the declaration of `ethtool_cmd_speed', and to 0 if
   you don't. */
#define HAVE_DECL_ETHTOOL_CMD_SPEED 1

/* Define to 1 if you have the declaration of `fabsf', and to 0 if you don't.
   */
#define HAVE_DECL_FABSF 1

/* Define to 1 if you have the declaration of `getexecname', and to 0 if you
   don't. */
#define HAVE_DECL_GETEXECNAME 0

/* Define to 1 if you have the declaration of `GetModuleFileName', and to 0 if
   you don't. */
#define HAVE_DECL_GETMODULEFILENAME 0

/* Define to 1 if you have the declaration of `getprogname', and to 0 if you
   don't. */
#define HAVE_DECL_GETPROGNAME 0

/* Define to 1 if you have the declaration of `HWLOC_OBJ_OSDEV_COPROC', and to
   0 if you don't. */
#define HAVE_DECL_HWLOC_OBJ_OSDEV_COPROC 1

/* Define to 1 if you have the declaration of `HW_NCPU', and to 0 if you
   don't. */
#define HAVE_DECL_HW_NCPU 0

/* Define to 1 if you have the declaration of `HZ', and to 0 if you don't. */
#define HAVE_DECL_HZ 1

/* Define to 1 if you have the declaration of `IBV_ACCESS_ALLOCATE_MR', and to
   0 if you don't. */
/* #undef HAVE_DECL_IBV_ACCESS_ALLOCATE_MR */

/* Define to 1 if you have the declaration of
   `IBV_ACCESS_SHARED_MR_USER_READ', and to 0 if you don't. */
/* #undef HAVE_DECL_IBV_ACCESS_SHARED_MR_USER_READ */

/* Define to 1 if you have the declaration of `IBV_ACCESS_SO', and to 0 if you
   don't. */
/* #undef HAVE_DECL_IBV_ACCESS_SO */

/* Define to 1 if you have the declaration of `IBV_ATOMIC_HCA', and to 0 if
   you don't. */
/* #undef HAVE_DECL_IBV_ATOMIC_HCA */

/* Define to 1 if you have the declaration of `ibv_cmd_open_xrcd', and to 0 if
   you don't. */
/* #undef HAVE_DECL_IBV_CMD_OPEN_XRCD */

/* Define to 1 if you have the declaration of `ibv_create_xrc_rcv_qp', and to
   0 if you don't. */
/* #undef HAVE_DECL_IBV_CREATE_XRC_RCV_QP */

/* Define to 1 if you have the declaration of `IBV_DEVICE_XRC', and to 0 if
   you don't. */
/* #undef HAVE_DECL_IBV_DEVICE_XRC */

/* Define to 1 if you have the declaration of `IBV_EVENT_CLIENT_REREGISTER',
   and to 0 if you don't. */
/* #undef HAVE_DECL_IBV_EVENT_CLIENT_REREGISTER */

/* Define to 1 if you have the declaration of `IBV_EXP_ACCESS_ALLOCATE_MR',
   and to 0 if you don't. */
/* #undef HAVE_DECL_IBV_EXP_ACCESS_ALLOCATE_MR */

/* Define to 1 if you have the declaration of
   `IBV_EXP_ACCESS_SHARED_MR_USER_READ', and to 0 if you don't. */
/* #undef HAVE_DECL_IBV_EXP_ACCESS_SHARED_MR_USER_READ */

/* Define to 1 if you have the declaration of `IBV_EXP_ATOMIC_HCA_REPLY_BE',
   and to 0 if you don't. */
#define HAVE_DECL_IBV_EXP_ATOMIC_HCA_REPLY_BE 0

/* Define to 1 if you have the declaration of `ibv_exp_create_qp', and to 0 if
   you don't. */
#define HAVE_DECL_IBV_EXP_CREATE_QP 0

/* Define to 1 if you have the declaration of
   `IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY', and to 0 if you don't. */
#define HAVE_DECL_IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY 0

/* Define to 1 if you have the declaration of
   `IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG', and to 0 if you don't. */
#define HAVE_DECL_IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG 0

/* Define to 1 if you have the declaration of `ibv_exp_query_device', and to 0
   if you don't. */
#define HAVE_DECL_IBV_EXP_QUERY_DEVICE 0

/* Define to 1 if you have the declaration of `IBV_LINK_LAYER_ETHERNET', and
   to 0 if you don't. */
/* #undef HAVE_DECL_IBV_LINK_LAYER_ETHERNET */

/* Define to 1 if you have the declaration of `IBV_SRQT_XRC', and to 0 if you
   don't. */
/* #undef HAVE_DECL_IBV_SRQT_XRC */

/* Define to 1 if you have the declaration of `lgrp_latency_cookie', and to 0
   if you don't. */
/* #undef HAVE_DECL_LGRP_LATENCY_COOKIE */

/* Define to 1 if you have the declaration of
   `nvmlDeviceGetMaxPcieLinkGeneration', and to 0 if you don't. */
/* #undef HAVE_DECL_NVMLDEVICEGETMAXPCIELINKGENERATION */

/* Define to 1 if you have the declaration of `PF_INET6', and to 0 if you
   don't. */
#define HAVE_DECL_PF_INET6 1

/* Define to 1 if you have the declaration of `PF_UNSPEC', and to 0 if you
   don't. */
#define HAVE_DECL_PF_UNSPEC 1

/* Define to 1 if you have the declaration of `pthread_getaffinity_np', and to
   0 if you don't. */
#define HAVE_DECL_PTHREAD_GETAFFINITY_NP 1

/* Define to 1 if you have the declaration of `pthread_setaffinity_np', and to
   0 if you don't. */
#define HAVE_DECL_PTHREAD_SETAFFINITY_NP 1

/* Define to 1 if you have the declaration of `RLIMIT_AS', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_AS 1

/* Define to 1 if you have the declaration of `RLIMIT_CORE', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_CORE 1

/* Define to 1 if you have the declaration of `RLIMIT_FSIZE', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_FSIZE 1

/* Define to 1 if you have the declaration of `RLIMIT_MEMLOCK', and to 0 if
   you don't. */
#define HAVE_DECL_RLIMIT_MEMLOCK 1

/* Define to 1 if you have the declaration of `RLIMIT_NOFILE', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_NOFILE 1

/* Define to 1 if you have the declaration of `RLIMIT_NPROC', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_NPROC 1

/* Define to 1 if you have the declaration of `RLIMIT_STACK', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_STACK 1

/* Embedded mode; just assume we do not have Valgrind support */
#define HAVE_DECL_RUNNING_ON_VALGRIND 0

/* Define to 1 if you have the declaration of `sched_getcpu', and to 0 if you
   don't. */
#define HAVE_DECL_SCHED_GETCPU 1

/* Define to 1 if you have the declaration of `SIOCETHTOOL', and to 0 if you
   don't. */
#define HAVE_DECL_SIOCETHTOOL 1

/* Define to 1 if you have the declaration of `snprintf', and to 0 if you
   don't. */
#define HAVE_DECL_SNPRINTF 1

/* Define to 1 if you have the declaration of `strcasecmp', and to 0 if you
   don't. */
#define HAVE_DECL_STRCASECMP 1

/* Define to 1 if you have the declaration of `strtoull', and to 0 if you
   don't. */
#define HAVE_DECL_STRTOULL 1

/* Define to 1 if you have the declaration of `ucm_test_events', and to 0 if
   you don't. */
#define HAVE_DECL_UCM_TEST_EVENTS 0

/* Define to 1 if you have the declaration of `UCP_ATOMIC_FETCH_OP_FAND', and
   to 0 if you don't. */
#define HAVE_DECL_UCP_ATOMIC_FETCH_OP_FAND 0

/* Define to 1 if you have the declaration of `UCP_ATOMIC_FETCH_OP_FOR', and
   to 0 if you don't. */
#define HAVE_DECL_UCP_ATOMIC_FETCH_OP_FOR 0

/* Define to 1 if you have the declaration of `UCP_ATOMIC_FETCH_OP_FXOR', and
   to 0 if you don't. */
#define HAVE_DECL_UCP_ATOMIC_FETCH_OP_FXOR 0

/* Define to 1 if you have the declaration of `UCP_ATOMIC_POST_OP_AND', and to
   0 if you don't. */
#define HAVE_DECL_UCP_ATOMIC_POST_OP_AND 0

/* Define to 1 if you have the declaration of `UCP_ATOMIC_POST_OP_OR', and to
   0 if you don't. */
#define HAVE_DECL_UCP_ATOMIC_POST_OP_OR 0

/* Define to 1 if you have the declaration of `UCP_ATOMIC_POST_OP_XOR', and to
   0 if you don't. */
#define HAVE_DECL_UCP_ATOMIC_POST_OP_XOR 0

/* Define to 1 if you have the declaration of `ucp_ep_flush_nb', and to 0 if
   you don't. */
#define HAVE_DECL_UCP_EP_FLUSH_NB 0

/* Define to 1 if you have the declaration of `ucp_get_nb', and to 0 if you
   don't. */
#define HAVE_DECL_UCP_GET_NB 0

/* Define to 1 if you have the declaration of `ucp_put_nb', and to 0 if you
   don't. */
#define HAVE_DECL_UCP_PUT_NB 0

/* Define to 1 if you have the declaration of `ucp_request_check_status', and
   to 0 if you don't. */
#define HAVE_DECL_UCP_REQUEST_CHECK_STATUS 0

/* Define to 1 if you have the declaration of `ucp_tag_send_nbr', and to 0 if
   you don't. */
#define HAVE_DECL_UCP_TAG_SEND_NBR 0

/* Define to 1 if you have the declaration of
   `UCP_WORKER_ATTR_FIELD_ADDRESS_FLAGS', and to 0 if you don't. */
#define HAVE_DECL_UCP_WORKER_ATTR_FIELD_ADDRESS_FLAGS 0

/* Define to 1 if you have the declaration of `ucp_worker_flush_nb', and to 0
   if you don't. */
#define HAVE_DECL_UCP_WORKER_FLUSH_NB 0

/* Define to 1 if you have the declaration of `UCT_CB_FLAG_SYNC', and to 0 if
   you don't. */
/* #undef HAVE_DECL_UCT_CB_FLAG_SYNC */

/* Define to 1 if you have the declaration of `UCT_PROGRESS_THREAD_SAFE', and
   to 0 if you don't. */
/* #undef HAVE_DECL_UCT_PROGRESS_THREAD_SAFE */

/* Define to 1 if you have the declaration of `_putenv', and to 0 if you
   don't. */
#define HAVE_DECL__PUTENV 0

/* Define to 1 if you have the declaration of `_SC_LARGE_PAGESIZE', and to 0
   if you don't. */
#define HAVE_DECL__SC_LARGE_PAGESIZE 0

/* Define to 1 if you have the declaration of `_SC_NPROCESSORS_CONF', and to 0
   if you don't. */
#define HAVE_DECL__SC_NPROCESSORS_CONF 1

/* Define to 1 if you have the declaration of `_SC_NPROCESSORS_ONLN', and to 0
   if you don't. */
#define HAVE_DECL__SC_NPROCESSORS_ONLN 1

/* Define to 1 if you have the declaration of `_SC_NPROC_CONF', and to 0 if
   you don't. */
#define HAVE_DECL__SC_NPROC_CONF 0

/* Define to 1 if you have the declaration of `_SC_NPROC_ONLN', and to 0 if
   you don't. */
#define HAVE_DECL__SC_NPROC_ONLN 0

/* Define to 1 if you have the declaration of `_SC_PAGESIZE', and to 0 if you
   don't. */
#define HAVE_DECL__SC_PAGESIZE 1

/* Define to 1 if you have the declaration of `_SC_PAGE_SIZE', and to 0 if you
   don't. */
#define HAVE_DECL__SC_PAGE_SIZE 1

/* Define to 1 if you have the declaration of `_strdup', and to 0 if you
   don't. */
#define HAVE_DECL__STRDUP 0

/* Define to 1 if you have the declaration of `__func__', and to 0 if you
   don't. */
#define HAVE_DECL___FUNC__ 1

/* Define to 1 if you have the declaration of `__mmap', and to 0 if you don't.
   */
#define HAVE_DECL___MMAP 0

/* Define to 1 if you have the declaration of `__syscall', and to 0 if you
   don't. */
#define HAVE_DECL___SYSCALL 0

/* Define to 1 if you have the <dirent.h> header file. */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if you have the `dlsym' function. */
/* #undef HAVE_DLSYM */

/* Define to 1 if the system has the type `Dl_info'. */
/* #undef HAVE_DL_INFO */

/* Define to 1 if the system has the type `double _Complex'. */
#define HAVE_DOUBLE__COMPLEX 1

/* Define to 1 if you have the <elf.h> header file. */
#define HAVE_ELF_H 1

/* Define to 1 if you have the <endian.h> header file. */
#define HAVE_ENDIAN_H 1

/* Define to 1 if you have the <err.h> header file. */
#define HAVE_ERR_H 1

/* Define to 1 if you have the <event2/event.h> header file. */
#define HAVE_EVENT2_EVENT_H 1

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* Define to 1 if you have the `execve' function. */
#define HAVE_EXECVE 1

/* Experimental verbs */
#define HAVE_EXP_VERBS 0

/* Define to 1 if you have the <fca/fca_api.h> header file. */
/* #undef HAVE_FCA_FCA_API_H */

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if you have the `ffs' function. */
#define HAVE_FFS 1

/* Define to 1 if you have the `ffsl' function. */
#define HAVE_FFSL 1

/* Define to 1 if the system has the type `float _Complex'. */
#define HAVE_FLOAT__COMPLEX 1

/* Define to 1 if you have the `fls' function. */
/* #undef HAVE_FLS */

/* Define to 1 if you have the `flsl' function. */
/* #undef HAVE_FLSL */

/* Flux support builds against external PMI library */
/* #undef HAVE_FLUX_PMI_LIBRARY */

/* Define to 1 if you have the `fork' function. */
#define HAVE_FORK 1

/* Define to 1 if you have the `getpagesize' function. */
#define HAVE_GETPAGESIZE 1

/* Define to 1 if you have the `getpwuid' function. */
#define HAVE_GETPWUID 1

/* Define to 1 if you have the <glob.h> header file. */
/* #undef HAVE_GLOB_H */

/* Define to 1 if you have the `GNI_GetJobResInfo' function. */
/* #undef HAVE_GNI_GETJOBRESINFO */

/* Define to 1 if the system has the type `GROUP_AFFINITY'. */
/* #undef HAVE_GROUP_AFFINITY */

/* Define to 1 if the system has the type `GROUP_RELATIONSHIP'. */
/* #undef HAVE_GROUP_RELATIONSHIP */

/* Define to 1 if you have the <grp.h> header file. */
#define HAVE_GRP_H 1

/* Define to 1 if you have the <hcoll/api/hcoll_api.h> header file. */
/* #undef HAVE_HCOLL_API_HCOLL_API_H */

/* Define to 1 if you have the `hcoll_context_free' function. */
/* #undef HAVE_HCOLL_CONTEXT_FREE */

/* Define to 1 if you have the <hostLib.h> header file. */
/* #undef HAVE_HOSTLIB_H */

/* Define to 1 if you have the `host_info' function. */
/* #undef HAVE_HOST_INFO */

/* Define to 1 if you have the <hwloc.h> header file. */
#define HAVE_HWLOC_H 1

/* Define to 1 if you have the `hwloc_topology_dup' function. */
#define HAVE_HWLOC_TOPOLOGY_DUP 1

/* Define to 1 if you have the `ibv_cmd_open_xrcd' function. */
/* #undef HAVE_IBV_CMD_OPEN_XRCD */

/* Define to 1 if you have the `ibv_create_xrc_rcv_qp' function. */
/* #undef HAVE_IBV_CREATE_XRC_RCV_QP */

/* Define to 1 if you have the `ibv_fork_init' function. */
/* #undef HAVE_IBV_FORK_INIT */

/* Define to 1 if you have the `ibv_get_device_list' function. */
/* #undef HAVE_IBV_GET_DEVICE_LIST */

/* Define to 1 if you have the `ibv_resize_cq' function. */
/* #undef HAVE_IBV_RESIZE_CQ */

/* Define to 1 if you have the <ieee754.h> header file. */
#define HAVE_IEEE754_H 1

/* Define to 1 if you have the <ifaddrs.h> header file. */
#define HAVE_IFADDRS_H 1

/* Define to 1 if you have the <infiniband/driver.h> header file. */
/* #undef HAVE_INFINIBAND_DRIVER_H */

/* Define to 1 if you have the <infiniband/verbs.h> header file. */
/* #undef HAVE_INFINIBAND_VERBS_H */

/* Define to 1 if the system has the type `int128_t'. */
/* #undef HAVE_INT128_T */

/* Define to 1 if the system has the type `int16_t'. */
#define HAVE_INT16_T 1

/* Define to 1 if the system has the type `int32_t'. */
#define HAVE_INT32_T 1

/* Define to 1 if the system has the type `int64_t'. */
#define HAVE_INT64_T 1

/* Define to 1 if the system has the type `int8_t'. */
#define HAVE_INT8_T 1

/* Define to 1 if the system has the type `intptr_t'. */
#define HAVE_INTPTR_T 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the <ioLib.h> header file. */
/* #undef HAVE_IOLIB_H */

/* Define to 1 if you have the `isatty' function. */
#define HAVE_ISATTY 1

/* Define to 1 if the system has the type `KAFFINITY'. */
/* #undef HAVE_KAFFINITY */

/* Define to 1 if you have the <knem_io.h> header file. */
/* #undef HAVE_KNEM_IO_H */

/* Define to 1 if you have the <kstat.h> header file. */
/* #undef HAVE_KSTAT_H */

/* Define to 1 if you have the `event' library (-levent). */
#define HAVE_LIBEVENT 1

/* Define to 1 if you have the `event_pthreads' library (-levent_pthreads). */
#define HAVE_LIBEVENT_PTHREADS 1

/* Define to 1 if we have -lgdi32 */
/* #undef HAVE_LIBGDI32 */

/* Define to 1 if you have the <libgen.h> header file. */
#define HAVE_LIBGEN_H 1

/* Define to 1 if we have -lkstat */
/* #undef HAVE_LIBKSTAT */

/* Define to 1 if we have -llgrp */
/* #undef HAVE_LIBLGRP */

/* Define to 1 if you have the <libudev.h> header file. */
/* #undef HAVE_LIBUDEV_H */

/* Define to 1 if you have the <libutil.h> header file. */
/* #undef HAVE_LIBUTIL_H */

/* Define to 1 if you have the <linux/ethtool.h> header file. */
#define HAVE_LINUX_ETHTOOL_H 1

/* Define to 1 if you have the <linux/mman.h> header file. */
#define HAVE_LINUX_MMAN_H 1

/* Define to 1 if you have the <linux/sockios.h> header file. */
#define HAVE_LINUX_SOCKIOS_H 1

/* Define to 1 if the system has the type `LOGICAL_PROCESSOR_RELATIONSHIP'. */
/* #undef HAVE_LOGICAL_PROCESSOR_RELATIONSHIP */

/* Define to 1 if the system has the type `long double'. */
#define HAVE_LONG_DOUBLE 1

/* Define to 1 if the system has the type `long double _Complex'. */
#define HAVE_LONG_DOUBLE__COMPLEX 1

/* Define to 1 if the system has the type `long long'. */
#define HAVE_LONG_LONG 1

/* Define to 1 if you have the <lsf/lsbatch.h> header file. */
/* #undef HAVE_LSF_LSBATCH_H */

/* Define to 1 if you have the <lsf/lsf.h> header file. */
/* #undef HAVE_LSF_LSF_H */

/* Define to 1 if you have the <ltdl.h> header file. */
/* #undef HAVE_LTDL_H */

/* Define to 1 if you have the <lustre/lustreapi.h> header file. */
/* #undef HAVE_LUSTRE_LUSTREAPI_H */

/* Define to 1 if you have the <mach/mach_host.h> header file. */
/* #undef HAVE_MACH_MACH_HOST_H */

/* Define to 1 if you have the <mach/mach_init.h> header file. */
/* #undef HAVE_MACH_MACH_INIT_H */

/* Define to 1 if you have the <mach/mach_time.h> header file. */
/* #undef HAVE_MACH_MACH_TIME_H */

/* Define to 1 if you have the <malloc.h> header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the <mapi.h> header file. */
/* #undef HAVE_MAPI_H */

/* Define to 1 if you have the `memalign' function. */
#define HAVE_MEMALIGN 1

/* Define to 1 if you have the <memkind.h> header file. */
/* #undef HAVE_MEMKIND_H */

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mkfifo' function. */
#define HAVE_MKFIFO 1

/* Define to 1 if you have the `mmap' function. */
#define HAVE_MMAP 1

/* Define to 1 if you have the <mntent.h> header file. */
#define HAVE_MNTENT_H 1

/* Define to 1 if the system has the type `mode_t'. */
#define HAVE_MODE_T 1

/* Define to 1 if you have the <mxm/api/mxm_api.h> header file. */
/* #undef HAVE_MXM_API_MXM_API_H */

/* Define to 1 if you have the <ndbm.h> header file. */
/* #undef HAVE_NDBM_H */

/* Define to 1 if you have the <netdb.h> header file. */
#define HAVE_NETDB_H 1

/* Define to 1 if you have the <netinet/in.h> header file. */
#define HAVE_NETINET_IN_H 1

/* Define to 1 if you have the <netinet/tcp.h> header file. */
#define HAVE_NETINET_TCP_H 1

/* Define to 1 if you have the <netlink/version.h> header file. */
/* #undef HAVE_NETLINK_VERSION_H */

/* Define to 1 if you have the <net/if.h> header file. */
#define HAVE_NET_IF_H 1

/* Define to 1 if you have the <net/uio.h> header file. */
/* #undef HAVE_NET_UIO_H */

/* Define to 1 if the system has the type `NUMA_NODE_RELATIONSHIP'. */
/* #undef HAVE_NUMA_NODE_RELATIONSHIP */

/* Define to 1 if you have the <NVCtrl/NVCtrl.h> header file. */
/* #undef HAVE_NVCTRL_NVCTRL_H */

/* Define to 1 if you have the <nvml.h> header file. */
/* #undef HAVE_NVML_H */

/* Define to 1 if you have the `on_exit' function. */
#define HAVE_ON_EXIT 1

/* Define to 1 if you have the `openat' function. */
#define HAVE_OPENAT 1

/* Define to 1 if you have the <OpenCL/cl_ext.h> header file. */
/* #undef HAVE_OPENCL_CL_EXT_H */

/* Define to 1 if you have the `openpty' function. */
/* #undef HAVE_OPENPTY */

/* Define to 1 if you have the <paths.h> header file. */
#define HAVE_PATHS_H 1

/* Define to 1 if you have the <picl.h> header file. */
/* #undef HAVE_PICL_H */

/* Define to 1 if you have the `pipe' function. */
#define HAVE_PIPE 1

/* Define to 1 if you have the <poll.h> header file. */
#define HAVE_POLL_H 1

/* Define to 1 if you have the <portals4.h> header file. */
/* #undef HAVE_PORTALS4_H */

/* Define to 1 if you have the `posix_memalign' function. */
#define HAVE_POSIX_MEMALIGN 1

/* Define to 1 if you have the `preadv' function. */
#define HAVE_PREADV 1

/* Define to 1 if you have the `printstack' function. */
/* #undef HAVE_PRINTSTACK */

/* Define to 1 if the system has the type `PROCESSOR_CACHE_TYPE'. */
/* #undef HAVE_PROCESSOR_CACHE_TYPE */

/* Define to 1 if the system has the type `PROCESSOR_GROUP_INFO'. */
/* #undef HAVE_PROCESSOR_GROUP_INFO */

/* Define to 1 if the system has the type `PROCESSOR_NUMBER'. */
/* #undef HAVE_PROCESSOR_NUMBER */

/* Define to 1 if the system has the type `PROCESSOR_RELATIONSHIP'. */
/* #undef HAVE_PROCESSOR_RELATIONSHIP */

/* Define to '1' if program_invocation_name is present and usable */
#define HAVE_PROGRAM_INVOCATION_NAME 1

/* Define to 1 if the system has the type `PSAPI_WORKING_SET_EX_BLOCK'. */
/* #undef HAVE_PSAPI_WORKING_SET_EX_BLOCK */

/* Define to 1 if the system has the type `PSAPI_WORKING_SET_EX_INFORMATION'.
   */
/* #undef HAVE_PSAPI_WORKING_SET_EX_INFORMATION */

/* Define to 1 if you have the <psm2.h> header file. */
/* #undef HAVE_PSM2_H */

/* Define to 1 if you have the <psm.h> header file. */
/* #undef HAVE_PSM_H */

/* Define to 1 if you have the `pthread_condattr_setpshared' function. */
#define HAVE_PTHREAD_CONDATTR_SETPSHARED 1

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define to 1 if you have the `pthread_mutexattr_setpshared' function. */
#define HAVE_PTHREAD_MUTEXATTR_SETPSHARED 1

/* Define to 1 if you have the <pthread_np.h> header file. */
/* #undef HAVE_PTHREAD_NP_H */

/* Define to 1 if the system has the type `pthread_t'. */
#define HAVE_PTHREAD_T 1

/* Define to 1 if the system has the type `ptrdiff_t'. */
#define HAVE_PTRDIFF_T 1

/* Define to 1 if you have the `ptsname' function. */
#define HAVE_PTSNAME 1

/* Define to 1 if you have the <pty.h> header file. */
#define HAVE_PTY_H 1

/* Define to 1 if you have the <pvfs2.h> header file. */
/* #undef HAVE_PVFS2_H */

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define to 1 if you have the `pwritev' function. */
#define HAVE_PWRITEV 1

/* Define to 1 if you have the <rdma/fabric.h> header file. */
/* #undef HAVE_RDMA_FABRIC_H */

/* Define to 1 if you have the <rdma/rdma_cma.h> header file. */
/* #undef HAVE_RDMA_RDMA_CMA_H */

/* Define to 1 if you have the <rdma/rsocket.h> header file. */
/* #undef HAVE_RDMA_RSOCKET_H */

/* Define to 1 if you have the `regcmp' function. */
/* #undef HAVE_REGCMP */

/* Define to 1 if you have the `regexec' function. */
#define HAVE_REGEXEC 1

/* Define to 1 if you have the <regex.h> header file. */
#define HAVE_REGEX_H 1

/* Define to 1 if you have the `regfree' function. */
#define HAVE_REGFREE 1

/* Define to 1 if the system has the type `RelationProcessorPackage'. */
/* #undef HAVE_RELATIONPROCESSORPACKAGE */

/* Define to 1 if you have the <sched.h> header file. */
#define HAVE_SCHED_H 1

/* Define to 1 if you have the `sem_init' function. */
#define HAVE_SEM_INIT 1

/* Define to 1 if you have the `sem_open' function. */
#define HAVE_SEM_OPEN 1

/* Define to 1 if you have the `setenv' function. */
#define HAVE_SETENV 1

/* Define to 1 if you have the `setlocale' function. */
#define HAVE_SETLOCALE 1

/* Define to 1 if you have the `setpgid' function. */
#define HAVE_SETPGID 1

/* Define to 1 if you have the `setsid' function. */
#define HAVE_SETSID 1

/* Define to 1 if you have the <shlwapi.h> header file. */
/* #undef HAVE_SHLWAPI_H */

/* Define to 1 if `si_band' is a member of `siginfo_t'. */
#define HAVE_SIGINFO_T_SI_BAND 1

/* Define to 1 if `si_fd' is a member of `siginfo_t'. */
#define HAVE_SIGINFO_T_SI_FD 1

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1

/* Define to 1 if you have the `socketpair' function. */
#define HAVE_SOCKETPAIR 1

/* Define to 1 if the system has the type `socklen_t'. */
#define HAVE_SOCKLEN_T 1

/* Define to 1 if you have the <sockLib.h> header file. */
/* #undef HAVE_SOCKLIB_H */

/* Define to 1 if the system has the type `ssize_t'. */
#define HAVE_SSIZE_T 1

/* Define to 1 if you have the `statfs' function. */
#define HAVE_STATFS 1

/* Define to 1 if you have the `statvfs' function. */
#define HAVE_STATVFS 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strftime' function. */
#define HAVE_STRFTIME 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strncasecmp' function. */
#define HAVE_STRNCASECMP 1

/* Define to 1 if you have the `strncpy_s' function. */
/* #undef HAVE_STRNCPY_S */

/* Define to 1 if you have the <stropts.h> header file. */
/* #undef HAVE_STROPTS_H */

/* Define to 1 if you have the `strsignal' function. */
#define HAVE_STRSIGNAL 1

/* Define to 1 if you have the `strtoull' function. */
/* #undef HAVE_STRTOULL */

/* Define to 1 if `d_type' is a member of `struct dirent'. */
#define HAVE_STRUCT_DIRENT_D_TYPE 1

/* Define to 1 if the system has the type `struct ethtool_cmd'. */
#define HAVE_STRUCT_ETHTOOL_CMD 1

/* Define to 1 if `speed_hi' is a member of `struct ethtool_cmd'. */
#define HAVE_STRUCT_ETHTOOL_CMD_SPEED_HI 1

/* Define to 1 if `transport_type' is a member of `struct ibv_device'. */
/* #undef HAVE_STRUCT_IBV_DEVICE_TRANSPORT_TYPE */

/* Define to 1 if `exp_atomic_cap' is a member of `struct
   ibv_exp_device_attr'. */
/* #undef HAVE_STRUCT_IBV_EXP_DEVICE_ATTR_EXP_ATOMIC_CAP */

/* Define to 1 if `ext_atom' is a member of `struct ibv_exp_device_attr'. */
/* #undef HAVE_STRUCT_IBV_EXP_DEVICE_ATTR_EXT_ATOM */

/* Define to 1 if the system has the type `struct ifreq'. */
#define HAVE_STRUCT_IFREQ 1

/* Define to 1 if `ifr_hwaddr' is a member of `struct ifreq'. */
#define HAVE_STRUCT_IFREQ_IFR_HWADDR 1

/* Define to 1 if `ifr_mtu' is a member of `struct ifreq'. */
#define HAVE_STRUCT_IFREQ_IFR_MTU 1

/* Define to 1 if the system has the type `struct sockaddr_in'. */
#define HAVE_STRUCT_SOCKADDR_IN 1

/* Define to 1 if the system has the type `struct sockaddr_in6'. */
#define HAVE_STRUCT_SOCKADDR_IN6 1

/* Define to 1 if `sa_len' is a member of `struct sockaddr'. */
/* #undef HAVE_STRUCT_SOCKADDR_SA_LEN */

/* Define to 1 if the system has the type `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE 1

/* Define to 1 if `f_fstypename' is a member of `struct statfs'. */
/* #undef HAVE_STRUCT_STATFS_F_FSTYPENAME */

/* Define to 1 if `f_type' is a member of `struct statfs'. */
#define HAVE_STRUCT_STATFS_F_TYPE 1

/* Define to 1 if `f_basetype' is a member of `struct statvfs'. */
/* #undef HAVE_STRUCT_STATVFS_F_BASETYPE */

/* Define to 1 if `f_fstypename' is a member of `struct statvfs'. */
/* #undef HAVE_STRUCT_STATVFS_F_FSTYPENAME */

/* Define to 1 if you have the `syscall' function. */
/* #undef HAVE_SYSCALL */

/* Define to 1 if you have the `sysconf' function. */
#define HAVE_SYSCONF 1

/* Define to '1' if sysctl is present and usable */
/* #undef HAVE_SYSCTL */

/* Define to '1' if sysctlbyname is present and usable */
/* #undef HAVE_SYSCTLBYNAME */

/* Define to 1 if you have the `syslog' function. */
#define HAVE_SYSLOG 1

/* Define to 1 if you have the <syslog.h> header file. */
#define HAVE_SYSLOG_H 1

/* Define to 1 if the system has the type
   `SYSTEM_LOGICAL_PROCESSOR_INFORMATION'. */
/* #undef HAVE_SYSTEM_LOGICAL_PROCESSOR_INFORMATION */

/* Define to 1 if the system has the type
   `SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX'. */
/* #undef HAVE_SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX */

/* Define to 1 if you have the <sys/auxv.h> header file. */
#define HAVE_SYS_AUXV_H 1

/* Define to 1 if you have the <sys/cpuset.h> header file. */
/* #undef HAVE_SYS_CPUSET_H */

/* Define to 1 if you have the <sys/fcntl.h> header file. */
#define HAVE_SYS_FCNTL_H 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/ipc.h> header file. */
#define HAVE_SYS_IPC_H 1

/* Define to 1 if you have the <sys/lgrp_user.h> header file. */
/* #undef HAVE_SYS_LGRP_USER_H */

/* Define to 1 if you have the <sys/mman.h> header file. */
#define HAVE_SYS_MMAN_H 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#define HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/poll.h> header file. */
#define HAVE_SYS_POLL_H 1

/* Define to 1 if you have the <sys/prctl.h> header file. */
#define HAVE_SYS_PRCTL_H 1

/* Define to 1 if you have the <sys/queue.h> header file. */
#define HAVE_SYS_QUEUE_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/select.h> header file. */
#define HAVE_SYS_SELECT_H 1

/* Define to 1 if you have the <sys/shm.h> header file. */
#define HAVE_SYS_SHM_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/sockio.h> header file. */
/* #undef HAVE_SYS_SOCKIO_H */

/* Define to 1 if you have the <sys/statfs.h> header file. */
#define HAVE_SYS_STATFS_H 1

/* Define to 1 if you have the <sys/statvfs.h> header file. */
#define HAVE_SYS_STATVFS_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/synch.h> header file. */
/* #undef HAVE_SYS_SYNCH_H */

/* Define to 1 if you have the <sys/syscall.h> header file. */
#define HAVE_SYS_SYSCALL_H 1

/* Define to 1 if you have the <sys/sysctl.h> header file. */
#define HAVE_SYS_SYSCTL_H 1

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/tree.h> header file. */
/* #undef HAVE_SYS_TREE_H */

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
#define HAVE_SYS_UIO_H 1

/* Define to 1 if you have the <sys/un.h> header file. */
#define HAVE_SYS_UN_H 1

/* Define to 1 if you have the <sys/utsname.h> header file. */
#define HAVE_SYS_UTSNAME_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#define HAVE_SYS_VFS_H 1

/* Define to 1 if you have the <sys/wait.h> header file. */
#define HAVE_SYS_WAIT_H 1

/* Define to 1 if you have the <TargetConditionals.h> header file. */
/* #undef HAVE_TARGETCONDITIONALS_H */

/* Define to 1 if you have the `tcgetpgrp' function. */
#define HAVE_TCGETPGRP 1

/* Define to 1 if you have the <termios.h> header file. */
#define HAVE_TERMIOS_H 1

/* Define to 1 if you have the <tm.h> header file. */
/* #undef HAVE_TM_H */

/* Define to 1 if you have the <tm_tree.h> header file. */
/* #undef HAVE_TM_TREE_H */

/* Define to 1 if you have the <ucontext.h> header file. */
#define HAVE_UCONTEXT_H 1

/* Define to 1 if you have the <ucp/api/ucp.h> header file. */
/* #undef HAVE_UCP_API_UCP_H */

/* have ucp_tag_send_nbr() */
/* #undef HAVE_UCP_TAG_SEND_NBR */

/* have worker address attribute */
/* #undef HAVE_UCP_WORKER_ADDRESS_FLAGS */

/* Define to 1 if the system has the type `uint128_t'. */
/* #undef HAVE_UINT128_T */

/* Define to 1 if the system has the type `uint16_t'. */
#define HAVE_UINT16_T 1

/* Define to 1 if the system has the type `uint32_t'. */
#define HAVE_UINT32_T 1

/* Define to 1 if the system has the type `uint64_t'. */
#define HAVE_UINT64_T 1

/* Define to 1 if the system has the type `uint8_t'. */
#define HAVE_UINT8_T 1

/* Define to 1 if the system has the type `uintptr_t'. */
#define HAVE_UINTPTR_T 1

/* Define to 1 if you have the <ulimit.h> header file. */
#define HAVE_ULIMIT_H 1

/* Define to 1 if you have the `uname' function. */
#define HAVE_UNAME 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* whether unix byteswap routines -- htonl, htons, nothl, ntohs -- are
   available */
#define HAVE_UNIX_BYTESWAP 1

/* Define to 1 if you have the `usleep' function. */
#define HAVE_USLEEP 1

/* Define to 1 if you have the <util.h> header file. */
/* #undef HAVE_UTIL_H */

/* Define to 1 if you have the <utmp.h> header file. */
#define HAVE_UTMP_H 1

/* Define to 1 if you have the <valgrind/valgrind.h> header file. */
/* #undef HAVE_VALGRIND_VALGRIND_H */

/* Define to 1 if you have the `vasprintf' function. */
#define HAVE_VASPRINTF 1

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1

/* Define to 1 if you have the `vsyslog' function. */
#define HAVE_VSYSLOG 1

/* Define to 1 if you have the `waitpid' function. */
#define HAVE_WAITPID 1

/* Define to 1 if you have the <X11/keysym.h> header file. */
/* #undef HAVE_X11_KEYSYM_H */

/* Define to 1 if you have the <X11/Xlib.h> header file. */
/* #undef HAVE_X11_XLIB_H */

/* Define to 1 if you have the <X11/Xutil.h> header file. */
/* #undef HAVE_X11_XUTIL_H */

/* Define to 1 if you have the <xpmem.h> header file. */
/* #undef HAVE_XPMEM_H */

/* Define to 1 if you have the <zlib.h> header file. */
#define HAVE_ZLIB_H 1

/* Define to 1 if you have the `_NSGetEnviron' function. */
/* #undef HAVE__NSGETENVIRON */

/* Define to 1 if you have the `__clear_cache' function. */
/* #undef HAVE___CLEAR_CACHE */

/* Define to 1 if you have the `__curbrk' function. */
#define HAVE___CURBRK 1

/* Define to 1 if the system has the type `__float128'. */
#define HAVE___FLOAT128 1

/* Define to 1 if the system has the type `__int128'. */
#define HAVE___INT128 1

/* Define to 1 if you have the `__malloc_initialize_hook' function. */
/* #undef HAVE___MALLOC_INITIALIZE_HOOK */

/* Define to 1 if you have the `__mmap' function. */
#define HAVE___MMAP 1

/* Define to 1 if you have the `__munmap' function. */
/* #undef HAVE___MUNMAP */

/* Define to '1' if __progname is present and usable */
#define HAVE___PROGNAME 1

/* Define to 1 if you have the `__syscall' function. */
/* #undef HAVE___SYSCALL */

/* Define to 1 on AIX */
/* #undef HWLOC_AIX_SYS */

/* Define to 1 on BlueGene/Q */
/* #undef HWLOC_BGQ_SYS */

/* Whether C compiler supports symbol visibility or not */
#define HWLOC_C_HAVE_VISIBILITY 1

/* Define to 1 on Darwin */
/* #undef HWLOC_DARWIN_SYS */

/* Whether we are in debugging mode or not */
/* #undef HWLOC_DEBUG */

/* Version of hwloc */
#define HWLOC_EXTERNAL_HWLOC_VERSION external

/* Define to 1 on *FREEBSD */
/* #undef HWLOC_FREEBSD_SYS */

/* Whether your compiler has __attribute__ or not */
#define HWLOC_HAVE_ATTRIBUTE 1

/* Whether your compiler has __attribute__ aligned or not */
#define HWLOC_HAVE_ATTRIBUTE_ALIGNED 1

/* Whether your compiler has __attribute__ always_inline or not */
#define HWLOC_HAVE_ATTRIBUTE_ALWAYS_INLINE 1

/* Whether your compiler has __attribute__ cold or not */
#define HWLOC_HAVE_ATTRIBUTE_COLD 1

/* Whether your compiler has __attribute__ const or not */
#define HWLOC_HAVE_ATTRIBUTE_CONST 1

/* Whether your compiler has __attribute__ deprecated or not */
#define HWLOC_HAVE_ATTRIBUTE_DEPRECATED 1

/* Whether your compiler has __attribute__ format or not */
#define HWLOC_HAVE_ATTRIBUTE_FORMAT 1

/* Whether your compiler has __attribute__ hot or not */
#define HWLOC_HAVE_ATTRIBUTE_HOT 1

/* Whether your compiler has __attribute__ malloc or not */
#define HWLOC_HAVE_ATTRIBUTE_MALLOC 1

/* Whether your compiler has __attribute__ may_alias or not */
#define HWLOC_HAVE_ATTRIBUTE_MAY_ALIAS 1

/* Whether your compiler has __attribute__ nonnull or not */
#define HWLOC_HAVE_ATTRIBUTE_NONNULL 1

/* Whether your compiler has __attribute__ noreturn or not */
#define HWLOC_HAVE_ATTRIBUTE_NORETURN 1

/* Whether your compiler has __attribute__ no_instrument_function or not */
#define HWLOC_HAVE_ATTRIBUTE_NO_INSTRUMENT_FUNCTION 1

/* Whether your compiler has __attribute__ packed or not */
#define HWLOC_HAVE_ATTRIBUTE_PACKED 1

/* Whether your compiler has __attribute__ pure or not */
#define HWLOC_HAVE_ATTRIBUTE_PURE 1

/* Whether your compiler has __attribute__ sentinel or not */
#define HWLOC_HAVE_ATTRIBUTE_SENTINEL 1

/* Whether your compiler has __attribute__ unused or not */
#define HWLOC_HAVE_ATTRIBUTE_UNUSED 1

/* Whether your compiler has __attribute__ warn unused result or not */
#define HWLOC_HAVE_ATTRIBUTE_WARN_UNUSED_RESULT 1

/* Whether your compiler has __attribute__ weak alias or not */
#define HWLOC_HAVE_ATTRIBUTE_WEAK_ALIAS 1

/* Define to 1 if your `ffs' function is known to be broken. */
/* #undef HWLOC_HAVE_BROKEN_FFS */

/* Define to 1 if you have the `clz' function. */
/* #undef HWLOC_HAVE_CLZ */

/* Define to 1 if you have the `clzl' function. */
/* #undef HWLOC_HAVE_CLZL */

/* Define to 1 if the CPU_SET macro works */
#define HWLOC_HAVE_CPU_SET 1

/* Define to 1 if the CPU_SET_S macro works */
#define HWLOC_HAVE_CPU_SET_S 1

/* Define to 1 if you have the `cudart' SDK. */
/* #undef HWLOC_HAVE_CUDART */

/* Define to 1 if function `clz' is declared by system headers */
/* #undef HWLOC_HAVE_DECL_CLZ */

/* Define to 1 if function `clzl' is declared by system headers */
/* #undef HWLOC_HAVE_DECL_CLZL */

/* Define to 1 if function `ffs' is declared by system headers */
#define HWLOC_HAVE_DECL_FFS 1

/* Define to 1 if function `ffsl' is declared by system headers */
#define HWLOC_HAVE_DECL_FFSL 1

/* Define to 1 if function `fls' is declared by system headers */
/* #undef HWLOC_HAVE_DECL_FLS */

/* Define to 1 if function `flsl' is declared by system headers */
/* #undef HWLOC_HAVE_DECL_FLSL */

/* Define to 1 if function `strncasecmp' is declared by system headers */
#define HWLOC_HAVE_DECL_STRNCASECMP 1

/* Define to 1 if you have the `ffs' function. */
#define HWLOC_HAVE_FFS 1

/* Define to 1 if you have the `ffsl' function. */
#define HWLOC_HAVE_FFSL 1

/* Define to 1 if you have the `fls' function. */
/* #undef HWLOC_HAVE_FLS */

/* Define to 1 if you have the `flsl' function. */
/* #undef HWLOC_HAVE_FLSL */

/* Define to 1 if you have the GL module components. */
/* #undef HWLOC_HAVE_GL */

/* Define to 1 if you have libudev. */
/* #undef HWLOC_HAVE_LIBUDEV */

/* Define to 1 if you have the `libxml2' library. */
/* #undef HWLOC_HAVE_LIBXML2 */

/* Define to 1 if building the Linux I/O component */
#define HWLOC_HAVE_LINUXIO 1

/* Define to 1 if enabling Linux-specific PCI discovery in the Linux I/O
   component */
#define HWLOC_HAVE_LINUXPCI 1

/* Define to 1 if you have the `NVML' library. */
/* #undef HWLOC_HAVE_NVML */

/* Define to 1 if glibc provides the old prototype (without length) of
   sched_setaffinity() */
/* #undef HWLOC_HAVE_OLD_SCHED_SETAFFINITY */

/* Define to 1 if you have the `OpenCL' library. */
/* #undef HWLOC_HAVE_OPENCL */

/* Define to 1 if the hwloc library should support dynamically-loaded plugins
   */
/* #undef HWLOC_HAVE_PLUGINS */

/* `Define to 1 if you have pthread_getthrds_np' */
/* #undef HWLOC_HAVE_PTHREAD_GETTHRDS_NP */

/* Define to 1 if pthread mutexes are available */
#define HWLOC_HAVE_PTHREAD_MUTEX 1

/* Define to 1 if glibc provides a prototype of sched_setaffinity() */
#define HWLOC_HAVE_SCHED_SETAFFINITY 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HWLOC_HAVE_STDINT_H 1

/* Define to 1 if function `syscall' is available with 6 parameters */
#define HWLOC_HAVE_SYSCALL 1

/* Define to 1 if you have the `windows.h' header. */
/* #undef HWLOC_HAVE_WINDOWS_H */

/* Define to 1 if X11 headers including Xutil.h and keysym.h are available. */
/* #undef HWLOC_HAVE_X11_KEYSYM */

/* Define to 1 if you have x86 cpuid */
#define HWLOC_HAVE_X86_CPUID 1

/* Define to 1 on HP-UX */
/* #undef HWLOC_HPUX_SYS */

/* Version of hwloc */
#define HWLOC_HWLOC201_HWLOC_VERSION "internal v2.0.2rc1-git"

/* Define to 1 on Irix */
/* #undef HWLOC_IRIX_SYS */

/* Define to 1 on Linux */
#define HWLOC_LINUX_SYS 1

/* Define to 1 on *NETBSD */
/* #undef HWLOC_NETBSD_SYS */

/* The size of `unsigned int', as computed by sizeof */
#define HWLOC_SIZEOF_UNSIGNED_INT 4

/* The size of `unsigned long', as computed by sizeof */
#define HWLOC_SIZEOF_UNSIGNED_LONG 8

/* Define to 1 on Solaris */
/* #undef HWLOC_SOLARIS_SYS */

/* The hwloc symbol prefix */
#define HWLOC_SYM_PREFIX hwloc_

/* The hwloc symbol prefix in all caps */
#define HWLOC_SYM_PREFIX_CAPS HWLOC_

/* Whether we need to re-define all the hwloc public symbols or not */
#define HWLOC_SYM_TRANSFORM 0

/* Define to 1 on unsupported systems */
/* #undef HWLOC_UNSUPPORTED_SYS */

/* The library version, always available, even in embedded mode, contrary to
   VERSION */
#define HWLOC_VERSION "2.0.2rc1-git"

/* Define to 1 on WINDOWS */
/* #undef HWLOC_WIN_SYS */

/* Define to 1 on x86_32 */
/* #undef HWLOC_X86_32_ARCH */

/* Define to 1 on x86_64 */
#define HWLOC_X86_64_ARCH 1

/* Define to the sub-directory where libtool stores uninstalled libraries. */
#define LT_OBJDIR ".libs/"

/* Header to include for event implementation */
#define MCA_event_IMPLEMENTATION_HEADER "opal/mca/event/external/external.h"

/* Header to include for hwloc implementation */
#define MCA_hwloc_IMPLEMENTATION_HEADER "opal/mca/hwloc/external/external.h"

/* Location of external hwloc header */
#define MCA_hwloc_external_header "hwloc.h"

/* Location of external hwloc OpenFabrics header */
#define MCA_hwloc_external_openfabrics_header "hwloc/openfabrics-verbs.h"

/* Location of external hwloc shmem header */
#define MCA_hwloc_external_shmem_header "hwloc/shmem.h"

/* Complete set of command line arguments given to ROMIOs configure script */
/* #undef MCA_io_romio321_COMPLETE_CONFIGURE_FLAGS */

/* Set of user-defined configure flags given to ROMIOs configure script via
   --with-io-romio-flags */
#define MCA_io_romio321_USER_CONFIGURE_FLAGS ""

/* Header to include for memcpy implementation */
#define MCA_memcpy_IMPLEMENTATION_HEADER "opal/mca/memcpy/base/memcpy_base_default.h"

/* Header to include for parts of the memory implementation */
#define MCA_memory_IMPLEMENTATION_HEADER "opal/mca/memory/base/empty.h"

/* Defined to 1 if ompi:mtl should use direct calls instead of components */
#define MCA_ompi_mtl_DIRECT_CALL 0

/* name of component to use for direct calls, if MCA_ompi_mtl_DIRECT_CALL is 1
   */
#define MCA_ompi_mtl_DIRECT_CALL_COMPONENT 

/* Header ompi:mtl includes to be direct called */
#define MCA_ompi_mtl_DIRECT_CALL_HEADER ""

/* Defined to 1 if ompi:pml should use direct calls instead of components */
#define MCA_ompi_pml_DIRECT_CALL 0

/* name of component to use for direct calls, if MCA_ompi_pml_DIRECT_CALL is 1
   */
#define MCA_ompi_pml_DIRECT_CALL_COMPONENT 

/* Header ompi:pml includes to be direct called */
#define MCA_ompi_pml_DIRECT_CALL_HEADER ""

/* Defined to 1 if oshmem:memheap should use direct calls instead of
   components */
#define MCA_oshmem_memheap_DIRECT_CALL 0

/* name of component to use for direct calls, if
   MCA_oshmem_memheap_DIRECT_CALL is 1 */
#define MCA_oshmem_memheap_DIRECT_CALL_COMPONENT 

/* Header oshmem:memheap includes to be direct called */
#define MCA_oshmem_memheap_DIRECT_CALL_HEADER ""

/* Defined to 1 if oshmem:spml should use direct calls instead of components
   */
#define MCA_oshmem_spml_DIRECT_CALL 0

/* name of component to use for direct calls, if MCA_oshmem_spml_DIRECT_CALL
   is 1 */
#define MCA_oshmem_spml_DIRECT_CALL_COMPONENT 

/* Header oshmem:spml includes to be direct called */
#define MCA_oshmem_spml_DIRECT_CALL_HEADER ""

/* Header to include for rte implementation */
#define MCA_rte_IMPLEMENTATION_HEADER "ompi/mca/rte/orte/rte_orte.h"

/* Header to include for timer implementation */
#define MCA_timer_IMPLEMENTATION_HEADER "opal/mca/timer/linux/timer_linux.h"

/* Whether we can use M-PAGE supported since MOFED 1.8 */
#define MPAGE_ENABLE 0

/* create_flags field is part of ibv_exp_reg_mr_in */
#define MPAGE_HAVE_IBV_EXP_REG_MR_CREATE_FLAGS 0

/* exp_access field is part of ibv_exp_reg_shared_mr_in */
#define MPAGE_HAVE_SMR_EXP_ACCESS 0

/* Macro that is set to 1 when CUDA-aware support is configured in and 0 when
   it is not */
#define MPIX_CUDA_AWARE_SUPPORT 1

/* Maximum value for an MPI_Count */
#define MPI_COUNT_MAX 0x7fffffffffffffffll

/* Whether we want to check MPI parameters always, never, or decide at
   run-time */
#define MPI_PARAM_CHECK ompi_mpi_param_check

/* Alignment of Fortran CHARACTER */
#define OMPI_ALIGNMENT_FORTRAN_CHARACTER 4

/* Alignment of Fortran COMPLEX */
#define OMPI_ALIGNMENT_FORTRAN_COMPLEX 4

/* Alignment of Fortran COMPLEX*16 */
#define OMPI_ALIGNMENT_FORTRAN_COMPLEX16 4

/* Alignment of Fortran COMPLEX*32 */
#define OMPI_ALIGNMENT_FORTRAN_COMPLEX32 4

/* Alignment of Fortran COMPLEX*4 */
#define OMPI_ALIGNMENT_FORTRAN_COMPLEX4 4

/* Alignment of Fortran COMPLEX*8 */
#define OMPI_ALIGNMENT_FORTRAN_COMPLEX8 4

/* Alignment of Fortran DOUBLE COMPLEX */
#define OMPI_ALIGNMENT_FORTRAN_DOUBLE_COMPLEX 4

/* Alignment of Fortran DOUBLE PRECISION */
#define OMPI_ALIGNMENT_FORTRAN_DOUBLE_PRECISION 4

/* Alignment of Fortran INTEGER */
#define OMPI_ALIGNMENT_FORTRAN_INTEGER 4

/* Alignment of Fortran INTEGER*1 */
#define OMPI_ALIGNMENT_FORTRAN_INTEGER1 4

/* Alignment of Fortran INTEGER*16 */
#define OMPI_ALIGNMENT_FORTRAN_INTEGER16 4

/* Alignment of Fortran INTEGER*2 */
#define OMPI_ALIGNMENT_FORTRAN_INTEGER2 4

/* Alignment of Fortran INTEGER*4 */
#define OMPI_ALIGNMENT_FORTRAN_INTEGER4 4

/* Alignment of Fortran INTEGER*8 */
#define OMPI_ALIGNMENT_FORTRAN_INTEGER8 4

/* Alignment of Fortran LOGICAL */
#define OMPI_ALIGNMENT_FORTRAN_LOGICAL 4

/* Alignment of Fortran LOGICAL*1 */
#define OMPI_ALIGNMENT_FORTRAN_LOGICAL1 4

/* Alignment of Fortran LOGICAL*2 */
#define OMPI_ALIGNMENT_FORTRAN_LOGICAL2 4

/* Alignment of Fortran LOGICAL*4 */
#define OMPI_ALIGNMENT_FORTRAN_LOGICAL4 4

/* Alignment of Fortran LOGICAL*8 */
#define OMPI_ALIGNMENT_FORTRAN_LOGICAL8 4

/* Alignment of Fortran REAL */
#define OMPI_ALIGNMENT_FORTRAN_REAL 4

/* Alignment of Fortran REAL*16 */
#define OMPI_ALIGNMENT_FORTRAN_REAL16 4

/* Alignment of Fortran REAL*2 */
#define OMPI_ALIGNMENT_FORTRAN_REAL2 4

/* Alignment of Fortran REAL*4 */
#define OMPI_ALIGNMENT_FORTRAN_REAL4 4

/* Alignment of Fortran REAL*8 */
#define OMPI_ALIGNMENT_FORTRAN_REAL8 4

/* Whether we want MPI C++ support or not */
#define OMPI_BUILD_CXX_BINDINGS 1

/* The level of fortran bindings to be built */
#define OMPI_BUILD_FORTRAN_BINDINGS 0

/* OMPI underlying C++ compiler */
#define OMPI_CXX "g++"

/* Whether C++ compiler supports __builtin_expect */
#define OMPI_CXX_HAVE_BUILTIN_EXPECT 1

/* Whether C++ compiler supports __builtin_prefetch */
#define OMPI_CXX_HAVE_BUILTIN_PREFETCH 1

/* Whether a const_cast on a 2-d array will work with the C++ compiler */
#define OMPI_CXX_SUPPORTS_2D_CONST_CAST 1

/* Enable contributed software package libompitrace */
#define OMPI_ENABLE_CONTRIB_libompitrace 1

/* whether we support Grequest extensions */
#define OMPI_ENABLE_GREQUEST_EXTENSIONS 0

/* whether we want MPI-1.x support */
#define OMPI_ENABLE_MPI1_COMPAT 0

/* Underlying Fortran compiler */
#define OMPI_FC "none"

/* Absolutey path to the underlying Fortran compiler found by configure */
#define OMPI_FC_ABSOLUTE "none"

/* Whether the mpif.h interface supports the MPI_SIZEOF interface or not */
#define OMPI_FORTRAN_BUILD_SIZEOF 0

/* Whether fortran symbols are all caps or not */
#define OMPI_FORTRAN_CAPS 0

/* Whether fortran symbols have a trailing double underscore or not */
#define OMPI_FORTRAN_DOUBLE_UNDERSCORE 0

/* How many bytes the mpi_f08 TYPE(MPI_<foo>) handles will be aligned to */
#define OMPI_FORTRAN_F08_HANDLE_ALIGNMENT 4

/* How many bytes the mpi_f08 TYPE(MPI_<foo>) handles will be */
#define OMPI_FORTRAN_F08_HANDLE_SIZE 4

/* Max handle value for fortran MPI handles, effectively min(INT_MAX, max
   fortran INTEGER value) */
#define OMPI_FORTRAN_HANDLE_MAX 2147483647

/* For mpi-f08-interfaces-callbacks.f90 and ompi_info: whether the compiler
   supports the "abstract" keyword or not */
#define OMPI_FORTRAN_HAVE_ABSTRACT 0

/* For ompi/mpi/fortran/use-mpi-f08/blah.F90 and blah.h and ompi_info: whether
   the compiler supports the "asynchronous" keyword or not */
#define OMPI_FORTRAN_HAVE_ASYNCHRONOUS 0

/* For ompi_info: Whether the compiler supports all forms of BIND(C) that we
   need */
#define OMPI_FORTRAN_HAVE_BIND_C 0

/* For ompi_info: Whether the compiler supports SUBROUTINE ... BIND(C) or not
   */
#define OMPI_FORTRAN_HAVE_BIND_C_SUB 0

/* For ompi_info: Whether the compiler supports TYPE, BIND(C) or not */
#define OMPI_FORTRAN_HAVE_BIND_C_TYPE 0

/* For ompi_info: Whether the compiler supports TYPE, BIND(C, NAME="name") or
   not */
#define OMPI_FORTRAN_HAVE_BIND_C_TYPE_NAME 0

/* For ompi/mpi/fortran/use-mpi-f08/blah.F90 and blah.h and ompi_info: whether
   the compiler supports c_funloc or not */
#define OMPI_FORTRAN_HAVE_C_FUNLOC 0

/* For ompi_info: Whether the Fortran compiler supports the Fortran 2008
   "assumed rank" syntax or not */
#define OMPI_FORTRAN_HAVE_F08_ASSUMED_RANK 0

/* Whether the Fortran compiler supports ignore TKR functionality or not */
#define OMPI_FORTRAN_HAVE_IGNORE_TKR 0

/* Whether the compiler supports INTERFACE or not */
#define OMPI_FORTRAN_HAVE_INTERFACE 0

/* For ompi_info: Whether the compiler supports ISO_C_BINDING or not */
#define OMPI_FORTRAN_HAVE_ISO_C_BINDING 0

/* Whether the compiler supports ISO_FORTRAN_ENV or not */
#define OMPI_FORTRAN_HAVE_ISO_FORTRAN_ENV 0

/* For ompi_info: whether the Fortran compiler supports optional arguments or
   not */
#define OMPI_FORTRAN_HAVE_OPTIONAL_ARGS 0

/* For mpi-f08-types.f90 and ompi_info: whether the compiler supports the
   "private" keyword or not (used in MPI_Status) */
#define OMPI_FORTRAN_HAVE_PRIVATE 0

/* For ompi/mpi/fortran/use-mpi-f08/blah.F90 and blah.h and ompi_info: whether
   the compiler supports the "procedure" keyword or not */
#define OMPI_FORTRAN_HAVE_PROCEDURE 0

/* For mpi-f08-types.f90 and .F90 and ompi_info: whether the compiler supports
   the "protected" keyword or not */
#define OMPI_FORTRAN_HAVE_PROTECTED 0

/* Whether the compiler supports STORAGE_SIZE on relevant types */
#define OMPI_FORTRAN_HAVE_STORAGE_SIZE 0

/* For ompi/mpi/fortran/use-mpi-f08/blah.F90 and blah.h and ompi_info: whether
   the compiler supports "USE ... ONLY" notation properly or not */
#define OMPI_FORTRAN_HAVE_USE_ONLY 0

/* Pre declaration for FORTRAN ignore parameter TKR behavior */
#define OMPI_FORTRAN_IGNORE_TKR_PREDECL ""

/* Type declaration for FORTRAN ignore parameter TKR behavior */
#define OMPI_FORTRAN_IGNORE_TKR_TYPE 

/* Max dimension rank of Fortran arrays */
#define OMPI_FORTRAN_MAX_ARRAY_RANK 0

/* Whether we are building support for the mpif.h bindings or not */
#define OMPI_FORTRAN_MPIFH_BINDINGS 1

/* Whether the mpi_f08 implementation is using wrapper routines ("bad" Fortran
   compiler) or weak symbols ("good" Fortran compiler) for the F08 interface
   definition implementations */
#define OMPI_FORTRAN_NEED_WRAPPER_ROUTINES 0

/* Whether fortran symbols have no trailing underscore or not */
#define OMPI_FORTRAN_PLAIN 0

/* Whether fortran symbols have a trailing underscore or not */
#define OMPI_FORTRAN_SINGLE_UNDERSCORE 0

/* Whether we are building support for the "use mpif08" bindings or not */
#define OMPI_FORTRAN_USEMPIF08_BINDINGS 3

/* Whether we are building support for the "use mpi" bindings or not */
#define OMPI_FORTRAN_USEMPI_BINDINGS 2

/* Fortran value for LOGICAL .TRUE. value */
#define OMPI_FORTRAN_VALUE_TRUE 77

/* Greek - alpha, beta, etc - release number of Open MPI */
#define OMPI_GREEK_VERSION ""

/* Whether we want sparse process groups */
#define OMPI_GROUP_SPARSE 0

/* Whether or not we have compiled with C++ exceptions support */
#define OMPI_HAVE_CXX_EXCEPTION_SUPPORT 1

/* Whether we have Fortran CHARACTER or not */
#define OMPI_HAVE_FORTRAN_CHARACTER 0

/* Whether we have Fortran COMPLEX or not */
#define OMPI_HAVE_FORTRAN_COMPLEX 0

/* Whether we have Fortran COMPLEX*16 or not */
#define OMPI_HAVE_FORTRAN_COMPLEX16 0

/* Whether we have Fortran COMPLEX*32 or not */
#define OMPI_HAVE_FORTRAN_COMPLEX32 0

/* Whether we have Fortran COMPLEX*4 or not */
#define OMPI_HAVE_FORTRAN_COMPLEX4 0

/* Whether we have Fortran COMPLEX*8 or not */
#define OMPI_HAVE_FORTRAN_COMPLEX8 0

/* Whether we have Fortran DOUBLE COMPLEX or not */
#define OMPI_HAVE_FORTRAN_DOUBLE_COMPLEX 0

/* Whether we have Fortran DOUBLE PRECISION or not */
#define OMPI_HAVE_FORTRAN_DOUBLE_PRECISION 0

/* Whether we have Fortran INTEGER or not */
#define OMPI_HAVE_FORTRAN_INTEGER 0

/* Whether we have Fortran INTEGER*1 or not */
#define OMPI_HAVE_FORTRAN_INTEGER1 0

/* Whether we have Fortran INTEGER*16 or not */
#define OMPI_HAVE_FORTRAN_INTEGER16 0

/* Whether we have Fortran INTEGER*2 or not */
#define OMPI_HAVE_FORTRAN_INTEGER2 0

/* Whether we have Fortran INTEGER*4 or not */
#define OMPI_HAVE_FORTRAN_INTEGER4 0

/* Whether we have Fortran INTEGER*8 or not */
#define OMPI_HAVE_FORTRAN_INTEGER8 0

/* Whether we have Fortran LOGICAL or not */
#define OMPI_HAVE_FORTRAN_LOGICAL 0

/* Whether we have Fortran LOGICAL*1 or not */
#define OMPI_HAVE_FORTRAN_LOGICAL1 0

/* Whether we have Fortran LOGICAL*2 or not */
#define OMPI_HAVE_FORTRAN_LOGICAL2 0

/* Whether we have Fortran LOGICAL*4 or not */
#define OMPI_HAVE_FORTRAN_LOGICAL4 0

/* Whether we have Fortran LOGICAL*8 or not */
#define OMPI_HAVE_FORTRAN_LOGICAL8 0

/* Whether we have Fortran REAL or not */
#define OMPI_HAVE_FORTRAN_REAL 0

/* Whether we have Fortran REAL*16 or not */
#define OMPI_HAVE_FORTRAN_REAL16 0

/* Whether we have Fortran REAL*2 or not */
#define OMPI_HAVE_FORTRAN_REAL2 0

/* Whether we have Fortran REAL*4 or not */
#define OMPI_HAVE_FORTRAN_REAL4 0

/* Whether we have Fortran REAL*8 or not */
#define OMPI_HAVE_FORTRAN_REAL8 0

/* Fortrn KIND number for CHARACTER */
#define OMPI_KIND_FORTRAN_CHARACTER 0

/* Fortrn KIND number for COMPLEX */
#define OMPI_KIND_FORTRAN_COMPLEX 0

/* Fortrn KIND number for COMPLEX*16 */
#define OMPI_KIND_FORTRAN_COMPLEX16 0

/* Fortrn KIND number for COMPLEX*32 */
#define OMPI_KIND_FORTRAN_COMPLEX32 0

/* Fortrn KIND number for COMPLEX*4 */
#define OMPI_KIND_FORTRAN_COMPLEX4 0

/* Fortrn KIND number for COMPLEX*8 */
#define OMPI_KIND_FORTRAN_COMPLEX8 0

/* Fortrn KIND number for DOUBLE COMPLEX */
#define OMPI_KIND_FORTRAN_DOUBLE_COMPLEX 0

/* Fortrn KIND number for DOUBLE PRECISION */
#define OMPI_KIND_FORTRAN_DOUBLE_PRECISION 0

/* Fortrn KIND number for INTEGER */
#define OMPI_KIND_FORTRAN_INTEGER 0

/* Fortrn KIND number for INTEGER*1 */
#define OMPI_KIND_FORTRAN_INTEGER1 0

/* Fortrn KIND number for INTEGER*16 */
#define OMPI_KIND_FORTRAN_INTEGER16 0

/* Fortrn KIND number for INTEGER*2 */
#define OMPI_KIND_FORTRAN_INTEGER2 0

/* Fortrn KIND number for INTEGER*4 */
#define OMPI_KIND_FORTRAN_INTEGER4 0

/* Fortrn KIND number for INTEGER*8 */
#define OMPI_KIND_FORTRAN_INTEGER8 0

/* Fortrn KIND number for LOGICAL */
#define OMPI_KIND_FORTRAN_LOGICAL 0

/* Fortrn KIND number for LOGICAL*1 */
#define OMPI_KIND_FORTRAN_LOGICAL1 0

/* Fortrn KIND number for LOGICAL*2 */
#define OMPI_KIND_FORTRAN_LOGICAL2 0

/* Fortrn KIND number for LOGICAL*4 */
#define OMPI_KIND_FORTRAN_LOGICAL4 0

/* Fortrn KIND number for LOGICAL*8 */
#define OMPI_KIND_FORTRAN_LOGICAL8 0

/* Fortrn KIND number for REAL */
#define OMPI_KIND_FORTRAN_REAL 0

/* Fortrn KIND number for REAL*16 */
#define OMPI_KIND_FORTRAN_REAL16 0

/* Fortrn KIND number for REAL*2 */
#define OMPI_KIND_FORTRAN_REAL2 0

/* Fortrn KIND number for REAL*4 */
#define OMPI_KIND_FORTRAN_REAL4 0

/* Fortrn KIND number for REAL*8 */
#define OMPI_KIND_FORTRAN_REAL8 0

/* Major release number of Open MPI */
#define OMPI_MAJOR_VERSION 4

/* Minor release number of Open MPI */
#define OMPI_MINOR_VERSION 0

/* MPI Extensions included in libmpi */
#define OMPI_MPIEXT_COMPONENTS "affinity, cuda, pcollreq"

/* Type of MPI_Aint */
#define OMPI_MPI_AINT_TYPE ptrdiff_t

/* Contributed software packages built with Open MPI */
#define OMPI_MPI_CONTRIBS "libompitrace"

/* Size of the MPI_Count datatype */
#define OMPI_MPI_COUNT_SIZE 8

/* Type of the MPI_Count datatype */
#define OMPI_MPI_COUNT_TYPE long long

/* Size of the MPI_Offset */
#define OMPI_MPI_OFFSET_SIZE 8

/* Type of MPI_Offset */
#define OMPI_MPI_OFFSET_TYPE long long

/* Enable flow control for Portals4 MTL */
#define OMPI_MTL_PORTALS4_FLOW_CONTROL 1

/* MPI datatype corresponding to MPI_Offset */
#define OMPI_OFFSET_DATATYPE MPI_LONG_LONG

/* Whether we want to check MPI parameters never or possible (an integer
   constant) */
#define OMPI_PARAM_CHECK 1

/* Index into endpoint array for BML */
#define OMPI_PROC_ENDPOINT_TAG_BML 0

/* Maximum number of endpoint entries to be attached to an ompi_proc_t */
#define OMPI_PROC_ENDPOINT_TAG_MAX 1

/* Index into endpoint array for MTL */
/* #undef OMPI_PROC_ENDPOINT_TAG_MTL */

/* Index into endpoint array for PML */
/* #undef OMPI_PROC_ENDPOINT_TAG_PML */

/* Index into endpoint array for PORTALS4 */
/* #undef OMPI_PROC_ENDPOINT_TAG_PORTALS4 */

/* Index into endpoint array for UCX */
/* #undef OMPI_PROC_ENDPOINT_TAG_UCX */

/* Whether Fortran REAL*16 matches the bit format of the equivalent C type */
#define OMPI_REAL16_MATCHES_C 0

/* Release date of Open MPI */
#define OMPI_RELEASE_DATE "Mar 26, 2019"

/* Release release number of Open MPI */
#define OMPI_RELEASE_VERSION 1

/* The repository version Open MPI */
#define OMPI_REPO_REV "v4.0.1"

/* Defined to 1 if the OMPI runtime component is ORTE */
#define OMPI_RTE_ORTE 1

/* Defined to 1 if the OMPI runtime component is PMIX */
#define OMPI_RTE_PMIX 0

/* Size of Fortran CHARACTER */
#define OMPI_SIZEOF_FORTRAN_CHARACTER 4

/* Size of Fortran COMPLEX */
#define OMPI_SIZEOF_FORTRAN_COMPLEX 4

/* Size of Fortran COMPLEX*16 */
#define OMPI_SIZEOF_FORTRAN_COMPLEX16 4

/* Size of Fortran COMPLEX*32 */
#define OMPI_SIZEOF_FORTRAN_COMPLEX32 4

/* Size of Fortran COMPLEX*4 */
#define OMPI_SIZEOF_FORTRAN_COMPLEX4 4

/* Size of Fortran COMPLEX*8 */
#define OMPI_SIZEOF_FORTRAN_COMPLEX8 4

/* Size of Fortran DOUBLE COMPLEX */
#define OMPI_SIZEOF_FORTRAN_DOUBLE_COMPLEX 4

/* Size of Fortran DOUBLE PRECISION */
#define OMPI_SIZEOF_FORTRAN_DOUBLE_PRECISION 4

/* Size of Fortran INTEGER */
#define OMPI_SIZEOF_FORTRAN_INTEGER 4

/* Size of Fortran INTEGER*1 */
#define OMPI_SIZEOF_FORTRAN_INTEGER1 4

/* Size of Fortran INTEGER*16 */
#define OMPI_SIZEOF_FORTRAN_INTEGER16 4

/* Size of Fortran INTEGER*2 */
#define OMPI_SIZEOF_FORTRAN_INTEGER2 4

/* Size of Fortran INTEGER*4 */
#define OMPI_SIZEOF_FORTRAN_INTEGER4 4

/* Size of Fortran INTEGER*8 */
#define OMPI_SIZEOF_FORTRAN_INTEGER8 4

/* Size of Fortran LOGICAL */
#define OMPI_SIZEOF_FORTRAN_LOGICAL 4

/* Size of Fortran LOGICAL*1 */
#define OMPI_SIZEOF_FORTRAN_LOGICAL1 4

/* Size of Fortran LOGICAL*2 */
#define OMPI_SIZEOF_FORTRAN_LOGICAL2 4

/* Size of Fortran LOGICAL*4 */
#define OMPI_SIZEOF_FORTRAN_LOGICAL4 4

/* Size of Fortran LOGICAL*8 */
#define OMPI_SIZEOF_FORTRAN_LOGICAL8 4

/* Size of Fortran REAL */
#define OMPI_SIZEOF_FORTRAN_REAL 4

/* Size of Fortran REAL*16 */
#define OMPI_SIZEOF_FORTRAN_REAL16 4

/* Size of Fortran REAL*2 */
#define OMPI_SIZEOF_FORTRAN_REAL2 4

/* Size of Fortran REAL*4 */
#define OMPI_SIZEOF_FORTRAN_REAL4 4

/* Size of Fortran REAL*8 */
#define OMPI_SIZEOF_FORTRAN_REAL8 4

/* Tarball filename version string of Open MPI */
#define OMPI_TARBALL_VERSION "4.0.1"

/* Complete release number of Open MPI */
#define OMPI_VERSION "1"

/* do we want java mpi bindings */
#define OMPI_WANT_JAVA_BINDINGS 0

/* do we want to try to work around C++ bindings SEEK_* issue? */
#define OMPI_WANT_MPI_CXX_SEEK 1

/* Enable warnings when using deprecated MPI functions */
#define OMPI_WANT_MPI_INTERFACE_WARNING 1

/* if the peruse interface should be enabled */
#define OMPI_WANT_PERUSE 0

/* Alignment of type bool */
#define OPAL_ALIGNMENT_BOOL 1

/* Alignment of type char */
#define OPAL_ALIGNMENT_CHAR 1

/* Alignment of type bool */
#define OPAL_ALIGNMENT_CXX_BOOL 1

/* Alignment of type double */
#define OPAL_ALIGNMENT_DOUBLE 8

/* Alignment of type double _Complex */
#define OPAL_ALIGNMENT_DOUBLE_COMPLEX 8

/* Alignment of type float */
#define OPAL_ALIGNMENT_FLOAT 4

/* Alignment of type float _Complex */
#define OPAL_ALIGNMENT_FLOAT_COMPLEX 4

/* Alignment of type int */
#define OPAL_ALIGNMENT_INT 4

/* Alignment of type int128_t */
/* #undef OPAL_ALIGNMENT_INT128 */

/* Alignment of type int16_t */
#define OPAL_ALIGNMENT_INT16 2

/* Alignment of type int32_t */
#define OPAL_ALIGNMENT_INT32 4

/* Alignment of type int64_t */
#define OPAL_ALIGNMENT_INT64 8

/* Alignment of type int8_t */
#define OPAL_ALIGNMENT_INT8 1

/* Alignment of type long */
#define OPAL_ALIGNMENT_LONG 8

/* Alignment of type long double */
#define OPAL_ALIGNMENT_LONG_DOUBLE 16

/* Alignment of type long double _Complex */
#define OPAL_ALIGNMENT_LONG_DOUBLE_COMPLEX 16

/* Alignment of type long long */
#define OPAL_ALIGNMENT_LONG_LONG 8

/* Alignment of type short */
#define OPAL_ALIGNMENT_SHORT 2

/* Alignment of type size_t */
#define OPAL_ALIGNMENT_SIZE_T 8

/* Alignment of type void * */
#define OPAL_ALIGNMENT_VOID_P 8

/* Alignment of type wchar_t */
#define OPAL_ALIGNMENT_WCHAR 4

/* Alignment of type __float128 */
#define OPAL_ALIGNMENT___FLOAT128 16

/* set to 1 if word-size integers must be aligned to word-size padding to
   prevent bus errors */
#define OPAL_ALIGN_WORD_SIZE_INTEGERS 0

/* OMPI architecture string */
#define OPAL_ARCH "x86_64-unknown-linux-gnu"

/* Assembly align directive expects logarithmic value */
#define OPAL_ASM_ALIGN_LOG 

/* What ARM assembly version to use */
/* #undef OPAL_ASM_ARM_VERSION */

/* Assembly directive for exporting symbols */
#define OPAL_ASM_GLOBAL ".globl"

/* Assembly prefix for gsym labels */
#define OPAL_ASM_GSYM ""

/* Assembly suffix for labels */
#define OPAL_ASM_LABEL_SUFFIX ":"

/* Assembly prefix for lsym labels */
#define OPAL_ASM_LSYM ".L"

/* Do we need to give a .size directive */
#define OPAL_ASM_SIZE "1"

/* Whether we can do 64bit assembly operations or not. Should not be used
   outside of the assembly header files */
/* #undef OPAL_ASM_SUPPORT_64BIT */

/* Whether 64-bit is supported by the __sync builtin atomics */
/* #undef OPAL_ASM_SYNC_HAVE_64BIT */

/* Assembly directive for setting text section */
#define OPAL_ASM_TEXT ".text"

/* How to set function type in .type directive */
#define OPAL_ASM_TYPE "@"

/* Architecture type of assembly to use for atomic operations and CMA */
#define OPAL_ASSEMBLY_ARCH OPAL_X86_64

/* Whether to use builtin atomics */
#define OPAL_ASSEMBLY_BUILTIN OPAL_BUILTIN_NO

/* Format of assembly file */
/* #undef OPAL_ASSEMBLY_FORMAT */

/* Whether we have support for RDTSCP instruction */
#define OPAL_ASSEMBLY_SUPPORTS_RDTSCP 0

/* Enable flow control for Portals4 BTL */
#define OPAL_BTL_PORTALS4_FLOW_CONTROL 0

/* define to 1 if usnic BTL unit tests are enabled, 0 otherwise */
#define OPAL_BTL_USNIC_UNIT_TESTS 0

/* If CMA support can be enabled within vader */
#define OPAL_BTL_VADER_HAVE_CMA 1

/* If KNEM support can be enabled within vader */
#define OPAL_BTL_VADER_HAVE_KNEM 0

/* If XPMEM support can be enabled within vader */
#define OPAL_BTL_VADER_HAVE_XPMEM 0

/* The compiler $lower which OMPI was built with */
#define OPAL_BUILD_PLATFORM_COMPILER_FAMILYID 1

/* The compiler $lower which OMPI was built with */
#define OPAL_BUILD_PLATFORM_COMPILER_FAMILYNAME GNU

/* The compiler $lower which OMPI was built with */
#define OPAL_BUILD_PLATFORM_COMPILER_VERSION 459776

/* The compiler $lower which OMPI was built with */
#define OPAL_BUILD_PLATFORM_COMPILER_VERSION_STR 7.4.0

/* OMPI underlying C compiler */
#define OPAL_CC "gcc"

/* Use static const char[] strings for C files */
#define OPAL_CC_USE_CONST_CHAR_IDENT 0

/* Use #ident strings for C files */
#define OPAL_CC_USE_IDENT 1

/* Use #pragma comment for C files */
#define OPAL_CC_USE_PRAGMA_COMMENT 

/* Use #pragma ident strings for C files */
#define OPAL_CC_USE_PRAGMA_IDENT 0

/* Need CMA syscalls defined */
#define OPAL_CMA_NEED_SYSCALL_DEFS 0

/* Whether the common/usnic_verbs component is being built or not */
#define OPAL_COMMON_VERBS_USNIC_HAPPY 0

/* Whether we have CUDA GDR support available */
#define OPAL_CUDA_GDR_SUPPORT 1

/* Whether we have CUDA cuPointerGetAttributes function available */
#define OPAL_CUDA_GET_ATTRIBUTES 1

/* Whether we want cuda device pointer support */
#define OPAL_CUDA_SUPPORT 1

/* Whether we have CUDA CU_POINTER_ATTRIBUTE_SYNC_MEMOPS support available */
#define OPAL_CUDA_SYNC_MEMOPS 1

/* OPAL underlying C++ compiler */
#define OPAL_CXX "g++"

/* Use static const char[] strings for C++ files */
#define OPAL_CXX_USE_CONST_CHAR_IDENT 0

/* Use #ident strings for C++ files */
#define OPAL_CXX_USE_IDENT 1

/* Use #pragma comment for C++ files */
#define OPAL_CXX_USE_PRAGMA_COMMENT 

/* Use #pragma ident strings for C++ files */
#define OPAL_CXX_USE_PRAGMA_IDENT 0

/* Whether C compiler supports GCC style inline assembly */
#define OPAL_C_GCC_INLINE_ASSEMBLY 1

/* Whether C compiler support atomic convenience variables in stdatomic.h */
#define OPAL_C_HAVE_ATOMIC_CONV_VAR 1

/* Whether C compiler supports __builtin_clz */
#define OPAL_C_HAVE_BUILTIN_CLZ 1

/* Whether C compiler supports __builtin_expect */
#define OPAL_C_HAVE_BUILTIN_EXPECT 1

/* Whether C compiler supports __builtin_prefetch */
#define OPAL_C_HAVE_BUILTIN_PREFETCH 1

/* Whether C compiler supports symbol visibility or not */
#define OPAL_C_HAVE_VISIBILITY 1

/* Whether C compiler supports __Atomic keyword */
#define OPAL_C_HAVE__ATOMIC 1

/* Whether C compiler supports __Generic keyword */
#define OPAL_C_HAVE__GENERIC 1

/* Whether C compiler support _Static_assert keyword */
#define OPAL_C_HAVE__STATIC_ASSERT 1

/* Whether C compiler supports __Thread_local */
#define OPAL_C_HAVE__THREAD_LOCAL 1

/* Whether C compiler supports __thread */
#define OPAL_C_HAVE___THREAD 1

/* Whether we have lt_dladvise or not */
#define OPAL_DL_LIBLTDL_HAVE_LT_DLADVISE 0

/* Whether we want checkpoint/restart enabled debugging functionality or not
   */
#define OPAL_ENABLE_CRDEBUG 0

/* Whether we want developer-level debugging code or not */
#define OPAL_ENABLE_DEBUG 0

/* Whether we want to enable dlopen support */
#define OPAL_ENABLE_DLOPEN_SUPPORT 1

/* Enable features required for dynamic SL support */
#define OPAL_ENABLE_DYNAMIC_SL 0

/* Enable fault tolerance general components and logic */
#define OPAL_ENABLE_FT 0

/* Enable fault tolerance checkpoint/restart components and logic */
#define OPAL_ENABLE_FT_CR 0

/* Enable fault tolerance thread in Open PAL */
#define OPAL_ENABLE_FT_THREAD 0

/* Disable getpwuid support (default: enabled) */
#define OPAL_ENABLE_GETPWUID 1

/* Enable features required for heterogeneous support */
#define OPAL_ENABLE_HETEROGENEOUS_SUPPORT 0

/* Enable IPv6 support, but only if the underlying system supports it */
#define OPAL_ENABLE_IPV6 1

/* Whether we want the memory profiling or not */
#define OPAL_ENABLE_MEM_DEBUG 0

/* Whether we want the memory profiling or not */
#define OPAL_ENABLE_MEM_PROFILE 0

/* Whether we should enable thread support within the OPAL code base */
#define OPAL_ENABLE_MULTI_THREADS 1

/* Whether we want BTL progress threads enabled */
#define OPAL_ENABLE_PROGRESS_THREADS 0

/* Whether user wants PTY support or not */
#define OPAL_ENABLE_PTY_SUPPORT 1

/* Whether we want developer-level timing framework or not */
#define OPAL_ENABLE_TIMING 0

/* Greek - alpha, beta, etc - release number of Open Portable Access Layer */
#define OPAL_GREEK_VERSION ""

/* Whether your compiler has __attribute__ or not */
#define OPAL_HAVE_ATTRIBUTE 1

/* Whether your compiler has __attribute__ aligned or not */
#define OPAL_HAVE_ATTRIBUTE_ALIGNED 1

/* Whether your compiler has __attribute__ always_inline or not */
#define OPAL_HAVE_ATTRIBUTE_ALWAYS_INLINE 1

/* Whether your compiler has __attribute__ cold or not */
#define OPAL_HAVE_ATTRIBUTE_COLD 1

/* Whether your compiler has __attribute__ const or not */
#define OPAL_HAVE_ATTRIBUTE_CONST 1

/* Whether your compiler has __attribute__ deprecated or not */
#define OPAL_HAVE_ATTRIBUTE_DEPRECATED 1

/* Whether your compiler has __attribute__ deprecated with optional argument
   */
#define OPAL_HAVE_ATTRIBUTE_DEPRECATED_ARGUMENT 1

/* Whether your compiler has __attribute__ destructor or not */
#define OPAL_HAVE_ATTRIBUTE_DESTRUCTOR 1

/* Whether your compiler has __attribute__ error or not */
#define OPAL_HAVE_ATTRIBUTE_ERROR 1

/* Whether your compiler has __attribute__ extension or not */
#define OPAL_HAVE_ATTRIBUTE_EXTENSION 1

/* Whether your compiler has __attribute__ format or not */
#define OPAL_HAVE_ATTRIBUTE_FORMAT 1

/* Whether your compiler has __attribute__ format and it works on function
   pointers */
#define OPAL_HAVE_ATTRIBUTE_FORMAT_FUNCPTR 1

/* Whether your compiler has __attribute__ hot or not */
#define OPAL_HAVE_ATTRIBUTE_HOT 1

/* Whether your compiler has __attribute__ malloc or not */
#define OPAL_HAVE_ATTRIBUTE_MALLOC 1

/* Whether your compiler has __attribute__ may_alias or not */
#define OPAL_HAVE_ATTRIBUTE_MAY_ALIAS 1

/* Whether your compiler has __attribute__ noinline or not */
#define OPAL_HAVE_ATTRIBUTE_NOINLINE 1

/* Whether your compiler has __attribute__ nonnull or not */
#define OPAL_HAVE_ATTRIBUTE_NONNULL 1

/* Whether your compiler has __attribute__ noreturn or not */
#define OPAL_HAVE_ATTRIBUTE_NORETURN 1

/* Whether your compiler has __attribute__ noreturn and it works on function
   pointers */
#define OPAL_HAVE_ATTRIBUTE_NORETURN_FUNCPTR 1

/* Whether your compiler has __attribute__ no_instrument_function or not */
#define OPAL_HAVE_ATTRIBUTE_NO_INSTRUMENT_FUNCTION 1

/* Whether your compiler has __attribute__ optnone or not */
#define OPAL_HAVE_ATTRIBUTE_OPTNONE 0

/* Whether your compiler has __attribute__ packed or not */
#define OPAL_HAVE_ATTRIBUTE_PACKED 1

/* Whether your compiler has __attribute__ pure or not */
#define OPAL_HAVE_ATTRIBUTE_PURE 1

/* Whether your compiler has __attribute__ sentinel or not */
#define OPAL_HAVE_ATTRIBUTE_SENTINEL 1

/* Whether your compiler has __attribute__ unused or not */
#define OPAL_HAVE_ATTRIBUTE_UNUSED 1

/* Whether your compiler has __attribute__ visibility or not */
#define OPAL_HAVE_ATTRIBUTE_VISIBILITY 1

/* Whether your compiler has __attribute__ warn unused result or not */
#define OPAL_HAVE_ATTRIBUTE_WARN_UNUSED_RESULT 1

/* Whether your compiler has __attribute__ weak alias or not */
#define OPAL_HAVE_ATTRIBUTE_WEAK_ALIAS 1

/* whether backtrace_execinfo is found and available */
#define OPAL_HAVE_BACKTRACE_EXECINFO 1

/* whether qsort is broken or not */
#define OPAL_HAVE_BROKEN_QSORT 0

/* whether ceil is found and available */
#define OPAL_HAVE_CEIL 1

/* whether clock_gettime is found and available */
#define OPAL_HAVE_CLOCK_GETTIME 1

/* Whether the processor supports the cmpxchg16b instruction */
#define OPAL_HAVE_CMPXCHG16B 1

/* Enable features required for ConnectX XRC support */
#define OPAL_HAVE_CONNECTX_XRC 0

/* Enable features required for XRC domains support */
#define OPAL_HAVE_CONNECTX_XRC_DOMAINS 0

/* whether dirname is found and available */
#define OPAL_HAVE_DIRNAME 1

/* Whether the OPAL DL framework is functional or not */
#define OPAL_HAVE_DL_SUPPORT 1

/* whether fbtl_posix is found and available */
#define OPAL_HAVE_FBTL_POSIX 1

/* Whether the __atomic builtin atomic compare swap is both supported and
   lock-free on 128-bit values */
#define OPAL_HAVE_GCC_BUILTIN_CSWAP_INT128 0

/* whether gethostbyname is found and available */
#define OPAL_HAVE_GETHOSTBYNAME 1

/* Do not use outside of mpi.h. Define to 1 if the system has the type `long
   long'. */
#define OPAL_HAVE_LONG_LONG 1

/* whether openpty is found and available */
#define OPAL_HAVE_OPENPTY 1

/* If PTHREADS implementation supports PTHREAD_MUTEX_ERRORCHECK */
#define OPAL_HAVE_PTHREAD_MUTEX_ERRORCHECK 1

/* If PTHREADS implementation supports PTHREAD_MUTEX_ERRORCHECK_NP */
#define OPAL_HAVE_PTHREAD_MUTEX_ERRORCHECK_NP 1

/* Whether RDMA CM is available or not */
/* #undef OPAL_HAVE_RDMACM */

/* Enable RDMAoE support */
/* #undef OPAL_HAVE_RDMAOE */

/* Whether we have SA_RESTART in <signal.h> or not */
#define OPAL_HAVE_SA_RESTART 1

/* whether sched_yield is found and available */
#define OPAL_HAVE_SCHED_YIELD 1

/* whether shmem_posix is found and available */
#define OPAL_HAVE_SHMEM_POSIX 1

/* whether socket is found and available */
#define OPAL_HAVE_SOCKET 1

/* Whether or not we have solaris */
#define OPAL_HAVE_SOLARIS 0

/* Whether the __sync builtin atomic compare and swap supports 128-bit values
   */
#define OPAL_HAVE_SYNC_BUILTIN_CSWAP_INT128 1

/* Do not use outside of mpi.h. Define to 1 if you have the <sys/synch.h>
   header file. */
/* #undef OPAL_HAVE_SYS_SYNCH_H */

/* Do not use outside of mpi.h. Define to 1 if you have the <sys/time.h>
   header file. */
#define OPAL_HAVE_SYS_TIME_H 1

/* Whether UD CM is available or not */
/* #undef OPAL_HAVE_UDCM */

/* Whether we have __va_copy or not */
#define OPAL_HAVE_UNDERSCORE_VA_COPY 1

/* Whether we have va_copy or not */
#define OPAL_HAVE_VA_COPY 1

/* Whether we have weak symbols or not */
#define OPAL_HAVE_WEAK_SYMBOLS 1

/* Whether our event component has working event operations or not (if not,
   then assumedly it only has working timers and signals) */
#define OPAL_HAVE_WORKING_EVENTOPS 1

/* whether yp_all_nsl is found and available */
#define OPAL_HAVE_YP_ALL_NSL 1

/* Whether or not we have zlib support */
#define OPAL_HAVE_ZLIB 1

/* Define to 1 ifyou have the declaration of _SC_NPROCESSORS_ONLN, and to 0
   otherwise */
#define OPAL_HAVE__SC_NPROCESSORS_ONLN 1

/* Number of arguments to ibv_create_cq */
/* #undef OPAL_IBV_CREATE_CQ_ARGS */

/* ident string for Open MPI */
#define OPAL_IDENT_STRING "4.0.1"

/* Major release number of Open Portable Access Layer */
#define OPAL_MAJOR_VERSION 4

/* Maximum length of datarep strings (default is 128) */
#define OPAL_MAX_DATAREP_STRING 128

/* Maximum length of error strings (default is 256) */
#define OPAL_MAX_ERROR_STRING 256

/* Maximum length of info keys (default is 36) */
#define OPAL_MAX_INFO_KEY 36

/* Maximum length of info vals (default is 256) */
#define OPAL_MAX_INFO_VAL 256

/* Maximum length of object names (default is 64) */
#define OPAL_MAX_OBJECT_NAME 64

/* Maximum length of port names (default is 1024) */
#define OPAL_MAX_PORT_NAME 1024

/* Maximum length of processor names (default is 256) */
#define OPAL_MAX_PROCESSOR_NAME 256

/* MCA cmd line identifier */
#define OPAL_MCA_CMD_LINE_ID "mca"

/* MCA prefix string for envars */
#define OPAL_MCA_PREFIX "OMPI_MCA_"

/* Whether any opal memory mca components were found */
#define OPAL_MEMORY_HAVE_COMPONENT 0

/* Minor release number of Open Portable Access Layer */
#define OPAL_MINOR_VERSION 0

/* Add padding bytes to the openib BTL control header */
#define OPAL_OPENIB_PAD_HDR 0

/* package/branding string for Open MPI */
#define OPAL_PACKAGE_STRING "Open MPI devtools@yandex-team.ru Distribution"

/* Whether the external PMIx library is v1 */
#define OPAL_PMIX_V1 0

/* Log base 2 of the maximum size in bytes of a memory descriptor. Set to 0 if
   MD can bind all of memory. */
#define OPAL_PORTALS4_MAX_MD_SIZE 0

/* Log base 2 of the maximum size in bytes of the user virtual address space.
   Set to 0 if MD can bind all of memory. */
#define OPAL_PORTALS4_MAX_VA_SIZE 0

/* Whether r notation is used for ppc registers */
/* #undef OPAL_POWERPC_R_REGISTERS */

/* Release date of Open Portable Access Layer */
#define OPAL_RELEASE_DATE "Mar 26, 2019"

/* Release release number of Open Portable Access Layer */
#define OPAL_RELEASE_VERSION 1

/* The repository version Open Portable Access Layer */
#define OPAL_REPO_REV "v4.0.1"

/* Whether we have shared memory support for mmap or not */
#define OPAL_SHMEM_MMAP 1

/* Whether we have shared memory support for POSIX or not */
#define OPAL_SHMEM_POSIX 1

/* Whether we have shared memory support for SYSV or not */
#define OPAL_SHMEM_SYSV 1

/* Default value for mca_base_component_show_load_errors MCA variable */
#define OPAL_SHOW_LOAD_ERRORS_DEFAULT 1

/* Path to Singularity binaries */
#define OPAL_SINGULARITY_PATH ""

/* Do not use outside of mpi.h. Define to 1 if you have the ANSI C header
   files. */
#define OPAL_STDC_HEADERS 1

/* Tarball filename version string of Open Portable Access Layer */
#define OPAL_TARBALL_VERSION "4.0.1"

/* Complete release number of Open Portable Access Layer */
#define OPAL_VERSION "1"

/* Enable per-user config files */
#define OPAL_WANT_HOME_CONFIG_FILES 1

/* if the memory and buffer checking should be enabled */
#define OPAL_WANT_MEMCHECKER 0

/* if want pretty-print stack trace feature */
#define OPAL_WANT_PRETTY_PRINT_STACKTRACE 1

/* Specific ps command to use in orte-clean */
#define ORTE_CLEAN_PS_CMD "unknown"

/* Greek - alpha, beta, etc - release number of Open MPI Run-Time Environment
   */
#define ORTE_GREEK_VERSION ""

/* Major release number of Open MPI Run-Time Environment */
#define ORTE_MAJOR_VERSION 4

/* Minor release number of Open MPI Run-Time Environment */
#define ORTE_MINOR_VERSION 0

/* Release date of Open MPI Run-Time Environment */
#define ORTE_RELEASE_DATE "Mar 26, 2019"

/* Release release number of Open MPI Run-Time Environment */
#define ORTE_RELEASE_VERSION 1

/* The repository version Open MPI Run-Time Environment */
#define ORTE_REPO_REV "v4.0.1"

/* Tarball filename version string of Open MPI Run-Time Environment */
#define ORTE_TARBALL_VERSION "4.0.1"

/* Complete release number of Open MPI Run-Time Environment */
#define ORTE_VERSION "1"

/* Whether we want orterun to effect "--prefix $prefix" by default */
#define ORTE_WANT_ORTERUN_PREFIX_BY_DEFAULT 0

/* Greek - alpha, beta, etc - release number of Open SHMEM */
#define OSHMEM_GREEK_VERSION ""

/* mxm support is available */
/* #undef OSHMEM_HAS_ATOMIC_MXM */

/* Major release number of Open SHMEM */
#define OSHMEM_MAJOR_VERSION 4

/* Minor release number of Open SHMEM */
#define OSHMEM_MINOR_VERSION 0

/* Whether we want to check OSHMEM parameters always or never */
#define OSHMEM_PARAM_CHECK 1

/* Release date of Open SHMEM */
#define OSHMEM_RELEASE_DATE "Mar 26, 2019"

/* Release release number of Open SHMEM */
#define OSHMEM_RELEASE_VERSION 1

/* The repository version Open SHMEM */
#define OSHMEM_REPO_REV "v4.0.1"

/* Whether user wants OSHMEM in compatibility mode or not */
#define OSHMEM_SPEC_COMPAT 1

/* Whether we have shared memory support for mmap or not */
#define OSHMEM_SSHMEM_MMAP 1

/* Whether we have shared memory support for SYSV or not */
#define OSHMEM_SSHMEM_SYSV 1

/* Whether we have shared memory support for verbs or not */
#define OSHMEM_SSHMEM_VERBS 0

/* Tarball filename version string of Open SHMEM */
#define OSHMEM_TARBALL_VERSION "4.0.1"

/* Complete release number of Open SHMEM */
#define OSHMEM_VERSION "1"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "http://www.open-mpi.org/community/help/"

/* Define to the full name of this package. */
#define PACKAGE_NAME "Open MPI"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "Open MPI 4.0.1"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "openmpi"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "4.0.1"

/* The size of `atomic_int', as computed by sizeof. */
#define SIZEOF_ATOMIC_INT 4

/* The size of `atomic_llong', as computed by sizeof. */
#define SIZEOF_ATOMIC_LLONG 8

/* The size of `atomic_long', as computed by sizeof. */
#define SIZEOF_ATOMIC_LONG 8

/* The size of `atomic_short', as computed by sizeof. */
#define SIZEOF_ATOMIC_SHORT 2

/* The size of `bool', as computed by sizeof. */
#define SIZEOF_BOOL 1

/* The size of `char', as computed by sizeof. */
#define SIZEOF_CHAR 1

/* The size of `double', as computed by sizeof. */
#define SIZEOF_DOUBLE 8

/* The size of `double _Complex', as computed by sizeof. */
#define SIZEOF_DOUBLE__COMPLEX 16

/* The size of `float', as computed by sizeof. */
#define SIZEOF_FLOAT 4

/* The size of `float _Complex', as computed by sizeof. */
#define SIZEOF_FLOAT__COMPLEX 8

/* The size of `int', as computed by sizeof. */
#define SIZEOF_INT 4

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* The size of `long double', as computed by sizeof. */
#define SIZEOF_LONG_DOUBLE 16

/* The size of `long double _Complex', as computed by sizeof. */
#define SIZEOF_LONG_DOUBLE__COMPLEX 32

/* The size of `long long', as computed by sizeof. */
#define SIZEOF_LONG_LONG 8

/* The size of `pid_t', as computed by sizeof. */
#define SIZEOF_PID_T 4

/* The size of `ptrdiff_t', as computed by sizeof. */
#define SIZEOF_PTRDIFF_T 8

/* The size of `short', as computed by sizeof. */
#define SIZEOF_SHORT 2

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8

/* The size of `ssize_t', as computed by sizeof. */
#define SIZEOF_SSIZE_T 8

/* The size of `unsigned int', as computed by sizeof. */
#define SIZEOF_UNSIGNED_INT 4

/* The size of `unsigned long', as computed by sizeof. */
#define SIZEOF_UNSIGNED_LONG 8

/* The size of `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 8

/* The size of `wchar_t', as computed by sizeof. */
#define SIZEOF_WCHAR_T 4

/* The size of `_Bool', as computed by sizeof. */
#define SIZEOF__BOOL 1

/* The size of `__float128', as computed by sizeof. */
#define SIZEOF___FLOAT128 16

/* defined to 1 if slurm cray env, 0 otherwise */
#define SLURM_CRAY_ENV 0

/* If the software-based performance counters capability should be enabled. */
#define SPC_ENABLE 0

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Enable extensions on HP-UX. */
#ifndef _HPUX_SOURCE
# define _HPUX_SOURCE 1
#endif


/* Whether to use the legacy Solaris munmap prototype or not */
/* #undef USE_SOLARIS_LEGACY_MUNMAP_PROTOTYPE */

/* Enable extensions on AIX 3, Interix.  */
#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
/* Enable threading extensions on Solaris.  */
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif
/* Enable extensions on HP NonStop.  */
#ifndef _TANDEM_SOURCE
# define _TANDEM_SOURCE 1
#endif
/* Enable general extensions on Solaris.  */
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif


/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* Additional CFLAGS to pass through the wrapper compilers */
#define WRAPPER_EXTRA_CFLAGS "-fexceptions -pthread "

/* Additional CFLAGS_PREFIX to pass through the wrapper compilers */
#define WRAPPER_EXTRA_CFLAGS_PREFIX ""

/* Additional CXXFLAGS to pass through the wrapper compilers */
#define WRAPPER_EXTRA_CXXFLAGS "-fexceptions -pthread "

/* Additional CXXFLAGS_PREFIX to pass through the wrapper compilers */
#define WRAPPER_EXTRA_CXXFLAGS_PREFIX ""

/* Additional FCFLAGS to pass through the wrapper compilers */
#define WRAPPER_EXTRA_FCFLAGS "-fexceptions "

/* Additional FCFLAGS to pass through the wrapper compilers */
#define WRAPPER_EXTRA_FCFLAGS_PREFIX ""

/* Additional LDFLAGS to pass through the wrapper compilers */
#define WRAPPER_EXTRA_LDFLAGS "    -Wl,-rpath -Wl,@{libdir} -Wl,--enable-new-dtags"

/* Additional LIBS to pass through the wrapper compilers */
#define WRAPPER_EXTRA_LIBS "-lhwloc -ldl -levent -levent_pthreads -lz -lrt -lutil -lm "

/* Whether the wrapper compilers add rpath flags by default */
#define WRAPPER_RPATH_SUPPORT "runpath"

/* Define to 1 if the X Window System is missing or not being used. */
#define X_DISPLAY_MISSING 1

/* Define to 1 if `lex' declares `yytext' as a `char *' by default, not a
   `char[]'. */
/* #undef YYTEXT_POINTER */

/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif

/* Are we building for HP-UX? */
#define _HPUX_SOURCE 1

/* Define to 1 if on MINIX. */
/* #undef _MINIX */

/* Define to 2 if the system does not provide POSIX.1 features except with
   this defined. */
/* #undef _POSIX_1_SOURCE */

/* Define to 1 if you need to in order for `stat' and other things to work. */
/* #undef _POSIX_SOURCE */

/* Define this to the process ID type */
#define hwloc_pid_t pid_t

/* Define this to the thread ID type */
#define hwloc_thread_t pthread_t

/* A bogus type that allows us to have sentinel type values that are still
   valid */
#define ompi_fortran_bogus_type_t int

/* C type corresponding to Fortran CHARACTER */
#define ompi_fortran_character_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran COMPLEX*16 */
/* #undef ompi_fortran_complex16_t */

/* C type corresponding to Fortran COMPLEX*32 */
/* #undef ompi_fortran_complex32_t */

/* C type corresponding to Fortran COMPLEX*4 */
/* #undef ompi_fortran_complex4_t */

/* C type corresponding to Fortran COMPLEX*8 */
/* #undef ompi_fortran_complex8_t */

/* C type corresponding to Fortran COMPLEX */
/* #undef ompi_fortran_complex_t */

/* C type corresponding to Fortran DOUBLE COMPLEX */
/* #undef ompi_fortran_double_complex_t */

/* C type corresponding to Fortran DOUBLE PRECISION */
#define ompi_fortran_double_precision_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran INTEGER*16 */
#define ompi_fortran_integer16_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran INTEGER*1 */
#define ompi_fortran_integer1_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran INTEGER*2 */
#define ompi_fortran_integer2_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran INTEGER*4 */
#define ompi_fortran_integer4_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran INTEGER*8 */
#define ompi_fortran_integer8_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran INTEGER */
#define ompi_fortran_integer_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran LOGICAL*1 */
#define ompi_fortran_logical1_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran LOGICAL*2 */
#define ompi_fortran_logical2_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran LOGICAL*4 */
#define ompi_fortran_logical4_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran LOGICAL*8 */
#define ompi_fortran_logical8_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran LOGICAL */
#define ompi_fortran_logical_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran REAL*16 */
#define ompi_fortran_real16_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran REAL*2 */
#define ompi_fortran_real2_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran REAL*4 */
#define ompi_fortran_real4_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran REAL*8 */
#define ompi_fortran_real8_t ompi_fortran_bogus_type_t

/* C type corresponding to Fortran REAL */
#define ompi_fortran_real_t ompi_fortran_bogus_type_t


#include "opal_config_bottom.h"
#endif /* OPAL_CONFIG_H */


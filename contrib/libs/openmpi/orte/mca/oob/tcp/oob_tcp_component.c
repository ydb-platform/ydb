/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2009-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2014      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * In windows, many of the socket functions return an EWOULDBLOCK
 * instead of things like EAGAIN, EINPROGRESS, etc. It has been
 * verified that this will not conflict with other error codes that
 * are returned by these functions under UNIX/Linux environments
 */

#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <fcntl.h>
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <ctype.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "opal/util/show_help.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/opal_socket_errno.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/argv.h"
#include "opal/class/opal_hash_table.h"
#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/runtime/opal_progress_threads.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/state/state.h"
#include "orte/util/attr.h"
#include "orte/util/name_fns.h"
#include "orte/util/parse_options.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/oob/tcp/oob_tcp.h"
#include "orte/mca/oob/tcp/oob_tcp_common.h"
#include "orte/mca/oob/tcp/oob_tcp_component.h"
#include "orte/mca/oob/tcp/oob_tcp_peer.h"
#include "orte/mca/oob/tcp/oob_tcp_connection.h"
#include "orte/mca/oob/tcp/oob_tcp_listener.h"
#include "oob_tcp_peer.h"

/*
 * Local utility functions
 */

static int tcp_component_register(void);
static int tcp_component_open(void);
static int tcp_component_close(void);

static int component_available(void);
static int component_startup(void);
static void component_shutdown(void);
static int component_send(orte_rml_send_t *msg);
static char* component_get_addr(void);
static int component_set_addr(orte_process_name_t *peer,
                              char **uris);
static bool component_is_reachable(char *rtmod, orte_process_name_t *peer);
static orte_rml_pathway_t* component_query_transports(void);
#if OPAL_ENABLE_FT_CR == 1
static int component_ft_event(int state);
#endif

/*
 * Struct of function pointers and all that to let us be initialized
 */
mca_oob_tcp_component_t mca_oob_tcp_component = {
    {
        .oob_base = {
            MCA_OOB_BASE_VERSION_2_0_0,
            .mca_component_name = "tcp",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),
            .mca_open_component = tcp_component_open,
            .mca_close_component = tcp_component_close,
            .mca_register_component_params = tcp_component_register,
        },
        .oob_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
        .priority = 30, // default priority of this transport
        .available = component_available,
        .startup = component_startup,
        .shutdown = component_shutdown,
        .send_nb = component_send,
        .get_addr = component_get_addr,
        .set_addr = component_set_addr,
        .is_reachable = component_is_reachable,
        .query_transports = component_query_transports,
#if OPAL_ENABLE_FT_CR == 1
        .ft_event = component_ft_event,
#endif
    },
};

/*
 * Initialize global variables used w/in this module.
 */
static int tcp_component_open(void)
{
    mca_oob_tcp_component.next_base = 0;
    OBJ_CONSTRUCT(&mca_oob_tcp_component.peers, opal_hash_table_t);
    opal_hash_table_init(&mca_oob_tcp_component.peers, 32);
    OBJ_CONSTRUCT(&mca_oob_tcp_component.ev_bases, opal_pointer_array_t);
    opal_pointer_array_init(&mca_oob_tcp_component.ev_bases,
                            orte_oob_base.num_threads, 256, 8);

    OBJ_CONSTRUCT(&mca_oob_tcp_component.listeners, opal_list_t);
    if (ORTE_PROC_IS_HNP) {
        OBJ_CONSTRUCT(&mca_oob_tcp_component.listen_thread, opal_thread_t);
        mca_oob_tcp_component.listen_thread_active = false;
        mca_oob_tcp_component.listen_thread_tv.tv_sec = 3600;
        mca_oob_tcp_component.listen_thread_tv.tv_usec = 0;
    }
    mca_oob_tcp_component.addr_count = 0;
    mca_oob_tcp_component.ipv4conns = NULL;
    mca_oob_tcp_component.ipv4ports = NULL;
    mca_oob_tcp_component.ipv6conns = NULL;
    mca_oob_tcp_component.ipv6ports = NULL;

    /* if_include and if_exclude need to be mutually exclusive */
    if (OPAL_SUCCESS !=
        mca_base_var_check_exclusive("orte",
        mca_oob_tcp_component.super.oob_base.mca_type_name,
        mca_oob_tcp_component.super.oob_base.mca_component_name,
        "if_include",
        mca_oob_tcp_component.super.oob_base.mca_type_name,
        mca_oob_tcp_component.super.oob_base.mca_component_name,
        "if_exclude")) {
        /* Return ERR_NOT_AVAILABLE so that a warning message about
           "open" failing is not printed */
        return ORTE_ERR_NOT_AVAILABLE;
    }
    return ORTE_SUCCESS;
}

/*
 * Cleanup of global variables used by this module.
 */
static int tcp_component_close(void)
{
    OBJ_DESTRUCT(&mca_oob_tcp_component.peers);

    if (NULL != mca_oob_tcp_component.ipv4conns) {
        opal_argv_free(mca_oob_tcp_component.ipv4conns);
    }
    if (NULL != mca_oob_tcp_component.ipv4ports) {
        opal_argv_free(mca_oob_tcp_component.ipv4ports);
    }

#if OPAL_ENABLE_IPV6
    if (NULL != mca_oob_tcp_component.ipv6conns) {
        opal_argv_free(mca_oob_tcp_component.ipv6conns);
    }
    if (NULL != mca_oob_tcp_component.ipv6ports) {
        opal_argv_free(mca_oob_tcp_component.ipv6ports);
    }
#endif

    OBJ_DESTRUCT(&mca_oob_tcp_component.ev_bases);

    return ORTE_SUCCESS;
}
static char *static_port_string;
#if OPAL_ENABLE_IPV6
static char *static_port_string6;
#endif // OPAL_ENABLE_IPV6

static char *dyn_port_string;
#if OPAL_ENABLE_IPV6
static char *dyn_port_string6;
#endif

static int tcp_component_register(void)
{
    mca_base_component_t *component = &mca_oob_tcp_component.super.oob_base;
    int var_id;

    /* register oob module parameters */
    mca_oob_tcp_component.peer_limit = -1;
    (void)mca_base_component_var_register(component, "peer_limit",
                                          "Maximum number of peer connections to simultaneously maintain (-1 = infinite)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_5,
                                          MCA_BASE_VAR_SCOPE_LOCAL,
                                          &mca_oob_tcp_component.peer_limit);

    mca_oob_tcp_component.max_retries = 2;
    (void)mca_base_component_var_register(component, "peer_retries",
                                          "Number of times to try shutting down a connection before giving up",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_5,
                                          MCA_BASE_VAR_SCOPE_LOCAL,
                                          &mca_oob_tcp_component.max_retries);

    mca_oob_tcp_component.tcp_sndbuf = 0;
    (void)mca_base_component_var_register(component, "sndbuf",
                                          "TCP socket send buffering size (in bytes, 0 => leave system default)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_LOCAL,
                                          &mca_oob_tcp_component.tcp_sndbuf);

    mca_oob_tcp_component.tcp_rcvbuf = 0;
    (void)mca_base_component_var_register(component, "rcvbuf",
                                          "TCP socket receive buffering size (in bytes, 0 => leave system default)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_LOCAL,
                                          &mca_oob_tcp_component.tcp_rcvbuf);

    mca_oob_tcp_component.if_include = NULL;
    var_id = mca_base_component_var_register(component, "if_include",
                                             "Comma-delimited list of devices and/or CIDR notation of TCP networks to use for Open MPI bootstrap communication (e.g., \"eth0,192.168.0.0/16\").  Mutually exclusive with oob_tcp_if_exclude.",
                                             MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                             OPAL_INFO_LVL_2,
                                             MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_oob_tcp_component.if_include);
    (void)mca_base_var_register_synonym(var_id, "orte", "oob", "tcp", "include",
                                        MCA_BASE_VAR_SYN_FLAG_DEPRECATED | MCA_BASE_VAR_SYN_FLAG_INTERNAL);

    mca_oob_tcp_component.if_exclude = NULL;
    var_id = mca_base_component_var_register(component, "if_exclude",
                                             "Comma-delimited list of devices and/or CIDR notation of TCP networks to NOT use for Open MPI bootstrap communication -- all devices not matching these specifications will be used (e.g., \"eth0,192.168.0.0/16\").  If set to a non-default value, it is mutually exclusive with oob_tcp_if_include.",
                                             MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                             OPAL_INFO_LVL_2,
                                             MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_oob_tcp_component.if_exclude);
    (void)mca_base_var_register_synonym(var_id, "orte", "oob", "tcp", "exclude",
                                        MCA_BASE_VAR_SYN_FLAG_DEPRECATED | MCA_BASE_VAR_SYN_FLAG_INTERNAL);

    /* if_include and if_exclude need to be mutually exclusive */
    if (NULL != mca_oob_tcp_component.if_include &&
        NULL != mca_oob_tcp_component.if_exclude) {
        /* Return ERR_NOT_AVAILABLE so that a warning message about
           "open" failing is not printed */
        orte_show_help("help-oob-tcp.txt", "include-exclude", true,
                       mca_oob_tcp_component.if_include,
                       mca_oob_tcp_component.if_exclude);
        return ORTE_ERR_NOT_AVAILABLE;
    }

    static_port_string = NULL;
    (void)mca_base_component_var_register(component, "static_ipv4_ports",
                                          "Static ports for daemons and procs (IPv4)",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_2,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &static_port_string);

    /* if ports were provided, parse the provided range */
    if (NULL != static_port_string) {
        orte_util_parse_range_options(static_port_string, &mca_oob_tcp_component.tcp_static_ports);
        if (0 == strcmp(mca_oob_tcp_component.tcp_static_ports[0], "-1")) {
            opal_argv_free(mca_oob_tcp_component.tcp_static_ports);
            mca_oob_tcp_component.tcp_static_ports = NULL;
        }
    } else {
        mca_oob_tcp_component.tcp_static_ports = NULL;
    }

#if OPAL_ENABLE_IPV6
    static_port_string6 = NULL;
    (void)mca_base_component_var_register(component, "static_ipv6_ports",
                                          "Static ports for daemons and procs (IPv6)",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_2,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &static_port_string6);

    /* if ports were provided, parse the provided range */
    if (NULL != static_port_string6) {
        orte_util_parse_range_options(static_port_string6, &mca_oob_tcp_component.tcp6_static_ports);
        if (0 == strcmp(mca_oob_tcp_component.tcp6_static_ports[0], "-1")) {
            opal_argv_free(mca_oob_tcp_component.tcp6_static_ports);
            mca_oob_tcp_component.tcp6_static_ports = NULL;
        }
    } else {
        mca_oob_tcp_component.tcp6_static_ports = NULL;
    }
#endif // OPAL_ENABLE_IPV6

    if (NULL != mca_oob_tcp_component.tcp_static_ports ||
        NULL != mca_oob_tcp_component.tcp6_static_ports) {
        /* can't fwd mpirun port _and_ have static ports */
        if (ORTE_PROC_IS_HNP && orte_fwd_mpirun_port) {
            orte_show_help("help-oob-tcp.txt", "static-fwd", true);
            return ORTE_ERR_NOT_AVAILABLE;
        }
        orte_static_ports = true;
    }

    dyn_port_string = NULL;
    (void)mca_base_component_var_register(component, "dynamic_ipv4_ports",
                                          "Range of ports to be dynamically used by daemons and procs (IPv4)",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &dyn_port_string);
    /* if ports were provided, parse the provided range */
    if (NULL != dyn_port_string) {
        /* can't have both static and dynamic ports! */
        if (orte_static_ports) {
            char *err = opal_argv_join(mca_oob_tcp_component.tcp_static_ports, ',');
            opal_show_help("help-oob-tcp.txt", "static-and-dynamic", true,
                           err, dyn_port_string);
            free(err);
            return ORTE_ERROR;
        }
        orte_util_parse_range_options(dyn_port_string, &mca_oob_tcp_component.tcp_dyn_ports);
        if (0 == strcmp(mca_oob_tcp_component.tcp_dyn_ports[0], "-1")) {
            opal_argv_free(mca_oob_tcp_component.tcp_dyn_ports);
            mca_oob_tcp_component.tcp_dyn_ports = NULL;
        }
    } else {
        mca_oob_tcp_component.tcp_dyn_ports = NULL;
    }

#if OPAL_ENABLE_IPV6
    dyn_port_string6 = NULL;
    (void)mca_base_component_var_register(component, "dynamic_ipv6_ports",
                                          "Range of ports to be dynamically used by daemons and procs (IPv6)",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &dyn_port_string6);
    /* if ports were provided, parse the provided range */
    if (NULL != dyn_port_string6) {
        /* can't have both static and dynamic ports! */
        if (orte_static_ports) {
            char *err4=NULL, *err6=NULL;
            if (NULL != mca_oob_tcp_component.tcp_static_ports) {
                err4 = opal_argv_join(mca_oob_tcp_component.tcp_static_ports, ',');
            }
            if (NULL != mca_oob_tcp_component.tcp6_static_ports) {
                err6 = opal_argv_join(mca_oob_tcp_component.tcp6_static_ports, ',');
            }
            opal_show_help("help-oob-tcp.txt", "static-and-dynamic-ipv6", true,
                           (NULL == err4) ? "N/A" : err4,
                           (NULL == err6) ? "N/A" : err6,
                           dyn_port_string6);
            if (NULL != err4) {
                free(err4);
            }
            if (NULL != err6) {
                free(err6);
            }
            return ORTE_ERROR;
        }
        orte_util_parse_range_options(dyn_port_string6, &mca_oob_tcp_component.tcp6_dyn_ports);
        if (0 == strcmp(mca_oob_tcp_component.tcp6_dyn_ports[0], "-1")) {
            opal_argv_free(mca_oob_tcp_component.tcp6_dyn_ports);
            mca_oob_tcp_component.tcp6_dyn_ports = NULL;
        }
    } else {
        mca_oob_tcp_component.tcp6_dyn_ports = NULL;
    }
#endif // OPAL_ENABLE_IPV6

    mca_oob_tcp_component.disable_ipv4_family = false;
    (void)mca_base_component_var_register(component, "disable_ipv4_family",
                                          "Disable the IPv4 interfaces",
                                          MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.disable_ipv4_family);

#if OPAL_ENABLE_IPV6
    mca_oob_tcp_component.disable_ipv6_family = false;
    (void)mca_base_component_var_register(component, "disable_ipv6_family",
                                          "Disable the IPv6 interfaces",
                                          MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.disable_ipv6_family);
#endif // OPAL_ENABLE_IPV6

    // Wait for this amount of time before sending the first keepalive probe
    mca_oob_tcp_component.keepalive_time = 300;
    (void)mca_base_component_var_register(component, "keepalive_time",
                                          "Idle time in seconds before starting to send keepalives (keepalive_time <= 0 disables keepalive functionality)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_5,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.keepalive_time);

    // Resend keepalive probe every INT seconds
    mca_oob_tcp_component.keepalive_intvl = 20;
    (void)mca_base_component_var_register(component, "keepalive_intvl",
                                          "Time between successive keepalive pings when peer has not responded, in seconds (ignored if keepalive_time <= 0)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_5,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.keepalive_intvl);

    // After sending PR probes every INT seconds consider the connection dead
    mca_oob_tcp_component.keepalive_probes = 9;
    (void)mca_base_component_var_register(component, "keepalive_probes",
                                          "Number of keepalives that can be missed before declaring error (ignored if keepalive_time <= 0)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_5,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.keepalive_probes);

    mca_oob_tcp_component.retry_delay = 0;
    (void)mca_base_component_var_register(component, "retry_delay",
                                          "Time (in sec) to wait before trying to connect to peer again",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.retry_delay);

    mca_oob_tcp_component.max_recon_attempts = 10;
    (void)mca_base_component_var_register(component, "max_recon_attempts",
                                          "Max number of times to attempt connection before giving up (-1 -> never give up)",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_4,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_oob_tcp_component.max_recon_attempts);

    return ORTE_SUCCESS;
}


static char **split_and_resolve(char **orig_str, char *name);

static int component_available(void)
{
    int i, rc;
    char **interfaces = NULL;
    bool including = false, excluding = false;
    char name[32];
    struct sockaddr_storage my_ss;
    int kindex;

    opal_output_verbose(5, orte_oob_base_framework.framework_output,
                        "oob:tcp: component_available called");

    /* if interface include was given, construct a list
     * of those interfaces which match the specifications - remember,
     * the includes could be given as named interfaces, IP addrs, or
     * subnet+mask
     */
    if (NULL != mca_oob_tcp_component.if_include) {
        interfaces = split_and_resolve(&mca_oob_tcp_component.if_include,
                                       "include");
        including = true;
        excluding = false;
    } else if (NULL != mca_oob_tcp_component.if_exclude) {
        interfaces = split_and_resolve(&mca_oob_tcp_component.if_exclude,
                                       "exclude");
        including = false;
        excluding = true;
    }

    /* look at all available interfaces */
    for (i = opal_ifbegin(); i >= 0; i = opal_ifnext(i)) {
        if (OPAL_SUCCESS != opal_ifindextoaddr(i, (struct sockaddr*) &my_ss,
                                               sizeof (my_ss))) {
            opal_output (0, "oob_tcp: problems getting address for index %i (kernel index %i)\n",
                         i, opal_ifindextokindex(i));
            continue;
        }
        /* ignore non-ip4/6 interfaces */
        if (AF_INET != my_ss.ss_family
#if OPAL_ENABLE_IPV6
            && AF_INET6 != my_ss.ss_family
#endif
            ) {
            continue;
        }
        kindex = opal_ifindextokindex(i);
        if (kindex <= 0) {
            continue;
        }
        opal_output_verbose(10, orte_oob_base_framework.framework_output,
                            "WORKING INTERFACE %d KERNEL INDEX %d FAMILY: %s", i, kindex,
                            (AF_INET == my_ss.ss_family) ? "V4" : "V6");

        /* get the name for diagnostic purposes */
        opal_ifindextoname(i, name, sizeof(name));

        /* ignore any virtual interfaces */
        if (0 == strncmp(name, "vir", 3)) {
            continue;
        }

        /* handle include/exclude directives */
        if (NULL != interfaces) {
            /* check for match */
            rc = opal_ifmatches(kindex, interfaces);
            /* if one of the network specifications isn't parseable, then
             * error out as we can't do what was requested
             */
            if (OPAL_ERR_NETWORK_NOT_PARSEABLE == rc) {
                orte_show_help("help-oob-tcp.txt", "not-parseable", true);
                opal_argv_free(interfaces);
                return ORTE_ERR_BAD_PARAM;
            }
            /* if we are including, then ignore this if not present */
            if (including) {
                if (OPAL_SUCCESS != rc) {
                    opal_output_verbose(20, orte_oob_base_framework.framework_output,
                                        "%s oob:tcp:init rejecting interface %s (not in include list)",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), name);
                    continue;
                }
            } else {
                /* we are excluding, so ignore if present */
                if (OPAL_SUCCESS == rc) {
                    opal_output_verbose(20, orte_oob_base_framework.framework_output,
                                        "%s oob:tcp:init rejecting interface %s (in exclude list)",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), name);
                    continue;
                }
            }
        } else {
            /* if no specific interfaces were provided, we ignore the loopback
             * interface unless nothing else is available
             */
            if (1 < opal_ifcount() && opal_ifisloopback(i)) {
                opal_output_verbose(20, orte_oob_base_framework.framework_output,
                                    "%s oob:tcp:init rejecting loopback interface %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), name);
                continue;
            }
        }

        /* Refs ticket #3019
         * it would probably be worthwhile to print out a warning if OMPI detects multiple
         * IP interfaces that are "up" on the same subnet (because that's a Bad Idea). Note
         * that we should only check for this after applying the relevant include/exclude
         * list MCA params. If we detect redundant ports, we can also automatically ignore
         * them so that applications won't hang.
         */

        /* add this address to our connections */
        if (AF_INET == my_ss.ss_family) {
            opal_output_verbose(10, orte_oob_base_framework.framework_output,
                                "%s oob:tcp:init adding %s to our list of %s connections",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                opal_net_get_hostname((struct sockaddr*) &my_ss),
                                (AF_INET == my_ss.ss_family) ? "V4" : "V6");
            opal_argv_append_nosize(&mca_oob_tcp_component.ipv4conns, opal_net_get_hostname((struct sockaddr*) &my_ss));
        } else if (AF_INET6 == my_ss.ss_family) {
#if OPAL_ENABLE_IPV6
            opal_output_verbose(10, orte_oob_base_framework.framework_output,
                                "%s oob:tcp:init adding %s to our list of %s connections",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                opal_net_get_hostname((struct sockaddr*) &my_ss),
                                (AF_INET == my_ss.ss_family) ? "V4" : "V6");
            opal_argv_append_nosize(&mca_oob_tcp_component.ipv6conns, opal_net_get_hostname((struct sockaddr*) &my_ss));
#endif // OPAL_ENABLE_IPV6
        } else {
            opal_output_verbose(10, orte_oob_base_framework.framework_output,
                                "%s oob:tcp:init ignoring %s from out list of connections",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                opal_net_get_hostname((struct sockaddr*) &my_ss));
        }
    }

    /* cleanup */
    if (NULL != interfaces) {
        opal_argv_free(interfaces);
    }

    if (0 == opal_argv_count(mca_oob_tcp_component.ipv4conns)
#if OPAL_ENABLE_IPV6
        && 0 == opal_argv_count(mca_oob_tcp_component.ipv6conns)
#endif
        ) {
        if (including) {
            orte_show_help("help-oob-tcp.txt", "no-included-found", true, mca_oob_tcp_component.if_include);
        } else if (excluding) {
            orte_show_help("help-oob-tcp.txt", "excluded-all", true, mca_oob_tcp_component.if_exclude);
        }
        return ORTE_ERR_NOT_AVAILABLE;
    }

    return ORTE_SUCCESS;
}

static orte_rml_pathway_t* component_query_transports(void)
{
    orte_rml_pathway_t *p;
    char *qual;

    /* if neither IPv4 or IPv6 connections are available, then
     * we have nothing to support */
    if (NULL == mca_oob_tcp_component.ipv4conns &&
        NULL == mca_oob_tcp_component.ipv6conns) {
        return NULL;
    }

    /* if we get here, then we support Ethernet and TCP */
    p = OBJ_NEW(orte_rml_pathway_t);
    p->component = strdup("oob");
    orte_set_attribute(&p->attributes, ORTE_RML_TRANSPORT_TYPE, ORTE_ATTR_LOCAL, "Ethernet", OPAL_STRING);
    orte_set_attribute(&p->attributes, ORTE_RML_PROTOCOL_TYPE, ORTE_ATTR_LOCAL, "TCP", OPAL_STRING);
    /* setup our qualifiers - we route communications, may have IPv4 and/or IPv6, etc. */
    if (NULL != mca_oob_tcp_component.ipv4conns &&
        NULL != mca_oob_tcp_component.ipv6conns) {
        qual = "routed=true:ipv4:ipv6";
    } else if (NULL == mca_oob_tcp_component.ipv6conns) {
        qual = "routed=true:ipv4";
    } else {
        qual = "routed=true:ipv6";
    }
    orte_set_attribute(&p->attributes, ORTE_RML_QUALIFIER_ATTRIB, ORTE_ATTR_LOCAL, qual, OPAL_STRING);

    return p;
}

/* Start all modules */
static int component_startup(void)
{
    int rc = ORTE_SUCCESS;
    int i;
    char *tmp;
    opal_event_base_t *evb;

    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "%s TCP STARTUP",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* initialize state */
    if (0 == orte_oob_base.num_threads) {
        opal_pointer_array_add(&mca_oob_tcp_component.ev_bases, orte_oob_base.ev_base);
    } else {
        for (i=0; i < orte_oob_base.num_threads; i++) {
            asprintf(&tmp, "OOB-TCP-%d", i);
            evb = opal_progress_thread_init(tmp);
            opal_pointer_array_add(&mca_oob_tcp_component.ev_bases, evb);
            opal_argv_append_nosize(&mca_oob_tcp_component.ev_threads, tmp);
            free(tmp);
        }
    }

    /* if we are a daemon/HNP, or we are a standalone app,
     * then it is possible that someone else may initiate a
     * connection to us. In these cases, we need to start the
     * listening thread/event. Otherwise, we will be the one
     * initiating communication, and there is no need for
     * a listener */
    if (ORTE_PROC_IS_HNP || ORTE_PROC_IS_DAEMON ||
        orte_standalone_operation) {
        if (ORTE_SUCCESS != (rc = orte_oob_tcp_start_listening())) {
            ORTE_ERROR_LOG(rc);
        }
    }

    return rc;
}

static void component_shutdown(void)
{
    mca_oob_tcp_peer_t *peer;
    int i = 0, rc;
    uint64_t key;
    void *node;

    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "%s TCP SHUTDOWN",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    if (0 < orte_oob_base.num_threads) {
        for (i=0; i < orte_oob_base.num_threads; i++) {
            opal_progress_thread_finalize(mca_oob_tcp_component.ev_threads[i]);
            opal_pointer_array_set_item(&mca_oob_tcp_component.ev_bases, i, NULL);
        }
        opal_argv_free(mca_oob_tcp_component.ev_threads);
    }

    if (ORTE_PROC_IS_HNP && mca_oob_tcp_component.listen_thread_active) {
        mca_oob_tcp_component.listen_thread_active = false;
        /* tell the thread to exit */
        write(mca_oob_tcp_component.stop_thread[1], &i, sizeof(int));
        opal_thread_join(&mca_oob_tcp_component.listen_thread, NULL);
    } else {
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "no hnp or not active");
    }

    /* release all peers from the hash table */
    rc = opal_hash_table_get_first_key_uint64(&mca_oob_tcp_component.peers, &key,
                                              (void **)&peer, &node);
    while (OPAL_SUCCESS == rc) {
        if (NULL != peer) {
            OBJ_RELEASE(peer);
            rc = opal_hash_table_set_value_uint64(&mca_oob_tcp_component.peers, key, NULL);
            if (OPAL_SUCCESS != rc) {
                ORTE_ERROR_LOG(rc);
            }
        }
        rc = opal_hash_table_get_next_key_uint64(&mca_oob_tcp_component.peers, &key,
                                                 (void **) &peer, node, &node);
    }

    /* cleanup listen event list */
    OPAL_LIST_DESTRUCT(&mca_oob_tcp_component.listeners);

    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "%s TCP SHUTDOWN done",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
}

static int component_send(orte_rml_send_t *msg)
{
    opal_output_verbose(5, orte_oob_base_framework.framework_output,
                        "%s oob:tcp:send_nb to peer %s:%d seq = %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&msg->dst), msg->tag, msg->seq_num );

    /* The module will first see if it knows
     * of a way to send the data to the target, and then
     * attempt to send the data. It  will call the cbfunc
     * with the status upon completion - if it can't do it for
     * some reason, it will pass the error to our fn below so
     * it can do something about it
     */
    mca_oob_tcp_module.send_nb(msg);
    return ORTE_SUCCESS;
}

static char* component_get_addr(void)
{
    char *cptr=NULL, *tmp, *tp;

    if (!mca_oob_tcp_component.disable_ipv4_family &&
        NULL != mca_oob_tcp_component.ipv4conns) {
        tmp = opal_argv_join(mca_oob_tcp_component.ipv4conns, ',');
        tp = opal_argv_join(mca_oob_tcp_component.ipv4ports, ',');
        asprintf(&cptr, "tcp://%s:%s", tmp, tp);
        free(tmp);
        free(tp);
    }
#if OPAL_ENABLE_IPV6
    if (!mca_oob_tcp_component.disable_ipv6_family &&
        NULL != mca_oob_tcp_component.ipv6conns) {
        char *tmp2;

        /* Fixes #2498
         * RFC 3986, section 3.2.2
         * The notation in that case is to encode the IPv6 IP number in square brackets:
         * "http://[2001:db8:1f70::999:de8:7648:6e8]:100/"
         * A host identified by an Internet Protocol literal address, version 6 [RFC3513]
         * or later, is distinguished by enclosing the IP literal within square brackets.
         * This is the only place where square bracket characters are allowed in the URI
         * syntax. In anticipation of future, as-yet-undefined IP literal address formats,
         * an implementation may use an optional version flag to indicate such a format
         * explicitly rather than rely on heuristic determination.
         */
        tmp = opal_argv_join(mca_oob_tcp_component.ipv6conns, ',');
        tp = opal_argv_join(mca_oob_tcp_component.ipv6ports, ',');
        if (NULL == cptr) {
            /* no ipv4 stuff */
            asprintf(&cptr, "tcp6://[%s]:%s", tmp, tp);
        } else {
            asprintf(&tmp2, "%s;tcp6://[%s]:%s", cptr, tmp, tp);
            free(cptr);
            cptr = tmp2;
        }
        free(tmp);
        free(tp);
    }
#endif // OPAL_ENABLE_IPV6

    /* return our uri */
    return cptr;
}

/* the host in this case is always in "dot" notation, and
 * thus we do not need to do a DNS lookup to convert it */
static int parse_uri(const uint16_t af_family,
                     const char* host,
                     const char *port,
                     struct sockaddr_storage* inaddr)
{
    struct sockaddr_in *in;

    if (AF_INET == af_family) {
        memset(inaddr, 0, sizeof(struct sockaddr_in));
        in = (struct sockaddr_in*) inaddr;
        in->sin_family = AF_INET;
        in->sin_addr.s_addr = inet_addr(host);
        if (in->sin_addr.s_addr == INADDR_NONE) {
            return ORTE_ERR_BAD_PARAM;
        }
        ((struct sockaddr_in*) inaddr)->sin_port = htons(atoi(port));
    }
#if OPAL_ENABLE_IPV6
    else if (AF_INET6 == af_family) {
        struct sockaddr_in6 *in6;
        memset(inaddr, 0, sizeof(struct sockaddr_in6));
        in6 = (struct sockaddr_in6*) inaddr;

        if (0 == inet_pton(AF_INET6, host, (void*)&in6->sin6_addr)) {
            opal_output (0, "oob_tcp_parse_uri: Could not convert %s\n", host);
            return ORTE_ERR_BAD_PARAM;
        }
        in6->sin6_family = AF_INET6;
        in6->sin6_port =  htons(atoi(port));
    }
#endif
    else {
        return ORTE_ERR_NOT_SUPPORTED;
    }
    return ORTE_SUCCESS;
}

static int component_set_addr(orte_process_name_t *peer,
                              char **uris)
{
    char **addrs, *hptr;
    char *tcpuri=NULL, *host, *ports;
    int i, j, rc;
    uint16_t af_family = AF_UNSPEC;
    uint64_t ui64;
    bool found;
    mca_oob_tcp_peer_t *pr;
    mca_oob_tcp_addr_t *maddr;

    memcpy(&ui64, (char*)peer, sizeof(uint64_t));
    /* cycle across component parts and see if one belongs to us */
    found = false;

    for (i=0; NULL != uris[i]; i++) {
        tcpuri = strdup(uris[i]);
        if (NULL == tcpuri) {
            opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                "%s oob:tcp: out of memory",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            continue;
        }
        if (0 == strncmp(uris[i], "tcp:", 4)) {
            af_family = AF_INET;
            host = tcpuri + strlen("tcp://");
        } else if (0 == strncmp(uris[i], "tcp6:", 5)) {
#if OPAL_ENABLE_IPV6
            af_family = AF_INET6;
            host = tcpuri + strlen("tcp6://");
#else // OPAL_ENABLE_IPV6
            /* we don't support this connection type */
            opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                "%s oob:tcp: address %s not supported",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), uris[i]);
            free(tcpuri);
            continue;
#endif // OPAL_ENABLE_IPV6
        } else {
            /* not one of ours */
            opal_output_verbose(2, orte_oob_base_framework.framework_output,
                                "%s oob:tcp: ignoring address %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), uris[i]);
            free(tcpuri);
            continue;
        }

        /* this one is ours - record the peer */
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s oob:tcp: working peer %s address %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(peer), uris[i]);
        /* separate the ports from the network addrs */
        ports = strrchr(tcpuri, ':');
        if (NULL == ports) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            free(tcpuri);
            continue;
        }
        *ports = '\0';
        ports++;

        /* split the addrs */
        /* if this is a tcp6 connection, the first one will have a '['
         * at the beginning of it, and the last will have a ']' at the
         * end - we need to remove those extra characters
         */
        hptr = host;
#if OPAL_ENABLE_IPV6
        if (AF_INET6 == af_family) {
            if ('[' == host[0]) {
                hptr = &host[1];
            }
            if (']' == host[strlen(host)-1]) {
                host[strlen(host)-1] = '\0';
            }
        }
#endif // OPAL_ENABLE_IPV6
        addrs = opal_argv_split(hptr, ',');


        /* cycle across the provided addrs */
        for (j=0; NULL != addrs[j]; j++) {
            /* if they gave us "localhost", then just take the first conn on our list */
            if (0 == strcasecmp(addrs[j], "localhost")) {
#if OPAL_ENABLE_IPV6
                if (AF_INET6 == af_family) {
                    if (NULL == mca_oob_tcp_component.ipv6conns ||
                        NULL == mca_oob_tcp_component.ipv6conns[0]) {
                        continue;
                    }
                    host = mca_oob_tcp_component.ipv6conns[0];
                } else {
#endif // OPAL_ENABLE_IPV6
                    if (NULL == mca_oob_tcp_component.ipv4conns ||
                        NULL == mca_oob_tcp_component.ipv4conns[0]) {
                        continue;
                    }
                    host = mca_oob_tcp_component.ipv4conns[0];
#if OPAL_ENABLE_IPV6
                }
#endif
            } else {
                host = addrs[j];
            }

            if (NULL == (pr = mca_oob_tcp_peer_lookup(peer))) {
                pr = OBJ_NEW(mca_oob_tcp_peer_t);
                pr->name.jobid = peer->jobid;
                pr->name.vpid = peer->vpid;
                opal_output_verbose(20, orte_oob_base_framework.framework_output,
                                    "%s SET_PEER ADDING PEER %s",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    ORTE_NAME_PRINT(peer));
                if (OPAL_SUCCESS != opal_hash_table_set_value_uint64(&mca_oob_tcp_component.peers, ui64, pr)) {
                    OBJ_RELEASE(pr);
                    return ORTE_ERR_TAKE_NEXT_OPTION;
                }
            }

            maddr = OBJ_NEW(mca_oob_tcp_addr_t);
            ((struct sockaddr_storage*) &(maddr->addr))->ss_family = af_family;
            if (ORTE_SUCCESS != (rc = parse_uri(af_family, host, ports, (struct sockaddr_storage*) &(maddr->addr)))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(maddr);
                rc = opal_hash_table_set_value_uint64(&mca_oob_tcp_component.peers, ui64, NULL);
                if (ORTE_SUCCESS != rc) {
                    ORTE_ERROR_LOG(rc);
                }
                OBJ_RELEASE(pr);
                return ORTE_ERR_TAKE_NEXT_OPTION;
            }

            opal_output_verbose(20, orte_oob_base_framework.framework_output,
                                "%s set_peer: peer %s is listening on net %s port %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(peer),
                                (NULL == host) ? "NULL" : host,
                                (NULL == ports) ? "NULL" : ports);
            opal_list_append(&pr->addrs, &maddr->super);

            found = true;
        }
        opal_argv_free(addrs);
        free(tcpuri);
    }
    if (found) {
        /* indicate that this peer is addressable by this component */
        return ORTE_SUCCESS;
    }

    /* otherwise indicate that it is not addressable by us */
    return ORTE_ERR_TAKE_NEXT_OPTION;
}

static bool component_is_reachable(char *rtmod, orte_process_name_t *peer)
{
    orte_process_name_t hop;

    /* if we have a route to this peer, then we can reach it */
    hop = orte_routed.get_route(rtmod, peer);
    if (ORTE_JOBID_INVALID == hop.jobid ||
        ORTE_VPID_INVALID == hop.vpid) {
        opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                            "%s is NOT reachable by TCP",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return false;
    }
    /* assume we can reach the hop - the module will tell us if it can't
     * when we try to send the first time, and then we'll correct it */
    return true;
}

#if OPAL_ENABLE_FT_CR == 1
static int component_ft_event(int state)
{
    opal_output_verbose(2, orte_oob_base_framework.framework_output,
                        "%s TCP FT EVENT", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    /* pass it into the module */
    if (NULL != mca_oob_tcp_module.api.ft_event) {
        mca_oob_tcp_module.api.ft_event(state);
    }

    return ORTE_SUCCESS;
}
#endif // OPAL_ENABLE_FT_CR

void mca_oob_tcp_component_set_module(int fd, short args, void *cbdata)
{
    mca_oob_tcp_peer_op_t *pop = (mca_oob_tcp_peer_op_t*)cbdata;
    uint64_t ui64;
    int rc;
    orte_oob_base_peer_t *bpr;

    ORTE_ACQUIRE_OBJECT(pop);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:set_module called for peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&pop->peer));

    /* make sure the OOB knows that we can reach this peer - we
     * are in the same event base as the OOB base, so we can
     * directly access its storage
     */
    memcpy(&ui64, (char*)&pop->peer, sizeof(uint64_t));
    if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&bpr) || NULL == bpr) {
        bpr = OBJ_NEW(orte_oob_base_peer_t);
    }
    opal_bitmap_set_bit(&bpr->addressable, mca_oob_tcp_component.super.idx);
    bpr->component = &mca_oob_tcp_component.super;
    if (OPAL_SUCCESS != (rc = opal_hash_table_set_value_uint64(&orte_oob_base.peers,
                                                               ui64, bpr))) {
        ORTE_ERROR_LOG(rc);
    }

    OBJ_RELEASE(pop);
}

void mca_oob_tcp_component_lost_connection(int fd, short args, void *cbdata)
{
    mca_oob_tcp_peer_op_t *pop = (mca_oob_tcp_peer_op_t*)cbdata;
    uint64_t ui64;
    orte_oob_base_peer_t *bpr;
    int rc;

    ORTE_ACQUIRE_OBJECT(pop);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:lost connection called for peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&pop->peer));

    /* Mark that we no longer support this peer */
    memcpy(&ui64, (char*)&pop->peer, sizeof(uint64_t));
    if (OPAL_SUCCESS == opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&bpr) && NULL != bpr) {
        opal_bitmap_clear_bit(&bpr->addressable, mca_oob_tcp_component.super.idx);
        OBJ_RELEASE(bpr);
    }
    if (OPAL_SUCCESS != (rc = opal_hash_table_set_value_uint64(&orte_oob_base.peers,
                                                               ui64, NULL))) {
        ORTE_ERROR_LOG(rc);
    }

    if (!orte_finalizing) {
        /* activate the proc state */
        if (ORTE_SUCCESS != orte_routed.route_lost(pop->rtmod, &pop->peer)) {
            ORTE_ACTIVATE_PROC_STATE(&pop->peer, ORTE_PROC_STATE_LIFELINE_LOST);
        } else {
            ORTE_ACTIVATE_PROC_STATE(&pop->peer, ORTE_PROC_STATE_COMM_FAILED);
        }
    }
    OBJ_RELEASE(pop);
}

void mca_oob_tcp_component_no_route(int fd, short args, void *cbdata)
{
    mca_oob_tcp_msg_error_t *mop = (mca_oob_tcp_msg_error_t*)cbdata;
    uint64_t ui64;
    int rc;
    orte_oob_base_peer_t *bpr;

    ORTE_ACQUIRE_OBJECT(mop);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:no route called for peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&mop->hop));

    /* mark that we cannot reach this hop */
    memcpy(&ui64, (char*)&(mop->hop), sizeof(uint64_t));
    if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&bpr) || NULL == bpr) {
        bpr = OBJ_NEW(orte_oob_base_peer_t);
    }
    opal_bitmap_clear_bit(&bpr->addressable, mca_oob_tcp_component.super.idx);
    if (OPAL_SUCCESS != (rc = opal_hash_table_set_value_uint64(&orte_oob_base.peers,
                                                               ui64, NULL))) {
        ORTE_ERROR_LOG(rc);
    }

    /* report the error back to the OOB and let it try other components
     * or declare a problem
     */
    mop->rmsg->retries++;
    /* activate the OOB send state */
    ORTE_OOB_SEND(mop->rmsg);

    OBJ_RELEASE(mop);
}

void mca_oob_tcp_component_hop_unknown(int fd, short args, void *cbdata)
{
    mca_oob_tcp_msg_error_t *mop = (mca_oob_tcp_msg_error_t*)cbdata;
    uint64_t ui64;
    orte_rml_send_t *snd;
    orte_oob_base_peer_t *bpr;

    ORTE_ACQUIRE_OBJECT(mop);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:unknown hop called for peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&mop->hop));

    if (orte_finalizing || orte_abnormal_term_ordered) {
        /* just ignore the problem */
        OBJ_RELEASE(mop);
        return;
    }

   /* mark that this component cannot reach this hop */
    memcpy(&ui64, (char*)&(mop->hop), sizeof(uint64_t));
    if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&bpr) ||
        NULL == bpr) {
        /* the overall OOB has no knowledge of this hop. Only
         * way this could happen is if the peer contacted us
         * via this component, and it wasn't entered into the
         * OOB framework hash table. We have no way of knowing
         * what to do next, so just output an error message and
         * abort */
        opal_output(0, "%s ERROR: message to %s requires routing and the OOB has no knowledge of the reqd hop %s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_NAME_PRINT(&mop->snd->hdr.dst),
                    ORTE_NAME_PRINT(&mop->hop));
        ORTE_ACTIVATE_PROC_STATE(&mop->hop, ORTE_PROC_STATE_UNABLE_TO_SEND_MSG);
        OBJ_RELEASE(mop);
        return;
    }
    opal_bitmap_clear_bit(&bpr->addressable, mca_oob_tcp_component.super.idx);

    /* mark that this component cannot reach this destination either */
    memcpy(&ui64, (char*)&(mop->snd->hdr.dst), sizeof(uint64_t));
    if (OPAL_SUCCESS != opal_hash_table_get_value_uint64(&orte_oob_base.peers,
                                                         ui64, (void**)&bpr) ||
        NULL == bpr) {
        opal_output(0, "%s ERROR: message to %s requires routing and the OOB has no knowledge of this process",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_NAME_PRINT(&mop->snd->hdr.dst));
        ORTE_ACTIVATE_PROC_STATE(&mop->hop, ORTE_PROC_STATE_UNABLE_TO_SEND_MSG);
        OBJ_RELEASE(mop);
        return;
    }
    opal_bitmap_clear_bit(&bpr->addressable, mca_oob_tcp_component.super.idx);

    /* post the message to the OOB so it can see
     * if another component can transfer it
     */
    MCA_OOB_TCP_HDR_NTOH(&mop->snd->hdr);
    snd = OBJ_NEW(orte_rml_send_t);
    snd->retries = mop->rmsg->retries + 1;
    snd->dst = mop->snd->hdr.dst;
    snd->origin = mop->snd->hdr.origin;
    snd->tag = mop->snd->hdr.tag;
    snd->seq_num = mop->snd->hdr.seq_num;
    snd->data = mop->snd->data;
    snd->count = mop->snd->hdr.nbytes;
    snd->cbfunc.iov = NULL;
    snd->cbdata = NULL;
    snd->routed = strdup(mop->snd->hdr.routed);
    /* activate the OOB send state */
    ORTE_OOB_SEND(snd);
    /* protect the data */
    mop->snd->data = NULL;

    OBJ_RELEASE(mop);
}

void mca_oob_tcp_component_failed_to_connect(int fd, short args, void *cbdata)
{
    mca_oob_tcp_peer_op_t *pop = (mca_oob_tcp_peer_op_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(pop);

    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:failed_to_connect called for peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&pop->peer));

   /* if we are terminating, then don't attempt to reconnect */
    if (orte_orteds_term_ordered || orte_finalizing || orte_abnormal_term_ordered) {
        OBJ_RELEASE(pop);
        return;
    }

    /* activate the proc state */
    opal_output_verbose(OOB_TCP_DEBUG_CONNECT, orte_oob_base_framework.framework_output,
                        "%s tcp:failed_to_connect unable to reach peer %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&pop->peer));

    ORTE_ACTIVATE_PROC_STATE(&pop->peer, ORTE_PROC_STATE_FAILED_TO_CONNECT);
    OBJ_RELEASE(pop);
}

/*
 * Go through a list of argv; if there are any subnet specifications
 * (a.b.c.d/e), resolve them to an interface name (Currently only
 * supporting IPv4).  If unresolvable, warn and remove.
 */
static char **split_and_resolve(char **orig_str, char *name)
{
    int i, ret, save, if_index;
    char **argv, *str, *tmp;
    char if_name[IF_NAMESIZE];
    struct sockaddr_storage argv_inaddr, if_inaddr;
    uint32_t argv_prefix;

    /* Sanity check */
    if (NULL == orig_str || NULL == *orig_str) {
        return NULL;
    }

    argv = opal_argv_split(*orig_str, ',');
    if (NULL == argv) {
        return NULL;
    }
    for (save = i = 0; NULL != argv[i]; ++i) {
        if (isalpha(argv[i][0])) {
            argv[save++] = argv[i];
            continue;
        }

        /* Found a subnet notation.  Convert it to an IP
           address/netmask.  Get the prefix first. */
        argv_prefix = 0;
        tmp = strdup(argv[i]);
        str = strchr(argv[i], '/');
        if (NULL == str) {
            orte_show_help("help-oob-tcp.txt", "invalid if_inexclude",
                           true, name, orte_process_info.nodename,
                           tmp, "Invalid specification (missing \"/\")");
            free(argv[i]);
            free(tmp);
            continue;
        }
        *str = '\0';
        argv_prefix = atoi(str + 1);

        /* Now convert the IPv4 address */
        ((struct sockaddr*) &argv_inaddr)->sa_family = AF_INET;
        ret = inet_pton(AF_INET, argv[i],
                        &((struct sockaddr_in*) &argv_inaddr)->sin_addr);
        free(argv[i]);

        if (1 != ret) {
            orte_show_help("help-oob-tcp.txt", "invalid if_inexclude",
                           true, name, orte_process_info.nodename, tmp,
                           "Invalid specification (inet_pton() failed)");
            free(tmp);
            continue;
        }
        opal_output_verbose(20, orte_oob_base_framework.framework_output,
                            "%s oob:tcp: Searching for %s address+prefix: %s / %u",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            name,
                            opal_net_get_hostname((struct sockaddr*) &argv_inaddr),
                            argv_prefix);

        /* Go through all interfaces and see if we can find a match */
        for (if_index = opal_ifbegin(); if_index >= 0;
                           if_index = opal_ifnext(if_index)) {
            opal_ifindextoaddr(if_index,
                               (struct sockaddr*) &if_inaddr,
                               sizeof(if_inaddr));
            if (opal_net_samenetwork((struct sockaddr*) &argv_inaddr,
                                     (struct sockaddr*) &if_inaddr,
                                     argv_prefix)) {
                break;
            }
        }
        /* If we didn't find a match, keep trying */
        if (if_index < 0) {
            orte_show_help("help-oob-tcp.txt", "invalid if_inexclude",
                           true, name, orte_process_info.nodename, tmp,
                           "Did not find interface matching this subnet");
            free(tmp);
            continue;
        }

        /* We found a match; get the name and replace it in the
           argv */
        opal_ifindextoname(if_index, if_name, sizeof(if_name));
        opal_output_verbose(20, orte_oob_base_framework.framework_output,
                            "%s oob:tcp: Found match: %s (%s)",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            opal_net_get_hostname((struct sockaddr*) &if_inaddr),
                            if_name);
        argv[save++] = strdup(if_name);
        free(tmp);
    }

    /* The list may have been compressed if there were invalid
       entries, so ensure we end it with a NULL entry */
    argv[save] = NULL;
    free(*orig_str);
    *orig_str = opal_argv_join(argv, ',');
    return argv;
}

/* OOB TCP Class instances */

static void peer_cons(mca_oob_tcp_peer_t *peer)
{
    peer->ev_base = NULL;
    peer->auth_method = NULL;
    peer->sd = -1;
    OBJ_CONSTRUCT(&peer->addrs, opal_list_t);
    peer->active_addr = NULL;
    peer->state = MCA_OOB_TCP_UNCONNECTED;
    peer->num_retries = 0;
    OBJ_CONSTRUCT(&peer->send_queue, opal_list_t);
    peer->send_msg = NULL;
    peer->recv_msg = NULL;
    peer->send_ev_active = false;
    peer->recv_ev_active = false;
    peer->timer_ev_active = false;
}
static void peer_des(mca_oob_tcp_peer_t *peer)
{
    if (NULL != peer->auth_method) {
        free(peer->auth_method);
    }
    if (peer->send_ev_active) {
        opal_event_del(&peer->send_event);
    }
    if (peer->recv_ev_active) {
        opal_event_del(&peer->recv_event);
    }
    if (peer->timer_ev_active) {
        opal_event_del(&peer->timer_event);
    }
    if (0 <= peer->sd) {
        opal_output_verbose(2, orte_oob_base_framework.framework_output,
                            "%s CLOSING SOCKET %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            peer->sd);
        CLOSE_THE_SOCKET(peer->sd);
    }
    OPAL_LIST_DESTRUCT(&peer->addrs);
    OPAL_LIST_DESTRUCT(&peer->send_queue);
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_peer_t,
                   opal_list_item_t,
                   peer_cons, peer_des);

static void padd_cons(mca_oob_tcp_addr_t *ptr)
{
    memset(&ptr->addr, 0, sizeof(ptr->addr));
    ptr->retries = 0;
    ptr->state = MCA_OOB_TCP_UNCONNECTED;
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_addr_t,
                   opal_list_item_t,
                   padd_cons, NULL);


static void pop_cons(mca_oob_tcp_peer_op_t *pop)
{
    pop->rtmod = NULL;
    pop->net = NULL;
    pop->port = NULL;
}
static void pop_des(mca_oob_tcp_peer_op_t *pop)
{
    if (NULL != pop->rtmod) {
        free(pop->rtmod);
    }
    if (NULL != pop->net) {
        free(pop->net);
    }
    if (NULL != pop->port) {
        free(pop->port);
    }
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_peer_op_t,
                   opal_object_t,
                   pop_cons, pop_des);

OBJ_CLASS_INSTANCE(mca_oob_tcp_msg_op_t,
                   opal_object_t,
                   NULL, NULL);

OBJ_CLASS_INSTANCE(mca_oob_tcp_conn_op_t,
                   opal_object_t,
                   NULL, NULL);

static void nicaddr_cons(mca_oob_tcp_nicaddr_t *ptr)
{
    ptr->af_family = PF_UNSPEC;
    memset(&ptr->addr, 0, sizeof(ptr->addr));
}
OBJ_CLASS_INSTANCE(mca_oob_tcp_nicaddr_t,
                   opal_list_item_t,
                   nicaddr_cons, NULL);

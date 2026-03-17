/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennptlee and The University
 *                         of Tennptlee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#include <errno.h>

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/fd.h"
#include "src/util/show_help.h"
#include "src/util/strnlen.h"
#include "src/mca/bfrops/base/base.h"
#include "src/mca/gds/base/base.h"
#include "src/mca/psec/base/base.h"
#include "src/server/pmix_server_ops.h"

#include "src/mca/ptl/base/base.h"
#include "src/mca/ptl/usock/ptl_usock.h"

static pmix_status_t component_open(void);
static pmix_status_t component_close(void);
static int component_query(pmix_mca_base_module_t **module, int *priority);
static pmix_status_t setup_listener(pmix_info_t info[], size_t ninfo,
                                    bool *need_listener);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */
PMIX_EXPORT pmix_ptl_usock_component_t mca_ptl_usock_component = {
    .super = {
        .base = {
            PMIX_PTL_BASE_VERSION_1_0_0,

            /* Component name and version */
            .pmix_mca_component_name = "usock",
            PMIX_MCA_BASE_MAKE_VERSION(component,
                                       PMIX_MAJOR_VERSION,
                                       PMIX_MINOR_VERSION,
                                       PMIX_RELEASE_VERSION),

            /* Component open and close functions */
            .pmix_mca_open_component = component_open,
            .pmix_mca_close_component = component_close,
            .pmix_mca_query_component = component_query
        },
        .priority = 15,
        .uri = NULL,
        .setup_listener = setup_listener
    },
    .tmpdir = NULL,
    .filename = NULL
};

static void connection_handler(int sd, short args, void *cbdata);
static void listener_cb(int incoming_sd, void *cbdata);

pmix_status_t component_open(void)
{
    char *tdir;

    memset(&mca_ptl_usock_component.connection, 0, sizeof(mca_ptl_usock_component.connection));

    /* check for environ-based directives
     * on system tmpdir to use */
    if (NULL == (tdir = getenv("PMIX_SYSTEM_TMPDIR"))) {
        if (NULL == (tdir = getenv("TMPDIR"))) {
            if (NULL == (tdir = getenv("TEMP"))) {
                if (NULL == (tdir = getenv("TMP"))) {
                    tdir = "/tmp";
                }
            }
        }
    }
    if (NULL != tdir) {
        mca_ptl_usock_component.tmpdir = strdup(tdir);
    }

    return PMIX_SUCCESS;
}


pmix_status_t component_close(void)
{
    if (NULL != mca_ptl_usock_component.tmpdir) {
        free(mca_ptl_usock_component.tmpdir);
    }
    if (NULL != mca_ptl_usock_component.super.uri) {
        free(mca_ptl_usock_component.super.uri);
    }
    if (NULL != mca_ptl_usock_component.filename) {
        /* remove the file */
        unlink(mca_ptl_usock_component.filename);
        free(mca_ptl_usock_component.filename);
    }

    return PMIX_SUCCESS;
}

static int component_query(pmix_mca_base_module_t **module, int *priority)
{
    *module = (pmix_mca_base_module_t*)&pmix_ptl_usock_module;
    return PMIX_SUCCESS;
}

/* if we are the server, then we need to setup a usock rendezvous
 * point for legacy releases, but only do so if requested as some
 * systems may not wish to support older releases. The system can,
 * of course, simply use the MCA param method to disable this
 * component (PMIX_MCA_ptl=^usock), or can tell us to disqualify
 * ourselves using an info key to this API.
 *
 * NOTE: we accept MCA parameters, but info keys override them
 */
static pmix_status_t setup_listener(pmix_info_t info[], size_t ninfo,
                                    bool *need_listener)
{
    int flags;
    size_t n;
    pmix_listener_t *lt;
    pmix_status_t rc;
    socklen_t addrlen;
    struct sockaddr_un *address;
    bool disabled = false;
    char *pmix_pid;
    pid_t mypid;

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "ptl:usock setup_listener");

    /* if we are not a server, then we shouldn't be doing this */
    if (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* scan the info keys and process any override instructions */
    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (0 == strcmp(info[n].key, PMIX_USOCK_DISABLE)) {
                disabled = PMIX_INFO_TRUE(&info[n]);;
                break;
            }
        }
    }

    /* see if we have been disabled */
    if (disabled) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "ptl:usock not available");
        return PMIX_ERR_NOT_AVAILABLE;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "ptl:usock setting up listener");

    addrlen = sizeof(struct sockaddr_un);
    address = (struct sockaddr_un*)&mca_ptl_usock_component.connection;
    address->sun_family = AF_UNIX;

    /* define the listener */
    lt = PMIX_NEW(pmix_listener_t);

    /* for now, just setup the v1.1 series rendezvous point
     * we use the pid to reduce collisions */
    mypid = getpid();
    if (0 > asprintf(&pmix_pid, "%s/pmix-%d", mca_ptl_usock_component.tmpdir, mypid)) {
        PMIX_RELEASE(lt);
        return PMIX_ERR_NOMEM;
    }
    if ((strlen(pmix_pid) + 1) > sizeof(address->sun_path)-1) {
        pmix_show_help("help-pmix-server.txt", "rnd-path-too-long", true,
                       mca_ptl_usock_component.tmpdir, pmix_pid);
        free(pmix_pid);
        PMIX_RELEASE(lt);
        return PMIX_ERR_INVALID_LENGTH;
    }
    snprintf(address->sun_path, sizeof(address->sun_path)-1, "%s", pmix_pid);
    free(pmix_pid);
    /* set the URI */
    lt->varname = strdup("PMIX_SERVER_URI:PMIX_SERVER_URI2USOCK");
    if (0 > asprintf(&lt->uri, "%s:%lu:%s", pmix_globals.myid.nspace,
                    (unsigned long)pmix_globals.myid.rank, address->sun_path)) {
        PMIX_RELEASE(lt);
        return PMIX_ERR_NOMEM;
    }
    /* save the rendezvous filename for later removal */
    mca_ptl_usock_component.filename = strdup(address->sun_path);

    lt->protocol = PMIX_PROTOCOL_V1;
    lt->ptl = (struct pmix_ptl_module_t*)&pmix_ptl_usock_module;
    lt->cbfunc = connection_handler;
    pmix_list_append(&pmix_ptl_globals.listeners, &lt->super);

    /* create a listen socket for incoming connection attempts */
    lt->socket = socket(PF_UNIX, SOCK_STREAM, 0);
    if (lt->socket < 0) {
        printf("%s:%d socket() failed\n", __FILE__, __LINE__);
        goto sockerror;
    }
    /* Set the socket to close-on-exec so that no children inherit
     * this FD */
    if (pmix_fd_set_cloexec(lt->socket) != PMIX_SUCCESS) {
        CLOSE_THE_SOCKET(lt->socket);
        goto sockerror;
    }

    if (bind(lt->socket, (struct sockaddr*)address, addrlen) < 0) {
        printf("%s:%d bind() failed\n", __FILE__, __LINE__);
        CLOSE_THE_SOCKET(lt->socket);
        goto sockerror;
    }
    /* chown as required */
    if (lt->owner_given) {
        if (0 != chown(address->sun_path, lt->owner, -1)) {
            pmix_output(0, "CANNOT CHOWN socket %s: %s", address->sun_path, strerror (errno));
            CLOSE_THE_SOCKET(lt->socket);
            goto sockerror;
        }
    }
    if (lt->group_given) {
        if (0 != chown(address->sun_path, -1, lt->group)) {
            pmix_output(0, "CANNOT CHOWN socket %s: %s", address->sun_path, strerror (errno));
            CLOSE_THE_SOCKET(lt->socket);
            goto sockerror;
        }
    }
    /* set the mode as required */
    if (0 != chmod(address->sun_path, lt->mode)) {
        pmix_output(0, "CANNOT CHMOD socket %s: %s", address->sun_path, strerror (errno));
        CLOSE_THE_SOCKET(lt->socket);
        goto sockerror;
    }

    /* setup listen backlog to maximum allowed by kernel */
    if (listen(lt->socket, SOMAXCONN) < 0) {
        printf("%s:%d listen() failed\n", __FILE__, __LINE__);
        CLOSE_THE_SOCKET(lt->socket);
        goto sockerror;
    }

    /* set socket up to be non-blocking, otherwise accept could block */
    if ((flags = fcntl(lt->socket, F_GETFL, 0)) < 0) {
        printf("%s:%d fcntl(F_GETFL) failed\n", __FILE__, __LINE__);
        CLOSE_THE_SOCKET(lt->socket);
        goto sockerror;
    }
    flags |= O_NONBLOCK;
    if (fcntl(lt->socket, F_SETFL, flags) < 0) {
        printf("%s:%d fcntl(F_SETFL) failed\n", __FILE__, __LINE__);
        CLOSE_THE_SOCKET(lt->socket);
        goto sockerror;
    }

    /* if the server will listen for us, then ask it to do so now */
    rc = PMIX_ERR_NOT_SUPPORTED;
    if (NULL != pmix_host_server.listener) {
        rc = pmix_host_server.listener(lt->socket, listener_cb, (void*)lt);
    }

    if (PMIX_SUCCESS != rc) {
        *need_listener = true;
    }

    return PMIX_SUCCESS;

  sockerror:
      pmix_list_remove_item(&pmix_ptl_globals.listeners, &lt->super);
      PMIX_RELEASE(lt);
      return PMIX_ERROR;
}

static void listener_cb(int incoming_sd, void *cbdata)
{
    pmix_pending_connection_t *pending_connection;

    /* throw it into our event library for processing */
    pmix_output_verbose(8, pmix_ptl_base_framework.framework_output,
                        "listen_cb: pushing new connection %d into evbase",
                        incoming_sd);
    pending_connection = PMIX_NEW(pmix_pending_connection_t);
    pending_connection->sd = incoming_sd;
    pmix_event_assign(&pending_connection->ev, pmix_globals.evbase, -1,
                      EV_WRITE, connection_handler, pending_connection);
    pmix_event_active(&pending_connection->ev, EV_WRITE, 1);
}

static void connection_handler(int sd, short args, void *cbdata)
{
    pmix_pending_connection_t *pnd = (pmix_pending_connection_t*)cbdata;
    char *msg, *ptr, *nspace, *version, *sec, *bfrops, *gds;
    pmix_status_t rc;
    unsigned int rank;
    pmix_usock_hdr_t hdr;
    pmix_namespace_t *nptr, *tmp;
    pmix_rank_info_t *info;
    pmix_peer_t *psave = NULL;
    bool found;
    pmix_proc_t proc;
    size_t len;
    pmix_bfrop_buffer_type_t bftype;
    char **vers;
    int major, minor, rel;
    unsigned int msglen;
    pmix_info_t ginfo;
    pmix_byte_object_t cred;

    /* acquire the object */
    PMIX_ACQUIRE_OBJECT(pnd);

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "USOCK CONNECTION FROM PEER ON SOCKET %d", pnd->sd);

    /* ensure all is zero'd */
    memset(&hdr, 0, sizeof(pmix_usock_hdr_t));

    /* get the header */
    if (PMIX_SUCCESS != (rc = pmix_ptl_base_recv_blocking(pnd->sd, (char*)&hdr, sizeof(pmix_usock_hdr_t)))) {
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* get the id, authentication and version payload (and possibly
     * security credential) - to guard against potential attacks,
     * we'll set an arbitrary limit per a define */
    if (PMIX_MAX_CRED_SIZE < hdr.nbytes) {
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }
    if (NULL == (msg = (char*)malloc(hdr.nbytes))) {
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }
    if (PMIX_SUCCESS != pmix_ptl_base_recv_blocking(pnd->sd, msg, hdr.nbytes)) {
        /* unable to complete the recv */
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "unable to complete recv of connect-ack with client ON SOCKET %d", pnd->sd);
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }
    len = hdr.nbytes;
    ptr = msg;

    /* extract the nspace of the requestor */
    PMIX_STRNLEN(msglen, ptr, len);
    if (msglen < len) {
        nspace = ptr;
        ptr += strlen(nspace) + 1;
        len -= strlen(nspace) + 1;
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* extract the rank */
    PMIX_STRNLEN(msglen, ptr, len);
    if (msglen <= len) {
        memcpy(&rank, ptr, sizeof(int));
        ptr += sizeof(int);
        len -= sizeof(int);
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* get their version string */
    PMIX_STRNLEN(msglen, ptr, len);
    if (msglen < len) {
        version = ptr;
        ptr += strlen(version) + 1;
        len -= strlen(version) + 1;
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* check the version - we do NOT support anything less than
     * v1.2.5 */
    vers = pmix_argv_split(version, '.');
    major = strtol(vers[0], NULL, 10);
    minor = strtol(vers[1], NULL, 10);
    rel = strtol(vers[2], NULL, 10);
    pmix_argv_free(vers);
    if (1 == major && (2 != minor || 5 > rel)) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "connection request from client of unsupported version %s", version);
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* get any provided credential */
    if (1 == major) {
        /* credential is always a string */
        PMIX_STRNLEN(msglen, ptr, len);
        if (msglen < len) {
            cred.bytes = ptr;
            cred.size = strlen(ptr);
            ptr += cred.size + 1;
            len -= cred.size + 1;
        } else {
            free(msg);
            CLOSE_THE_SOCKET(pnd->sd);
            PMIX_RELEASE(pnd);
            return;
        }
    } else {
        /* credential could be something else */
        if (sizeof(size_t) < len) {
            memcpy(&cred.size, ptr, sizeof(size_t));
            ptr += sizeof(size_t);
            len -= sizeof(size_t);
        } else {
            free(msg);
            CLOSE_THE_SOCKET(pnd->sd);
            PMIX_RELEASE(pnd);
            return;
        }
        if (0 < cred.size) {
            cred.bytes = ptr;
            ptr += cred.size;
            len -= cred.size;
        } else {
            /* set cred pointer to NULL to guard against validation
             * methods that assume a zero length credential is NULL */
            cred.bytes = NULL;
        }
    }

    /* get their sec module */
    PMIX_STRNLEN(msglen, ptr, len);
    if (msglen < len) {
        sec = ptr;
        ptr += strlen(sec) + 1;
        len -= strlen(sec) + 1;
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* get their bfrops module */
    PMIX_STRNLEN(msglen, ptr, len);
    if (msglen < len) {
        bfrops = ptr;
        ptr += strlen(bfrops) + 1;
        len -= strlen(bfrops) + 1;
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* get their buffer type */
    if (0 < len) {
        bftype = ptr[0];
        ptr += 1;
        len -= 1;
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* get their gds module */
    PMIX_STRNLEN(msglen, ptr, len);
    if (msglen < len) {
        gds = ptr;
        ptr += strlen(gds) + 1;
        len -= strlen(gds) + 1;
    } else {
        free(msg);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "connect-ack recvd from peer %s:%d:%s on socket %d",
                        nspace, rank, version, pnd->sd);

    /* see if we know this nspace */
    nptr = NULL;
    PMIX_LIST_FOREACH(tmp, &pmix_server_globals.nspaces, pmix_namespace_t) {
        if (0 == strcmp(tmp->nspace, nspace)) {
            nptr = tmp;
            break;
        }
    }
    if (NULL == nptr) {
        /* we don't know this namespace, reject it */
        free(msg);
        /* send an error reply to the client */
        rc = PMIX_ERR_NOT_FOUND;
        goto error;
    }

    /* see if we have this peer in our list */
    info = NULL;
    found = false;
    PMIX_LIST_FOREACH(info, &nptr->ranks, pmix_rank_info_t) {
        if (info->pname.rank == rank) {
            found = true;
            break;
        }
    }
    if (!found) {
        /* rank unknown, reject it */
        free(msg);
        /* send an error reply to the client */
        rc = PMIX_ERR_NOT_FOUND;
        goto error;
    }
    /* a peer can connect on multiple sockets since it can fork/exec
     * a child that also calls PMIx_Init, so add it here if necessary.
     * Create the tracker for this peer */
    psave = PMIX_NEW(pmix_peer_t);
    if (NULL == psave) {
        free(msg);
        rc = PMIX_ERR_NOMEM;
        goto error;
    }
    /* mark it as being a client of the correct type */
    if (1 == major) {
        psave->proc_type = PMIX_PROC_CLIENT | PMIX_PROC_V1;
    } else if (2 == major && 0 == minor) {
        psave->proc_type = PMIX_PROC_CLIENT | PMIX_PROC_V20;
    } else if (2 == major && 1 == minor) {
        psave->proc_type = PMIX_PROC_CLIENT | PMIX_PROC_V21;
    } else if (3 == major) {
        psave->proc_type = PMIX_PROC_CLIENT | PMIX_PROC_V3;
    } else {
        /* we don't recognize this version */
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "connection request from client of unrecognized version %s", version);
        free(msg);
        PMIX_RELEASE(psave);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }
    /* save the protocol */
    psave->protocol = pnd->protocol;
    /* add the nspace tracker */
    PMIX_RETAIN(nptr);
    psave->nptr = nptr;
    PMIX_RETAIN(info);
    psave->info = info;
    /* save the epilog info */
    psave->epilog.uid = info->uid;
    psave->epilog.gid = info->gid;
    nptr->epilog.uid = info->uid;
    nptr->epilog.gid = info->gid;
    info->proc_cnt++; /* increase number of processes on this rank */
    psave->sd = pnd->sd;
    if (0 > (psave->index = pmix_pointer_array_add(&pmix_server_globals.clients, psave))) {
        free(msg);
        info->proc_cnt--;
        PMIX_RELEASE(info);
        PMIX_RELEASE(psave);
        /* probably cannot send an error reply if we are out of memory */
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }
    info->peerid = psave->index;

    /* get the appropriate compatibility modules */
    nptr->compat.psec = pmix_psec_base_assign_module(sec);
    if (NULL == nptr->compat.psec) {
        free(msg);
        info->proc_cnt--;
        PMIX_RELEASE(info);
        pmix_pointer_array_set_item(&pmix_server_globals.clients, psave->index, NULL);
        PMIX_RELEASE(psave);
        /* send an error reply to the client */
        goto error;
    }

    /* set the bfrops module to match this peer */
    nptr->compat.bfrops = pmix_bfrops_base_assign_module(bfrops);
    if (NULL == nptr->compat.bfrops) {
        free(msg);
        info->proc_cnt--;
        PMIX_RELEASE(info);
        pmix_pointer_array_set_item(&pmix_server_globals.clients, psave->index, NULL);
        PMIX_RELEASE(psave);
       /* send an error reply to the client */
        goto error;
    }
    /* set the buffer type */
    nptr->compat.type = bftype;

    /* set the gds module to match this peer */
    if (NULL != gds) {
        PMIX_INFO_LOAD(&ginfo, PMIX_GDS_MODULE, gds, PMIX_STRING);
        nptr->compat.gds = pmix_gds_base_assign_module(&ginfo, 1);
        PMIX_INFO_DESTRUCT(&ginfo);
    } else {
        nptr->compat.gds = pmix_gds_base_assign_module(NULL, 0);
    }
    if (NULL == nptr->compat.gds) {
        free(msg);
        info->proc_cnt--;
        pmix_pointer_array_set_item(&pmix_server_globals.clients, psave->index, NULL);
        PMIX_RELEASE(psave);
        /* send an error reply to the client */
        goto error;
    }

    /* if we haven't previously stored the version for this
     * nspace, do so now */
    if (!nptr->version_stored) {
        PMIX_INFO_LOAD(&ginfo, PMIX_BFROPS_MODULE, nptr->compat.bfrops->version, PMIX_STRING);
        PMIX_GDS_CACHE_JOB_INFO(rc, pmix_globals.mypeer, nptr, &ginfo, 1);
        PMIX_INFO_DESTRUCT(&ginfo);
        nptr->version_stored = true;
    }

    /* the choice of PTL module was obviously made by the connecting
     * tool as we received this request via that channel, so simply
     * record it here for future use */
    nptr->compat.ptl = &pmix_ptl_usock_module;

    /* validate the connection - the macro will send the status result to the client */
    PMIX_PSEC_VALIDATE_CONNECTION(rc, psave, NULL, 0, NULL, 0, &cred);
    /* now done with the msg */
    free(msg);

    if (PMIX_SUCCESS != rc) {
        pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                            "validation of client credentials failed: %s",
                            PMIx_Error_string(rc));
        info->proc_cnt--;
        PMIX_RELEASE(info);
        pmix_pointer_array_set_item(&pmix_server_globals.clients, psave->index, NULL);
        PMIX_RELEASE(psave);
        /* error reply was sent by the above macro */
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    /* send the client's array index */
    if (PMIX_SUCCESS != (rc = pmix_ptl_base_send_blocking(pnd->sd, (char*)&psave->index, sizeof(int)))) {
        PMIX_ERROR_LOG(rc);
        info->proc_cnt--;
        PMIX_RELEASE(info);
        pmix_pointer_array_set_item(&pmix_server_globals.clients, psave->index, NULL);
        PMIX_RELEASE(psave);
        CLOSE_THE_SOCKET(pnd->sd);
        PMIX_RELEASE(pnd);
        return;
    }

    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "connect-ack from client completed");

    /* let the host server know that this client has connected */
    if (NULL != pmix_host_server.client_connected) {
        pmix_strncpy(proc.nspace, psave->info->pname.nspace, PMIX_MAX_NSLEN);
        proc.rank = psave->info->pname.rank;
        rc = pmix_host_server.client_connected(&proc, psave->info->server_object, NULL, NULL);
        if (PMIX_SUCCESS != rc && PMIX_OPERATION_SUCCEEDED != rc) {
            PMIX_ERROR_LOG(rc);
            info->proc_cnt--;
            PMIX_RELEASE(info);
            pmix_pointer_array_set_item(&pmix_server_globals.clients, psave->index, NULL);
            PMIX_RELEASE(psave);
            CLOSE_THE_SOCKET(pnd->sd);
        }
    }

    /* start the events for this client */
    pmix_event_assign(&psave->recv_event, pmix_globals.evbase, pnd->sd,
                      EV_READ|EV_PERSIST, pmix_usock_recv_handler, psave);
    pmix_event_add(&psave->recv_event, NULL);
    psave->recv_ev_active = true;
    pmix_event_assign(&psave->send_event, pmix_globals.evbase, pnd->sd,
                      EV_WRITE|EV_PERSIST, pmix_usock_send_handler, psave);
    pmix_output_verbose(2, pmix_ptl_base_framework.framework_output,
                        "pmix:server client %s:%u has connected on socket %d",
                        psave->info->pname.nspace, psave->info->pname.rank, psave->sd);

    PMIX_RELEASE(pnd);
    return;

  error:
    /* send an error reply to the client */
    if (PMIX_SUCCESS != pmix_ptl_base_send_blocking(pnd->sd, (char*)&rc, sizeof(int))) {
        PMIX_ERROR_LOG(rc);
    }
    CLOSE_THE_SOCKET(pnd->sd);
    PMIX_RELEASE(pnd);
    return;
}

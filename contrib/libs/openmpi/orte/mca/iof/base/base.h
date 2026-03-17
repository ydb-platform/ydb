/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * I/O Forwarding Service
 */

#ifndef MCA_IOF_BASE_H
#define MCA_IOF_BASE_H

#include "orte_config.h"
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif
#ifdef HAVE_NET_UIO_H
#include <net/uio.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <signal.h>

#include "opal/class/opal_list.h"
#include "opal/class/opal_bitmap.h"
#include "orte/mca/mca.h"
#include "opal/mca/event/event.h"
#include "opal/util/fd.h"

#include "orte/mca/iof/iof.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/util/threads.h"
#include "orte/mca/errmgr/errmgr.h"

BEGIN_C_DECLS

/*
 * MCA framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_iof_base_framework;
/*
 * Select an available component.
 */
ORTE_DECLSPEC int orte_iof_base_select(void);

/* track xon/xoff of processes */
typedef struct {
    opal_object_t super;
    orte_job_t *jdata;
    opal_bitmap_t xoff;
} orte_iof_job_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_iof_job_t);

/*
 * Maximum size of single msg
 */
#define ORTE_IOF_BASE_MSG_MAX           4096
#define ORTE_IOF_BASE_TAG_MAX             50
#define ORTE_IOF_BASE_TAGGED_OUT_MAX    8192
#define ORTE_IOF_MAX_INPUT_BUFFERS        50

typedef struct {
    opal_list_item_t super;
    bool pending;
    bool always_writable;
    opal_event_t *ev;
    struct timeval tv;
    int fd;
    opal_list_t outputs;
} orte_iof_write_event_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_iof_write_event_t);

typedef struct {
    opal_list_item_t super;
    orte_process_name_t name;
    orte_process_name_t daemon;
    orte_iof_tag_t tag;
    orte_iof_write_event_t *wev;
    bool xoff;
    bool exclusive;
    bool closed;
} orte_iof_sink_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_iof_sink_t);

struct orte_iof_proc_t;
typedef struct {
    opal_object_t super;
    struct orte_iof_proc_t *proc;
    opal_event_t *ev;
    struct timeval tv;
    int fd;
    orte_iof_tag_t tag;
    bool active;
    bool always_readable;
    orte_iof_sink_t *sink;
} orte_iof_read_event_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_iof_read_event_t);

typedef struct {
    opal_list_item_t super;
    orte_process_name_t name;
    orte_iof_sink_t *stdinev;
    orte_iof_read_event_t *revstdout;
    orte_iof_read_event_t *revstderr;
#if OPAL_PMIX_V1
    orte_iof_read_event_t *revstddiag;
#endif
    opal_list_t *subscribers;
    bool copy;
} orte_iof_proc_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_iof_proc_t);

typedef struct {
    opal_list_item_t super;
    char data[ORTE_IOF_BASE_TAGGED_OUT_MAX];
    int numbytes;
} orte_iof_write_output_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_iof_write_output_t);

/* the iof globals struct */
struct orte_iof_base_t {
    size_t                  output_limit;
    orte_iof_sink_t         *iof_write_stdout;
    orte_iof_sink_t         *iof_write_stderr;
    bool                    redirect_app_stderr_to_stdout;
};
typedef struct orte_iof_base_t orte_iof_base_t;

/* Write event macro's */

static inline bool
orte_iof_base_fd_always_ready(int fd)
{
    return opal_fd_is_regular(fd) ||
           (opal_fd_is_chardev(fd) && !isatty(fd)) ||
           opal_fd_is_blkdev(fd);
}

#define ORTE_IOF_SINK_BLOCKSIZE (1024)

#define ORTE_IOF_SINK_ACTIVATE(wev)                                     \
    do {                                                                \
        struct timeval *tv = NULL;                                      \
        wev->pending = true;                                            \
        ORTE_POST_OBJECT(wev);                                          \
        if (wev->always_writable) {                                     \
            /* Regular is always write ready. Use timer to activate */  \
            tv = &wev->tv;                                        \
        }                                                               \
        if (opal_event_add(wev->ev, tv)) {                              \
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);                         \
        }                                                               \
    } while(0);


/* define an output "sink", adding it to the provided
 * endpoint list for this proc */
#define ORTE_IOF_SINK_DEFINE(snk, nm, fid, tg, wrthndlr)                \
    do {                                                                \
        orte_iof_sink_t *ep;                                            \
        OPAL_OUTPUT_VERBOSE((1,                                         \
                            orte_iof_base_framework.framework_output,   \
                            "defining endpt: file %s line %d fd %d",    \
                            __FILE__, __LINE__, (fid)));                \
        ep = OBJ_NEW(orte_iof_sink_t);                                  \
        ep->name.jobid = (nm)->jobid;                                   \
        ep->name.vpid = (nm)->vpid;                                     \
        ep->tag = (tg);                                                 \
        if (0 <= (fid)) {                                               \
            ep->wev->fd = (fid);                                        \
            ep->wev->always_writable =                                  \
                    orte_iof_base_fd_always_ready(fid);                 \
            if(ep->wev->always_writable) {                              \
                opal_event_evtimer_set(orte_event_base,                 \
                                       ep->wev->ev,  wrthndlr, ep);     \
            } else {                                                    \
                opal_event_set(orte_event_base,                         \
                               ep->wev->ev, ep->wev->fd,                \
                               OPAL_EV_WRITE,                           \
                               wrthndlr, ep);                           \
            }                                                           \
            opal_event_set_priority(ep->wev->ev, ORTE_MSG_PRI);         \
        }                                                               \
        *(snk) = ep;                                                    \
        ORTE_POST_OBJECT(ep);                                           \
    } while(0);

/* Read event macro's */
#define ORTE_IOF_READ_ADDEV(rev)                                \
    do {                                                        \
        struct timeval *tv = NULL;                              \
        if (rev->always_readable) {                             \
            tv = &rev->tv;                                      \
        }                                                       \
        if (opal_event_add(rev->ev, tv)) {                      \
            ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);                 \
        }                                                       \
    } while(0);

#define ORTE_IOF_READ_ACTIVATE(rev)                             \
    do {                                                        \
        rev->active = true;                                     \
        ORTE_POST_OBJECT(rev);                                  \
        ORTE_IOF_READ_ADDEV(rev);                               \
    } while(0);


/* add list of structs that has name of proc + orte_iof_tag_t - when
 * defining a read event, search list for proc, add flag to the tag.
 * when closing a read fd, find proc on list and zero out that flag
 * when all flags = 0, then iof is complete - set message event to
 * daemon processor indicating proc iof is terminated
 */
#define ORTE_IOF_READ_EVENT(rv, p, fid, tg, cbfunc, actv)               \
    do {                                                                \
        orte_iof_read_event_t *rev;                                     \
        OPAL_OUTPUT_VERBOSE((1,                                         \
                            orte_iof_base_framework.framework_output,   \
                            "%s defining read event for %s: %s %d",     \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            ORTE_NAME_PRINT(&(p)->name),                \
                            __FILE__, __LINE__));                       \
        rev = OBJ_NEW(orte_iof_read_event_t);                           \
        OBJ_RETAIN((p));                                                \
        rev->proc = (struct orte_iof_proc_t*)(p);                       \
        rev->tag = (tg);                                                \
        rev->fd = (fid);                                                \
        rev->always_readable = orte_iof_base_fd_always_ready(fid);      \
        *(rv) = rev;                                                    \
        if(rev->always_readable) {                                      \
            opal_event_evtimer_set(orte_event_base,                     \
                                   rev->ev, (cbfunc), rev);             \
        } else {                                                        \
            opal_event_set(orte_event_base,                             \
                           rev->ev, (fid),                              \
                           OPAL_EV_READ,                                \
                           (cbfunc), rev);                              \
        }                                                               \
        opal_event_set_priority(rev->ev, ORTE_MSG_PRI);                 \
        if ((actv)) {                                                   \
            ORTE_IOF_READ_ACTIVATE(rev)                                 \
        }                                                               \
    } while(0);


ORTE_DECLSPEC int orte_iof_base_flush(void);

ORTE_DECLSPEC extern orte_iof_base_t orte_iof_base;

/* base functions */
ORTE_DECLSPEC int orte_iof_base_write_output(const orte_process_name_t *name, orte_iof_tag_t stream,
                                             const unsigned char *data, int numbytes,
                                             orte_iof_write_event_t *channel);
ORTE_DECLSPEC void orte_iof_base_static_dump_output(orte_iof_read_event_t *rev);
ORTE_DECLSPEC void orte_iof_base_write_handler(int fd, short event, void *cbdata);

END_C_DECLS

#endif /* MCA_IOF_BASE_H */

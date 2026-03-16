/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
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
 * I/O Forwarding Service
 * The I/O forwarding service (IOF) is used to connect stdin, stdout, and
 * stderr file descriptor streams from MPI processes to the user
 *
 * The design is fairly simple: when a proc is spawned, the IOF establishes
 * connections between its stdin, stdout, and stderr to a
 * corresponding IOF stream. In addition, the IOF designates a separate
 * stream for passing OMPI/ORTE internal diagnostic/help output to mpirun.
 * This is done specifically to separate such output from the user's
 * stdout/err - basically, it allows us to present it to the user in
 * a separate format for easier recognition. Data read from a source
 * on any stream (e.g., printed to stdout by the proc) is relayed
 * by the local daemon to the other end of the stream - i.e., stdin
 * is relayed to the local proc, while stdout/err is relayed to mpirun.
 * Thus, the eventual result is to connect ALL streams to/from
 * the application process and mpirun.
 *
 * Note: By default, data read from stdin is forwarded -only- to rank=0.
 * Stdin for all other procs is tied to "/dev/null".
 *
 * External tools can "pull" copies of stdout/err and
 * the diagnostic stream from mpirun for any process. In this case,
 * mpirun will send a copy of the output to the "pulling" process. Note that external tools
 * cannot "push" something into stdin unless the user specifically directed
 * that stdin remain open, nor under any conditions "pull" a copy of the
 * stdin being sent to rank=0.
 *
 * Tools can exploit either of two mechanisms for this purpose:
 *
 * (a) call orte_init themselves and utilize the ORTE tool comm
 *     library to access the IOF. This also provides access to
 *     other tool library functions - e.g., to order that a job
 *     be spawned; or
 *
 * (b) fork/exec the "orte-iof" tool and let it serve as the interface
 *     to mpirun. This lets the tool avoid calling orte_init, and means
 *     the tool will not have to compile against the ORTE/OMPI libraries.
 *     However, the orte-iof tool is limited solely to interfacing
 *     stdio and cannot be used for other functions included in
 *     the tool comm library
 *
 * Thus, mpirun acts as a "switchyard" for IO, taking input from stdin
 * and passing it to rank=0 of the job, and taking stdout/err/diag from all
 * ranks and passing it to its own stdout/err/diag plus any "pull"
 * requestors.
 *
 * Streams are identified by ORTE process name (to include wildcards,
 * such as "all processes in ORTE job X") and tag.  There are
 * currently only 4 allowed predefined tags:
 *
 * - ORTE_IOF_STDIN (value 0)
 * - ORTE_IOF_STDOUT (value 1)
 * - ORTE_IOF_STDERR (value 2)
 * - ORTE_IOF_INTERNAL (value 3): for "internal" messages
 *   from the infrastructure, just to differentiate them from user job
 *   stdout/stderr
 *
 * Note that since streams are identified by ORTE process name, the
 * caller has no idea whether the stream is on the local node or a
 * remote node -- it's just a stream.
 *
 * IOF components are selected on a "one of many" basis, meaning that
 * only one IOF component will be selected for a given process.
 * Details for the various components are given in their source code
 * bases.
 *
 * Each IOF component must support the following API:
 *
 * push: Tie a local file descriptor (*not* a stream!) to the stdin
 * of the specified process. If the user has not specified that stdin
 * of the specified process is to remain open, this will return an error.
 *
 * pull: Tie a local file descriptor (*not* a stream!) to a stream.
 * Subsequent input that appears via the stream will
 * automatically be sent to the target file descriptor until the
 * stream is "closed" or an EOF is received on the local file descriptor.
 * Valid source values include ORTE_IOF_STDOUT, ORTE_IOF_STDERR, and
 * ORTE_IOF_INTERNAL
 *
 * close: Closes a stream, flushing any pending data down it and
 * terminating any "push/pull" connections against it. Unclear yet
 * if this needs to be blocking, or can be done non-blocking.
 *
 * flush: Block until all pending data on all open streams has been
 * written down local file descriptors and/or completed sending across
 * the OOB to remote process targets.
 *
 */

#ifndef ORTE_IOF_H
#define ORTE_IOF_H

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/mca.h"

#include "orte/runtime/orte_globals.h"

#include "iof_types.h"

BEGIN_C_DECLS

/* define a macro for requesting a proxy PULL of IO on
 * behalf of a tool that had the HNP spawn a job. First
 * argument is the orte_job_t of the spawned job, second
 * is a pointer to the name of the requesting tool */
#define ORTE_IOF_PROXY_PULL(a, b)                                       \
    do {                                                                \
        opal_buffer_t *buf;                                             \
        orte_iof_tag_t tag;                                             \
        orte_process_name_t nm;                                         \
                                                                        \
        buf = OBJ_NEW(opal_buffer_t);                                   \
                                                                        \
        /* setup the tag to pull from HNP */                            \
        tag = ORTE_IOF_STDOUTALL | ORTE_IOF_PULL | ORTE_IOF_EXCLUSIVE;  \
        opal_dss.pack(buf, &tag, 1, ORTE_IOF_TAG);                      \
        /* pack the name of the source we want to pull */               \
        nm.jobid = (a)->jobid;                                          \
        nm.vpid = ORTE_VPID_WILDCARD;                                   \
        opal_dss.pack(buf, &nm, 1, ORTE_NAME);                          \
        /* pack the name of the tool */                                 \
        opal_dss.pack(buf, (b), 1, ORTE_NAME);                          \
                                                                        \
        /* send the buffer to the HNP */                                \
        orte_rml.send_buffer_nb(orte_mgmt_conduit,                      \
                                ORTE_PROC_MY_HNP, buf,                  \
                                ORTE_RML_TAG_IOF_HNP,                   \
                                orte_rml_send_callback, NULL);          \
    } while(0);

/* Initialize the selected module */
typedef int (*orte_iof_base_init_fn_t)(void);

/**
 * Explicitly push data from the specified input file descriptor to
 * the stdin of the indicated peer(s). The provided peer name can
 * include wildcard values.
 *
 * @param peer  Name of target peer(s)
 * @param fd    Local file descriptor for input.
 */
typedef int (*orte_iof_base_push_fn_t)(const orte_process_name_t* peer,
                                       orte_iof_tag_t src_tag, int fd);

/**
 * Explicitly pull data from the specified set of SOURCE peers and
 * dump to the indicated output file descriptor. Any fragments that
 * arrive on the stream will automatically be written down the fd.
 *
 * @param peer          Name used to qualify set of origin peers.
 * @param source_tag    Indicates the output streams to be forwarded
 * @param fd            Local file descriptor for output.
 */
typedef int (*orte_iof_base_pull_fn_t)(const orte_process_name_t* peer,
                                       orte_iof_tag_t source_tag,
                                       int fd);

/**
 * Close the specified iof stream(s) from the indicated peer(s)
 */
typedef int (*orte_iof_base_close_fn_t)(const orte_process_name_t* peer,
                                        orte_iof_tag_t source_tag);

/**
 * Output something via the IOF subsystem
 */
typedef int (*orte_iof_base_output_fn_t)(const orte_process_name_t* peer,
                                         orte_iof_tag_t source_tag,
                                         const char *msg);

/* Flag that a job is complete */
typedef void (*orte_iof_base_complete_fn_t)(const orte_job_t *jdata);

/* finalize the selected module */
typedef int (*orte_iof_base_finalize_fn_t)(void);

/**
 * FT Event Notification
 */
typedef int (*orte_iof_base_ft_event_fn_t)(int state);

/**
 *  IOF module.
 */
struct orte_iof_base_module_2_0_0_t {
    orte_iof_base_init_fn_t     init;
    orte_iof_base_push_fn_t     push;
    orte_iof_base_pull_fn_t     pull;
    orte_iof_base_close_fn_t    close;
    orte_iof_base_output_fn_t   output;
    orte_iof_base_complete_fn_t complete;
    orte_iof_base_finalize_fn_t finalize;
    orte_iof_base_ft_event_fn_t ft_event;
};

typedef struct orte_iof_base_module_2_0_0_t orte_iof_base_module_2_0_0_t;
typedef orte_iof_base_module_2_0_0_t orte_iof_base_module_t;
ORTE_DECLSPEC extern orte_iof_base_module_t orte_iof;

struct orte_iof_base_component_2_0_0_t {
  mca_base_component_t iof_version;
  mca_base_component_data_t iof_data;
};
typedef struct orte_iof_base_component_2_0_0_t orte_iof_base_component_2_0_0_t;
typedef struct orte_iof_base_component_2_0_0_t orte_iof_base_component_t;

END_C_DECLS

/*
 * Macro for use in components that are of type iof
 */
#define ORTE_IOF_BASE_VERSION_2_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("iof", 2, 0, 0)

#endif /* ORTE_IOF_H */

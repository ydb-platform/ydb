/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_BACKTRACE_BACKTRACE_H
#define OPAL_MCA_BACKTRACE_BACKTRACE_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/stacktrace.h"

BEGIN_C_DECLS

/*
 * Most of this file is just for ompi_info.  There are two interface
 * functions, both of which are called directly.  The joy of link-time
 * components.
 */


/*
 * Print back trace to FILE file with a prefix for each line.
 * First strip lines are not printed.
 * If 'file' is NULL then the component should try to use the file descriptor
 * saved in opal_stacktrace_output_fileno
 *
 * \note some attempts made to be signal safe.
 */
OPAL_DECLSPEC int opal_backtrace_print(FILE *file, char *prefix, int strip);

/*
 * Return back trace in buffer.  buffer will be allocated by the
 * backtrace component, but should be free'ed by the caller.
 *
 * \note Probably bad to call this from a signal handler.
 *
 */
OPAL_DECLSPEC int opal_backtrace_buffer(char*** messages, int *len);


/**
 * Structure for backtrace components.
 */
struct opal_backtrace_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t backtracec_version;
    /** MCA base data */
    mca_base_component_data_t backtracec_data;
};
/**
 * Convenience typedef
 */
typedef struct opal_backtrace_base_component_2_0_0_t opal_backtrace_base_component_2_0_0_t;

/*
 * Macro for use in components that are of type backtrace
 */
#define OPAL_BACKTRACE_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("backtrace", 2, 0, 0)

END_C_DECLS

#endif /* OPAL_MCA_BACKTRACE_BACKTRACE_H */

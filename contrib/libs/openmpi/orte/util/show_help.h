/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file:
 *
 * Populates global structure with system-specific information.
 *
 * Notes: add limits.h, compute size of integer and other types via sizeof(type)*CHAR_BIT
 *
 */

#ifndef _ORTE_SHOW_HELP_H_
#define _ORTE_SHOW_HELP_H_

#include "orte_config.h"
#include "orte/types.h"


#include "orte/mca/rml/rml_types.h"

BEGIN_C_DECLS

/**
 * Initializes the output stream system and opens a default
 * "verbose" stream.
 *
 * @retval success Upon success.
 * @retval error Upon failure.
 *
 * This should be the first function invoked in the output
 * subsystem.  After this call, the default "verbose" stream is open
 * and can be written to via calls to orte_output_verbose() and
 * orte_output_error().
 *
 * By definition, the default verbose stream has a handle ID of 0,
 * and has a verbose level of 0.
 */
ORTE_DECLSPEC int orte_show_help_init(void);

/**
 * Allow other parts of the code base to know if the ORTE show_help
 * system is available or not (does not necessarily indicate that
 * aggregating is available; on no-ORTE systems, ORTE show_help is
 * available, but aggregating is not).
 */
ORTE_DECLSPEC bool orte_show_help_is_available(void);

/**
 * Shut down the output stream system.
 *
 * Shut down the output stream system, including the default verbose
 * stream.
 */
ORTE_DECLSPEC void orte_show_help_finalize(void);

/**
 * Show help.
 *
 * Sends show help messages to the HNP if on a backend node.  Note
 * that aggregation is not currently supported on HNP-less systems
 * (e.g., cray).
 */
ORTE_DECLSPEC int orte_show_help(const char *filename, const char *topic,
                                 bool want_error_header, ...);

/**
 * Exactly the same as orte_show_help, but pass in a rendered string,
 * rather than a varargs list which must be rendered.
 */
ORTE_DECLSPEC int orte_show_help_norender(const char *filename,
                                          const char *topic,
                                          bool want_error_header,
                                          const char *output);

/**
 * Pretend that this message has already been shown.
 *
 * Sends a control message to the HNP that will effecitvely suppress
 * this message from being shown.  Primitive *-wildcarding is
 * possible.
 *
 * Not currently supported on HNP-less systems (e.g., cray).
 */
ORTE_DECLSPEC int orte_show_help_suppress(const char *filename,
                                          const char *topic);

ORTE_DECLSPEC void orte_show_help_recv(int status, orte_process_name_t* sender,
                                       opal_buffer_t *buffer, orte_rml_tag_t tag,
                                       void* cbdata);

END_C_DECLS
#endif

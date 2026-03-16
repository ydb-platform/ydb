/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef OPAL_BACKTRACE_BASE_H
#define OPAL_BACKTRACE_BASE_H

#include "opal_config.h"

#include "opal/mca/base/mca_base_framework.h"

#include "opal/mca/backtrace/backtrace.h"

/*
 * Global functions for MCA overall backtrace open and close
 */

BEGIN_C_DECLS

OPAL_DECLSPEC extern mca_base_framework_t opal_backtrace_base_framework;

END_C_DECLS
#endif /* OPAL_BASE_BACKTRACE_H */

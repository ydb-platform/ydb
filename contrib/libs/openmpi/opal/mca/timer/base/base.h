/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef OPAL_TIMER_BASE_H
#define OPAL_TIMER_BASE_H

#include "opal_config.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/timer/timer.h"


/*
 * Global functions for MCA overall timer open and close
 */

BEGIN_C_DECLS

/**
 * Framework structure declaration
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_timer_base_framework;

/**
 * MCA param to force monotonic timers.
 */
OPAL_DECLSPEC extern bool mca_timer_base_monotonic;

END_C_DECLS

/* include implementation to call */
#include MCA_timer_IMPLEMENTATION_HEADER

#endif /* OPAL_BASE_TIMER_H */

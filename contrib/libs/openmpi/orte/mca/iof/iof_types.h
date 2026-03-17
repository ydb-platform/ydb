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
 * Copyright (c) 2016-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */

#ifndef ORTE_IOF_TYPES_H
#define ORTE_IOF_TYPES_H

#include "orte_config.h"
#include "orte/types.h"


BEGIN_C_DECLS

/* Predefined tag values */
typedef uint16_t orte_iof_tag_t;
#define ORTE_IOF_TAG_T  OPAL_UINT16

#define ORTE_IOF_STDIN      0x0001
#define ORTE_IOF_STDOUT     0x0002
#define ORTE_IOF_STDERR     0x0004
#define ORTE_IOF_STDMERGE   0x0006
#define ORTE_IOF_STDDIAG    0x0008
#define ORTE_IOF_STDOUTALL  0x000e
#define ORTE_IOF_STDALL     0x000f
#define ORTE_IOF_EXCLUSIVE  0x0100

/* flow control flags */
#define ORTE_IOF_XON        0x1000
#define ORTE_IOF_XOFF       0x2000
/* tool requests */
#define ORTE_IOF_PULL       0x4000
#define ORTE_IOF_CLOSE      0x8000

END_C_DECLS

#endif /* ORTE_IOF_TYPES_H */

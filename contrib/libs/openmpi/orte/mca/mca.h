/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Top-level interface for \em all orte MCA components.
 */

#ifndef ORTE_MCA_H
#define ORTE_MCA_H

#include "orte_config.h"
#include "opal/mca/mca.h"

#define ORTE_MCA_BASE_VERSION_2_1_0(type, type_major, type_minor, type_release) \
    MCA_BASE_VERSION_2_1_0("orte", ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,      \
                           ORTE_RELEASE_VERSION, type, type_major, type_minor,  \
                           type_release)

#endif /* ORTE_MCA_H */

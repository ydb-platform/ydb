/* -*- C -*-
 *
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX_PSHMEM_BASE_H_
#define PMIX_PSHMEM_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_list.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/pshmem/pshmem.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_pshmem_base_framework;
/**
 * PSHMEM select function
 *
 * Cycle across available components and construct the list
 * of active modules
 */
PMIX_EXPORT pmix_status_t pmix_pshmem_base_select(void);

END_C_DECLS

#endif

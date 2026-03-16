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
#ifndef PMIX_PSEC_BASE_H_
#define PMIX_PSEC_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_pointer_array.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/psec/psec.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_psec_base_framework;
/**
 * PSEC select function
 *
 * Cycle across available components and construct the list
 * of active modules
 */
PMIX_EXPORT pmix_status_t pmix_psec_base_select(void);

/**
 * Track an active component / module
 */
struct pmix_psec_base_active_module_t {
    pmix_list_item_t super;
    int pri;
    pmix_psec_module_t *module;
    pmix_psec_base_component_t *component;
};
typedef struct pmix_psec_base_active_module_t pmix_psec_base_active_module_t;
PMIX_CLASS_DECLARATION(pmix_psec_base_active_module_t);


/* framework globals */
struct pmix_psec_globals_t {
  pmix_list_t actives;
  bool initialized;
};
typedef struct pmix_psec_globals_t pmix_psec_globals_t;

extern pmix_psec_globals_t pmix_psec_globals;

PMIX_EXPORT char* pmix_psec_base_get_available_modules(void);
PMIX_EXPORT pmix_psec_module_t* pmix_psec_base_assign_module(const char *options);


END_C_DECLS

#endif

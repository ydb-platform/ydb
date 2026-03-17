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
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX_PLOG_BASE_H_
#define PMIX_PLOG_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_list.h"
#include "src/class/pmix_pointer_array.h"
#include "src/threads/threads.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/mca/plog/plog.h"


BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_plog_base_framework;
/**
 * PLOG select function
 *
 * Cycle across available components and construct the array
 * of active modules
 */
PMIX_EXPORT pmix_status_t pmix_plog_base_select(void);

/**
 * Track an active component / module
 */
struct pmix_plog_base_active_module_t {
    pmix_list_item_t super;
    bool reqd;
    bool added;
    int pri;
    pmix_plog_module_t *module;
    pmix_plog_base_component_t *component;
};
typedef struct pmix_plog_base_active_module_t pmix_plog_base_active_module_t;
PMIX_CLASS_DECLARATION(pmix_plog_base_active_module_t);


/* framework globals */
struct pmix_plog_globals_t {
    pmix_lock_t lock;
    pmix_pointer_array_t actives;
    bool initialized;
    char **channels;
};
typedef struct pmix_plog_globals_t pmix_plog_globals_t;

PMIX_EXPORT extern pmix_plog_globals_t pmix_plog_globals;

PMIX_EXPORT pmix_status_t pmix_plog_base_log(const pmix_proc_t *source,
                                             const pmix_info_t data[], size_t ndata,
                                             const pmix_info_t directives[], size_t ndirs,
                                             pmix_op_cbfunc_t cbfunc, void *cbdata);

END_C_DECLS

#endif

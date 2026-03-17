/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 *
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * This interface is for use by PMIx servers to obtain network-related info
 * such as security keys that need to be shared across applications, and to
 * setup network support for applications prior to launch
 *
 * Available plugins may be defined at runtime via the typical MCA parameter
 * syntax.
 */

#ifndef PMIX_PLOG_H
#define PMIX_PLOG_H

#include <src/include/pmix_config.h>
#include "pmix_common.h"

#include "src/class/pmix_list.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/include/pmix_globals.h"

BEGIN_C_DECLS

/******    MODULE DEFINITION    ******/

/**
 * Initialize the module. Returns an error if the module cannot
 * run, success if it can and wants to be used.
 */
typedef pmix_status_t (*pmix_plog_base_module_init_fn_t)(void);


/**
 * Finalize the module
 */
typedef void (*pmix_plog_base_module_fini_fn_t)(void);

/**
 * Log data to channel, if possible
 */
typedef pmix_status_t (*pmix_plog_base_module_log_fn_t)(const pmix_proc_t *source,
                                                        const pmix_info_t data[], size_t ndata,
                                                        const pmix_info_t directives[], size_t ndirs,
                                                        pmix_op_cbfunc_t cbfunc, void *cbdata);

/**
 * Base structure for a PLOG module
 */
typedef struct {
    char *name;
    char **channels;
    /* init/finalize */
    pmix_plog_base_module_init_fn_t     init;
    pmix_plog_base_module_fini_fn_t     finalize;
    pmix_plog_base_module_log_fn_t      log;
} pmix_plog_module_t;

/**
 * Base structure for a PLOG API
 */
typedef struct {
    pmix_plog_base_module_log_fn_t      log;
} pmix_plog_API_module_t;


/* declare the global APIs */
PMIX_EXPORT extern pmix_plog_API_module_t pmix_plog;

/*
 * the standard component data structure
 */
struct pmix_plog_base_component_t {
    pmix_mca_base_component_t                        base;
    pmix_mca_base_component_data_t                   data;
};
typedef struct pmix_plog_base_component_t pmix_plog_base_component_t;

/*
 * Macro for use in components that are of type plog
 */
#define PMIX_PLOG_BASE_VERSION_1_0_0 \
    PMIX_MCA_BASE_VERSION_1_0_0("plog", 1, 0, 0)

END_C_DECLS

#endif

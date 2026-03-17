/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * memchecker (memory checker) framework component interface.
 *
 * Intent
 *
 * This is a very thin framework to abstract memory checking tools,
 * such as valgrind and possibly Sun rtc (memory checking available
 * possibly only under Solaris/Sparc).
 *
 * Currently, only functionality for hiding and unhiding of memory
 * is added; further functions provided by the memory checker/api
 * checker could be added, however, this comes (at least for valgrind)
 * with considerable overhead.
 * One possible option would be to have error_print_callbacks, that
 * output different error messages, depending on the memory location
 * being hit by certain error.
 */

#ifndef OPAL_MCA_MEMCHECKER_MEMCHECKER_H
#define OPAL_MCA_MEMCHECKER_MEMCHECKER_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

/**
 * Module initialization function.  Should return OPAL_SUCCESS.
 */
typedef int (*opal_memchecker_base_module_init_1_0_0_fn_t)(void);

/**
 * Module function to query, whether we're under the memory
 * checking program, like valgrind
 */
typedef int (*opal_memchecker_base_module_runindebugger_fn_t)(void);

/**
 * Module function to check, whether memory region is addressable
 */
typedef int (*opal_memchecker_base_module_isaddressable_fn_t)(void * p, size_t len);

/**
 * Module function to check, whether memory region is defined
 */
typedef int (*opal_memchecker_base_module_isdefined_fn_t)(void * p, size_t len);

/**
 * Module function to set memory region to not accessible
 */
typedef int (*opal_memchecker_base_module_mem_noaccess_fn_t)(void * p, size_t len);

/**
 * Module function to set memory region to undefined
 */
typedef int (*opal_memchecker_base_module_mem_undefined_fn_t)(void * p, size_t len);

/**
 * Module function to set memory region to defined
 */
typedef int (*opal_memchecker_base_module_mem_defined_fn_t)(void * p, size_t len);

/**
 * Module function to set memory region to defined, but only if addressable
 */
typedef int (*opal_memchecker_base_module_mem_defined_if_addressable_fn_t)(void * p, size_t len);

/**
 * Module function name a specific memory region
 */
typedef int (*opal_memchecker_base_module_create_block_fn_t)(void * p, size_t len, char * description);

/**
 * Module function to discard a named memory region
 */
typedef int (*opal_memchecker_base_module_discard_block_fn_t)(void * p); /* Here, we need to do some mapping for valgrind */

/**
 * Module function to check for any leaks
 */
typedef int (*opal_memchecker_base_module_leakcheck_fn_t)(void);

/**
 * Module function to get vbits
 */
typedef int (*opal_memchecker_base_module_get_vbits_fn_t)(void * p, char * vbits, size_t len);

/**
 * Module function to set vbits
 */
typedef int (*opal_memchecker_base_module_set_vbits_fn_t)(void * p, char * vbits, size_t len);



/**
 * Structure for memchecker components.
 */
struct opal_memchecker_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;
};

/**
 * Convenience typedef
 */
typedef struct opal_memchecker_base_component_2_0_0_t opal_memchecker_base_component_2_0_0_t;
typedef struct opal_memchecker_base_component_2_0_0_t opal_memchecker_base_component_t;

/**
 * Structure for memchecker modules
 */
struct opal_memchecker_base_module_1_0_0_t {
    /** Module initialization function */
    opal_memchecker_base_module_init_1_0_0_fn_t init;

    /** Module function to check, whether we are executed by memory debugger */
    opal_memchecker_base_module_runindebugger_fn_t runindebugger;

    /** Module function to check, whether memory region is addressable */
    opal_memchecker_base_module_isaddressable_fn_t isaddressable;

    /** Module function to check, whether memory region is defined */
    opal_memchecker_base_module_isdefined_fn_t isdefined;

    /** Module function to set memory region to not accessible */
    opal_memchecker_base_module_mem_noaccess_fn_t mem_noaccess;

    /** Module function to set memory region to undefined */
    opal_memchecker_base_module_mem_undefined_fn_t mem_undefined;

    /** Module function to set memory region to defined */
    opal_memchecker_base_module_mem_defined_fn_t mem_defined;

    /** Module function to set memory region to defined, but only if addressable */
    opal_memchecker_base_module_mem_defined_if_addressable_fn_t mem_defined_if_addressable;

    /** Module function name a specific memory region */
    opal_memchecker_base_module_create_block_fn_t create_block;

    /** Module function to discard a named memory region */
    opal_memchecker_base_module_discard_block_fn_t discard_block;

    /** Module function to check for any leaks */
    opal_memchecker_base_module_leakcheck_fn_t leakcheck;

    /** Module function to get vbits */
    opal_memchecker_base_module_get_vbits_fn_t get_vbits;

    /** Module function to set vbits */
    opal_memchecker_base_module_set_vbits_fn_t set_vbits;
};

/**
 * Convenience typedef
 */
typedef struct opal_memchecker_base_module_1_0_0_t opal_memchecker_base_module_1_0_0_t;
typedef struct opal_memchecker_base_module_1_0_0_t opal_memchecker_base_module_t;


/**
 * Macro for use in components that are of type memchecker
 */
#define OPAL_MEMCHECKER_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("memchecker", 2, 0, 0)

#endif /* OPAL_MCA_MEMCHECKER_MEMCHECKER_H */

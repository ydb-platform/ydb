/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/* NOTE: This framework accomodates two kinds of memory hooks systems:

   1. Those that only require being setup once and then will
      automatically call back to internal component/module functions
      as required (e.g., the ptmalloc2 component).  It is expected
      that such components will have a NULL value for memoryc_process
      and empty (base) implementations of memoryc_register and
      memoryc_deregister.

   2. Those that must be queried periodically to find out if something
      has happened to the registered memory mappings (e.g., the
      ummunot component).  It is expected that such components will
      have complete implementations for memoryc_process,
      memoryc_register, and memoryc_deregister.

   There will only be one of these components compiled in to Open MPI
   (it will be compiled in libopen-pal; it will never be a DSO).  so
   the componente will rule "yes, I can run" or "no, I cannot run".
   If it elects not to run, then there is no memory manager support in
   this process.

   Because there will only be one memory manager component in the
   process, there is no need for a separate module structure --
   everything is in the component for simplicity.

   Note that there is an interface relevant to this framework that is
   not described in this file: the opal_memory_changed() macro.  This
   macro should return 1 if an asynchronous agent has noticed that
   memory mappings have changed.  The macro should be as cheap/fast as
   possible (which is why it's a macro).  If it returns 1, the
   memoryc_process() function pointer (below), will be invoked.  If
   the component does not provide this macro, then a default macro is
   used that always returns 0 and therefore memoryc_process() will
   never be invoked (and can legally be NULL).
*/

#ifndef OPAL_MCA_MEMORY_MEMORY_H
#define OPAL_MCA_MEMORY_MEMORY_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"

BEGIN_C_DECLS

/**
 * Prototype for doing something once memory changes have been
 * detected.  This function is assumed to do whatever is necessary to
 * a) figured out what has changed, and b) adjust OMPI as relevant
 * (e.g., call opal_mem_hooks_release_hook).  It only needs to be
 * supplied for components that provide opal_memory_changed() macros
 * that can return non-zero.
 */
typedef int (*opal_memory_base_component_process_fn_t)(void);

/**
 * Prototype for a function that is invoked when the memory base is
 * trying to select a component. This funtionality is required.
 */
typedef int (*opal_memory_base_component_query_fn_t)(int *priority);

/**
 * Prototype for a function that is invoked when Open MPI starts to
 * "care" about a specific memory region.  That is, Open MPI declares
 * that it wants to be notified if the memory mapping for this region
 * changes.
 *
 * If a component does not want/need to provide this functionality, it
 * can use the value opal_memory_base_register_empty (an empty
 * implementation of this function).
 */
typedef int (*opal_memory_base_component_register_fn_t)(void *base,
                                                        size_t len,
                                                        uint64_t cookie);


/**
 * Prototype for a function that is the opposite of
 * opal_memory_base_component_register_fn_t: this function is invoked
 * when Open MPI stops to "caring" about a specific memory region.
 * That is, Open MPI declares that it no longer wants to be notified
 * if the memory mapping for this region changes.
 *
 * The parameters of this function will exactly match parameters that
 * were previously passed to the call to
 * opal_memory_base_component_register_fn_t.
 *
 * If a component does not want/need to provide this functionality, it
 * can use the value opal_memory_base_deregister_empty (an empty
 * implementation of this function).
 */
typedef int (*opal_memory_base_component_deregister_fn_t)(void *base,
                                                          size_t len,
                                                          uint64_t cookie);


/**
 * Prototype for a function that set the memory alignment
 */
typedef void (*opal_memory_base_component_set_alignment_fn_t)(int use_memalign,
                                                              size_t memalign_threshold);

/**
 * Function to be called when initializing malloc hooks
 */
typedef void (*opal_memory_base_component_init_hook_fn_t)(void);

/**
 * Structure for memory components.
 */
typedef struct opal_memory_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t memoryc_version;
    /** MCA base data */
    mca_base_component_data_t memoryc_data;

    opal_memory_base_component_query_fn_t memoryc_query;

    /** This function will be called when the malloc hooks are
     * initialized. It may be NULL if no hooks are needed. */
    opal_memory_base_component_init_hook_fn_t memoryc_init_hook;

    /** Function to call when something has changed, as indicated by
        opal_memory_changed().  Will be ignored if the component does
        not provide an opal_memory_changed() macro that returns
        nonzero.  */
    opal_memory_base_component_process_fn_t memoryc_process;

    /** Function invoked when Open MPI starts "caring" about a
        specific memory region */
    opal_memory_base_component_register_fn_t memoryc_register;
    /** Function invoked when Open MPI stops "caring" about a
        specific memory region */
    opal_memory_base_component_deregister_fn_t memoryc_deregister;
    /** Function invoked in order to set malloc'ed memory alignment */
    opal_memory_base_component_set_alignment_fn_t memoryc_set_alignment;
} opal_memory_base_component_2_0_0_t;

OPAL_DECLSPEC extern opal_memory_base_component_2_0_0_t *opal_memory;

END_C_DECLS

/*
 * Macro for use in components that are of type memory
 */
#define OPAL_MEMORY_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("memory", 2, 0, 0)

#endif /* OPAL_MCA_MEMORY_MEMORY_H */

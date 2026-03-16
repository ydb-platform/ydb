/*
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_MEMCPY_BASE_MEMORY_BASE_DEFAULT_H
#define OPAL_MCA_MEMCPY_BASE_MEMORY_BASE_DEFAULT_H

#include "opal_config.h"

/**
 * Provided for memory components that don't provide an asynchronous
 * method for determining if memory mappings have changed.  If a
 * component only has a synchronous / interactive method of checking,
 * then it can override this macro with some logic to do a (hopefully)
 * cheap check to see if memory mappings have changed.  The intent is
 * that if this cheap check returns true, the upper layer will then
 * invoke the memoryc_process() function to actually process what
 * changed.  This function will be invoked by the upper layer with the
 * syntax:
 *
 * if (opal_memory_changed()) { ... }
 *
 * Hence, if you need any kind of sophisticated logic, you might want
 * to put it in an inline function and have the #define call the
 * inline function.
 */
#define opal_memory_changed() 0

BEGIN_C_DECLS

/**
 * Default (empty) implementation of the memoryc_register function.
 *
 * See opal/mca/memory/memory.h for a description of the parameters.
 */
OPAL_DECLSPEC int opal_memory_base_component_register_empty(void *start,
                                                            size_t len,
                                                            uint64_t cookie);

/**
 * Default (empty) implementation of the memoryc_deregister function
 *
 * See opal/mca/memory/memory.h for a description of the parameters.
 */
OPAL_DECLSPEC int opal_memory_base_component_deregister_empty(void *start,
                                                              size_t len,
                                                              uint64_t cookie);

/**
 * Default (empty) implementation of the memoryc_set_alignment function
 *
 * See opal/mca/memory/memory.h for a description of the parameters.
 */
OPAL_DECLSPEC void opal_memory_base_component_set_alignment_empty(int use_memalign,
                                                                  size_t memalign_threshold);


END_C_DECLS

#endif

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
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/mca/memory/base/empty.h"


int opal_memory_base_component_register_empty(void *base, size_t len,
                                              uint64_t cookie)
{
    return OPAL_SUCCESS;
}


int opal_memory_base_component_deregister_empty(void *base, size_t len,
                                                uint64_t cookie)
{
    return OPAL_SUCCESS;
}

void opal_memory_base_component_set_alignment_empty(int use_memalign,
                                                    size_t memalign_threshold)
{
}


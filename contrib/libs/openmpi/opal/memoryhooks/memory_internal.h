/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MEMORY_MEMORY_INTERNAL_H
#define OPAL_MEMORY_MEMORY_INTERNAL_H

#include "opal_config.h"
#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif

/* see memory.h for meaning */
#define OPAL_MEMORY_FREE_SUPPORT   0x0001
#define OPAL_MEMORY_MUNMAP_SUPPORT 0x0002
#define OPAL_MEMORY_CHUNK_SUPPORT  0x0004

BEGIN_C_DECLS

OPAL_DECLSPEC void opal_mem_hooks_set_support(int support);

OPAL_DECLSPEC void opal_mem_hooks_release_hook(void *buf, size_t length, bool from_alloc);

END_C_DECLS


#endif /* OPAL_MEMORY_MEMORY_INTERNAL_H */

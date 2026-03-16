/*
 * Copyright (c) 2011-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * Copyright (c) 2016-2019 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * this file is included in the rest of
 * the code base via pmix/hwloc/hwloc-internal.h.
 */

#ifndef PMIX_HWLOC_INTERNAL_H
#define PMIX_HWLOC_INTERNAL_H


#include <src/include/pmix_config.h>
#include <pmix_common.h>

#if PMIX_HAVE_HWLOC
#include PMIX_HWLOC_HEADER

#if HWLOC_API_VERSION < 0x00010b00
#define HWLOC_OBJ_NUMANODE HWLOC_OBJ_NODE
#define HWLOC_OBJ_PACKAGE HWLOC_OBJ_SOCKET
#endif

extern hwloc_topology_t pmix_hwloc_topology;
#endif

BEGIN_C_DECLS

typedef enum {
    VM_HOLE_NONE = -1,
    VM_HOLE_BEGIN = 0,        /* use hole at the very beginning */
    VM_HOLE_AFTER_HEAP = 1,   /* use hole right after heap */
    VM_HOLE_BEFORE_STACK = 2, /* use hole right before stack */
    VM_HOLE_BIGGEST = 3,      /* use biggest hole */
    VM_HOLE_IN_LIBS = 4,      /* use biggest hole between heap and stack */
    VM_HOLE_CUSTOM = 5,       /* use given address if available */
} pmix_hwloc_vm_hole_kind_t;

typedef enum {
    VM_MAP_FILE = 0,
    VM_MAP_ANONYMOUS = 1,
    VM_MAP_HEAP = 2,
    VM_MAP_STACK = 3,
    VM_MAP_OTHER = 4 /* vsyscall/vdso/vvar shouldn't occur since we stop after stack */
} pmix_hwloc_vm_map_kind_t;

PMIX_EXPORT pmix_status_t pmix_hwloc_get_topology(pmix_info_t *info, size_t ninfo);
PMIX_EXPORT void pmix_hwloc_cleanup(void);

END_C_DECLS

#endif /* PMIX_HWLOC_INTERNAL_H */

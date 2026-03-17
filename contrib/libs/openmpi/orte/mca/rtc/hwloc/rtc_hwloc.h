/*
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef ORTE_RTC_HWLOC_H
#define ORTE_RTC_HWLOC_H

#include "orte_config.h"

#include "orte/mca/rtc/rtc.h"

BEGIN_C_DECLS

typedef enum {
    VM_HOLE_NONE = -1,
    VM_HOLE_BEGIN = 0,        /* use hole at the very beginning */
    VM_HOLE_AFTER_HEAP = 1,   /* use hole right after heap */
    VM_HOLE_BEFORE_STACK = 2, /* use hole right before stack */
    VM_HOLE_BIGGEST = 3,      /* use biggest hole */
    VM_HOLE_IN_LIBS = 4,      /* use biggest hole between heap and stack */
    VM_HOLE_CUSTOM = 5,       /* use given address if available */
} orte_rtc_hwloc_vm_hole_kind_t;

typedef enum {
    VM_MAP_FILE = 0,
    VM_MAP_ANONYMOUS = 1,
    VM_MAP_HEAP = 2,
    VM_MAP_STACK = 3,
    VM_MAP_OTHER = 4 /* vsyscall/vdso/vvar shouldn't occur since we stop after stack */
} orte_rtc_hwloc_vm_map_kind_t;

typedef struct {
    orte_rtc_base_component_t super;
    orte_rtc_hwloc_vm_hole_kind_t kind;
} orte_rtc_hwloc_component_t;

ORTE_MODULE_DECLSPEC extern orte_rtc_hwloc_component_t mca_rtc_hwloc_component;

extern orte_rtc_base_module_t orte_rtc_hwloc_module;


END_C_DECLS

#endif /* ORTE_RTC_HWLOC_H */

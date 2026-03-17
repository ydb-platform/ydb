/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef OPAL_PATCHER_BASE_H
#define OPAL_PATCHER_BASE_H

#include "opal_config.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/patcher/patcher.h"


BEGIN_C_DECLS

#define MCA_BASE_PATCHER_MAX_PATCH 32

struct mca_patcher_base_patch_t;

typedef void (*mca_patcher_base_restore_fn_t) (struct mca_patcher_base_patch_t *);

struct mca_patcher_base_patch_t {
    /** patches are list items */
    opal_list_item_t super;
    /** name symbol to patch */
    char            *patch_symbol;
    /** address of function to call instead */
    uintptr_t        patch_value;
    /** original address of function */
    uintptr_t        patch_orig;
    /** patch data */
    unsigned char    patch_data[MCA_BASE_PATCHER_MAX_PATCH];
    /** original data */
    unsigned char    patch_orig_data[MCA_BASE_PATCHER_MAX_PATCH];
    /** size of patch data */
    unsigned         patch_data_size;
    /** function to undo the patch */
    mca_patcher_base_restore_fn_t patch_restore;
};

typedef struct mca_patcher_base_patch_t mca_patcher_base_patch_t;

OBJ_CLASS_DECLARATION(mca_patcher_base_patch_t);

/**
 * Framework struct declaration for this framework
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_patcher_base_framework;
OPAL_DECLSPEC int opal_patcher_base_select (void);
OPAL_DECLSPEC int mca_patcher_base_patch_hook (mca_patcher_base_module_t *module, uintptr_t hook);
OPAL_DECLSPEC void mca_base_patcher_patch_apply_binary (mca_patcher_base_patch_t *patch);

static inline uintptr_t mca_patcher_base_addr_text (uintptr_t addr) {
#if (OPAL_ASSEMBLY_ARCH == OPAL_POWERPC64) && (!defined (_CALL_ELF) || (_CALL_ELF != 2))
    struct odp_t {
        uintptr_t text;
        uintptr_t toc;
    } *odp = (struct odp_t *) addr;
    return (odp)?odp->text:0;
#else
    return addr;
#endif
}

END_C_DECLS
#endif /* OPAL_BASE_PATCHER_H */

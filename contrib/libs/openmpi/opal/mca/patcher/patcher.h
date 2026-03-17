/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_PATCHER_PATCHER_H
#define OPAL_MCA_PATCHER_PATCHER_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/class/opal_list.h"

/* Any function being patched in as a hook must use SYMBOLPATCH_BEGIN at the top,
 * and SYMBOLPATCH_END before it returns (this is just for PPC). */

#if (OPAL_ASSEMBLY_ARCH == OPAL_POWERPC64)

/* special processing for ppc64 to save and restore TOC (r2)
 * Reference: "64-bit PowerPC ELF Application Binary Interface Supplement 1.9" */
#define OPAL_PATCHER_BEGIN \
    unsigned long toc_save; \
    asm volatile ("std 2, %0" : "=m" (toc_save)); \
    asm volatile ("nop; nop; nop; nop; nop");
#define OPAL_PATCHER_END \
    asm volatile ("ld  2, %0" : : "m" (toc_save));

#else /* !__PPC64__ */

#define OPAL_PATCHER_BEGIN
#define OPAL_PATCHER_END

#endif

/**
 * Make any calls to the named function redirect to a new function
 *
 * @param[in] func_symbol_name  function to hook
 * @param[in] func_new_addr     function pointer of hook
 * @param[out] func_old_addr    address of func_symbol_name
 *
 * This function redirects all calls to the function func_symbol_name to
 * the function pointer func_new_addr. If it is possible for the hook
 * function to call the original function the patcher module will return
 * the old function's address in func_old_addr.
 */
typedef int (*mca_patcher_base_patch_symbol_fn_t)(const char *func_symbol_name, uintptr_t func_new_addr,
                                                   uintptr_t *func_old_addr);

/**
 * Make any calls to a function redirect to a new function
 *
 * @param[in] func_symbol_name  function to hook
 * @param[in] func_new_addr     function pointer of hook
 * @param[out] func_old_addr    address of func_symbol_name
 *
 * This function redirects all calls to the function at func_addr to
 * the function pointer func_new_addr.
 */
typedef int (*mca_patcher_base_patch_address_fn_t)(uintptr_t func_addr, uintptr_t func_new_addr);

/**
 * Set up the patcher module
 */
typedef int (*mca_patcher_base_init_fn_t) (void);

/**
 * Finalize the patcher module
 */
typedef int (*mca_patcher_base_fini_fn_t) (void);

/**
 * Structure for patcher modules.
 */
typedef struct mca_patcher_base_module_t {
    mca_base_module_t super;
    /** list of patches */
    opal_list_t                         patch_list;
    /** lock for patch list */
    opal_mutex_t                        patch_list_mutex;
    /** function to call if the patcher module is used. can
     * be NULL. */
    mca_patcher_base_init_fn_t          patch_init;
    /** function to call when patcher is unloaded. this function
     * MUST clean up all active patches. can be NULL. */
    mca_patcher_base_fini_fn_t          patch_fini;
    /** hook a symbol. may be NULL */
    mca_patcher_base_patch_symbol_fn_t  patch_symbol;
    /** hook a function pointer. may be NULL */
    mca_patcher_base_patch_address_fn_t patch_address;
} mca_patcher_base_module_t;


OPAL_DECLSPEC extern mca_patcher_base_module_t *opal_patcher;

/**
 * Structure for patcher components.
 */
typedef struct mca_patcher_base_component_1_0_0_t {
    /** MCA base component */
    mca_base_component_t patcherc_version;
    /** MCA base data */
    mca_base_component_data_t patcherc_data;
} mca_patcher_base_component_1_0_0_t;

typedef mca_patcher_base_component_1_0_0_t mca_patcher_base_component_t;

/*
 * Macro for use in components that are of type patcher
 */
#define OPAL_PATCHER_BASE_VERSION_1_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("patcher", 1, 0, 0)

#endif /* OPAL_MCA_PATCHER_PATCHER_H */

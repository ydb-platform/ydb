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
/**
 * @file pather_overwrite.h
 *
 * This component works by overwritting the first couple instructions in
 * the target function with a jump instruction to the hook function. The
 * hook function will be expected to implement the functionality of the
 * hooked function when using this module.
 *
 * Note: This component only supports x86, x86_64, ia64, and powerpc/power.
 */

#if !defined(OPAL_PATCHER_OVERWRITE_H)
#define OPAL_PATCHER_OVERWRITE_H

#include "opal_config.h"
#include "opal/mca/patcher/patcher.h"
#include "opal/class/opal_list.h"

extern mca_patcher_base_module_t mca_patcher_overwrite_module;
extern mca_patcher_base_component_t mca_patcher_overwrite_component;

#endif /* !defined(OPAL_PATCHER_OVERWRITE_H) */

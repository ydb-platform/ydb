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
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_MEM_BASE_H
#define MCA_MEM_BASE_H

#include "opal_config.h"

#include "opal/class/opal_list.h"
#include "opal/mca/base/base.h"
#include "opal/mca/mpool/mpool.h"

BEGIN_C_DECLS

struct mca_mpool_base_selected_module_t {
    opal_list_item_t super;
    mca_mpool_base_component_t *mpool_component;
    mca_mpool_base_module_t *mpool_module;
};
typedef struct mca_mpool_base_selected_module_t mca_mpool_base_selected_module_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_mpool_base_selected_module_t);

/*
 * Data structures for the tree of allocated memory
 */

/*
 * Global functions for MCA: overall mpool open and close
 */

OPAL_DECLSPEC mca_mpool_base_component_t* mca_mpool_base_component_lookup(const char* name);
OPAL_DECLSPEC mca_mpool_base_module_t* mca_mpool_base_module_lookup(const char* name);

OPAL_DECLSPEC mca_mpool_base_module_t *mca_mpool_basic_create (void *base, size_t size, unsigned min_align);

/*
 * Globals
 */
extern opal_list_t mca_mpool_base_modules;
extern mca_mpool_base_module_t *mca_mpool_base_default_module;
extern int mca_mpool_base_default_priority;


OPAL_DECLSPEC extern mca_base_framework_t opal_mpool_base_framework;

END_C_DECLS

#endif /* MCA_MEM_BASE_H */

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_BML_BASE_H
#define MCA_BML_BASE_H

#include "ompi_config.h"

#include "ompi/mca/mca.h"
#include "opal/mca/base/mca_base_framework.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/proc/proc.h"


/*
 * Global functions for the BML
 */

BEGIN_C_DECLS

struct mca_bml_base_selected_module_t {
    opal_list_item_t super;
    mca_bml_base_component_t *bml_component;
    mca_bml_base_module_t *bml_module;
};
typedef struct mca_bml_base_selected_module_t mca_bml_base_selected_module_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_bml_base_selected_module_t);

/*
 * Global functions for MCA: overall BTL open and close
 */

OMPI_DECLSPEC  int mca_bml_base_init(bool enable_progress_threads,
                                     bool enable_mpi_threads);
OMPI_DECLSPEC  bool mca_bml_base_inited(void);

OMPI_DECLSPEC  int mca_bml_base_ft_event(int state);


/*
 * Globals
 */
OMPI_DECLSPEC extern mca_bml_base_component_t mca_bml_component;
OMPI_DECLSPEC extern mca_bml_base_module_t mca_bml;
OMPI_DECLSPEC extern mca_base_framework_t ompi_bml_base_framework;
OMPI_DECLSPEC extern opal_mutex_t mca_bml_lock;

static inline struct mca_bml_base_endpoint_t *mca_bml_base_get_endpoint (struct ompi_proc_t *proc) {
    if (OPAL_UNLIKELY(NULL == proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML])) {
        OPAL_THREAD_LOCK(&mca_bml_lock);
        if (NULL == proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]) {
            mca_bml.bml_add_proc (proc);
        }
        OPAL_THREAD_UNLOCK(&mca_bml_lock);
    }

    return (struct mca_bml_base_endpoint_t *) proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML];
}


END_C_DECLS
#endif /* MCA_BML_BASE_H */

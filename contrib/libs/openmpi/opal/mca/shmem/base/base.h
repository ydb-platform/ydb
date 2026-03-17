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
 * Copyright (c) 2007-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_SHMEM_BASE_H
#define OPAL_SHMEM_BASE_H

#include "opal_config.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/shmem/shmem.h"

BEGIN_C_DECLS

/* ////////////////////////////////////////////////////////////////////////// */
/* Public API for the shmem framework */
/* ////////////////////////////////////////////////////////////////////////// */
OPAL_DECLSPEC int
opal_shmem_segment_create(opal_shmem_ds_t *ds_buf,
                          const char *file_name,
                          size_t size);

OPAL_DECLSPEC int
opal_shmem_ds_copy(const opal_shmem_ds_t *from,
                   opal_shmem_ds_t *to);

OPAL_DECLSPEC void *
opal_shmem_segment_attach(opal_shmem_ds_t *ds_buf);

OPAL_DECLSPEC int
opal_shmem_segment_detach(opal_shmem_ds_t *ds_buf);

OPAL_DECLSPEC int
opal_shmem_unlink(opal_shmem_ds_t *ds_buf);
/* ////////////////////////////////////////////////////////////////////////// */
/* End Public API for the shmem framework */
/* ////////////////////////////////////////////////////////////////////////// */

/*
 * Global functions for MCA overall shmem open and close
 */

/**
 * returns the name of the best, runnable shmem component.  the caller is
 * responsible for freeing returned resources.
 *
 * @retval name of best component.  NULL if no component is found.
 *
 * see: orte/mca/odls/base/odls_base_default_fns.c
 */
OPAL_DECLSPEC char *
opal_shmem_base_best_runnable_component_name(void);

/**
 * Select an available component.
 *
 * @return OPAL_SUCCESS Upon success.
 * @return OPAL_NOT_FOUND If no component can be selected.
 * @return OPAL_ERROR Upon other failure.
 *
 * This function invokes the selection process for shmem components,
 * which works as follows:
 *
 * - If the \em shmem MCA parameter is not specified, the
 *   selection set is all available shmem components.
 * - If the \em shmem MCA parameter is specified, the
 *   selection set is just that component.
 * - All components in the selection set are queried to see if
 *   they want to run.  All components that want to run are ranked
 *   by their priority and the highest priority component is
 *   selected.  All non-selected components have their "close"
 *   function invoked to let them know that they were not selected.
 * - The selected component will have its "init" function invoked to
 *   let it know that it was selected.
 *
 * If we fall through this entire process and no component is
 * selected, then return OPAL_NOT_FOUND (this is not a fatal
 * error).
 *
 * At the end of this process, we'll either have a single
 * component that is selected and initialized, or no component was
 * selected.  If no component was selected, subsequent invocation
 * of the shmem wrapper functions will return an error.
 */
OPAL_DECLSPEC int
opal_shmem_base_select(void);

/**
 * Shut down the shmem MCA framework.
 *
 * @retval OPAL_SUCCESS Always
 *
 * This function shuts down everything in the shmem MCA
 * framework, and is called during opal_finalize().
 *
 * It must be the last function invoked on the shmem MCA
 * framework.
 */
OPAL_DECLSPEC int
opal_shmem_base_close(void);

/**
 * Indication of whether a component was successfully selected or
 * not
 */
OPAL_DECLSPEC extern bool opal_shmem_base_selected;

/**
 * Global component struct for the selected component
 */
OPAL_DECLSPEC extern opal_shmem_base_component_t *opal_shmem_base_component;

/**
 * Global module struct for the selected module
 */
OPAL_DECLSPEC extern opal_shmem_base_module_t *opal_shmem_base_module;

/**
 * Runtime hint
 */
OPAL_DECLSPEC extern char *opal_shmem_base_RUNTIME_QUERY_hint;

/**
 * Framework structure declaration
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_shmem_base_framework;

END_C_DECLS

#endif /* OPAL_BASE_SHMEM_H */

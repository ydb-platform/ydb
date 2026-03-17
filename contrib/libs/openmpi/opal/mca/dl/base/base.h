/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_DL_BASE_H
#define OPAL_DL_BASE_H

#include "opal_config.h"
#include "opal/mca/dl/dl.h"
#include "opal/util/opal_environ.h"
#include "opal/runtime/opal_cr.h"

#include "opal/mca/base/base.h"


BEGIN_C_DECLS

/**
 * Globals
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_dl_base_framework;
OPAL_DECLSPEC extern opal_dl_base_component_t
*opal_dl_base_selected_component;
OPAL_DECLSPEC extern opal_dl_base_module_t *opal_dl;


/**
 * Initialize the DL MCA framework
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR   Upon failures
 *
 * This function is invoked during opal_init();
 */
OPAL_DECLSPEC int opal_dl_base_open(mca_base_open_flag_t flags);

/**
 * Select an available component.
 *
 * @retval OPAL_SUCCESS Upon Success
 * @retval OPAL_NOT_FOUND If no component can be selected
 * @retval OPAL_ERROR Upon other failure
 *
 */
OPAL_DECLSPEC int opal_dl_base_select(void);

/**
 * Finalize the DL MCA framework
 *
 * @retval OPAL_SUCCESS Upon success
 * @retval OPAL_ERROR   Upon failures
 *
 * This function is invoked during opal_finalize();
 */
OPAL_DECLSPEC int opal_dl_base_close(void);

/**
 * Open a DSO
 *
 * (see opal_dl_base_module_open_ft_t in opal/mca/dl/dl.h for
 * documentation of this function)
 */
OPAL_DECLSPEC int opal_dl_open(const char *fname,
                               bool use_ext, bool private_namespace,
                               opal_dl_handle_t **handle, char **err_msg);

/**
 * Lookup a symbol in a DSO
 *
 * (see opal_dl_base_module_lookup_ft_t in opal/mca/dl/dl.h for
 * documentation of this function)
 */
OPAL_DECLSPEC int opal_dl_lookup(opal_dl_handle_t *handle,
                                 const char *symbol,
                                 void **ptr, char **err_msg);

/**
 * Close a DSO
 *
 * (see opal_dl_base_module_close_ft_t in opal/mca/dl/dl.h for
 * documentation of this function)
 */
OPAL_DECLSPEC int opal_dl_close(opal_dl_handle_t *handle);

/**
 * Iterate over files in a path
 *
 * (see opal_dl_base_module_foreachfile_ft_t in opal/mca/dl/dl.h for
 * documentation of this function)
 */
OPAL_DECLSPEC int opal_dl_foreachfile(const char *search_path,
                                      int (*cb_func)(const char *filename,
                                                     void *context),
                                      void *context);

END_C_DECLS

#endif /* OPAL_DL_BASE_H */

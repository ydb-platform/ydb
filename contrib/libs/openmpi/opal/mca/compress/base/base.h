/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OPAL_COMPRESS_BASE_H
#define OPAL_COMPRESS_BASE_H

#include "opal_config.h"
#include "opal/mca/compress/compress.h"
#include "opal/util/opal_environ.h"
#include "opal/runtime/opal_cr.h"

#include "opal/mca/base/base.h"

/*
 * Global functions for MCA overall COMPRESS
 */

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

    /**
     * Initialize the COMPRESS MCA framework
     *
     * @retval OPAL_SUCCESS Upon success
     * @retval OPAL_ERROR   Upon failures
     *
     * This function is invoked during opal_init();
     */
    OPAL_DECLSPEC int opal_compress_base_open(mca_base_open_flag_t flags);

    /**
     * Select an available component.
     *
     * @retval OPAL_SUCCESS Upon Success
     * @retval OPAL_NOT_FOUND If no component can be selected
     * @retval OPAL_ERROR Upon other failure
     *
     */
    OPAL_DECLSPEC int opal_compress_base_select(void);

    /**
     * Finalize the COMPRESS MCA framework
     *
     * @retval OPAL_SUCCESS Upon success
     * @retval OPAL_ERROR   Upon failures
     *
     * This function is invoked during opal_finalize();
     */
    OPAL_DECLSPEC int opal_compress_base_close(void);

    /**
     * Globals
     */
    OPAL_DECLSPEC extern mca_base_framework_t opal_compress_base_framework;
    OPAL_DECLSPEC extern opal_compress_base_component_t opal_compress_base_selected_component;
    OPAL_DECLSPEC extern opal_compress_base_module_t opal_compress;

    /**
     *
     */
    OPAL_DECLSPEC int opal_compress_base_tar_create(char ** target);
    OPAL_DECLSPEC int opal_compress_base_tar_extract(char ** target);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif /* OPAL_COMPRESS_BASE_H */

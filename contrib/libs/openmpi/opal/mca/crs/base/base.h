/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 *
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OPAL_CRS_BASE_H
#define OPAL_CRS_BASE_H

#include "opal_config.h"
#include "opal/mca/base/base.h"
#include "opal/mca/crs/crs.h"
#include "opal/util/opal_environ.h"
#include "opal/runtime/opal_cr.h"

/*
 * Global functions for MCA overall CRS
 */

BEGIN_C_DECLS

/* Some local strings to use genericly with the local metadata file */
#define CRS_METADATA_BASE       ("# ")
#define CRS_METADATA_COMP       ("# OPAL CRS Component: ")
#define CRS_METADATA_PID        ("# PID: ")
#define CRS_METADATA_CONTEXT    ("# CONTEXT: ")
#define CRS_METADATA_MKDIR      ("# MKDIR: ")
#define CRS_METADATA_TOUCH      ("# TOUCH: ")

    /**
     * Initialize the CRS MCA framework
     *
     * @retval OPAL_SUCCESS Upon success
     * @retval OPAL_ERROR   Upon failures
     *
     * This function is invoked during opal_init();
     */
    OPAL_DECLSPEC int opal_crs_base_open(mca_base_open_flag_t flags);

    /**
     * Select an available component.
     *
     * @retval OPAL_SUCCESS Upon Success
     * @retval OPAL_NOT_FOUND If no component can be selected
     * @retval OPAL_ERROR Upon other failure
     *
     */
    OPAL_DECLSPEC int opal_crs_base_select(void);

    /**
     * Finalize the CRS MCA framework
     *
     * @retval OPAL_SUCCESS Upon success
     * @retval OPAL_ERROR   Upon failures
     *
     * This function is invoked during opal_finalize();
     */
    OPAL_DECLSPEC int opal_crs_base_close(void);

    /**
     * Globals
     */
    OPAL_DECLSPEC extern mca_base_framework_t opal_crs_base_framework;
    OPAL_DECLSPEC extern opal_crs_base_component_t opal_crs_base_selected_component;
    OPAL_DECLSPEC extern opal_crs_base_module_t opal_crs;

    /**
     * Some utility functions
     */
    OPAL_DECLSPEC char * opal_crs_base_state_str(opal_crs_state_type_t state);

    /*
     * Extract the expected component and pid from the metadata
     */
    OPAL_DECLSPEC int opal_crs_base_extract_expected_component(FILE *metadata, char ** component_name, int *prev_pid);

    /*
     * Read a token to the metadata file
     */
    OPAL_DECLSPEC int opal_crs_base_metadata_read_token(FILE *metadata, char * token, char ***value);

    /*
     * Register a file for cleanup.
     * Useful in C/R when files only need to temporarily exist for restart
     */
    OPAL_DECLSPEC int opal_crs_base_cleanup_append(char* filename, bool is_dir);

    /*
     * Flush the cleanup of all registered files.
     */
    OPAL_DECLSPEC int opal_crs_base_cleanup_flush(void);

    /*
     * Copy the options structure
     */
    OPAL_DECLSPEC int opal_crs_base_copy_options(opal_crs_base_ckpt_options_t *from,
                                                 opal_crs_base_ckpt_options_t *to);
    /*
     * Clear the options structure
     */
    OPAL_DECLSPEC int opal_crs_base_clear_options(opal_crs_base_ckpt_options_t *target);

    /*
     * CRS self application interface functions
     */
    typedef int (*opal_crs_base_self_checkpoint_fn_t)(char **restart_cmd);
    typedef int (*opal_crs_base_self_restart_fn_t)(void);
    typedef int (*opal_crs_base_self_continue_fn_t)(void);

    extern opal_crs_base_self_checkpoint_fn_t ompi_crs_base_self_checkpoint_fn;
    extern opal_crs_base_self_restart_fn_t    ompi_crs_base_self_restart_fn;
    extern opal_crs_base_self_continue_fn_t   ompi_crs_base_self_continue_fn;

    OPAL_DECLSPEC int opal_crs_base_self_register_checkpoint_callback
                      (opal_crs_base_self_checkpoint_fn_t  function);
    OPAL_DECLSPEC int opal_crs_base_self_register_restart_callback
                      (opal_crs_base_self_restart_fn_t  function);
    OPAL_DECLSPEC int opal_crs_base_self_register_continue_callback
                      (opal_crs_base_self_continue_fn_t  function);

END_C_DECLS

#endif /* OPAL_CRS_BASE_H */

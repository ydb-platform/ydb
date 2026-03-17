/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * NONE CRS component
 *
 * Simple, braindead implementation.
 */

#ifndef MCA_CRS_NONE_EXPORT_H
#define MCA_CRS_NONE_EXPORT_H

#include "opal_config.h"


#include "opal/mca/mca.h"
#include "opal/mca/crs/crs.h"

BEGIN_C_DECLS

    /*
     * Local Component structures
     */
    struct opal_crs_none_component_t {
        opal_crs_base_component_t super;  /** Base CRS component */

    };
    typedef struct opal_crs_none_component_t opal_crs_none_component_t;
    OPAL_MODULE_DECLSPEC extern opal_crs_none_component_t mca_crs_none_component;

    int opal_crs_none_component_query(mca_base_module_t **module, int *priority);

    /*
     * Module functions
     */
    int opal_crs_none_module_init(void);
    int opal_crs_none_module_finalize(void);

    /*
     * Actual funcationality
     */
    int opal_crs_none_checkpoint( pid_t pid,
                                  opal_crs_base_snapshot_t *snapshot,
                                  opal_crs_base_ckpt_options_t *options,
                                  opal_crs_state_type_t *state);

    int opal_crs_none_restart(    opal_crs_base_snapshot_t *snapshot, bool spawn_child, pid_t *child_pid);

    int opal_crs_none_disable_checkpoint(void);
    int opal_crs_none_enable_checkpoint(void);

    int opal_crs_none_prelaunch(int32_t rank,
                                char *base_snapshot_dir,
                                char **app,
                                char **cwd,
                                char ***argv,
                                char ***env);

    int opal_crs_none_reg_thread(void);

    extern bool opal_crs_none_select_warning;

END_C_DECLS

#endif /* MCA_CRS_NONE_EXPORT_H */

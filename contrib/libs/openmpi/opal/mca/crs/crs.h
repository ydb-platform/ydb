/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Checkpoint and Restart Service (CRS) Interface
 *
 * General Description:
 *
 * The OPAL Checkpoint and Restart Service (CRS) has been created to create an
 * abstract notion of a single process checkpointer for upper levels to
 * incorporate checkpoint/restart calls genericly into their code. This keeps
 * the upper levels from becoming too tied to a specfic checkpoint and restart
 * implementation.
 *
 * This interface will change in the future to allow for some additional
 * specialized functionality such as memory inclusion/exclusion, explicit
 * restarting while running, and others.
 *
 * Words to the Wise:
 *
 * The CRS module must adhere to the API exactly inorder to be fully supported.
 * How the module goes about conforming to the API is an internal module issue
 * and in no cases should the module impose restrictions upon the upper layers
 * as this is an API violation.
 *
 */

#ifndef MCA_CRS_H
#define MCA_CRS_H

#include "opal_config.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/class/opal_object.h"

BEGIN_C_DECLS

/**
 * States of the module
 */
enum opal_crs_state_type_t {
    OPAL_CRS_NONE        = 0,
    OPAL_CRS_CHECKPOINT  = 1,
    OPAL_CRS_RESTART_PRE = 2,
    OPAL_CRS_RESTART     = 3, /* RESTART_POST */
    OPAL_CRS_CONTINUE    = 4,
    OPAL_CRS_TERM        = 5,
    OPAL_CRS_RUNNING     = 6,
    OPAL_CRS_ERROR       = 7,
    OPAL_CRS_STATE_MAX   = 8
};
typedef enum opal_crs_state_type_t opal_crs_state_type_t;

/*
 * Possible checkpoint options
 */
struct opal_crs_base_ckpt_options_1_0_0_t {
    /** Parent is an object type */
    opal_object_t super;

    /** Terminate after checkpoint */
    bool term;
    /** Send SIGSTOP after checkpoint */
    bool stop;

    /** INC Prep Only */
    bool inc_prep_only;

    /** INC Recover Only */
    bool inc_recover_only;

#if OPAL_ENABLE_CRDEBUG == 1
    /** Wait for debugger to attach after checkpoint */
    bool attach_debugger;
    /** Do not wait for debugger to reattach after checkpoint */
    bool detach_debugger;
#endif
};
typedef struct opal_crs_base_ckpt_options_1_0_0_t opal_crs_base_ckpt_options_1_0_0_t;
typedef struct opal_crs_base_ckpt_options_1_0_0_t opal_crs_base_ckpt_options_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_crs_base_ckpt_options_t);

/**
 * Structure for Single process snapshot
 * Each component is assumed to have extened this definition
 * in the same way they exten the opal_crs_base_compoinent_t below.
 */
struct opal_crs_base_snapshot_1_0_0_t {
    /** This is an object, so must have super */
    opal_list_item_t super;

    /** MCA Component name */
    char * component_name;

    /** Metadata filename */
    char * metadata_filename;

    /** Metadata fd */
    FILE * metadata;

    /** Absolute path the the snapshot directory */
    char * snapshot_directory;

    /** Cold Start:
     * If we are restarting cold, then we need to recreate this structure
     *  opal_restart would set this, and let the component do the heavy lifting
     *  of recreating the structure, sicne it doesn't know exactly how to.
     */
    bool cold_start;
};
typedef struct opal_crs_base_snapshot_1_0_0_t opal_crs_base_snapshot_1_0_0_t;
typedef struct opal_crs_base_snapshot_1_0_0_t opal_crs_base_snapshot_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_crs_base_snapshot_t);

/**
 * Module initialization function.
 * Returns OPAL_SUCCESS
 */
typedef int (*opal_crs_base_module_init_fn_t)
     (void);

/**
 * Module finalization function.
 * Returns OPAL_SUCCESS
 */
typedef int (*opal_crs_base_module_finalize_fn_t)
     (void);

/**
 * Call the underlying checkpointer.
 * Returns OPAL_SUCCESS upon success, and OPAL_ERROR otherwise.
 *
 * Arguments:
 *   pid    = PID of the process to checkpoint, or 0 if checkpointing self.
 *   fname  = the filename where the checkpoint has been written.
 *   state = The state at which the checkpoint is exiting
 *     - OPAL_CRS_CONTINUE
 *       Continuing after a checkpoint has been taken
 *     - OPAL_CRS_RESTART
 *       Restarting from a checkpoint
 *     - OPAL_CRS_ERROR
 *       Checkpoint was not successful.
 *
 * The 'fname' string is owned by the caller: if appropriate, it must be eventually
 * freed by the caller.
 */
typedef int (*opal_crs_base_module_checkpoint_fn_t)
     (pid_t pid,
      opal_crs_base_snapshot_t *snapshot,
      opal_crs_base_ckpt_options_t *options,
      opal_crs_state_type_t *state);

/**
 * Call the underlying restart command for this process
 * Returns OPAL_SUCCESS or OPAL_CRS_ERROR
 *
 * Arguments:
 *  fname = Checkpoint filename
 *  spawn_child  = true if the restarted process should be forked as a new process,
 *                      in which case 'child_pid' will be returned.
 *                 false if the restarted process should overwrite the current
 *                       process space.
 *  child_pid = PID of the child that was started, if applicable
 *
 */
typedef int (*opal_crs_base_module_restart_fn_t)
     (opal_crs_base_snapshot_t *snapshot,
      bool spawn_child,
      pid_t *child_pid);

/**
 * Disable the checkpointer
 * Returns OPAL_SUCCESS or OPAL_CRS_ERROR
 *
 * This should set a flag/mutex to disallow checkpoints to occur.
 * If a checkpoint were to occur while checkpoints are disabled,
 * they should block until reenabled.
 * A quality module implementation would notify the user that the
 * checkpoint has been delayed until the program is out of this critical
 * section of code.
 */
typedef int (*opal_crs_base_module_disable_checkpoint_fn_t)
     (void);

/**
 * Enable the checkpointer
 * Returns OPAL_SUCCESS or OPAL_CRS_ERROR
 *
 * This should set a flag/mutex to allow checkpoints to occur
 */
typedef int (*opal_crs_base_module_enable_checkpoint_fn_t)
     (void);

/**
 * Prepare the CRS component for process launch.
 * Some CRS components need to take action before the
 * process is ever launched to do such things as:
 * - seed the process environment
 * - LD_PRELOAD
 * - Analyze the binary before launch
 *
 * @param rank Rank of the process to be started
 * @param app  Absolute pathname of argv[0]
 * @param argv Standard argv-style array, including a final NULL pointer
 * @param env  Standard environ-style array, including a final NULL pointer
 */
typedef int (*opal_crs_base_module_prelaunch_fn_t)
         (int32_t rank,
          char *base_snapshot_dir,
          char **app,
          char **cwd,
          char ***argv,
          char ***env);

/**
 * Register another thread that may call this library.
 * Some CR systems require that each thread that will call into their library
 * register individually before doing so.
 *
 * Returns OPAL_SUCCESS or OPAL_ERROR
 */
typedef int (*opal_crs_base_module_reg_thread_fn_t)
     (void);

/**
 * Structure for CRS components.
 */
struct opal_crs_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;

    /** Verbosity Level */
    int verbose;
    /** Output Handle for opal_output */
    int output_handle;
    /** Default Priority */
    int priority;
};
typedef struct opal_crs_base_component_2_0_0_t opal_crs_base_component_2_0_0_t;
typedef struct opal_crs_base_component_2_0_0_t opal_crs_base_component_t;

/**
 * Structure for CRS modules
 */
struct opal_crs_base_module_1_0_0_t {
    /** Initialization Function */
    opal_crs_base_module_init_fn_t           crs_init;
    /** Finalization Function */
    opal_crs_base_module_finalize_fn_t       crs_finalize;

    /** Checkpoint interface */
    opal_crs_base_module_checkpoint_fn_t     crs_checkpoint;

    /** Restart Interface */
    opal_crs_base_module_restart_fn_t        crs_restart;

    /** Disable checkpoints */
    opal_crs_base_module_disable_checkpoint_fn_t crs_disable_checkpoint;
    /** Enable checkpoints */
    opal_crs_base_module_enable_checkpoint_fn_t  crs_enable_checkpoint;

    /** Pre Launch */
    opal_crs_base_module_prelaunch_fn_t      crs_prelaunch;

    /** Per thread registration */
    opal_crs_base_module_reg_thread_fn_t      crs_reg_thread;
};
typedef struct opal_crs_base_module_1_0_0_t opal_crs_base_module_1_0_0_t;
typedef struct opal_crs_base_module_1_0_0_t opal_crs_base_module_t;

OPAL_DECLSPEC extern opal_crs_base_module_t opal_crs;

/**
 * Macro for use in components that are of type CRS
 */
#define OPAL_CRS_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("crs", 2, 0, 0)

END_C_DECLS

#endif /* OPAL_CRS_H */


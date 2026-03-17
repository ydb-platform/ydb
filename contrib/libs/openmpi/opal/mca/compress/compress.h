/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
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
 * Compression Framework
 *
 * General Description:
 *
 * The OPAL Compress framework has been created to provide an abstract interface
 * to the compression agent library on the host machine. This fromework is useful
 * when distributing files that can be compressed before sending to dimish the
 * load on the network.
 *
 */

#ifndef MCA_COMPRESS_H
#define MCA_COMPRESS_H

#include "opal_config.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/class/opal_object.h"

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

/**
 * Module initialization function.
 * Returns OPAL_SUCCESS
 */
typedef int (*opal_compress_base_module_init_fn_t)
     (void);

/**
 * Module finalization function.
 * Returns OPAL_SUCCESS
 */
typedef int (*opal_compress_base_module_finalize_fn_t)
     (void);

/**
 * Compress the file provided
 *
 * Arguments:
 *   fname   = Filename to compress
 *   cname   = Compressed filename
 *   postfix = postfix added to filename to create compressed filename
 * Returns:
 *   OPAL_SUCCESS on success, ow OPAL_ERROR
 */
typedef int (*opal_compress_base_module_compress_fn_t)
    (char * fname, char **cname, char **postfix);

typedef int (*opal_compress_base_module_compress_nb_fn_t)
    (char * fname, char **cname, char **postfix, pid_t *child_pid);

/**
 * Decompress the file provided
 *
 * Arguments:
 *   fname = Filename to compress
 *   cname = Compressed filename
 * Returns:
 *   OPAL_SUCCESS on success, ow OPAL_ERROR
 */
typedef int (*opal_compress_base_module_decompress_fn_t)
    (char * cname, char **fname);
typedef int (*opal_compress_base_module_decompress_nb_fn_t)
    (char * cname, char **fname, pid_t *child_pid);

/**
 * Structure for COMPRESS components.
 */
struct opal_compress_base_component_2_0_0_t {
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
typedef struct opal_compress_base_component_2_0_0_t opal_compress_base_component_2_0_0_t;
typedef struct opal_compress_base_component_2_0_0_t opal_compress_base_component_t;

/**
 * Structure for COMPRESS modules
 */
struct opal_compress_base_module_1_0_0_t {
    /** Initialization Function */
    opal_compress_base_module_init_fn_t           init;
    /** Finalization Function */
    opal_compress_base_module_finalize_fn_t       finalize;

    /** Compress interface */
    opal_compress_base_module_compress_fn_t       compress;
    opal_compress_base_module_compress_nb_fn_t    compress_nb;

    /** Decompress Interface */
    opal_compress_base_module_decompress_fn_t     decompress;
    opal_compress_base_module_decompress_nb_fn_t  decompress_nb;
};
typedef struct opal_compress_base_module_1_0_0_t opal_compress_base_module_1_0_0_t;
typedef struct opal_compress_base_module_1_0_0_t opal_compress_base_module_t;

OPAL_DECLSPEC extern opal_compress_base_module_t opal_compress;

/**
 * Macro for use in components that are of type COMPRESS
 */
#define OPAL_COMPRESS_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("compress", 2, 0, 0)

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif /* OPAL_COMPRESS_H */


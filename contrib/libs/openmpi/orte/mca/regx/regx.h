/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The Open RTE Personality Framework (regx)
 *
 * Multi-select framework so that multiple personalities can be
 * simultaneously supported
 *
 */

#ifndef ORTE_MCA_REGX_H
#define ORTE_MCA_REGX_H

#include "orte_config.h"
#include "orte/types.h"

#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss_types.h"
#include "orte/mca/mca.h"

#include "orte/runtime/orte_globals.h"


BEGIN_C_DECLS

/*
 * regx module functions
 */

#define ORTE_MAX_NODE_PREFIX        50
#define ORTE_CONTIG_NODE_CMD        0x01
#define ORTE_NON_CONTIG_NODE_CMD    0x02

/**
* REGX module functions - the modules are accessed via
* the base stub functions
*/
typedef struct {
    opal_list_item_t super;
    int ctx;
    int nprocs;
    int cnt;
} orte_nidmap_regex_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_nidmap_regex_t);

/* initialize the module - allow it to do whatever one-time
 * things it requires */
typedef int (*orte_regx_base_module_init_fn_t)(void);

typedef int (*orte_regx_base_module_nidmap_create_fn_t)(opal_pointer_array_t *pool, char **regex);
typedef int (*orte_regx_base_module_nidmap_parse_fn_t)(char *regex);
typedef int (*orte_regx_base_module_extract_node_names_fn_t)(char *regexp, char ***names);

/* create a regular expression describing the nodes in the
 * allocation */
typedef int (*orte_regx_base_module_encode_nodemap_fn_t)(opal_buffer_t *buffer);

/* decode a regular expression created by the encode function
 * into the orte_node_pool array */
typedef int (*orte_regx_base_module_decode_daemon_nodemap_fn_t)(opal_buffer_t *buffer);

typedef int (*orte_regx_base_module_build_daemon_nidmap_fn_t)(void);

/* create a regular expression describing the ppn for a job */
typedef int (*orte_regx_base_module_generate_ppn_fn_t)(orte_job_t *jdata, char **ppn);

/* decode the ppn */
typedef int (*orte_regx_base_module_parse_ppn_fn_t)(orte_job_t *jdata, char *ppn);


/* give the component a chance to cleanup */
typedef void (*orte_regx_base_module_finalize_fn_t)(void);

/*
 * regx module version 1.0.0
 */
typedef struct {
    orte_regx_base_module_init_fn_t                   init;
    orte_regx_base_module_nidmap_create_fn_t          nidmap_create;
    orte_regx_base_module_nidmap_parse_fn_t           nidmap_parse;
    orte_regx_base_module_extract_node_names_fn_t     extract_node_names;
    orte_regx_base_module_encode_nodemap_fn_t         encode_nodemap;
    orte_regx_base_module_decode_daemon_nodemap_fn_t  decode_daemon_nodemap;
    orte_regx_base_module_build_daemon_nidmap_fn_t    build_daemon_nidmap;
    orte_regx_base_module_generate_ppn_fn_t           generate_ppn;
    orte_regx_base_module_parse_ppn_fn_t              parse_ppn;
    orte_regx_base_module_finalize_fn_t               finalize;
} orte_regx_base_module_t;

ORTE_DECLSPEC extern orte_regx_base_module_t orte_regx;

/*
 * regx component
 */

/**
 * regx component version 1.0.0
 */
typedef struct {
    /** Base MCA structure */
    mca_base_component_t base_version;
    /** Base MCA data */
    mca_base_component_data_t base_data;
} orte_regx_base_component_t;

/**
 * Macro for use in components that are of type regx
 */
#define MCA_REGX_BASE_VERSION_1_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("regx", 1, 0, 0)


END_C_DECLS

#endif

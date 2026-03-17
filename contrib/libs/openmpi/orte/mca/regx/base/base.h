/*
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 * regx framework base functionality.
 */

#ifndef ORTE_MCA_REGX_BASE_H
#define ORTE_MCA_REGX_BASE_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/types.h"

#include "opal/class/opal_list.h"
#include "orte/mca/mca.h"

#include "orte/runtime/orte_globals.h"

#include "orte/mca/regx/regx.h"

BEGIN_C_DECLS

/*
 * MCA Framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_regx_base_framework;
/* select all components */
ORTE_DECLSPEC    int orte_regx_base_select(void);

/*
 * common stuff
 */
typedef struct {
    opal_list_item_t super;
    int vpid;
    int cnt;
    int slots;
    orte_topology_t *t;
} orte_regex_range_t;

OBJ_CLASS_DECLARATION(orte_regex_range_t);

typedef struct {
    /* list object */
    opal_list_item_t super;
    char *prefix;
    char *suffix;
    int num_digits;
    opal_list_t ranges;
} orte_regex_node_t;
END_C_DECLS

OBJ_CLASS_DECLARATION(orte_regex_node_t);

ORTE_DECLSPEC extern int orte_regx_base_nidmap_parse(char *regex);

ORTE_DECLSPEC extern int orte_regx_base_encode_nodemap(opal_buffer_t *buffer);

ORTE_DECLSPEC int orte_regx_base_decode_daemon_nodemap(opal_buffer_t *buffer);

ORTE_DECLSPEC int orte_regx_base_generate_ppn(orte_job_t *jdata, char **ppn);

ORTE_DECLSPEC int orte_regx_base_parse_ppn(orte_job_t *jdata, char *regex);

ORTE_DECLSPEC int orte_regx_base_extract_node_names(char *regexp, char ***names);
#endif

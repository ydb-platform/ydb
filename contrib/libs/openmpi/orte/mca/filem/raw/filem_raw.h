/*
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FILEM_RAW_EXPORT_H
#define MCA_FILEM_RAW_EXPORT_H

#include "orte_config.h"

#include "orte/mca/mca.h"
#include "opal/class/opal_object.h"
#include "opal/mca/event/event.h"

#include "orte/mca/filem/filem.h"

BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_filem_base_component_t mca_filem_raw_component;
ORTE_DECLSPEC extern orte_filem_base_module_t mca_filem_raw_module;

extern bool orte_filem_raw_flatten_trees;

#define ORTE_FILEM_RAW_CHUNK_MAX 16384

/* local classes */
typedef struct {
    opal_list_item_t super;
    opal_list_t xfers;
    int32_t status;
    orte_filem_completion_cbfunc_t cbfunc;
    void *cbdata;
} orte_filem_raw_outbound_t;
OBJ_CLASS_DECLARATION(orte_filem_raw_outbound_t);

typedef struct {
    opal_list_item_t super;
    orte_filem_raw_outbound_t *outbound;
    orte_app_idx_t app_idx;
    opal_event_t ev;
    bool pending;
    char *src;
    char *file;
    int32_t type;
    int32_t nchunk;
    int status;
    orte_vpid_t nrecvd;
} orte_filem_raw_xfer_t;
OBJ_CLASS_DECLARATION(orte_filem_raw_xfer_t);

typedef struct {
    opal_list_item_t super;
    orte_app_idx_t app_idx;
    opal_event_t ev;
    bool pending;
    int fd;
    char *file;
    char *top;
    char *fullpath;
    int32_t type;
    char **link_pts;
    opal_list_t outputs;
} orte_filem_raw_incoming_t;
OBJ_CLASS_DECLARATION(orte_filem_raw_incoming_t);

typedef struct {
    opal_list_item_t super;
    int numbytes;
    unsigned char data[ORTE_FILEM_RAW_CHUNK_MAX];
} orte_filem_raw_output_t;
OBJ_CLASS_DECLARATION(orte_filem_raw_output_t);

END_C_DECLS

#endif /* MCA_FILEM_RAW_EXPORT_H */

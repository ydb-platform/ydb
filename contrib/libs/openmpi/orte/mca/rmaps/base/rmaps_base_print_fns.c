/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>

#include "opal/util/if.h"
#include "opal/util/output.h"
#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/threads/tsd.h"

#include "orte/types.h"
#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/hostfile/hostfile.h"
#include "orte/util/dash_host/dash_host.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

#include "orte/mca/rmaps/base/rmaps_private.h"
#include "orte/mca/rmaps/base/base.h"

#define ORTE_RMAPS_PRINT_MAX_SIZE   50
#define ORTE_RMAPS_PRINT_NUM_BUFS   16

static bool fns_init=false;
static opal_tsd_key_t print_tsd_key;
static char* orte_rmaps_print_null = "NULL";
typedef struct {
    char *buffers[ORTE_RMAPS_PRINT_NUM_BUFS];
    int cntr;
} orte_rmaps_print_buffers_t;

static void buffer_cleanup(void *value)
{
    int i;
    orte_rmaps_print_buffers_t *ptr;

    if (NULL != value) {
        ptr = (orte_rmaps_print_buffers_t*)value;
        for (i=0; i < ORTE_RMAPS_PRINT_NUM_BUFS; i++) {
            free(ptr->buffers[i]);
        }
    }
}

static orte_rmaps_print_buffers_t *get_print_buffer(void)
{
    orte_rmaps_print_buffers_t *ptr;
    int ret, i;

    if (!fns_init) {
        /* setup the print_args function */
        if (ORTE_SUCCESS != (ret = opal_tsd_key_create(&print_tsd_key, buffer_cleanup))) {
            ORTE_ERROR_LOG(ret);
            return NULL;
        }
        fns_init = true;
    }

    ret = opal_tsd_getspecific(print_tsd_key, (void**)&ptr);
    if (OPAL_SUCCESS != ret) return NULL;

    if (NULL == ptr) {
        ptr = (orte_rmaps_print_buffers_t*)malloc(sizeof(orte_rmaps_print_buffers_t));
        for (i=0; i < ORTE_RMAPS_PRINT_NUM_BUFS; i++) {
            ptr->buffers[i] = (char *) malloc((ORTE_RMAPS_PRINT_MAX_SIZE+1) * sizeof(char));
        }
        ptr->cntr = 0;
        ret = opal_tsd_setspecific(print_tsd_key, (void*)ptr);
    }

    return (orte_rmaps_print_buffers_t*) ptr;
}

char* orte_rmaps_base_print_mapping(orte_mapping_policy_t mapping)
{
    char *ret, *map, *mymap, *tmp;
    orte_rmaps_print_buffers_t *ptr;

    if (ORTE_MAPPING_CONFLICTED & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
        return "CONFLICTED";
    }

    ptr = get_print_buffer();
    if (NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return orte_rmaps_print_null;
    }
    /* cycle around the ring */
    if (ORTE_RMAPS_PRINT_NUM_BUFS == ptr->cntr) {
        ptr->cntr = 0;
    }

    switch(ORTE_GET_MAPPING_POLICY(mapping)) {
    case ORTE_MAPPING_BYNODE:
        map = "BYNODE";
        break;
    case ORTE_MAPPING_BYBOARD:
        map = "BYBOARD";
        break;
    case ORTE_MAPPING_BYNUMA:
        map = "BYNUMA";
        break;
    case ORTE_MAPPING_BYSOCKET:
        map = "BYSOCKET";
        break;
    case ORTE_MAPPING_BYL3CACHE:
        map = "BYL3CACHE";
        break;
    case ORTE_MAPPING_BYL2CACHE:
        map = "BYL2CACHE";
        break;
    case ORTE_MAPPING_BYL1CACHE:
        map = "BYL1CACHE";
        break;
    case ORTE_MAPPING_BYCORE:
        map = "BYCORE";
        break;
    case ORTE_MAPPING_BYHWTHREAD:
        map = "BYHWTHREAD";
        break;
    case ORTE_MAPPING_BYSLOT:
        map = "BYSLOT";
        break;
    case ORTE_MAPPING_SEQ:
        map = "SEQUENTIAL";
        break;
    case ORTE_MAPPING_BYUSER:
        map = "BYUSER";
        break;
    case ORTE_MAPPING_BYDIST:
        map = "MINDIST";
        break;
    default:
        if (ORTE_MAPPING_PPR & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
            map = "PPR";
        } else {
            map = "UNKNOWN";
        }
    }
    if (0 != strcmp(map, "PPR") && (ORTE_MAPPING_PPR & ORTE_GET_MAPPING_DIRECTIVE(mapping))) {
        asprintf(&mymap, "%s[PPR]:", map);
    } else {
        asprintf(&mymap, "%s:", map);
    }
    if (ORTE_MAPPING_NO_USE_LOCAL & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
        asprintf(&tmp, "%sNO_USE_LOCAL,", mymap);
        free(mymap);
        mymap = tmp;
    }
    if (ORTE_MAPPING_NO_OVERSUBSCRIBE & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
        asprintf(&tmp, "%sNOOVERSUBSCRIBE,", mymap);
        free(mymap);
        mymap = tmp;
    } else if (ORTE_MAPPING_SUBSCRIBE_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
        asprintf(&tmp, "%sOVERSUBSCRIBE,", mymap);
        free(mymap);
        mymap = tmp;
    }
    if (ORTE_MAPPING_SPAN & ORTE_GET_MAPPING_DIRECTIVE(mapping)) {
        asprintf(&tmp, "%sSPAN,", mymap);
        free(mymap);
        mymap = tmp;
    }

    /* remove the trailing mark */
    mymap[strlen(mymap)-1] = '\0';

    snprintf(ptr->buffers[ptr->cntr], ORTE_RMAPS_PRINT_MAX_SIZE, "%s", mymap);
    free(mymap);
    ret = ptr->buffers[ptr->cntr];
    ptr->cntr++;

    return ret;
}

char* orte_rmaps_base_print_ranking(orte_ranking_policy_t ranking)
{
    switch(ORTE_GET_RANKING_POLICY(ranking)) {
    case ORTE_RANK_BY_NODE:
        return "NODE";
    case ORTE_RANK_BY_BOARD:
        return "BOARD";
    case ORTE_RANK_BY_NUMA:
        return "NUMA";
    case ORTE_RANK_BY_SOCKET:
        return "SOCKET";
    case ORTE_RANK_BY_CORE:
        return "CORE";
    case ORTE_RANK_BY_HWTHREAD:
        return "HWTHREAD";
    case ORTE_RANK_BY_SLOT:
        return "SLOT";
    default:
        return "UNKNOWN";
    }
}

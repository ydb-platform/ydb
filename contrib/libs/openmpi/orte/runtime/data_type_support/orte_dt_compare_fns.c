/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2011 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>

#include <sys/types.h>

#include "orte/mca/grpcomm/grpcomm.h"

#include "orte/runtime/data_type_support/orte_dt_support.h"

int orte_dt_compare_std_cntr(orte_std_cntr_t *value1, orte_std_cntr_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/**
 * JOB
 */
int orte_dt_compare_job(orte_job_t *value1, orte_job_t *value2, opal_data_type_t type)
{
    /** check jobids */
    if (value1->jobid > value2->jobid) return OPAL_VALUE1_GREATER;
    if (value1->jobid < value2->jobid) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/**
* NODE
 */
int orte_dt_compare_node(orte_node_t *value1, orte_node_t *value2, opal_data_type_t type)
{
    int test;

    /** check node names */
    test = strcmp(value1->name, value2->name);
    if (0 == test) return OPAL_EQUAL;
    if (0 < test) return OPAL_VALUE2_GREATER;

    return OPAL_VALUE1_GREATER;
}

/**
* PROC
 */
int orte_dt_compare_proc(orte_proc_t *value1, orte_proc_t *value2, opal_data_type_t type)
{
    orte_ns_cmp_bitmask_t mask;

    /** check vpids */
    mask = ORTE_NS_CMP_VPID;

    return orte_util_compare_name_fields(mask, &value1->name, &value2->name);
}

/*
 * APP CONTEXT
 */
int orte_dt_compare_app_context(orte_app_context_t *value1, orte_app_context_t *value2, opal_data_type_t type)
{
    if (value1->idx > value2->idx) return OPAL_VALUE1_GREATER;
    if (value2->idx > value1->idx) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/*
 * EXIT CODE
 */
int orte_dt_compare_exit_code(orte_exit_code_t *value1,
                                    orte_exit_code_t *value2,
                                    opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/*
 * NODE STATE
 */
int orte_dt_compare_node_state(orte_node_state_t *value1,
                                     orte_node_state_t *value2,
                                     orte_node_state_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/*
 * PROC STATE
 */
int orte_dt_compare_proc_state(orte_proc_state_t *value1,
                                     orte_proc_state_t *value2,
                                     orte_proc_state_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/*
 * JOB STATE
 */
int orte_dt_compare_job_state(orte_job_state_t *value1,
                                    orte_job_state_t *value2,
                                    orte_job_state_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/*
 * JOB_MAP
 */
int orte_dt_compare_map(orte_job_map_t *value1, orte_job_map_t *value2, opal_data_type_t type)
{
    return OPAL_EQUAL;
}

/*
 * RML tags
 */
int orte_dt_compare_tags(orte_rml_tag_t *value1, orte_rml_tag_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) {
        return OPAL_VALUE1_GREATER;
    } else if (*value1 < *value2) {
        return OPAL_VALUE2_GREATER;
    } else {
        return OPAL_EQUAL;
    }
}

/* ORTE_DAEMON_CMD */
int orte_dt_compare_daemon_cmd(orte_daemon_cmd_flag_t *value1, orte_daemon_cmd_flag_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/* ORTE_IOF_TAG */
int orte_dt_compare_iof_tag(orte_iof_tag_t *value1, orte_iof_tag_t *value2, opal_data_type_t type)
{
    if (*value1 > *value2) return OPAL_VALUE1_GREATER;

    if (*value2 > *value1) return OPAL_VALUE2_GREATER;

    return OPAL_EQUAL;
}

/* ORTE_ATTR */
int orte_dt_compare_attr(orte_attribute_t *value1, orte_attribute_t *value2, opal_data_type_t type)
{
    if (value1->key > value2->key) {
        return OPAL_VALUE1_GREATER;
    }
    if (value2->key > value1->key) {
        return OPAL_VALUE2_GREATER;
    }

    return OPAL_EQUAL;
}

/* ORTE_SIGNATURE */
int orte_dt_compare_sig(orte_grpcomm_signature_t *value1, orte_grpcomm_signature_t *value2, opal_data_type_t type)
{
    if (value1->sz > value2->sz) {
        return OPAL_VALUE1_GREATER;
    }
    if (value2->sz > value1->sz) {
        return OPAL_VALUE2_GREATER;
    }
    /* same size - check contents */
    if (0 == memcmp(value1->signature, value2->signature, value1->sz*sizeof(orte_process_name_t))) {
        return OPAL_EQUAL;
    }
    return OPAL_VALUE2_GREATER;
}

/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
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
/** @file:
 */

#ifndef ORTE_DT_SUPPORT_H
#define ORTE_DT_SUPPORT_H

/*
 * includes
 */
#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include "opal/dss/dss_types.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/odls/odls_types.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/iof/iof_types.h"

#include "orte/runtime/orte_globals.h"


BEGIN_C_DECLS

/** Data type compare functions */
int orte_dt_compare_std_cntr(orte_std_cntr_t *value1, orte_std_cntr_t *value2, opal_data_type_t type);
int orte_dt_compare_job(orte_job_t *value1, orte_job_t *value2, opal_data_type_t type);
int orte_dt_compare_node(orte_node_t *value1, orte_node_t *value2, opal_data_type_t type);
int orte_dt_compare_proc(orte_proc_t *value1, orte_proc_t *value2, opal_data_type_t type);
int orte_dt_compare_app_context(orte_app_context_t *value1, orte_app_context_t *value2, opal_data_type_t type);
int orte_dt_compare_exit_code(orte_exit_code_t *value1,
                                    orte_exit_code_t *value2,
                                    opal_data_type_t type);
int orte_dt_compare_node_state(orte_node_state_t *value1,
                                     orte_node_state_t *value2,
                                     orte_node_state_t type);
int orte_dt_compare_proc_state(orte_proc_state_t *value1,
                                     orte_proc_state_t *value2,
                                     orte_proc_state_t type);
int orte_dt_compare_job_state(orte_job_state_t *value1,
                                    orte_job_state_t *value2,
                                    orte_job_state_t type);
int orte_dt_compare_map(orte_job_map_t *value1, orte_job_map_t *value2, opal_data_type_t type);
int orte_dt_compare_tags(orte_rml_tag_t *value1,
                         orte_rml_tag_t *value2,
                         opal_data_type_t type);
int orte_dt_compare_daemon_cmd(orte_daemon_cmd_flag_t *value1, orte_daemon_cmd_flag_t *value2, opal_data_type_t type);
int orte_dt_compare_iof_tag(orte_iof_tag_t *value1, orte_iof_tag_t *value2, opal_data_type_t type);
int orte_dt_compare_attr(orte_attribute_t *value1, orte_attribute_t *value2, opal_data_type_t type);
int orte_dt_compare_sig(orte_grpcomm_signature_t *value1, orte_grpcomm_signature_t *value2, opal_data_type_t type);

/** Data type copy functions */
int orte_dt_copy_std_cntr(orte_std_cntr_t **dest, orte_std_cntr_t *src, opal_data_type_t type);
int orte_dt_copy_job(orte_job_t **dest, orte_job_t *src, opal_data_type_t type);
int orte_dt_copy_node(orte_node_t **dest, orte_node_t *src, opal_data_type_t type);
int orte_dt_copy_proc(orte_proc_t **dest, orte_proc_t *src, opal_data_type_t type);
int orte_dt_copy_app_context(orte_app_context_t **dest, orte_app_context_t *src, opal_data_type_t type);
int orte_dt_copy_proc_state(orte_proc_state_t **dest, orte_proc_state_t *src, opal_data_type_t type);
int orte_dt_copy_job_state(orte_job_state_t **dest, orte_job_state_t *src, opal_data_type_t type);
int orte_dt_copy_node_state(orte_node_state_t **dest, orte_node_state_t *src, opal_data_type_t type);
int orte_dt_copy_exit_code(orte_exit_code_t **dest, orte_exit_code_t *src, opal_data_type_t type);
int orte_dt_copy_map(orte_job_map_t **dest, orte_job_map_t *src, opal_data_type_t type);
int orte_dt_copy_tag(orte_rml_tag_t **dest,
                           orte_rml_tag_t *src,
                           opal_data_type_t type);
int orte_dt_copy_daemon_cmd(orte_daemon_cmd_flag_t **dest, orte_daemon_cmd_flag_t *src, opal_data_type_t type);
int orte_dt_copy_iof_tag(orte_iof_tag_t **dest, orte_iof_tag_t *src, opal_data_type_t type);
int orte_dt_copy_attr(orte_attribute_t **dest, orte_attribute_t *src, opal_data_type_t type);
int orte_dt_copy_sig(orte_grpcomm_signature_t **dest, orte_grpcomm_signature_t *src, opal_data_type_t type);

/** Data type pack functions */
int orte_dt_pack_std_cntr(opal_buffer_t *buffer, const void *src,
                            int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_job(opal_buffer_t *buffer, const void *src,
                     int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_node(opal_buffer_t *buffer, const void *src,
                      int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_proc(opal_buffer_t *buffer, const void *src,
                      int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_app_context(opal_buffer_t *buffer, const void *src,
                                   int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_exit_code(opal_buffer_t *buffer, const void *src,
                                 int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_node_state(opal_buffer_t *buffer, const void *src,
                                  int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_proc_state(opal_buffer_t *buffer, const void *src,
                                  int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_job_state(opal_buffer_t *buffer, const void *src,
                                 int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_map(opal_buffer_t *buffer, const void *src,
                             int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_tag(opal_buffer_t *buffer,
                           const void *src,
                           int32_t num_vals,
                           opal_data_type_t type);
int orte_dt_pack_daemon_cmd(opal_buffer_t *buffer, const void *src,
                          int32_t num_vals, opal_data_type_t type);
int orte_dt_pack_iof_tag(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                         opal_data_type_t type);
int orte_dt_pack_attr(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                      opal_data_type_t type);
int orte_dt_pack_sig(opal_buffer_t *buffer, const void *src, int32_t num_vals,
                     opal_data_type_t type);

/** Data type print functions */
int orte_dt_std_print(char **output, char *prefix, void *src, opal_data_type_t type);
int orte_dt_print_job(char **output, char *prefix, orte_job_t *src, opal_data_type_t type);
int orte_dt_print_node(char **output, char *prefix, orte_node_t *src, opal_data_type_t type);
int orte_dt_print_proc(char **output, char *prefix, orte_proc_t *src, opal_data_type_t type);
int orte_dt_print_app_context(char **output, char *prefix, orte_app_context_t *src, opal_data_type_t type);
int orte_dt_print_map(char **output, char *prefix, orte_job_map_t *src, opal_data_type_t type);
int orte_dt_print_attr(char **output, char *prefix, orte_attribute_t *src, opal_data_type_t type);
int orte_dt_print_sig(char **output, char *prefix, orte_grpcomm_signature_t *src, opal_data_type_t type);

/** Data type unpack functions */
int orte_dt_unpack_std_cntr(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_job(opal_buffer_t *buffer, void *dest,
                       int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_node(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_proc(opal_buffer_t *buffer, void *dest,
                        int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_app_context(opal_buffer_t *buffer, void *dest,
                                     int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_exit_code(opal_buffer_t *buffer, void *dest,
                                   int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_node_state(opal_buffer_t *buffer, void *dest,
                                    int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_proc_state(opal_buffer_t *buffer, void *dest,
                                    int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_job_state(opal_buffer_t *buffer, void *dest,
                                   int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_map(opal_buffer_t *buffer, void *dest,
                               int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_tag(opal_buffer_t *buffer,
                             void *dest,
                             int32_t *num_vals,
                             opal_data_type_t type);
int orte_dt_unpack_daemon_cmd(opal_buffer_t *buffer, void *dest,
                            int32_t *num_vals, opal_data_type_t type);
int orte_dt_unpack_iof_tag(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                           opal_data_type_t type);
int orte_dt_unpack_attr(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                        opal_data_type_t type);
int orte_dt_unpack_sig(opal_buffer_t *buffer, void *dest, int32_t *num_vals,
                       opal_data_type_t type);

END_C_DECLS

#endif

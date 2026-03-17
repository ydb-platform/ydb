/*
 * Copyright (c) 2016-2017 Inria.  All rights reserved.
 * Copyright (c) 2017      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COMMON_MONITORING_COLL_H
#define MCA_COMMON_MONITORING_COLL_H

BEGIN_C_DECLS

#include <ompi_config.h>
#include <opal/mca/base/mca_base_pvar.h>

OMPI_DECLSPEC void mca_common_monitoring_coll_flush(FILE *pf, mca_monitoring_coll_data_t*data);

OMPI_DECLSPEC void mca_common_monitoring_coll_flush_all(FILE *pf);

OMPI_DECLSPEC void mca_common_monitoring_coll_reset( void );

OMPI_DECLSPEC int mca_common_monitoring_coll_messages_notify(mca_base_pvar_t *pvar,
                                                             mca_base_pvar_event_t event,
                                                             void *obj_handle,
                                                             int *count);

OMPI_DECLSPEC int mca_common_monitoring_coll_get_o2a_count(const struct mca_base_pvar_t *pvar,
                                                           void *value,
                                                           void *obj_handle);

OMPI_DECLSPEC int mca_common_monitoring_coll_get_o2a_size(const struct mca_base_pvar_t *pvar,
                                                          void *value,
                                                          void *obj_handle);

OMPI_DECLSPEC int mca_common_monitoring_coll_get_a2o_count(const struct mca_base_pvar_t *pvar,
                                                           void *value,
                                                           void *obj_handle);

OMPI_DECLSPEC int mca_common_monitoring_coll_get_a2o_size(const struct mca_base_pvar_t *pvar,
                                                          void *value,
                                                          void *obj_handle);

OMPI_DECLSPEC int mca_common_monitoring_coll_get_a2a_count(const struct mca_base_pvar_t *pvar,
                                                           void *value,
                                                           void *obj_handle);

OMPI_DECLSPEC int mca_common_monitoring_coll_get_a2a_size(const struct mca_base_pvar_t *pvar,
                                                          void *value,
                                                          void *obj_handle);

OMPI_DECLSPEC void mca_common_monitoring_coll_finalize( void );
END_C_DECLS

#endif  /* MCA_COMMON_MONITORING_COLL_H */

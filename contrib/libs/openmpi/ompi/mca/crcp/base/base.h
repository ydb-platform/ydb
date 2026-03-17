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
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OMPI_CRCP_BASE_H
#define OMPI_CRCP_BASE_H

#include "ompi_config.h"

#include "opal/mca/base/base.h"
#include "ompi/constants.h"

#include "ompi/mca/crcp/crcp.h"

/*
 * Global functions for MCA overall CRCP
 */

BEGIN_C_DECLS

    /**
     * Select an available component.
     *
     * @retval OMPI_SUCCESS Upon Success
     * @retval OMPI_NOT_FOUND If no component can be selected
     * @retval OMPI_ERROR Upon other failure
     *
     */
    OMPI_DECLSPEC int ompi_crcp_base_select(void);

    /**
     * Quiesce Interface (For MPI Ext.)
     */
    OMPI_DECLSPEC int ompi_crcp_base_quiesce_start(MPI_Info *info);
    OMPI_DECLSPEC int ompi_crcp_base_quiesce_end(MPI_Info *info);

    /**
     * 'None' component functions
     * These are to be used when no component is selected.
     * They just return success, and empty strings as necessary.
     */
    int ompi_crcp_base_none_open(void);
    int ompi_crcp_base_none_close(void);
    int ompi_crcp_base_none_query(mca_base_module_t **module, int *priority);

    int ompi_crcp_base_module_init(void);
    int ompi_crcp_base_module_finalize(void);

    /* Quiesce Interface */
    int ompi_crcp_base_none_quiesce_start(MPI_Info *info);
    int ompi_crcp_base_none_quiesce_end(MPI_Info *info);

    /* PML Interface */
    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_enable( bool enable, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_add_comm( struct ompi_communicator_t* comm, ompi_crcp_base_pml_state_t* );
    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_del_comm( struct ompi_communicator_t* comm, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_add_procs( struct ompi_proc_t **procs, size_t nprocs, ompi_crcp_base_pml_state_t* );
    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_del_procs( struct ompi_proc_t **procs, size_t nprocs, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_progress(ompi_crcp_base_pml_state_t*);

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_iprobe(int dst, int tag, struct ompi_communicator_t* comm, int *matched, ompi_status_public_t* status, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_probe( int dst, int tag, struct ompi_communicator_t* comm, ompi_status_public_t* status, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_isend_init( void *buf, size_t count, ompi_datatype_t *datatype, int dst, int tag,
                                            mca_pml_base_send_mode_t mode, struct ompi_communicator_t* comm, struct ompi_request_t **request, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_isend( void *buf, size_t count, ompi_datatype_t *datatype, int dst, int tag,
                                       mca_pml_base_send_mode_t mode, struct ompi_communicator_t* comm, struct ompi_request_t **request, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_send(  void *buf, size_t count, ompi_datatype_t *datatype, int dst, int tag,
                                       mca_pml_base_send_mode_t mode, struct ompi_communicator_t* comm, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_irecv_init( void *buf, size_t count, ompi_datatype_t *datatype, int src, int tag,
                                            struct ompi_communicator_t* comm,  struct ompi_request_t **request, ompi_crcp_base_pml_state_t*);

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_irecv( void *buf, size_t count, ompi_datatype_t *datatype, int src, int tag,
                                       struct ompi_communicator_t* comm, struct ompi_request_t **request, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_recv(  void *buf, size_t count, ompi_datatype_t *datatype, int src, int tag,
                                       struct ompi_communicator_t* comm,  ompi_status_public_t* status, ompi_crcp_base_pml_state_t*);

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_dump( struct ompi_communicator_t* comm, int verbose, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_start( size_t count, ompi_request_t** requests, ompi_crcp_base_pml_state_t* );

    ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_ft_event(int state, ompi_crcp_base_pml_state_t*);

    /* Request Interface */
    int ompi_crcp_base_none_request_complete( struct ompi_request_t *request );

    /* BTL Interface */
    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_add_procs( struct mca_btl_base_module_t* btl,
                                       size_t nprocs,
                                       struct ompi_proc_t** procs,
                                       struct mca_btl_base_endpoint_t** endpoints,
                                       struct opal_bitmap_t* reachable,
                                       ompi_crcp_base_btl_state_t* );

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_del_procs( struct mca_btl_base_module_t* btl,
                                       size_t nprocs,
                                       struct ompi_proc_t** procs,
                                       struct mca_btl_base_endpoint_t**,
                                       ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_register( struct mca_btl_base_module_t* btl,
                                      mca_btl_base_tag_t tag,
                                      mca_btl_base_module_recv_cb_fn_t cbfunc,
                                      void* cbdata,
                                      ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_finalize( struct mca_btl_base_module_t* btl,
                                      ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_alloc( struct mca_btl_base_module_t* btl,
                                   size_t size,
                                   ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_free( struct mca_btl_base_module_t* btl,
                                  mca_btl_base_descriptor_t* descriptor,
                                  ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_prepare_src( struct mca_btl_base_module_t* btl,
                                         struct mca_btl_base_endpoint_t* endpoint,
                                         mca_rcache_base_registration_t* registration,
                                         struct opal_convertor_t* convertor,
                                         size_t reserve,
                                         size_t* size,
                                         ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_prepare_dst( struct mca_btl_base_module_t* btl,
                                         struct mca_btl_base_endpoint_t* endpoint,
                                         mca_rcache_base_registration_t* registration,
                                         struct opal_convertor_t* convertor,
                                         size_t reserve,
                                         size_t* size,
                                         ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_send( struct mca_btl_base_module_t* btl,
                                  struct mca_btl_base_endpoint_t* endpoint,
                                  struct mca_btl_base_descriptor_t* descriptor,
                                  mca_btl_base_tag_t tag,
                                  ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_put( struct mca_btl_base_module_t* btl,
                                 struct mca_btl_base_endpoint_t* endpoint,
                                 struct mca_btl_base_descriptor_t* descriptor,
                                 ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_get( struct mca_btl_base_module_t* btl,
                                 struct mca_btl_base_endpoint_t* endpoint,
                                 struct mca_btl_base_descriptor_t* descriptor,
                                 ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_dump( struct mca_btl_base_module_t* btl,
                                  struct mca_btl_base_endpoint_t* endpoint,
                                  int verbose,
                                  ompi_crcp_base_btl_state_t*);

    ompi_crcp_base_btl_state_t*
    ompi_crcp_base_none_btl_ft_event(int state,
                                     ompi_crcp_base_btl_state_t*);

    OMPI_DECLSPEC extern mca_base_framework_t ompi_crcp_base_framework;
    OMPI_DECLSPEC extern ompi_crcp_base_component_t ompi_crcp_base_selected_component;
    OMPI_DECLSPEC extern ompi_crcp_base_module_t ompi_crcp;

END_C_DECLS

#endif /* OMPI_CRCP_BASE_H */

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
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

#include "ompi_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <time.h>
#include <ctype.h>

#include "opal/class/opal_bitmap.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/base.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"

#include "ompi/communicator/communicator.h"
#include "ompi/proc/proc.h"
#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/crcp/base/base.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/info/info.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/pml/base/pml_base_request.h"

/******************
 * Local Functions
 ******************/

/******************
 * Object stuff
 ******************/
OBJ_CLASS_INSTANCE(ompi_crcp_base_pml_state_t,
                   opal_free_list_item_t,
                   NULL,
                   NULL
                   );

OBJ_CLASS_INSTANCE(ompi_crcp_base_btl_state_t,
                   opal_free_list_item_t,
                   NULL,
                   NULL
                   );

/***********************
 * None component stuff
 ************************/
int ompi_crcp_base_none_open(void)
{
    return OMPI_SUCCESS;
}

int ompi_crcp_base_none_close(void)
{
    return OMPI_SUCCESS;
}

int ompi_crcp_base_none_query(mca_base_module_t **module, int *priority)
{
    *module = NULL;
    *priority = 0;

    return OPAL_SUCCESS;
}

int ompi_crcp_base_module_init(void)
{
    return OMPI_SUCCESS;
}

int ompi_crcp_base_module_finalize(void)
{
    return OMPI_SUCCESS;
}

/****************
 * MPI Quiesce Interface
 ****************/
int ompi_crcp_base_none_quiesce_start(MPI_Info *info)
{
    return OMPI_SUCCESS;
}

int ompi_crcp_base_none_quiesce_end(MPI_Info *info)
{
    return OMPI_SUCCESS;
}

/****************
 * PML Wrapper
 ****************/
ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_enable( bool enable,
                                                            ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_add_comm( struct ompi_communicator_t* comm,
                                                              ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_del_comm( struct ompi_communicator_t* comm,
                                                              ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_add_procs( struct ompi_proc_t **procs,
                                                               size_t nprocs,
                                                               ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_del_procs( struct ompi_proc_t **procs,
                                                               size_t nprocs,
                                                               ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_progress(ompi_crcp_base_pml_state_t* pml_state)
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_iprobe(int dst, int tag,
                                                           struct ompi_communicator_t* comm,
                                                           int *matched, ompi_status_public_t* status,
                                                           ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_probe( int dst, int tag,
                                                           struct ompi_communicator_t* comm,
                                                           ompi_status_public_t* status,
                                                           ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_isend_init( void *buf, size_t count,
                                                                ompi_datatype_t *datatype,
                                                                int dst, int tag,
                                                                mca_pml_base_send_mode_t mode,
                                                                struct ompi_communicator_t* comm,
                                                                struct ompi_request_t **request,
                                                                ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_isend( void *buf, size_t count,
                                                           ompi_datatype_t *datatype,
                                                           int dst, int tag,
                                                           mca_pml_base_send_mode_t mode,
                                                           struct ompi_communicator_t* comm,
                                                           struct ompi_request_t **request,
                                                           ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_send(  void *buf, size_t count,
                                                           ompi_datatype_t *datatype,
                                                           int dst, int tag,
                                                           mca_pml_base_send_mode_t mode,
                                                           struct ompi_communicator_t* comm,
                                                           ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_irecv_init( void *buf, size_t count,
                                                                ompi_datatype_t *datatype,
                                                                int src, int tag,
                                                                struct ompi_communicator_t* comm,
                                                                struct ompi_request_t **request,
                                                                ompi_crcp_base_pml_state_t* pml_state)
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_irecv( void *buf, size_t count,
                                                           ompi_datatype_t *datatype,
                                                           int src, int tag,
                                                           struct ompi_communicator_t* comm,
                                                           struct ompi_request_t **request,
                                                           ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_recv(  void *buf, size_t count,
                                                           ompi_datatype_t *datatype,
                                                           int src, int tag,
                                                           struct ompi_communicator_t* comm,
                                                           ompi_status_public_t* status,
                                                           ompi_crcp_base_pml_state_t* pml_state)
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_dump( struct ompi_communicator_t* comm,
                                                          int verbose,
                                                          ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_start( size_t count,
                                                           ompi_request_t** requests,
                                                           ompi_crcp_base_pml_state_t* pml_state )
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

ompi_crcp_base_pml_state_t* ompi_crcp_base_none_pml_ft_event(int state,
                                                             ompi_crcp_base_pml_state_t* pml_state)
{
    pml_state->error_code = OMPI_SUCCESS;
    return pml_state;
}

/********************
 * Request Interface
 ********************/
int ompi_crcp_base_none_request_complete( struct ompi_request_t *request ) {
    return OMPI_SUCCESS;
}

/********************
 * BTL Interface
 ********************/
ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_add_procs( struct mca_btl_base_module_t* btl,
                                   size_t nprocs,
                                   struct ompi_proc_t** procs,
                                   struct mca_btl_base_endpoint_t** endpoints,
                                   struct opal_bitmap_t* reachable,
                                   ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_del_procs( struct mca_btl_base_module_t* btl,
                                   size_t nprocs,
                                   struct ompi_proc_t** procs,
                                   struct mca_btl_base_endpoint_t** endpoints,
                                   ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_register( struct mca_btl_base_module_t* btl,
                                  mca_btl_base_tag_t tag,
                                  mca_btl_base_module_recv_cb_fn_t cbfunc,
                                  void* cbdata,
                                  ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_finalize( struct mca_btl_base_module_t* btl,
                                  ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_alloc( struct mca_btl_base_module_t* btl,
                               size_t size,
                               ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_free( struct mca_btl_base_module_t* btl,
                              mca_btl_base_descriptor_t* descriptor,
                              ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_prepare_src( struct mca_btl_base_module_t* btl,
                                     struct mca_btl_base_endpoint_t* endpoint,
                                     mca_rcache_base_registration_t* registration,
                                     struct opal_convertor_t* convertor,
                                     size_t reserve,
                                     size_t* size,
                                     ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_prepare_dst( struct mca_btl_base_module_t* btl,
                                     struct mca_btl_base_endpoint_t* endpoint,
                                     mca_rcache_base_registration_t* registration,
                                     struct opal_convertor_t* convertor,
                                     size_t reserve,
                                     size_t* size,
                                     ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_send( struct mca_btl_base_module_t* btl,
                              struct mca_btl_base_endpoint_t* endpoint,
                              struct mca_btl_base_descriptor_t* descriptor,
                              mca_btl_base_tag_t tag,
                              ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_put( struct mca_btl_base_module_t* btl,
                             struct mca_btl_base_endpoint_t* endpoint,
                             struct mca_btl_base_descriptor_t* descriptor,
                             ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_get( struct mca_btl_base_module_t* btl,
                             struct mca_btl_base_endpoint_t* endpoint,
                             struct mca_btl_base_descriptor_t* descriptor,
                             ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}


ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_dump( struct mca_btl_base_module_t* btl,
                              struct mca_btl_base_endpoint_t* endpoint,
                              int verbose,
                              ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}

ompi_crcp_base_btl_state_t*
ompi_crcp_base_none_btl_ft_event(int state,
                                 ompi_crcp_base_btl_state_t* btl_state)
{
    btl_state->error_code = OMPI_SUCCESS;
    return btl_state;
}


/********************
 * Utility functions
 ********************/

/******************
 * MPI Interface Functions
 ******************/
int ompi_crcp_base_quiesce_start(MPI_Info *info)
{
    if( NULL != ompi_crcp.quiesce_start ) {
        return ompi_crcp.quiesce_start(info);
    } else {
        return OMPI_SUCCESS;
    }
}

int ompi_crcp_base_quiesce_end(MPI_Info *info)
{
    if( NULL != ompi_crcp.quiesce_end ) {
        return ompi_crcp.quiesce_end(info);
    } else {
        return OMPI_SUCCESS;
    }
}

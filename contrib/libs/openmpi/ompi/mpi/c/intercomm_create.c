/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2006-2009 University of Houston.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/communicator/communicator.h"
#include "ompi/request/request.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Intercomm_create = PMPI_Intercomm_create
#endif
#define MPI_Intercomm_create PMPI_Intercomm_create
#endif

static const char FUNC_NAME[] = "MPI_Intercomm_create";


int MPI_Intercomm_create(MPI_Comm local_comm, int local_leader,
                         MPI_Comm bridge_comm, int remote_leader,
                         int tag, MPI_Comm *newintercomm)
{
    int local_size=0, local_rank=0;
    int lleader=0, rleader=0;
    ompi_communicator_t *newcomp=NULL;
    struct ompi_proc_t **rprocs=NULL;
    int rc=0, rsize=0;
    ompi_proc_t **proc_list=NULL;
    int j;
    ompi_group_t *new_group_pointer;

    MEMCHECKER(
        memchecker_comm(local_comm);
        memchecker_comm(bridge_comm);
    );

    if ( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if ( ompi_comm_invalid ( local_comm ) ||
             ( local_comm->c_flags & OMPI_COMM_INTER ) )
            return OMPI_ERRHANDLER_INVOKE ( MPI_COMM_WORLD, MPI_ERR_COMM,
                                            FUNC_NAME);

        if ( NULL == newintercomm )
            return OMPI_ERRHANDLER_INVOKE ( local_comm, MPI_ERR_ARG,
                                            FUNC_NAME);

        /* if ( tag < 0 || tag > MPI_TAG_UB )
             return OMPI_ERRHANDLER_INVOKE ( local_comm, MPI_ERR_ARG,
                                             FUNC_NAME);
        */
    }

    OPAL_CR_ENTER_LIBRARY();

    local_size = ompi_comm_size ( local_comm );
    local_rank = ompi_comm_rank ( local_comm );
    lleader = local_leader;
    rleader = remote_leader;

    if ( MPI_PARAM_CHECK ) {
        if ( (0 > local_leader) || (local_leader >= local_size) )
            return OMPI_ERRHANDLER_INVOKE ( local_comm, MPI_ERR_ARG,
                                            FUNC_NAME);

        /* remember that the remote_leader and bridge_comm arguments
           just have to be valid at the local_leader */
        if ( local_rank == local_leader ) {
            if ( ompi_comm_invalid ( bridge_comm ) ||
                 (bridge_comm->c_flags & OMPI_COMM_INTER) ) {
                OPAL_CR_EXIT_LIBRARY();
                return OMPI_ERRHANDLER_INVOKE ( local_comm, MPI_ERR_COMM,
                                                FUNC_NAME);
            }
            if ( (remote_leader < 0) || (remote_leader >= ompi_comm_size(bridge_comm))) {
                OPAL_CR_EXIT_LIBRARY();
                return OMPI_ERRHANDLER_INVOKE ( local_comm, MPI_ERR_ARG,
                                                FUNC_NAME);
            }
        } /* if ( local_rank == local_leader ) */
    }

    if ( local_rank == local_leader ) {
        MPI_Request req;

        /* local leader exchange group sizes lists */
        rc = MCA_PML_CALL(irecv(&rsize, 1, MPI_INT, rleader, tag, bridge_comm,
                                &req));
        if ( rc != MPI_SUCCESS ) {
            goto err_exit;
        }
        rc = MCA_PML_CALL(send (&local_size, 1, MPI_INT, rleader, tag,
                                MCA_PML_BASE_SEND_STANDARD, bridge_comm));
        if ( rc != MPI_SUCCESS ) {
            goto err_exit;
        }
        rc = ompi_request_wait( &req, MPI_STATUS_IGNORE);
        if ( rc != MPI_SUCCESS ) {
            goto err_exit;
        }
    }

    /* bcast size and list of remote processes to all processes in local_comm */
    rc = local_comm->c_coll->coll_bcast ( &rsize, 1, MPI_INT, lleader,
                                         local_comm,
                                         local_comm->c_coll->coll_bcast_module);
    if ( rc != MPI_SUCCESS ) {
        goto err_exit;
    }

    rprocs = ompi_comm_get_rprocs( local_comm, bridge_comm, lleader,
                                   remote_leader, tag, rsize );
    if ( NULL == rprocs ) {
        goto err_exit;
    }

    if ( MPI_PARAM_CHECK ) {
        if(OMPI_GROUP_IS_DENSE(local_comm->c_local_group)) {
            rc = ompi_comm_overlapping_groups(local_comm->c_local_group->grp_proc_count,
                                              local_comm->c_local_group->grp_proc_pointers,
                                              rsize,
                                              rprocs);
        }
        else {
            proc_list = (ompi_proc_t **) calloc (local_comm->c_local_group->grp_proc_count,
                                                 sizeof (ompi_proc_t *));
            for(j=0 ; j<local_comm->c_local_group->grp_proc_count ; j++) {
                proc_list[j] = ompi_group_peer_lookup(local_comm->c_local_group,j);
            }
            rc = ompi_comm_overlapping_groups(local_comm->c_local_group->grp_proc_count,
                                              proc_list,
                                              rsize,
                                              rprocs);
        }
        if ( OMPI_SUCCESS != rc ) {
            goto err_exit;
        }
    }
    new_group_pointer = ompi_group_allocate(rsize);
    if( NULL == new_group_pointer ) {
        rc = MPI_ERR_GROUP;
        goto err_exit;
    }

    /* put group elements in the list */
    for (j = 0; j < rsize; j++) {
        new_group_pointer->grp_proc_pointers[j] = rprocs[j];
        OBJ_RETAIN(rprocs[j]);
    }

    rc = ompi_comm_set ( &newcomp,                                     /* new comm */
                         local_comm,                                   /* old comm */
                         local_comm->c_local_group->grp_proc_count,    /* local_size */
                         NULL,                                         /* local_procs*/
                         rsize,                                        /* remote_size */
                         NULL,                                         /* remote_procs */
                         NULL,                                         /* attrs */
                         local_comm->error_handler,                    /* error handler*/
                         false,                                        /* dont copy the topo */
                         local_comm->c_local_group,                    /* local group */
                         new_group_pointer                             /* remote group */
                         );

    if ( MPI_SUCCESS != rc ) {
        goto err_exit;
    }

    OBJ_RELEASE(new_group_pointer);
    new_group_pointer = MPI_GROUP_NULL;

    /* Determine context id. It is identical to f_2_c_handle */
    rc = ompi_comm_nextcid (newcomp, local_comm, bridge_comm, &lleader,
                            &rleader, false, OMPI_COMM_CID_INTRA_BRIDGE);
    if ( MPI_SUCCESS != rc ) {
        goto err_exit;
    }

    /* activate comm and init coll-module */
    rc = ompi_comm_activate (&newcomp, local_comm, bridge_comm, &lleader, &rleader,
                             false, OMPI_COMM_CID_INTRA_BRIDGE);
    if ( MPI_SUCCESS != rc ) {
        goto err_exit;
    }

 err_exit:
    OPAL_CR_EXIT_LIBRARY();

    if ( NULL != rprocs ) {
        free ( rprocs );
    }
    if ( NULL != proc_list ) {
        free ( proc_list );
    }
    if ( OMPI_SUCCESS != rc ) {
        *newintercomm = MPI_COMM_NULL;
        return OMPI_ERRHANDLER_INVOKE(local_comm, MPI_ERR_INTERN,
                                      FUNC_NAME);
    }

    *newintercomm = newcomp;
    return MPI_SUCCESS;
}


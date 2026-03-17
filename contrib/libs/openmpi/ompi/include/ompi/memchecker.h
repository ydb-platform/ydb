/*
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2010-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 *
 * Copyright (c) 2016 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#ifndef OMPI_MEMCHECKER_H
#define OMPI_MEMCHECKER_H

#include "ompi_config.h"

#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_datatype_internal.h"

#include "ompi/communicator/communicator.h"
#include "ompi/group/group.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"
#include "opal/mca/memchecker/base/base.h"


#if OPAL_WANT_MEMCHECKER
#  define MEMCHECKER(x) do {       \
            x;                     \
   } while(0)
#else
#  define MEMCHECKER(x)
#endif /* OPAL_WANT_MEMCHECKER */


static inline int memchecker_convertor_call (int (*f)(void *, size_t), opal_convertor_t* pConvertor)
{
    if (!opal_memchecker_base_runindebugger() ||
        (0 == pConvertor->count) ) {
        return OMPI_SUCCESS;
    }

    if( OPAL_LIKELY(pConvertor->flags & CONVERTOR_NO_OP) ) {
        /*  We have a contiguous type. */
        f( (void *)pConvertor->pBaseBuf , pConvertor->local_size);
    } else {
        /* Now we got a noncontigous data. */
        uint32_t elem_pos = 0, i;
        ptrdiff_t  stack_disp  = 0;
        dt_elem_desc_t*  description = pConvertor->use_desc->desc;
        dt_elem_desc_t*  pElem       = &(description[elem_pos]);
        unsigned char   *source_base = pConvertor->pBaseBuf;

        if ( NULL != pConvertor->pDesc )
            stack_disp = pConvertor->pDesc->ub - pConvertor->pDesc->lb;

        for (i = 0; i < pConvertor->count; i++){
            while ( OPAL_DATATYPE_LOOP == pElem->elem.common.flags ) {
                elem_pos++;
                pElem = &(description[elem_pos]);
            }

            while( pElem->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
                /* now here we have a basic datatype */
                f( (void *)(source_base + pElem->elem.disp), pElem->elem.count*pElem->elem.extent );
                elem_pos++;       /* advance to the next data */
                pElem = &(description[elem_pos]);
                continue;
            }

            elem_pos = 0;
            pElem = &(description[elem_pos]);
            /* starting address of next stack. */
            source_base += stack_disp;
        }
    }

    return OMPI_SUCCESS;
}


/*
 * Set the corresponding memory area of count elements of type ty
 *
 */
static inline int memchecker_call (int (*f)(void *, size_t), const void * addr,
                                   size_t count, struct ompi_datatype_t * datatype)
{
    if (!opal_memchecker_base_runindebugger()) {
        return OMPI_SUCCESS;
    }

    if ((0 == count) || (0 == datatype->super.size)) {
        return OMPI_SUCCESS;
    }

    if( datatype->super.size == (size_t) (datatype->super.true_ub - datatype->super.true_lb) ) {
        /*  We have a contiguous type. */
        f( (void*)addr , datatype->super.size * count );
    } else {
        /* Now we got a noncontigous type. */
        uint32_t         elem_pos = 0, i;
        ptrdiff_t        stack_disp  = 0;
        dt_elem_desc_t*  description = datatype->super.opt_desc.desc;
        dt_elem_desc_t*  pElem       = &(description[elem_pos]);
        unsigned char   *source_base = (unsigned char *) addr;

        if ( NULL != datatype ) {
            stack_disp = datatype->super.ub - datatype->super.lb;
        }

        for (i = 0; i < count; i++) {
            while ( OPAL_DATATYPE_LOOP == pElem->elem.common.flags ) {
                elem_pos++;
                pElem = &(description[elem_pos]);
            }

            while( pElem->elem.common.flags & OPAL_DATATYPE_FLAG_DATA ) {
                /* now here we have a basic datatype */
                f( (void *)(source_base + pElem->elem.disp), pElem->elem.count*pElem->elem.extent );
                elem_pos++;       /* advance to the next data */
                pElem = &(description[elem_pos]);
                continue;
            }

            elem_pos = 0;
            pElem = &(description[elem_pos]);
            /* starting address of next stack. */
            source_base += stack_disp;
        }
    }

    return OMPI_SUCCESS;
}



/*
 * Flag: OMPI_WANT_MEMCHECKER_MPI_OBJECTS
 *
 * If set, definedness of Open MPI-internal objects is being checked.
 * To handle alignment, only the used members of structures are
 * being used -- therefore this depends on the corresponding
 * configure-flags.
 *
 * This is off by default, as this is rather expensive (for each member
 * the valgrind-magic is being inlined.
 * Only turn on, if You want to debug ompi-internal datastructures.
 */
/*#define OMPI_WANT_MEMCHECKER_MPI_OBJECTS*/


/*
 * Check every member of the communicator, whether their memory areas are defined.
 */
#ifdef OMPI_WANT_MEMCHECKER_MPI_OBJECTS
static inline int memchecker_comm(MPI_Comm comm)
{
    if (!opal_memchecker_base_runindebugger()) {
        return OMPI_SUCCESS;
    }

    /*
     * We should not check underlying objects in this way -- either another opal/include/memchecker.h
     * However, let us assume, that underlying objects are initialized correctly
     */
#if 0
    /* c_base */
    opal_memchecker_base_isdefined (&comm->c_base.obj_class, sizeof(opal_class_t *));
    opal_memchecker_base_isdefined ((void*)&comm->c_base.obj_reference_count, sizeof(volatile int32_t));
#if OPAL_ENABLE_DEBUG
    opal_memchecker_base_isdefined (&comm->c_base.obj_magic_id, sizeof(opal_object_t));
    opal_memchecker_base_isdefined (&comm->c_base.cls_init_file_name, sizeof(const char *));
    opal_memchecker_base_isdefined (&comm->c_base.cls_init_lineno, sizeof(int));
#endif
    /* c_lock */
    opal_memchecker_base_isdefined (&comm->c_lock.super.obj_class, sizeof(opal_class_t *));
    opal_memchecker_base_isdefined ((void*)&comm->c_lock.super.obj_reference_count, sizeof(volatile int32_t));
#if OPAL_ENABLE_DEBUG
    opal_memchecker_base_isdefined (&comm->c_lock.super.obj_magic_id, sizeof(uint64_t));
    opal_memchecker_base_isdefined (&comm->c_lock.super.cls_init_file_name, sizeof(const char *));
    opal_memchecker_base_isdefined (&comm->c_lock.super.cls_init_lineno, sizeof(int));
#endif
/*
  opal_memchecker_base_isdefined (&comm->c_lock.m_lock_pthread.__m_reserved, sizeof(int));
  opal_memchecker_base_isdefined (&comm->c_lock.m_lock_pthread.__m_count, sizeof(int));
  opal_memchecker_base_isdefined (&comm->c_lock.m_lock_pthread.__m_owner, sizeof(_pthread_descr));
  opal_memchecker_base_isdefined (&comm->c_lock.m_lock_pthread.__m_kind, sizeof(int));
  opal_memchecker_base_isdefined (&comm->c_lock.m_lock_pthread.__m_lock.__status, sizeof(long int));
  opal_memchecker_base_isdefined (&comm->c_lock.m_lock_pthread.__m_lock.__spinlock, sizeof(int));
*/
    /*
     * The storage of a union has the size of the initialized member.
     * Here we check the whole union.
     */
    opal_memchecker_base_isdefined (&comm->c_lock.m_lock_atomic, sizeof(opal_atomic_lock_t));
#endif /* 0 */
    opal_memchecker_base_isdefined (&comm->c_name, MPI_MAX_OBJECT_NAME);
    opal_memchecker_base_isdefined (&comm->c_my_rank, sizeof(int));
    opal_memchecker_base_isdefined (&comm->c_flags, sizeof(uint32_t));
    opal_memchecker_base_isdefined (&comm->c_id_available, sizeof(int));
    opal_memchecker_base_isdefined (&comm->c_id_start_index, sizeof(int));
    opal_memchecker_base_isdefined (&comm->c_local_group, sizeof(ompi_group_t *));
    opal_memchecker_base_isdefined (&comm->c_remote_group, sizeof(ompi_group_t *));
    opal_memchecker_base_isdefined (&comm->c_keyhash, sizeof(struct opal_hash_table_t *));
    opal_memchecker_base_isdefined (&comm->c_cube_dim, sizeof(int));
    opal_memchecker_base_isdefined (&comm->c_topo, sizeof(const struct mca_topo_base_module_t *));
    opal_memchecker_base_isdefined (&comm->c_f_to_c_index, sizeof(int));
#ifdef OMPI_WANT_PERUSE
    opal_memchecker_base_isdefined (&comm->c_peruse_handles, sizeof(struct ompi_peruse_handle_t **));
#endif
    opal_memchecker_base_isdefined (&comm->error_handler, sizeof(ompi_errhandler_t *));
    opal_memchecker_base_isdefined (&comm->errhandler_type, sizeof(ompi_errhandler_type_t));
    opal_memchecker_base_isdefined (&comm->c_pml_comm, sizeof(struct mca_pml_comm_t *));
    /* c_coll */
    opal_memchecker_base_isdefined (&comm->c_coll->coll_module_init, sizeof(mca_coll_base_module_init_1_0_0_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_module_finalize, sizeof(mca_coll_base_module_finalize_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_allgather, sizeof(mca_coll_base_module_allgather_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_allgatherv, sizeof(mca_coll_base_module_allgatherv_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_allreduce, sizeof(mca_coll_base_module_allreduce_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_alltoall, sizeof(mca_coll_base_module_alltoall_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_alltoallv, sizeof(mca_coll_base_module_alltoallv_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_alltoallw, sizeof(mca_coll_base_module_alltoallw_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_barrier, sizeof(mca_coll_base_module_barrier_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_bcast, sizeof(mca_coll_base_module_bcast_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_exscan, sizeof(mca_coll_base_module_exscan_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_gather, sizeof(mca_coll_base_module_gather_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_gatherv, sizeof(mca_coll_base_module_gatherv_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_reduce, sizeof(mca_coll_base_module_reduce_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_reduce_scatter, sizeof(mca_coll_base_module_reduce_scatter_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_scan, sizeof(mca_coll_base_module_scan_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_scatter, sizeof(mca_coll_base_module_scatter_fn_t));
    opal_memchecker_base_isdefined (&comm->c_coll->coll_scatterv, sizeof(mca_coll_base_module_scatterv_fn_t));

    opal_memchecker_base_isdefined (&comm->c_coll_selected_component, sizeof(const mca_coll_base_component_2_0_0_t *));
    opal_memchecker_base_isdefined (&comm->c_coll_selected_module, sizeof(const mca_coll_base_module_1_0_0_t *));
    /* Somehow, this often shows up in petsc with comm_dup'ed communicators*/
    /* opal_memchecker_base_isdefined (&comm->c_coll_selected_data, sizeof(struct mca_coll_base_comm_t *)); */
    opal_memchecker_base_isdefined (&comm->c_coll_basic_module, sizeof(const mca_coll_base_module_1_0_0_t *));
    opal_memchecker_base_isdefined (&comm->c_coll_basic_data, sizeof(struct mca_coll_base_comm_t *));

    return OMPI_SUCCESS;
}
#else
#define memchecker_comm(comm)
#endif /* OMPI_WANT_MEMCHECKER_MPI_OBJECTS */


/*
 * Check every member of the request, whether their memory areas are defined.
 */
#ifdef OMPI_WANT_MEMCHECKER_MPI_OBJECTS
static inline int memchecker_request(MPI_Request *request)
{
    if (!opal_memchecker_base_runindebugger()) {
        return OMPI_SUCCESS;
    }

#if 0
    opal_memchecker_base_isdefined (&(*request)->super.super.super.obj_class, sizeof(opal_class_t *));
    opal_memchecker_base_isdefined ((void*)&(*request)->super.super.super.obj_reference_count, sizeof(volatile int32_t));
#if OPAL_ENABLE_DEBUG
    opal_memchecker_base_isdefined (&(*request)->super.super.super.obj_magic_id, sizeof(uint64_t));
    opal_memchecker_base_isdefined (&(*request)->super.super.super.cls_init_file_name, sizeof(const char *));
    opal_memchecker_base_isdefined (&(*request)->super.super.super.cls_init_lineno, sizeof(int));
#endif

    opal_memchecker_base_isdefined ((void*)&(*request)->super.super.opal_list_next, sizeof(volatile struct opal_list_item_t *));
    opal_memchecker_base_isdefined ((void*)&(*request)->super.super.opal_list_prev, sizeof(volatile struct opal_list_item_t *));
#if OPAL_ENABLE_DEBUG
    opal_memchecker_base_isdefined ((void*)&(*request)->super.super.opal_list_item_refcount, sizeof(volatile int32_t));
    opal_memchecker_base_isdefined ((void*)&(*request)->super.super.opal_list_item_belong_to, sizeof(volatile struct opal_list_t *));
#endif
/*    opal_memchecker_base_isdefined (&(*request)->super.user_data, sizeof(void *)); */
#endif /* 0 */
    opal_memchecker_base_isdefined (&(*request)->req_type, sizeof(ompi_request_type_t));
    /* req_status */
#if 0
    /* We do never initialize the req_status in the creation functions,
     * they are just used to transport values back up....
     */
    opal_memchecker_base_isdefined (&(*request)->req_status.MPI_SOURCE, sizeof(int));
    opal_memchecker_base_isdefined (&(*request)->req_status.MPI_TAG, sizeof(int));
    opal_memchecker_base_isdefined (&(*request)->req_status.MPI_ERROR, sizeof(int));
    opal_memchecker_base_isdefined (&(*request)->req_status._cancelled, sizeof(int));
    opal_memchecker_base_isdefined (&(*request)->req_status._ucount, sizeof(size_t));
#endif

    opal_memchecker_base_isdefined ((void*)&(*request)->req_complete, sizeof(volatile _Bool));
    opal_memchecker_base_isdefined ((void*)&(*request)->req_state, sizeof(volatile ompi_request_state_t));
    opal_memchecker_base_isdefined (&(*request)->req_persistent, sizeof(_Bool));
    opal_memchecker_base_isdefined (&(*request)->req_f_to_c_index, sizeof(int));
    opal_memchecker_base_isdefined (&(*request)->req_free, sizeof(ompi_request_free_fn_t));
    opal_memchecker_base_isdefined (&(*request)->req_cancel, sizeof(ompi_request_cancel_fn_t));
    /* req_mpi_object */
    opal_memchecker_base_isdefined (&(*request)->req_mpi_object.comm, sizeof(struct ompi_communicator_t *));
    opal_memchecker_base_isdefined (&(*request)->req_mpi_object.file, sizeof(struct ompi_file_t *));
    opal_memchecker_base_isdefined (&(*request)->req_mpi_object.win, sizeof(struct ompi_win_t *));

    return OMPI_SUCCESS;
}
#else
#define memchecker_request(request)
#endif /* OMPI_WANT_MEMCHECKER_MPI_OBJECTS */


/*
 * Check every member of the status, whether their memory areas are defined.
 */
#ifdef OMPI_WANT_MEMCHECKER_MPI_OBJECTS
static inline int memchecker_status(MPI_Status *status)
{
    if (!opal_memchecker_base_runindebugger()) {
        return OMPI_SUCCESS;
    }

    opal_memchecker_base_isdefined (&status->MPI_SOURCE, sizeof(int));
    opal_memchecker_base_isdefined (&status->MPI_TAG, sizeof(int));
    opal_memchecker_base_isdefined (&status->MPI_ERROR, sizeof(int));
    opal_memchecker_base_isdefined (&status->_cancelled, sizeof(int));
    opal_memchecker_base_isdefined (&status->_ucount, sizeof(size_t));

    return OMPI_SUCCESS;
}
#else
#define memchecker_status(status)
#endif /* OMPI_WANT_MEMCHECKER_MPI_OBJECTS */


/*
 * Check every member of the datatype, whether their memory areas are defined.
 */
#ifdef OMPI_WANT_MEMCHECKER_MPI_OBJECTS
static inline int memchecker_datatype(MPI_Datatype type)
{
    if (!opal_memchecker_base_runindebugger()) {
        return OMPI_SUCCESS;
    }

    /* the data description in the opal_datatype_t super class */
    opal_memchecker_base_isdefined (&type->super.flags, sizeof(uint16_t));
    opal_memchecker_base_isdefined (&type->super.id, sizeof(uint16_t));
    opal_memchecker_base_isdefined (&type->super.bdt_used, sizeof(uint32_t));
    opal_memchecker_base_isdefined (&type->super.size, sizeof(size_t));
    opal_memchecker_base_isdefined (&type->super.true_lb, sizeof(ptrdiff_t));
    opal_memchecker_base_isdefined (&type->super.true_ub, sizeof(ptrdiff_t));
    opal_memchecker_base_isdefined (&type->super.lb, sizeof(ptrdiff_t));
    opal_memchecker_base_isdefined (&type->super.ub, sizeof(ptrdiff_t));
    opal_memchecker_base_isdefined (&type->super.align, sizeof(uint32_t));
    opal_memchecker_base_isdefined (&type->super.nbElems, sizeof(uint32_t));
    /* name... */
    opal_memchecker_base_isdefined (&type->super.desc.length, sizeof(opal_datatype_count_t));
    opal_memchecker_base_isdefined (&type->super.desc.used, sizeof(opal_datatype_count_t));
    opal_memchecker_base_isdefined (&type->super.desc.desc, sizeof(dt_elem_desc_t *));
    opal_memchecker_base_isdefined (&type->super.opt_desc.length, sizeof(opal_datatype_count_t));
    opal_memchecker_base_isdefined (&type->super.opt_desc.used, sizeof(opal_datatype_count_t));
    opal_memchecker_base_isdefined (&type->super.opt_desc.desc, sizeof(dt_elem_desc_t *));
    if( NULL != type->super.ptypes )
        opal_memchecker_base_isdefined (&type->super.ptypes, OPAL_DATATYPE_MAX_PREDEFINED * sizeof(size_t));

    opal_memchecker_base_isdefined (&type->id, sizeof(int32_t));
    opal_memchecker_base_isdefined (&type->d_f_to_c_index, sizeof(int32_t));
    opal_memchecker_base_isdefined (&type->d_keyhash, sizeof(opal_hash_table_t *));
    opal_memchecker_base_isdefined (&type->args, sizeof(void *));
    opal_memchecker_base_isdefined (&type->packed_description, sizeof(void *));
    opal_memchecker_base_isdefined (&type->name, MPI_MAX_OBJECT_NAME * sizeof(char));

    return OMPI_SUCCESS;
}
#else
#define memchecker_datatype(type)
#endif /* OMPI_WANT_MEMCHECKER_MPI_OBJECTS */

/*
 * Check every member of the message, whether their memory areas are defined.
 */
#ifdef OMPI_WANT_MEMCHECKER_MPI_OBJECTS
static inline int memchecker_message(MPI_Message *message)
{
    if (!opal_memchecker_base_runindebugger()) {
        return OMPI_SUCCESS;
    }

    opal_memchecker_base_isdefined (&message->m_f_to_c_index, sizeof(int));
    opal_memchecker_base_isdefined (&message->comm, sizeof(ompi_communicator_t *));
    opal_memchecker_base_isdefined (&message->req_ptr, sizeof(void *));
    opal_memchecker_base_isdefined (&message->peer, sizeof(int));
    opal_memchecker_base_isdefined (&message->count, sizeof(sizeof_t));

    return OMPI_SUCCESS;
}
#else
#define memchecker_message(message)
#endif /* OMPI_WANT_MEMCHECKER_MPI_OBJECTS */


#endif /* OMPI_MEMCHECKER_H */


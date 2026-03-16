/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2012 Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2012-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_MESSAGE_H
#define OMPI_MESSAGE_H

#include "mpi.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_pointer_array.h"

BEGIN_C_DECLS

struct ompi_communicator_t;

struct ompi_message_t {
    opal_free_list_item_t super;        /**< Base type */
    int m_f_to_c_index;                 /**< Fortran handle for this message */
    struct ompi_communicator_t *comm;   /**< communicator used in probe */
    void* req_ptr;                      /**< PML data */
    int peer;                           /**< peer, same as status.MPI_SOURCE */
    size_t count;                       /**< same value as status._ucount */
};
typedef struct ompi_message_t ompi_message_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_message_t);

/**
 * Padded struct to maintain back compatibiltiy.
 * See ompi/communicator/communicator.h comments with struct ompi_communicator_t
 * for full explanation why we chose the following padding construct for predefines.
 */
#define PREDEFINED_MESSAGE_PAD 256

struct ompi_predefined_message_t {
    struct ompi_message_t message;
    char padding[PREDEFINED_MESSAGE_PAD - sizeof(ompi_message_t)];
};

typedef struct ompi_predefined_message_t ompi_predefined_message_t;

int ompi_message_init(void);

int ompi_message_finalize(void);

OMPI_DECLSPEC extern opal_free_list_t ompi_message_free_list;
OMPI_DECLSPEC extern opal_pointer_array_t  ompi_message_f_to_c_table;
OMPI_DECLSPEC extern ompi_predefined_message_t  ompi_message_no_proc;

static inline
ompi_message_t*
ompi_message_alloc(void)
{
    return (ompi_message_t *) opal_free_list_get (&ompi_message_free_list);
}

static inline
void
ompi_message_return(ompi_message_t* msg)
{
    if (MPI_UNDEFINED != msg->m_f_to_c_index) {
        opal_pointer_array_set_item(&ompi_message_f_to_c_table,
                                    msg->m_f_to_c_index, NULL);
        msg->m_f_to_c_index = MPI_UNDEFINED;
    }

    opal_free_list_return (&ompi_message_free_list,
                           &msg->super);
}


END_C_DECLS

#endif

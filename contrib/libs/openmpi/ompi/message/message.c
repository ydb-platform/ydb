/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2012      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/constants.h"

#include "opal/class/opal_object.h"
#include "ompi/message/message.h"
#include "ompi/constants.h"

static void ompi_message_constructor(ompi_message_t *msg);

OBJ_CLASS_INSTANCE(ompi_message_t,
                   opal_free_list_item_t,
                   ompi_message_constructor, NULL);

opal_free_list_t ompi_message_free_list = {{{0}}};
opal_pointer_array_t  ompi_message_f_to_c_table = {{0}};

ompi_predefined_message_t ompi_message_null = {{{{{0}}}}};
ompi_predefined_message_t ompi_message_no_proc = {{{{{0}}}}};

static void ompi_message_constructor(ompi_message_t *msg)
{
    msg->comm = NULL;
    msg->req_ptr = NULL;
    msg->m_f_to_c_index = MPI_UNDEFINED;
    msg->count = 0;
}

int
ompi_message_init(void)
{
    int rc;

    OBJ_CONSTRUCT(&ompi_message_free_list, opal_free_list_t);
    rc = opal_free_list_init(&ompi_message_free_list,
                             sizeof(ompi_message_t), 8,
                             OBJ_CLASS(ompi_message_t),
                             0, 0, 8, -1, 8, NULL, 0, NULL, NULL, NULL);

    OBJ_CONSTRUCT(&ompi_message_f_to_c_table, opal_pointer_array_t);

    ompi_message_null.message.req_ptr = NULL;
    ompi_message_null.message.count = 0;
    ompi_message_null.message.m_f_to_c_index =
        opal_pointer_array_add(&ompi_message_f_to_c_table, &ompi_message_null);

    OBJ_CONSTRUCT(&ompi_message_no_proc, ompi_message_t);
    ompi_message_no_proc.message.m_f_to_c_index =
        opal_pointer_array_add(&ompi_message_f_to_c_table,
                               &ompi_message_no_proc);
    if (1 != ompi_message_no_proc.message.m_f_to_c_index) {
        return OMPI_ERR_NOT_FOUND;
    }

    return rc;
}

int
ompi_message_finalize(void)
{
    OBJ_DESTRUCT(&ompi_message_no_proc);
    OBJ_DESTRUCT(&ompi_message_free_list);
    OBJ_DESTRUCT(&ompi_message_f_to_c_table);

    return OMPI_SUCCESS;
}

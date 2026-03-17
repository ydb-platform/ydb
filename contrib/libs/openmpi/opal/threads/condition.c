/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/threads/condition.h"


static void opal_condition_construct(opal_condition_t *c)
{
    c->c_waiting = 0;
    c->c_signaled = 0;
}


static void opal_condition_destruct(opal_condition_t *c)
{
}

OBJ_CLASS_INSTANCE(opal_condition_t,
                   opal_object_t,
                   opal_condition_construct,
                   opal_condition_destruct);

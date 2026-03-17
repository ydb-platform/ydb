/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_INFOSUBSCRIBE_H
#define OMPI_INFOSUBSCRIBE_H

#include <string.h>

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_hash_table.h"
#include "opal/threads/mutex.h"
#include "opal/util/info.h"

#include "opal/mca/base/mca_base_var_enum.h"


#define INFO_SUBSCRIBER_SIZE 5

struct opal_infosubscriber_t {
    opal_object_t	s_base;
    opal_hash_table_t 	s_subscriber_table;
    opal_info_t 	*s_info;
};
typedef struct opal_infosubscriber_t opal_infosubscriber_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_infosubscriber_t);

typedef char*(opal_key_interest_callback_t)(opal_infosubscriber_t*, char*, char*);

/**
 *   opal_infosubscribe_change_info - Make changes to a Comm/Win/File Info
 *
 *   @param type Comm/Win/File
 *   @param object corresponding Com/Win/File object
 *   @param old_info previous info setting
 *   @param new_info new info setting
 *
 *   @retval OPAL status
 *
 *   Notifies subscribers of info's that have gone away and new info settings
 */
int opal_infosubscribe_change_info(opal_infosubscriber_t*, opal_info_t *);


/**
 *   opal_infosubscribe_subscribe - Request to be updated about info changes to a Comm/Win/File Info
 *
 *   @param type Comm/Win/File of obj
 *   @param obj either a comm, win or file
 *   @param key info key being set
 *   @param initial_value default value (or NULL if none)
 *   @param callback callback to be called when key changes
 *
 *   @retval OPAL status
 *
 *   Notifies subscribers of info's that have gone away and new info settings
 *   Does not try to optimize settings that are the same between old and new
 *   info's.
 */
int opal_infosubscribe_subscribe(opal_infosubscriber_t*, char *, char *, opal_key_interest_callback_t);

#endif /* OMPI_INFO_H */

/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/constants.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/mca/shmem/base/base.h"

/* ////////////////////////////////////////////////////////////////////////// */
int
opal_shmem_segment_create(opal_shmem_ds_t *ds_buf,
                          const char *file_name,
                          size_t size)
{
    if (!opal_shmem_base_selected) {
        return OPAL_ERROR;
    }

    return opal_shmem_base_module->segment_create(ds_buf, file_name, size);
}

/* ////////////////////////////////////////////////////////////////////////// */
int
opal_shmem_ds_copy(const opal_shmem_ds_t *from,
                   opal_shmem_ds_t *to)
{
    if (!opal_shmem_base_selected) {
        return OPAL_ERROR;
    }

    return opal_shmem_base_module->ds_copy(from, to);
}

/* ////////////////////////////////////////////////////////////////////////// */
void *
opal_shmem_segment_attach(opal_shmem_ds_t *ds_buf)
{
    if (!opal_shmem_base_selected) {
        return NULL;
    }

    return opal_shmem_base_module->segment_attach(ds_buf);
}

/* ////////////////////////////////////////////////////////////////////////// */
int
opal_shmem_segment_detach(opal_shmem_ds_t *ds_buf)
{
    if (!opal_shmem_base_selected) {
        return OPAL_ERROR;
    }

    return opal_shmem_base_module->segment_detach(ds_buf);
}

/* ////////////////////////////////////////////////////////////////////////// */
int
opal_shmem_unlink(opal_shmem_ds_t *ds_buf)
{
    if (!opal_shmem_base_selected) {
        return OPAL_ERROR;
    }

    return opal_shmem_base_module->unlink(ds_buf);
}


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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * This is the base request type for all IO requests.
 */

#ifndef IO_BASE_REQUEST_H
#define IO_BASE_REQUEST_H

#include "ompi_config.h"
#include "opal/class/opal_object.h"
#include "ompi/request/request.h"
#include "ompi/file/file.h"
#include "ompi/mca/io/base/base.h"

/**
 * Base request type.
 */
struct mca_io_base_request_t {
    /** Base request */
    ompi_request_t super;

    /** ompi_file_t of the file that owns this request */
    ompi_file_t *req_file;

    /** io component version number of the module that owns this
        request (i.e., this defines what follows this entry in
        memory) */
    mca_io_base_version_t req_ver;
    /** True if free has been called on this request (before it has
        been finalized */
    volatile bool free_called;
};
/**
 * Convenience typedef
 */
typedef struct mca_io_base_request_t mca_io_base_request_t;

BEGIN_C_DECLS

    /**
     * Declare the class
     */
    OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_io_base_request_t);

END_C_DECLS

#endif

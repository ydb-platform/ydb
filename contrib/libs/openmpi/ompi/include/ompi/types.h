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

#ifndef OMPI_TYPES_H
#define OMPI_TYPES_H


/*
 * handle to describe a parallel job
 */
typedef char* ompi_job_handle_t;

/*
 * Predefine some internal types so we dont need all the include
 * dependencies.
 */
struct ompi_communicator_t;
struct ompi_datatype_t;
struct ompi_op_t;

#endif

/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_OP_BASE_FUNCTIONS_H
#define OMPI_OP_BASE_FUNCTIONS_H

#include "ompi_config.h"
#include "ompi/mca/op/op.h"


BEGIN_C_DECLS

/**
 * Globals holding all the "base" function pointers, indexed by op and
 * datatype.
 */
OMPI_DECLSPEC extern ompi_op_base_handler_fn_t
    ompi_op_base_functions[OMPI_OP_BASE_FORTRAN_OP_MAX][OMPI_OP_BASE_TYPE_MAX];
OMPI_DECLSPEC extern ompi_op_base_3buff_handler_fn_t
    ompi_op_base_3buff_functions[OMPI_OP_BASE_FORTRAN_OP_MAX][OMPI_OP_BASE_TYPE_MAX];

END_C_DECLS

#endif /* OMPI_OP_BASE_FUNCTIONS_H */

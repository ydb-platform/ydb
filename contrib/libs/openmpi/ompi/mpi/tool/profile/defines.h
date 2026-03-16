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
 * Copyright (c) 2009-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c)      2012 Oak Rigde National Laboratory. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPIT_PROFILE_DEFINES_H
#define OMPIT_PROFILE_DEFINES_H
/*
 * This file is included in the top directory only if
 * profiling is required. Once profiling is required,
 * this file will replace all MPI_* symbols with
 * PMPI_* symbols
 */
#define MPI_T_category_changed PMPI_T_category_changed
#define MPI_T_category_get_categories PMPI_T_category_get_categories
#define MPI_T_category_get_cvars PMPI_T_category_get_cvars
#define MPI_T_category_get_info PMPI_T_category_get_info
#define MPI_T_category_get_index PMPI_T_category_get_index
#define MPI_T_category_get_num PMPI_T_category_get_num
#define MPI_T_category_get_pvars PMPI_T_category_get_pvars
#define MPI_T_cvar_get_info PMPI_T_cvar_get_info
#define MPI_T_cvar_get_index PMPI_T_cvar_get_index
#define MPI_T_cvar_get_num PMPI_T_cvar_get_num
#define MPI_T_cvar_handle_alloc PMPI_T_cvar_handle_alloc
#define MPI_T_cvar_handle_free PMPI_T_cvar_handle_free
#define MPI_T_cvar_read PMPI_T_cvar_read
#define MPI_T_cvar_write PMPI_T_cvar_write
#define MPI_T_enum_get_info PMPI_T_enum_get_info
#define MPI_T_enum_get_item PMPI_T_enum_get_item
#define MPI_T_finalize PMPI_T_finalize
#define MPI_T_init_thread PMPI_T_init_thread
#define MPI_T_pvar_get_info PMPI_T_pvar_get_info
#define MPI_T_pvar_get_index PMPI_T_pvar_get_index
#define MPI_T_pvar_get_num PMPI_T_pvar_get_num
#define MPI_T_pvar_handle_alloc PMPI_T_pvar_handle_alloc
#define MPI_T_pvar_handle_free PMPI_T_pvar_handle_free
#define MPI_T_pvar_read PMPI_T_pvar_read
#define MPI_T_pvar_readreset PMPI_T_pvar_readreset
#define MPI_T_pvar_reset PMPI_T_pvar_reset
#define MPI_T_pvar_session_create PMPI_T_pvar_session_create
#define MPI_T_pvar_session_free PMPI_T_pvar_session_free
#define MPI_T_pvar_start PMPI_T_pvar_start
#define MPI_T_pvar_stop PMPI_T_pvar_stop
#define MPI_T_pvar_write PMPI_T_pvar_write
#endif /* OMPIT_C_PROFILE_DEFINES_H */

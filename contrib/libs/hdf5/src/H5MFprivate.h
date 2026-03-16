/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*-------------------------------------------------------------------------
 *
 * Created:             H5MFprivate.h
 *
 * Purpose:             Private header file for file memory management.
 *
 *-------------------------------------------------------------------------
 */
#ifndef H5MFprivate_H
#define H5MFprivate_H

/* Private headers needed by this file */
#include "H5Fprivate.h"  /* File access				*/
#include "H5FDprivate.h" /* File Drivers				*/

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* File space manager routines */
H5_DLL herr_t H5MF_init_merge_flags(H5F_shared_t *f_sh);
H5_DLL herr_t H5MF_get_freespace(H5F_t *f, hsize_t *tot_space, hsize_t *meta_size);
H5_DLL herr_t H5MF_close(H5F_t *f);
H5_DLL herr_t H5MF_try_close(H5F_t *f);

/* File space allocation routines */
H5_DLL haddr_t H5MF_alloc(H5F_t *f, H5FD_mem_t type, hsize_t size);
H5_DLL haddr_t H5MF_aggr_vfd_alloc(H5F_t *f, H5FD_mem_t type, hsize_t size);
H5_DLL herr_t  H5MF_xfree(H5F_t *f, H5FD_mem_t type, haddr_t addr, hsize_t size);
H5_DLL herr_t H5MF_try_extend(H5F_t *f, H5FD_mem_t type, haddr_t addr, hsize_t size, hsize_t extra_requested);
H5_DLL htri_t H5MF_try_shrink(H5F_t *f, H5FD_mem_t alloc_type, haddr_t addr, hsize_t size);
H5_DLL herr_t H5MF_get_free_sections(H5F_t *f, H5FD_mem_t type, size_t nsects, H5F_sect_info_t *sect_info,
                                     size_t *sect_count);

/* File 'temporary' space allocation routines */
H5_DLL haddr_t H5MF_alloc_tmp(H5F_t *f, hsize_t size);

/* 'block aggregator' routines */
H5_DLL herr_t H5MF_free_aggrs(H5F_t *f);

/* Free space manager settling routines */
H5_DLL herr_t H5MF_settle_raw_data_fsm(H5F_t *f, bool *fsm_settled);
H5_DLL herr_t H5MF_settle_meta_data_fsm(H5F_t *f, bool *fsm_settled);

/* This function has to be declared in H5MFprivate.h as it is needed
 * in our test code to allow us to manually start a self referential
 * free space manager prior to the first file space allocations /
 * deallocation without causing assertion failures on the first
 * file space allocation / deallocation.
 */
H5_DLL herr_t H5MF_tidy_self_referential_fsm_hack(H5F_t *f);

/* Debugging routines */
#ifdef H5MF_DEBUGGING
H5_DLL herr_t H5MF_sects_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth);
#endif /* H5MF_DEBUGGING */

#endif /* end H5MFprivate_H */

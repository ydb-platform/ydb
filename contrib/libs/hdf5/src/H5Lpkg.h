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

/*
 * Purpose:     This file contains declarations which are visible
 *              only within the H5L package. Source files outside the
 *              H5L package should include H5Lprivate.h instead.
 */
#if !(defined H5L_FRIEND || defined H5L_MODULE)
#error "Do not include this file outside the H5L package!"
#endif

#ifndef H5Lpkg_H
#define H5Lpkg_H

/* Get package's private header */
#include "H5Lprivate.h"

/* Other private headers needed by this file */

/**************************/
/* Package Private Macros */
/**************************/

/****************************/
/* Package Private Typedefs */
/****************************/

/*****************************/
/* Package Private Variables */
/*****************************/

/******************************/
/* Package Private Prototypes */
/******************************/

H5_DLL herr_t H5L__create_hard(H5G_loc_t *cur_loc, const char *cur_name, const H5G_loc_t *link_loc,
                               const char *link_name, hid_t lcpl_id);
H5_DLL herr_t H5L__create_soft(const char *target_path, const H5G_loc_t *cur_loc, const char *cur_name,
                               hid_t lcpl_id);
H5_DLL herr_t H5L__create_ud(const H5G_loc_t *link_loc, const char *link_name, const void *ud_data,
                             size_t ud_data_size, H5L_type_t type, hid_t lcpl_id);
H5_DLL herr_t H5L__exists(const H5G_loc_t *loc, const char *name, bool *exists);
H5_DLL herr_t H5L__get_info_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type,
                                   H5_iter_order_t order, hsize_t n, H5L_info2_t *linfo /*out*/);
H5_DLL herr_t H5L__get_name_by_idx(const H5G_loc_t *loc, const char *group_name, H5_index_t idx_type,
                                   H5_iter_order_t order, hsize_t n, char *name /*out*/, size_t size,
                                   size_t *name_len);
H5_DLL herr_t H5L__get_val(const H5G_loc_t *loc, const char *name, void *buf /*out*/, size_t size);
H5_DLL herr_t H5L__get_val_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type,
                                  H5_iter_order_t order, hsize_t n, void *buf /*out*/, size_t size);
H5_DLL herr_t H5L__move(const H5G_loc_t *src_loc, const char *src_name, const H5G_loc_t *dst_loc,
                        const char *dst_name, bool copy_flag, hid_t lcpl_id);
H5_DLL herr_t H5L__delete(const H5G_loc_t *loc, const char *name);
H5_DLL herr_t H5L__delete_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type,
                                 H5_iter_order_t order, hsize_t n);
H5_DLL herr_t H5L__link_copy_file(H5F_t *dst_file, const H5O_link_t *_src_lnk, const H5O_loc_t *src_oloc,
                                  H5O_link_t *dst_lnk, H5O_copy_t *cpy_info);

#endif /* H5Lpkg_H */

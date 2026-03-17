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
 * Purpose:	The private header file for the native VOL connector.
 */

#ifndef H5VLnative_private_H
#define H5VLnative_private_H

/* Private headers needed by this file */
#include "H5Fprivate.h" /* Files                                    */
#include "H5VLnative.h" /* Native VOL connector                     */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/*****************************/
/* Library Private Variables */
/*****************************/

/******************************/
/* Library Private Prototypes */
/******************************/

#ifdef __cplusplus
extern "C" {
#endif

/* Attribute callbacks */
H5_DLL void  *H5VL__native_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name,
                                       hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                                       hid_t dxpl_id, void **req);
void         *H5VL__native_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name,
                                     hid_t aapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_attr_read(void *attr, hid_t dtype_id, void *buf, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_attr_write(void *attr, hid_t dtype_id, const void *buf, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                         H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
H5_DLL void  *H5VL__native_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                          hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id,
                                          hid_t dapl_id, hid_t dxpl_id, void **req);
H5_DLL void  *H5VL__native_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                        hid_t dapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_dataset_read(size_t count, void *obj[], hid_t mem_type_id[], hid_t mem_space_id[],
                                        hid_t file_space_id[], hid_t dxpl_id, void *buf[], void **req);
H5_DLL herr_t H5VL__native_dataset_write(size_t count, void *obj[], hid_t mem_type_id[], hid_t mem_space_id[],
                                         hid_t file_space_id[], hid_t dxpl_id, const void *buf[], void **req);
H5_DLL herr_t H5VL__native_dataset_get(void *dset, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_dataset_specific(void *dset, H5VL_dataset_specific_args_t *args, hid_t dxpl_id,
                                            void **req);
H5_DLL herr_t H5VL__native_dataset_optional(void *dset, H5VL_optional_args_t *args, hid_t dxpl_id,
                                            void **req);
H5_DLL herr_t H5VL__native_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
H5_DLL void  *H5VL__native_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                           hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
                                           hid_t dxpl_id, void **req);
H5_DLL void  *H5VL__native_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                         hid_t tapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_datatype_get(void *dt, H5VL_datatype_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_datatype_specific(void *dt, H5VL_datatype_specific_args_t *args, hid_t dxpl_id,
                                             void **req);
H5_DLL herr_t H5VL__native_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
H5_DLL void  *H5VL__native_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                                       hid_t dxpl_id, void **req);
H5_DLL void  *H5VL__native_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id,
                                     void **req);
H5_DLL herr_t H5VL__native_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_file_specific(void *file, H5VL_file_specific_args_t *args, hid_t dxpl_id,
                                         void **req);
H5_DLL herr_t H5VL__native_file_optional(void *file, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
H5_DLL void  *H5VL__native_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                        hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id,
                                        void **req);
H5_DLL void  *H5VL__native_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                      hid_t gapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id,
                                          void **req);
H5_DLL herr_t H5VL__native_group_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
H5_DLL herr_t H5VL__native_link_create(H5VL_link_create_args_t *args, void *obj,
                                       const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
                                       hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                     const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                     hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                     const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                     hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_link_get(void *obj, const H5VL_loc_params_t *loc_params,
                                    H5VL_link_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                         H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req);

/* Object callbacks */
H5_DLL void *H5VL__native_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type,
                                      hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_object_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
                                       const char *src_name, void *dst_obj,
                                       const H5VL_loc_params_t *loc_params2, const char *dst_name,
                                       hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_object_get(void *obj, const H5VL_loc_params_t *loc_params,
                                      H5VL_object_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                           H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL__native_object_optional(void *obj, const H5VL_loc_params_t *loc_params,
                                           H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Connector/container introspection functions */
H5_DLL herr_t H5VL__native_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                                                   const H5VL_class_t **conn_cls);
H5_DLL herr_t H5VL__native_introspect_get_cap_flags(const void *info, uint64_t *cap_flags);
H5_DLL herr_t H5VL__native_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type,
                                                uint64_t *flags);

/* Blob callbacks */
H5_DLL herr_t H5VL__native_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
H5_DLL herr_t H5VL__native_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
H5_DLL herr_t H5VL__native_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t *args);

/* Token callbacks */
H5_DLL herr_t H5VL__native_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2,
                                     int *cmp_value);
H5_DLL herr_t H5VL__native_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token,
                                        char **token_str);
H5_DLL herr_t H5VL__native_str_to_token(void *obj, H5I_type_t obj_type, const char *token_str,
                                        H5O_token_t *token);

/* Helper functions */
H5_DLL herr_t H5VL_native_get_file_addr_len(hid_t loc_id, size_t *addr_len);
H5_DLL herr_t H5VL__native_get_file_addr_len(void *obj, H5I_type_t obj_type, size_t *addr_len);
H5_DLL herr_t H5VL_native_addr_to_token(void *obj, H5I_type_t obj_type, haddr_t addr, H5O_token_t *token);
H5_DLL herr_t H5VL_native_token_to_addr(void *obj, H5I_type_t obj_type, H5O_token_t token, haddr_t *addr);
H5_DLL herr_t H5VL_native_get_file_struct(void *obj, H5I_type_t type, H5F_t **file);

#ifdef __cplusplus
}
#endif

#endif /* H5VLnative_private_H */

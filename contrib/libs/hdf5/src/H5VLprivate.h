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

#ifndef H5VLprivate_H
#define H5VLprivate_H

/* Include package's public header */
#include "H5VLpublic.h" /* Generic Functions                    */

/* Include connector author public header(s) */
#include "H5VLconnector.h"          /* VOL connector author routines */
#include "H5VLconnector_passthru.h" /* Pass-through VOL connector author routines */
#include "H5VLnative.h"             /* Native VOL connector macros, for VOL connector authors */

/* Private headers needed by this file */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Internal struct to track VOL connector information for objects */
typedef struct H5VL_t {
    const H5VL_class_t *cls;   /* Pointer to connector class struct                    */
    int64_t             nrefs; /* Number of references by objects using this struct    */
    hid_t               id;    /* Identifier for the VOL connector                     */
} H5VL_t;

/* Internal vol object structure returned to the API */
typedef struct H5VL_object_t {
    void   *data;      /* Pointer to connector-managed data for this object    */
    H5VL_t *connector; /* Pointer to VOL connector struct                      */
    size_t  rc;        /* Reference count                                      */
} H5VL_object_t;

/* Internal structure to hold the connector ID & info for FAPLs */
typedef struct H5VL_connector_prop_t {
    hid_t       connector_id;   /* VOL connector's ID                                   */
    const void *connector_info; /* VOL connector info, for open callbacks               */
} H5VL_connector_prop_t;

/* Which kind of VOL connector field to use for searching */
typedef enum H5VL_get_connector_kind_t {
    H5VL_GET_CONNECTOR_BY_NAME, /* Name field is set */
    H5VL_GET_CONNECTOR_BY_VALUE /* Value field is set */
} H5VL_get_connector_kind_t;

/*****************************/
/* Library Private Variables */
/*****************************/

/******************************/
/* Library Private Prototypes */
/******************************/

/* Utility functions */
H5_DLL herr_t  H5VL_init_phase1(void);
H5_DLL herr_t  H5VL_init_phase2(void);
H5_DLL H5VL_t *H5VL_new_connector(hid_t connector_id);
H5_DLL herr_t  H5VL_cmp_connector_cls(int *cmp_value, const H5VL_class_t *cls1, const H5VL_class_t *cls2);
H5_DLL herr_t  H5VL_conn_copy(H5VL_connector_prop_t *value);
H5_DLL int64_t H5VL_conn_inc_rc(H5VL_t *connector);
H5_DLL int64_t H5VL_conn_dec_rc(H5VL_t *connector);
H5_DLL herr_t  H5VL_conn_free(const H5VL_connector_prop_t *info);
H5_DLL herr_t  H5VL_get_cap_flags(const H5VL_connector_prop_t *prop, uint64_t *cap_flags);

/* Functions that deal with VOL connectors */
union H5PL_key_t;
H5_DLL herr_t H5VL_check_plugin_load(const H5VL_class_t *cls, const union H5PL_key_t *key, bool *success);

/* NOTE:    The object and ID functions below deal in VOL objects (i.e.;
 *          H5VL_object_t). Similar non-VOL calls exist in H5Iprivate.h. Use
 *          the H5VL calls with objects that go through the VOL, such as
 *          datasets and groups, and the H5I calls with objects
 *          that do not, such as property lists and dataspaces. Datatypes
 *          are can be either named, where they will use the VOL, or not,
 *          and thus require special treatment. See the datatype docs for
 *          how to handle this.
 */

/* Functions that manipulate VOL objects */
H5_DLL void          *H5VL_object(hid_t id);
H5_DLL void          *H5VL_object_data(const H5VL_object_t *vol_obj);
H5_DLL void          *H5VL_object_unwrap(const H5VL_object_t *vol_obj);
H5_DLL void          *H5VL_object_verify(hid_t id, H5I_type_t obj_type);
H5_DLL H5VL_object_t *H5VL_vol_object(hid_t id);
H5_DLL H5VL_object_t *H5VL_create_object(void *object, H5VL_t *vol_connector);
H5_DLL H5VL_object_t *H5VL_create_object_using_vol_id(H5I_type_t type, void *obj, hid_t connector_id);
H5_DLL hsize_t        H5VL_object_inc_rc(H5VL_object_t *obj);
H5_DLL herr_t         H5VL_free_object(H5VL_object_t *obj);
H5_DLL herr_t         H5VL_object_is_native(const H5VL_object_t *obj, bool *is_native);
H5_DLL herr_t         H5VL_file_is_same(const H5VL_object_t *vol_obj1, const H5VL_object_t *vol_obj2,
                                        bool *same_file);

/* Functions that wrap / unwrap VOL objects */
H5_DLL herr_t H5VL_get_wrap_ctx(const H5VL_class_t *connector, void *obj, void **wrap_ctx);
H5_DLL void  *H5VL_wrap_object(const H5VL_class_t *connector, void *wrap_ctx, void *obj, H5I_type_t obj_type);
H5_DLL void  *H5VL_unwrap_object(const H5VL_class_t *connector, void *obj);
H5_DLL herr_t H5VL_free_wrap_ctx(const H5VL_class_t *connector, void *wrap_ctx);
H5_DLL herr_t H5VL_set_vol_wrapper(const H5VL_object_t *vol_obj);
H5_DLL herr_t H5VL_inc_vol_wrapper(void *vol_wrap_ctx);
H5_DLL herr_t H5VL_dec_vol_wrapper(void *vol_wrap_ctx);
H5_DLL herr_t H5VL_reset_vol_wrapper(void);

/* Library state functions */
H5_DLL herr_t H5VL_retrieve_lib_state(void **state);
H5_DLL herr_t H5VL_start_lib_state(void);
H5_DLL herr_t H5VL_restore_lib_state(const void *state);
H5_DLL herr_t H5VL_finish_lib_state(void);
H5_DLL herr_t H5VL_free_lib_state(void *state);

/* ID registration functions */
H5_DLL hid_t  H5VL_register(H5I_type_t type, void *object, H5VL_t *vol_connector, bool app_ref);
H5_DLL hid_t  H5VL_wrap_register(H5I_type_t type, void *obj, bool app_ref);
H5_DLL hid_t  H5VL_register_using_vol_id(H5I_type_t type, void *obj, hid_t connector_id, bool app_ref);
H5_DLL herr_t H5VL_register_using_existing_id(H5I_type_t type, void *object, H5VL_t *vol_connector,
                                              bool app_ref, hid_t existing_id);

/* Object access functions */
struct H5P_libclass_t;
H5_DLL herr_t H5VL_setup_args(hid_t loc_id, H5I_type_t id_type, H5VL_object_t **vol_obj);
H5_DLL herr_t H5VL_setup_loc_args(hid_t loc_id, H5VL_object_t **vol_obj, H5VL_loc_params_t *loc_params);
H5_DLL herr_t H5VL_setup_acc_args(hid_t loc_id, const struct H5P_libclass_t *libclass, bool is_collective,
                                  hid_t *acspl_id, H5VL_object_t **vol_obj, H5VL_loc_params_t *loc_params);
H5_DLL herr_t H5VL_setup_self_args(hid_t loc_id, H5VL_object_t **vol_obj, H5VL_loc_params_t *loc_params);
H5_DLL herr_t H5VL_setup_name_args(hid_t loc_id, const char *name, bool is_collective, hid_t lapl_id,
                                   H5VL_object_t **vol_obj, H5VL_loc_params_t *loc_params);
H5_DLL herr_t H5VL_setup_idx_args(hid_t loc_id, const char *name, H5_index_t idx_type, H5_iter_order_t order,
                                  hsize_t n, bool is_collective, hid_t lapl_id, H5VL_object_t **vol_obj,
                                  H5VL_loc_params_t *loc_params);
H5_DLL herr_t H5VL_setup_token_args(hid_t loc_id, H5O_token_t *obj_token, H5VL_object_t **vol_obj,
                                    H5VL_loc_params_t *loc_params);

/**********************************
 * VOL connector callback wrappers
 *********************************/

/* Connector "management" functions */
H5_DLL int    H5VL_copy_connector_info(const H5VL_class_t *connector, void **dst_info, const void *src_info);
H5_DLL herr_t H5VL_cmp_connector_info(const H5VL_class_t *connector, int *cmp_value, const void *info1,
                                      const void *info2);
H5_DLL herr_t H5VL_free_connector_info(hid_t connector_id, const void *info);

/* Attribute functions */
H5_DLL void  *H5VL_attr_create(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                               const char *attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id,
                               hid_t aapl_id, hid_t dxpl_id, void **req);
H5_DLL void  *H5VL_attr_open(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                             const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_attr_read(const H5VL_object_t *vol_obj, hid_t dtype_id, void *buf, hid_t dxpl_id,
                             void **req);
H5_DLL herr_t H5VL_attr_write(const H5VL_object_t *vol_obj, hid_t dtype_id, const void *buf, hid_t dxpl_id,
                              void **req);
H5_DLL herr_t H5VL_attr_get(const H5VL_object_t *vol_obj, H5VL_attr_get_args_t *args, hid_t dxpl_id,
                            void **req);
H5_DLL herr_t H5VL_attr_specific(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                 H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_attr_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                                 void **req);
H5_DLL herr_t H5VL_attr_close(const H5VL_object_t *vol_obj, hid_t dxpl_id, void **req);

/* Dataset functions */
H5_DLL void  *H5VL_dataset_create(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                  const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
                                  hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
H5_DLL void  *H5VL_dataset_open(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_dataset_read(size_t count, const H5VL_object_t *vol_obj[], hid_t mem_type_id[],
                                hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id, void *buf[],
                                void **req);
H5_DLL herr_t H5VL_dataset_read_direct(size_t count, void *obj[], H5VL_t *connector, hid_t mem_type_id[],
                                       hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id,
                                       void *buf[], void **req);
H5_DLL herr_t H5VL_dataset_write(size_t count, const H5VL_object_t *vol_obj[], hid_t mem_type_id[],
                                 hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id,
                                 const void *buf[], void **req);
H5_DLL herr_t H5VL_dataset_write_direct(size_t count, void *obj[], H5VL_t *connector, hid_t mem_type_id[],
                                        hid_t mem_space_id[], hid_t file_space_id[], hid_t dxpl_id,
                                        const void *buf[], void **req);
H5_DLL herr_t H5VL_dataset_get(const H5VL_object_t *vol_obj, H5VL_dataset_get_args_t *args, hid_t dxpl_id,
                               void **req);
H5_DLL herr_t H5VL_dataset_specific(const H5VL_object_t *cls, H5VL_dataset_specific_args_t *args,
                                    hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_dataset_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                                    void **req);
H5_DLL herr_t H5VL_dataset_close(const H5VL_object_t *vol_obj, hid_t dxpl_id, void **req);

/* Datatype functions */
H5_DLL void  *H5VL_datatype_commit(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                   const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                                   hid_t tapl_id, hid_t dxpl_id, void **req);
H5_DLL void  *H5VL_datatype_open(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                 const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_datatype_get(const H5VL_object_t *vol_obj, H5VL_datatype_get_args_t *args, hid_t dxpl_id,
                                void **req);
H5_DLL herr_t H5VL_datatype_specific(const H5VL_object_t *vol_obj, H5VL_datatype_specific_args_t *args,
                                     hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_datatype_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                                     void **req);
H5_DLL herr_t H5VL_datatype_optional_op(H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                                        void **req, H5VL_object_t **vol_obj_ptr);
H5_DLL herr_t H5VL_datatype_close(const H5VL_object_t *vol_obj, hid_t dxpl_id, void **req);

/* File functions */
H5_DLL void  *H5VL_file_create(const H5VL_connector_prop_t *connector_prop, const char *name, unsigned flags,
                               hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
H5_DLL void  *H5VL_file_open(H5VL_connector_prop_t *connector_prop, const char *name, unsigned flags,
                             hid_t fapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_file_get(const H5VL_object_t *vol_obj, H5VL_file_get_args_t *args, hid_t dxpl_id,
                            void **req);
H5_DLL herr_t H5VL_file_specific(const H5VL_object_t *vol_obj, H5VL_file_specific_args_t *args, hid_t dxpl_id,
                                 void **req);
H5_DLL herr_t H5VL_file_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                                 void **req);
H5_DLL herr_t H5VL_file_close(const H5VL_object_t *vol_obj, hid_t dxpl_id, void **req);

/* Group functions */
H5_DLL void  *H5VL_group_create(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id,
                                void **req);
H5_DLL void  *H5VL_group_open(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                              const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_group_get(const H5VL_object_t *vol_obj, H5VL_group_get_args_t *args, hid_t dxpl_id,
                             void **req);
H5_DLL herr_t H5VL_group_specific(const H5VL_object_t *vol_obj, H5VL_group_specific_args_t *args,
                                  hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_group_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                                  void **req);
H5_DLL herr_t H5VL_group_close(const H5VL_object_t *vol_obj, hid_t dxpl_id, void **req);

/* Link functions */
H5_DLL herr_t H5VL_link_create(H5VL_link_create_args_t *args, const H5VL_object_t *vol_obj,
                               const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
                               hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_link_copy(const H5VL_object_t *src_vol_obj, const H5VL_loc_params_t *loc_params1,
                             const H5VL_object_t *dst_vol_obj, const H5VL_loc_params_t *loc_params2,
                             hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_link_move(const H5VL_object_t *src_vol_obj, const H5VL_loc_params_t *loc_params1,
                             const H5VL_object_t *dst_vol_obj, const H5VL_loc_params_t *loc_params2,
                             hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_link_get(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                            H5VL_link_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_link_specific(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                 H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_link_optional(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                 H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Object functions */
H5_DLL void  *H5VL_object_open(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *params,
                               H5I_type_t *opened_type, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_object_copy(const H5VL_object_t *src_obj, const H5VL_loc_params_t *src_loc_params,
                               const char *src_name, const H5VL_object_t *dst_obj,
                               const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id,
                               hid_t lcpl_id, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_object_get(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                              H5VL_object_get_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_object_specific(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                   H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req);
H5_DLL herr_t H5VL_object_optional(const H5VL_object_t *vol_obj, const H5VL_loc_params_t *loc_params,
                                   H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Connector/container introspection functions */
H5_DLL herr_t H5VL_introspect_get_conn_cls(const H5VL_object_t *vol_obj, H5VL_get_conn_lvl_t lvl,
                                           const H5VL_class_t **conn_cls);
H5_DLL herr_t H5VL_introspect_get_cap_flags(const void *info, const H5VL_class_t *cls, uint64_t *cap_flags);
H5_DLL herr_t H5VL_introspect_opt_query(const H5VL_object_t *vol_obj, H5VL_subclass_t subcls, int opt_type,
                                        uint64_t *flags);

/* Asynchronous functions */
H5_DLL herr_t H5VL_request_wait(const H5VL_object_t *vol_obj, uint64_t timeout,
                                H5VL_request_status_t *status);
H5_DLL herr_t H5VL_request_notify(const H5VL_object_t *vol_obj, H5VL_request_notify_t cb, void *ctx);
H5_DLL herr_t H5VL_request_cancel(const H5VL_object_t *vol_obj, H5VL_request_status_t *status);
H5_DLL herr_t H5VL_request_specific(const H5VL_object_t *vol_obj, H5VL_request_specific_args_t *args);
H5_DLL herr_t H5VL_request_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args);
H5_DLL herr_t H5VL_request_free(const H5VL_object_t *vol_obj);

/* Blob functions */
H5_DLL herr_t H5VL_blob_put(const H5VL_object_t *vol_obj, const void *buf, size_t size, void *blob_id,
                            void *ctx);
H5_DLL herr_t H5VL_blob_get(const H5VL_object_t *vol_obj, const void *blob_id, void *buf, size_t size,
                            void *ctx);
H5_DLL herr_t H5VL_blob_specific(const H5VL_object_t *vol_obj, void *blob_id,
                                 H5VL_blob_specific_args_t *args);
H5_DLL herr_t H5VL_blob_optional(const H5VL_object_t *vol_obj, void *blob_id, H5VL_optional_args_t *args);

/* Token functions */
H5_DLL herr_t H5VL_token_cmp(const H5VL_object_t *vol_obj, const H5O_token_t *token1,
                             const H5O_token_t *token2, int *cmp_value);
H5_DLL herr_t H5VL_token_to_str(const H5VL_object_t *vol_obj, H5I_type_t obj_type, const H5O_token_t *token,
                                char **token_str);
H5_DLL herr_t H5VL_token_from_str(const H5VL_object_t *vol_obj, H5I_type_t obj_type, const char *token_str,
                                  H5O_token_t *token);

/* Generic functions */
H5_DLL herr_t H5VL_optional(const H5VL_object_t *vol_obj, H5VL_optional_args_t *args, hid_t dxpl_id,
                            void **req);

#endif /* H5VLprivate_H */

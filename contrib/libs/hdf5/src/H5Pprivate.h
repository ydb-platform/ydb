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
 * This file contains private information about the H5P module
 */
#ifndef H5Pprivate_H
#define H5Pprivate_H

/* Early typedefs to avoid circular dependencies */
typedef struct H5P_genplist_t H5P_genplist_t;

/* Include package's public header */
#include "H5Ppublic.h"

/* Private headers needed by this file */
#include "H5private.h" /* Generic Functions			*/

/**************************/
/* Library Private Macros */
/**************************/

/* ========  String creation property names ======== */
#define H5P_STRCRT_CHAR_ENCODING_NAME "character_encoding" /* Character set encoding for string */

/* If the module using this macro is allowed access to the private variables, access them directly */
#ifdef H5P_MODULE
#define H5P_PLIST_ID(P) ((P)->plist_id)
#define H5P_CLASS(P)    ((P)->pclass)
#else /* H5P_MODULE */
#define H5P_PLIST_ID(P) (H5P_get_plist_id(P))
#define H5P_CLASS(P)    (H5P_get_class(P))
#endif /* H5P_MODULE */

#define H5_COLL_MD_READ_FLAG_NAME "collective_metadata_read"

/****************************/
/* Library Private Typedefs */
/****************************/

typedef enum H5P_coll_md_read_flag_t {
    H5P_FORCE_FALSE = -1,
    H5P_USER_FALSE  = 0,
    H5P_USER_TRUE   = 1
} H5P_coll_md_read_flag_t;

/* Forward declarations for anonymous H5P objects */
typedef struct H5P_genclass_t H5P_genclass_t;

typedef enum H5P_plist_type_t {
    H5P_TYPE_USER             = 0,
    H5P_TYPE_ROOT             = 1,
    H5P_TYPE_OBJECT_CREATE    = 2,
    H5P_TYPE_FILE_CREATE      = 3,
    H5P_TYPE_FILE_ACCESS      = 4,
    H5P_TYPE_DATASET_CREATE   = 5,
    H5P_TYPE_DATASET_ACCESS   = 6,
    H5P_TYPE_DATASET_XFER     = 7,
    H5P_TYPE_FILE_MOUNT       = 8,
    H5P_TYPE_GROUP_CREATE     = 9,
    H5P_TYPE_GROUP_ACCESS     = 10,
    H5P_TYPE_DATATYPE_CREATE  = 11,
    H5P_TYPE_DATATYPE_ACCESS  = 12,
    H5P_TYPE_STRING_CREATE    = 13,
    H5P_TYPE_ATTRIBUTE_CREATE = 14,
    H5P_TYPE_OBJECT_COPY      = 15,
    H5P_TYPE_LINK_CREATE      = 16,
    H5P_TYPE_LINK_ACCESS      = 17,
    H5P_TYPE_ATTRIBUTE_ACCESS = 18,
    H5P_TYPE_VOL_INITIALIZE   = 19,
    H5P_TYPE_MAP_CREATE       = 20,
    H5P_TYPE_MAP_ACCESS       = 21,
    H5P_TYPE_REFERENCE_ACCESS = 22,
    H5P_TYPE_MAX_TYPE
} H5P_plist_type_t;

/* Function pointer for library classes with properties to register */
typedef herr_t (*H5P_reg_prop_func_t)(H5P_genclass_t *pclass);

/*
 * Each library property list class has a variable of this type that contains
 * class variables and methods used to initialize the class.
 */
typedef struct H5P_libclass_t {
    const char      *name; /* Class name */
    H5P_plist_type_t type; /* Class type */

    H5P_genclass_t    **par_pclass;    /* Pointer to global parent class property list class */
    H5P_genclass_t    **pclass;        /* Pointer to global property list class */
    hid_t *const        class_id;      /* Pointer to global property list class ID */
    hid_t *const        def_plist_id;  /* Pointer to global default property list ID */
    H5P_reg_prop_func_t reg_prop_func; /* Register class's properties */

    /* Class callback function pointers & info */
    H5P_cls_create_func_t create_func; /* Function to call when a property list is created */
    void                 *create_data; /* Pointer to user data to pass along to create callback */
    H5P_cls_copy_func_t   copy_func;   /* Function to call when a property list is copied */
    void                 *copy_data;   /* Pointer to user data to pass along to copy callback */
    H5P_cls_close_func_t  close_func;  /* Function to call when a property list is closed */
    void                 *close_data;  /* Pointer to user data to pass along to close callback */
} H5P_libclass_t;

/*****************************/
/* Library Private Variables */
/*****************************/

/* Predefined property list classes. */
H5_DLLVAR H5P_genclass_t *H5P_CLS_ROOT_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_OBJECT_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_FILE_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_FILE_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_DATASET_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_DATASET_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_DATASET_XFER_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_FILE_MOUNT_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_GROUP_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_GROUP_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_DATATYPE_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_DATATYPE_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_MAP_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_MAP_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_ATTRIBUTE_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_ATTRIBUTE_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_OBJECT_COPY_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_LINK_CREATE_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_LINK_ACCESS_g;
H5_DLLVAR H5P_genclass_t *H5P_CLS_STRING_CREATE_g;

/* Internal property list classes */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_LCRT[1]; /* Link creation */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_LACC[1]; /* Link access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_AACC[1]; /* Attribute access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_DACC[1]; /* Dataset access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_GACC[1]; /* Group access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_TACC[1]; /* Named datatype access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_MACC[1]; /* Map access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_FACC[1]; /* File access */
H5_DLLVAR const struct H5P_libclass_t H5P_CLS_OCPY[1]; /* Object copy */

/******************************/
/* Library Private Prototypes */
/******************************/

/* Forward declaration of structs used below */
struct H5O_fill_t;
struct H5T_t;
struct H5VL_connector_prop_t;

/* Package initialization routines */
H5_DLL herr_t H5P_init_phase1(void);
H5_DLL herr_t H5P_init_phase2(void);

/* Internal versions of API routines */
H5_DLL herr_t H5P_close(H5P_genplist_t *plist);
H5_DLL hid_t  H5P_create_id(H5P_genclass_t *pclass, bool app_ref);
H5_DLL hid_t  H5P_copy_plist(const H5P_genplist_t *old_plist, bool app_ref);
H5_DLL herr_t H5P_get(H5P_genplist_t *plist, const char *name, void *value);
H5_DLL herr_t H5P_set(H5P_genplist_t *plist, const char *name, const void *value);
H5_DLL herr_t H5P_peek(H5P_genplist_t *plist, const char *name, void *value);
H5_DLL herr_t H5P_poke(H5P_genplist_t *plist, const char *name, const void *value);
H5_DLL herr_t H5P_insert(H5P_genplist_t *plist, const char *name, size_t size, void *value,
                         H5P_prp_set_func_t prp_set, H5P_prp_get_func_t prp_get,
                         H5P_prp_encode_func_t prp_encode, H5P_prp_decode_func_t prp_decode,
                         H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy,
                         H5P_prp_compare_func_t prp_cmp, H5P_prp_close_func_t prp_close);
H5_DLL herr_t H5P_remove(H5P_genplist_t *plist, const char *name);
H5_DLL htri_t H5P_exist_plist(const H5P_genplist_t *plist, const char *name);
H5_DLL htri_t H5P_class_isa(const H5P_genclass_t *pclass1, const H5P_genclass_t *pclass2);
H5_DLL char  *H5P_get_class_name(H5P_genclass_t *pclass);

/* Internal helper routines */
H5_DLL herr_t      H5P_get_nprops_pclass(const H5P_genclass_t *pclass, size_t *nprops, bool recurse);
H5_DLL hid_t       H5P_peek_driver(H5P_genplist_t *plist);
H5_DLL const void *H5P_peek_driver_info(H5P_genplist_t *plist);
H5_DLL const char *H5P_peek_driver_config_str(H5P_genplist_t *plist);
H5_DLL herr_t      H5P_set_driver(H5P_genplist_t *plist, hid_t new_driver_id, const void *new_driver_info,
                                  const char *new_driver_config_str);
H5_DLL herr_t      H5P_set_driver_by_name(H5P_genplist_t *plist, const char *driver_name,
                                          const char *driver_config, bool app_ref);
H5_DLL herr_t      H5P_set_driver_by_value(H5P_genplist_t *plist, H5FD_class_value_t driver_value,
                                           const char *driver_config, bool app_ref);
H5_DLL herr_t      H5P_set_vol(H5P_genplist_t *plist, hid_t vol_id, const void *vol_info);
H5_DLL herr_t H5P_reset_vol_class(const H5P_genclass_t *pclass, const struct H5VL_connector_prop_t *vol_prop);
H5_DLL herr_t H5P_set_vlen_mem_manager(H5P_genplist_t *plist, H5MM_allocate_t alloc_func, void *alloc_info,
                                       H5MM_free_t free_func, void *free_info);
H5_DLL herr_t H5P_is_fill_value_defined(const struct H5O_fill_t *fill, H5D_fill_value_t *status);
H5_DLL int    H5P_fill_value_cmp(const void *value1, const void *value2, size_t size);
H5_DLL herr_t H5P_modify_filter(H5P_genplist_t *plist, H5Z_filter_t filter, unsigned flags, size_t cd_nelmts,
                                const unsigned cd_values[]);
H5_DLL herr_t H5P_get_filter_by_id(H5P_genplist_t *plist, H5Z_filter_t id, unsigned int *flags,
                                   size_t *cd_nelmts, unsigned cd_values[], size_t namelen, char name[],
                                   unsigned *filter_config);
H5_DLL htri_t H5P_filter_in_pline(H5P_genplist_t *plist, H5Z_filter_t id);

/* Query internal fields of the property list struct */
H5_DLL hid_t           H5P_get_plist_id(const H5P_genplist_t *plist);
H5_DLL H5P_genclass_t *H5P_get_class(const H5P_genplist_t *plist);

/* *SPECIAL* Don't make more of these! -QAK */
H5_DLL htri_t          H5P_isa_class(hid_t plist_id, hid_t pclass_id);
H5_DLL H5P_genplist_t *H5P_object_verify(hid_t plist_id, hid_t pclass_id);

/* Private DCPL routines */
H5_DLL herr_t H5P_fill_value_defined(H5P_genplist_t *plist, H5D_fill_value_t *status);
H5_DLL herr_t H5P_get_fill_value(H5P_genplist_t *plist, const struct H5T_t *type, void *value);
H5_DLL int    H5P_ignore_cmp(const void H5_ATTR_UNUSED *val1, const void H5_ATTR_UNUSED *val2,
                             size_t H5_ATTR_UNUSED size);

#endif /* H5Pprivate_H */

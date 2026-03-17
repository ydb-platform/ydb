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
 * Purpose: This file contains declarations which are visible only within
 *          the H5P package.  Source files outside the H5P package should
 *          include H5Pprivate.h instead.
 */
#if !(defined H5P_FRIEND || defined H5P_MODULE)
#error "Do not include this file outside the H5P package!"
#endif

#ifndef H5Ppkg_H
#define H5Ppkg_H

/* Get package's private header */
#include "H5Pprivate.h"

/* Other private headers needed by this file */
#include "H5SLprivate.h" /* Skip lists				*/

/**************************/
/* Package Private Macros */
/**************************/

/****************************/
/* Package Private Typedefs */
/****************************/

/* Define enum for type of object that property is within */
typedef enum {
    H5P_PROP_WITHIN_UNKNOWN = 0, /* Property container is unknown */
    H5P_PROP_WITHIN_LIST,        /* Property is within a list */
    H5P_PROP_WITHIN_CLASS        /* Property is within a class */
} H5P_prop_within_t;

/* Define enum for modifications to class */
typedef enum {
    H5P_MOD_ERR = (-1), /* Indicate an error */
    H5P_MOD_INC_CLS,    /* Increment the dependent class count*/
    H5P_MOD_DEC_CLS,    /* Decrement the dependent class count*/
    H5P_MOD_INC_LST,    /* Increment the dependent list count*/
    H5P_MOD_DEC_LST,    /* Decrement the dependent list count*/
    H5P_MOD_INC_REF,    /* Increment the ID reference count*/
    H5P_MOD_DEC_REF,    /* Decrement the ID reference count*/
    H5P_MOD_MAX         /* Upper limit on class modifications */
} H5P_class_mod_t;

/* Define structure to hold property information */
typedef struct H5P_genprop_t {
    /* Values for this property */
    char             *name;        /* Name of property */
    size_t            size;        /* Size of property value */
    void             *value;       /* Pointer to property value */
    H5P_prop_within_t type;        /* Type of object the property is within */
    bool              shared_name; /* Whether the name is shared or not */

    /* Callback function pointers & info */
    H5P_prp_create_func_t  create; /* Function to call when a property is created */
    H5P_prp_set_func_t     set;    /* Function to call when a property value is set */
    H5P_prp_get_func_t     get;    /* Function to call when a property value is retrieved */
    H5P_prp_encode_func_t  encode; /* Function to call when a property is encoded */
    H5P_prp_decode_func_t  decode; /* Function to call when a property is decoded */
    H5P_prp_delete_func_t  del;    /* Function to call when a property is deleted */
    H5P_prp_copy_func_t    copy;   /* Function to call when a property is copied */
    H5P_prp_compare_func_t cmp;    /* Function to call when a property is compared */
    H5P_prp_close_func_t   close;  /* Function to call when a property is closed */
} H5P_genprop_t;

/* Define structure to hold class information */
struct H5P_genclass_t {
    struct H5P_genclass_t *parent; /* Pointer to parent class */
    char                  *name;   /* Name of property list class */
    H5P_plist_type_t       type;   /* Type of property */
    size_t                 nprops; /* Number of properties in class */
    unsigned
        plists; /* Number of property lists that have been created since the last modification to the class */
    unsigned classes; /* Number of classes that have been derived since the last modification to the class */
    unsigned ref_count; /* Number of outstanding ID's open on this class object */
    bool     deleted;  /* Whether this class has been deleted and is waiting for dependent classes & proplists
                             to close */
    unsigned revision; /* Revision number of a particular class (global) */
    H5SL_t  *props;    /* Skip list containing properties */

    /* Callback function pointers & info */
    H5P_cls_create_func_t create_func; /* Function to call when a property list is created */
    void                 *create_data; /* Pointer to user data to pass along to create callback */
    H5P_cls_copy_func_t   copy_func;   /* Function to call when a property list is copied */
    void                 *copy_data;   /* Pointer to user data to pass along to copy callback */
    H5P_cls_close_func_t  close_func;  /* Function to call when a property list is closed */
    void                 *close_data;  /* Pointer to user data to pass along to close callback */
};

/* Define structure to hold property list information */
struct H5P_genplist_t {
    H5P_genclass_t *pclass;     /* Pointer to class info */
    hid_t           plist_id;   /* Copy of the property list ID (for use in close callback) */
    size_t          nprops;     /* Number of properties in class */
    bool            class_init; /* Whether the class initialization callback finished successfully */
    H5SL_t         *del;        /* Skip list containing names of deleted properties */
    H5SL_t         *props;      /* Skip list containing properties */
};

/* Property list/class iterator callback function pointer */
typedef int (*H5P_iterate_int_t)(H5P_genprop_t *prop, void *udata);

/* Forward declarations (for prototypes & struct definitions) */
struct H5Z_filter_info_t;

/*****************************/
/* Package Private Variables */
/*****************************/

/******************************/
/* Package Private Prototypes */
/******************************/

/* Private functions, not part of the publicly documented API */
H5_DLL H5P_genclass_t *H5P__create_class(H5P_genclass_t *par_class, const char *name, H5P_plist_type_t type,
                                         H5P_cls_create_func_t cls_create, void *create_data,
                                         H5P_cls_copy_func_t cls_copy, void *copy_data,
                                         H5P_cls_close_func_t cls_close, void *close_data);
H5_DLL H5P_genclass_t *H5P__copy_pclass(H5P_genclass_t *pclass);
H5_DLL herr_t H5P__register_real(H5P_genclass_t *pclass, const char *name, size_t size, const void *def_value,
                                 H5P_prp_create_func_t prp_create, H5P_prp_set_func_t prp_set,
                                 H5P_prp_get_func_t prp_get, H5P_prp_encode_func_t prp_encode,
                                 H5P_prp_decode_func_t prp_decode, H5P_prp_delete_func_t prp_delete,
                                 H5P_prp_copy_func_t prp_copy, H5P_prp_compare_func_t prp_cmp,
                                 H5P_prp_close_func_t prp_close);
H5_DLL herr_t H5P__register(H5P_genclass_t **pclass, const char *name, size_t size, const void *def_value,
                            H5P_prp_create_func_t prp_create, H5P_prp_set_func_t prp_set,
                            H5P_prp_get_func_t prp_get, H5P_prp_encode_func_t prp_encode,
                            H5P_prp_decode_func_t prp_decode, H5P_prp_delete_func_t prp_delete,
                            H5P_prp_copy_func_t prp_copy, H5P_prp_compare_func_t prp_cmp,
                            H5P_prp_close_func_t prp_close);
H5_DLL herr_t H5P__add_prop(H5SL_t *props, H5P_genprop_t *prop);
H5_DLL herr_t H5P__access_class(H5P_genclass_t *pclass, H5P_class_mod_t mod);
H5_DLL herr_t H5P__class_get(const H5P_genclass_t *pclass, const char *name, void *value);
H5_DLL herr_t H5P__class_set(const H5P_genclass_t *pclass, const char *name, const void *value);
H5_DLL htri_t H5P__exist_pclass(H5P_genclass_t *pclass, const char *name);
H5_DLL herr_t H5P__get_size_plist(const H5P_genplist_t *plist, const char *name, size_t *size);
H5_DLL herr_t H5P__get_size_pclass(H5P_genclass_t *pclass, const char *name, size_t *size);
H5_DLL herr_t H5P__get_nprops_plist(const H5P_genplist_t *plist, size_t *nprops);
H5_DLL int    H5P__cmp_class(const H5P_genclass_t *pclass1, const H5P_genclass_t *pclass2);
H5_DLL herr_t H5P__cmp_plist(const H5P_genplist_t *plist1, const H5P_genplist_t *plist2, int *cmp_ret);
H5_DLL int    H5P__iterate_plist(const H5P_genplist_t *plist, bool iter_all_prop, int *idx,
                                 H5P_iterate_int_t iter_func, void *iter_data);
H5_DLL int    H5P__iterate_pclass(const H5P_genclass_t *pclass, int *idx, H5P_iterate_int_t iter_func,
                                  void *iter_data);
H5_DLL herr_t H5P__copy_prop_plist(hid_t dst_id, hid_t src_id, const char *name);
H5_DLL herr_t H5P__copy_prop_pclass(hid_t dst_id, hid_t src_id, const char *name);
H5_DLL herr_t H5P__unregister(H5P_genclass_t *pclass, const char *name);
H5_DLL char  *H5P__get_class_path(H5P_genclass_t *pclass);
H5_DLL H5P_genclass_t *H5P__open_class_path(const char *path);
H5_DLL H5P_genclass_t *H5P__get_class_parent(const H5P_genclass_t *pclass);
H5_DLL herr_t          H5P__close_class(H5P_genclass_t *pclass);
H5_DLL H5P_genprop_t  *H5P__find_prop_plist(const H5P_genplist_t *plist, const char *name);
H5_DLL hid_t           H5P__new_plist_of_type(H5P_plist_type_t type);

/* Encode/decode routines */
H5_DLL herr_t H5P__encode(const H5P_genplist_t *plist, bool enc_all_prop, void *buf, size_t *nalloc);
H5_DLL hid_t  H5P__decode(const void *buf);
H5_DLL herr_t H5P__encode_hsize_t(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__encode_size_t(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__encode_unsigned(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__encode_uint8_t(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__encode_bool(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__encode_double(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__decode_hsize_t(const void **_pp, void *value);
H5_DLL herr_t H5P__decode_size_t(const void **_pp, void *value);
H5_DLL herr_t H5P__decode_unsigned(const void **_pp, void *value);
H5_DLL herr_t H5P__decode_uint8_t(const void **_pp, void *value);
H5_DLL herr_t H5P__decode_bool(const void **_pp, void *value);
H5_DLL herr_t H5P__decode_double(const void **_pp, void *value);
H5_DLL herr_t H5P__encode_coll_md_read_flag_t(const void *value, void **_pp, size_t *size);
H5_DLL herr_t H5P__decode_coll_md_read_flag_t(const void **_pp, void *value);

/* Private FAPL routines */
H5_DLL herr_t H5P__facc_set_def_driver(void);

/* Private OCPL routines */
H5_DLL herr_t H5P__get_filter(const struct H5Z_filter_info_t *filter, unsigned int *flags, size_t *cd_nelmts,
                              unsigned cd_values[], size_t namelen, char name[], unsigned *filter_config);

/* Testing functions */
#ifdef H5P_TESTING
H5_DLL char *H5P__get_class_path_test(hid_t pclass_id);
H5_DLL hid_t H5P__open_class_path_test(const char *path);
#endif /* H5P_TESTING */

#endif /* H5Ppkg_H */

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

/* Purpose:     This file contains declarations which are visible
 *              only within the H5R package. Source files outside the
 *              H5R package should include H5Rprivate.h instead.
 */
#if !(defined H5R_FRIEND || defined H5R_MODULE)
#error "Do not include this file outside the H5R package!"
#endif

#ifndef H5Rpkg_H
#define H5Rpkg_H

/* Get package's private header */
#include "H5Rprivate.h"

/* Other private headers needed by this file */
#include "H5Fprivate.h" /* Files                                    */
#include "H5Gprivate.h" /* Groups                                   */
#include "H5Oprivate.h" /* Object headers                           */
#include "H5Sprivate.h" /* Dataspaces                               */

/**************************/
/* Package Private Macros */
/**************************/

/* Encode flags */
#define H5R_IS_EXTERNAL 0x1 /* Set when encoding reference to external file */

/* Macros for convenience */
#define H5R_REF_FILENAME(x) ((x)->info.obj.filename)
#define H5R_REF_ATTRNAME(x) ((x)->info.attr.name)

/* Header size */
#define H5R_ENCODE_HEADER_SIZE (2 * sizeof(uint8_t))

/****************************/
/* Package Private Typedefs */
/****************************/

/* Object reference */
typedef struct H5R_ref_priv_obj_t {
    H5O_token_t token;    /* Object token     */
    char       *filename; /* File name        */
} H5R_ref_priv_obj_t;

/* Region reference */
typedef struct H5R_ref_priv_reg_t {
    H5R_ref_priv_obj_t obj;   /* Object reference */
    H5S_t             *space; /* Selection        */
} H5R_ref_priv_reg_t;

/* Attribute reference */
typedef struct H5R_ref_priv_attr_t {
    H5R_ref_priv_obj_t obj;  /* Object reference */
    char              *name; /* Attribute name   */
} H5R_ref_priv_attr_t;

/* Generic reference type (keep it cache aligned) */
typedef struct H5R_ref_priv_t {
    union {
        H5R_ref_priv_obj_t  obj;  /* Object reference                 */
        H5R_ref_priv_reg_t  reg;  /* Region reference                 */
        H5R_ref_priv_attr_t attr; /* Attribute Reference             */
    } info;
    hid_t    loc_id;      /* Cached location identifier       */
    uint32_t encode_size; /* Cached encoding size             */
    int8_t   type;        /* Reference type                   */
    uint8_t  token_size;  /* Cached token size                */
    bool     app_ref;     /* App ref on loc_id                */
} H5R_ref_priv_t;

/*****************************/
/* Package Private Variables */
/*****************************/

/******************************/
/* Package Private Prototypes */
/******************************/
H5_DLL herr_t H5R__create_object(const H5O_token_t *obj_token, size_t token_size, H5R_ref_priv_t *ref);
H5_DLL herr_t H5R__create_region(const H5O_token_t *obj_token, size_t token_size, H5S_t *space,
                                 H5R_ref_priv_t *ref);
H5_DLL herr_t H5R__create_attr(const H5O_token_t *obj_token, size_t token_size, const char *attr_name,
                               H5R_ref_priv_t *ref);
H5_DLL herr_t H5R__destroy(H5R_ref_priv_t *ref);

H5_DLL herr_t H5R__set_loc_id(H5R_ref_priv_t *ref, hid_t id, bool inc_ref, bool app_ref);
H5_DLL hid_t  H5R__get_loc_id(const H5R_ref_priv_t *ref);
H5_DLL hid_t  H5R__reopen_file(H5R_ref_priv_t *ref, hid_t fapl_id);

H5_DLL H5R_type_t H5R__get_type(const H5R_ref_priv_t *ref);
H5_DLL htri_t     H5R__equal(const H5R_ref_priv_t *ref1, const H5R_ref_priv_t *ref2);
H5_DLL herr_t     H5R__copy(const H5R_ref_priv_t *src_ref, H5R_ref_priv_t *dst_ref);

H5_DLL herr_t H5R__get_obj_token(const H5R_ref_priv_t *ref, H5O_token_t *obj_token, size_t *token_size);
H5_DLL herr_t H5R__set_obj_token(H5R_ref_priv_t *ref, const H5O_token_t *obj_token, size_t token_size);
H5_DLL herr_t H5R__get_region(const H5R_ref_priv_t *ref, H5S_t *space);

H5_DLL ssize_t H5R__get_file_name(const H5R_ref_priv_t *ref, char *buf, size_t size);
H5_DLL ssize_t H5R__get_attr_name(const H5R_ref_priv_t *ref, char *buf, size_t size);

H5_DLL herr_t H5R__encode(const char *filename, const H5R_ref_priv_t *ref, unsigned char *buf, size_t *nalloc,
                          unsigned flags);
H5_DLL herr_t H5R__decode(const unsigned char *buf, size_t *nbytes, H5R_ref_priv_t *ref);

/* Native HDF5 specific routines */
H5_DLL herr_t H5R__encode_heap(H5F_t *f, unsigned char *buf, size_t *nalloc, const unsigned char *data,
                               size_t data_size);
H5_DLL herr_t H5R__decode_heap(H5F_t *f, const unsigned char *buf, size_t *nbytes, unsigned char **data_ptr,
                               size_t *data_size);

H5_DLL herr_t H5R__encode_token_obj_compat(const H5O_token_t *obj_token, size_t token_size,
                                           unsigned char *buf, size_t *nalloc);
H5_DLL herr_t H5R__decode_token_obj_compat(const unsigned char *buf, size_t *nbytes, H5O_token_t *obj_token,
                                           size_t token_size);

H5_DLL herr_t H5R__decode_token_region_compat(H5F_t *f, const unsigned char *buf, size_t *nbytes,
                                              H5O_token_t *obj_token, size_t token_size, H5S_t **space_ptr);

#endif /* H5Rpkg_H */

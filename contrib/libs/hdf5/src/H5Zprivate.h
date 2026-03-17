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

#ifndef H5Zprivate_H
#define H5Zprivate_H

/* Early typedefs to avoid circular dependencies */
typedef struct H5Z_filter_info_t H5Z_filter_info_t;

/* Include package's public headers */
#include "H5Zpublic.h"
#include "H5Zdevelop.h"

/* Private headers needed by this file */
#include "H5Tprivate.h" /* Datatypes				*/
#include "H5Sprivate.h" /* Dataspace                */

/**************************/
/* Library Private Macros */
/**************************/

/* Special parameters for szip compression */
/* [These are aliases for the similar definitions in szlib.h, which we can't
 * include directly due to the duplication of various symbols with the zlib.h
 * header file] */
#define H5_SZIP_LSB_OPTION_MASK 8
#define H5_SZIP_MSB_OPTION_MASK 16
#define H5_SZIP_RAW_OPTION_MASK 128

/* Common # of 'client data values' for filters */
/* (avoids dynamic memory allocation in most cases) */
#define H5Z_COMMON_CD_VALUES 4

/* Common size of filter name */
/* (avoids dynamic memory allocation in most cases) */
#define H5Z_COMMON_NAME_LEN 12

/****************************/
/* Library Private Typedefs */
/****************************/

/* Structure to store information about each filter's parameters */
struct H5Z_filter_info_t {
    H5Z_filter_t id;                               /*filter identification number	     */
    unsigned     flags;                            /*defn and invocation flags	     */
    char         _name[H5Z_COMMON_NAME_LEN];       /*internal filter name		     */
    char        *name;                             /*optional filter name		     */
    size_t       cd_nelmts;                        /*number of elements in cd_values[]  */
    unsigned     _cd_values[H5Z_COMMON_CD_VALUES]; /*internal client data values		     */
    unsigned    *cd_values;                        /*client data values		     */
};

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
struct H5O_pline_t; /*forward decl*/

/* Internal API routines */
H5_DLL herr_t H5Z_init(void);
H5_DLL herr_t H5Z_register(const H5Z_class2_t *cls);
H5_DLL herr_t H5Z_append(struct H5O_pline_t *pline, H5Z_filter_t filter, unsigned flags, size_t cd_nelmts,
                         const unsigned int cd_values[]);
H5_DLL herr_t H5Z_modify(const struct H5O_pline_t *pline, H5Z_filter_t filter, unsigned flags,
                         size_t cd_nelmts, const unsigned int cd_values[]);
H5_DLL herr_t H5Z_pipeline(const struct H5O_pline_t *pline, unsigned flags, unsigned *filter_mask /*in,out*/,
                           H5Z_EDC_t edc_read, H5Z_cb_t cb_struct, size_t *nbytes /*in,out*/,
                           size_t *buf_size /*in,out*/, void **buf /*in,out*/);
H5_DLL H5Z_class2_t      *H5Z_find(H5Z_filter_t id);
H5_DLL herr_t             H5Z_can_apply(hid_t dcpl_id, hid_t type_id);
H5_DLL herr_t             H5Z_set_local(hid_t dcpl_id, hid_t type_id);
H5_DLL herr_t             H5Z_can_apply_direct(const struct H5O_pline_t *pline);
H5_DLL herr_t             H5Z_set_local_direct(const struct H5O_pline_t *pline);
H5_DLL htri_t             H5Z_ignore_filters(hid_t dcpl_id, const H5T_t *type, const H5S_t *space);
H5_DLL H5Z_filter_info_t *H5Z_filter_info(const struct H5O_pline_t *pline, H5Z_filter_t filter);
H5_DLL htri_t             H5Z_filter_in_pline(const struct H5O_pline_t *pline, H5Z_filter_t filter);
H5_DLL htri_t             H5Z_all_filters_avail(const struct H5O_pline_t *pline);
H5_DLL htri_t             H5Z_filter_avail(H5Z_filter_t id);
H5_DLL herr_t             H5Z_delete(struct H5O_pline_t *pline, H5Z_filter_t filter);
H5_DLL herr_t             H5Z_get_filter_info(H5Z_filter_t filter, unsigned int *filter_config_flags);

/* Data Transform Functions */
typedef struct H5Z_data_xform_t H5Z_data_xform_t; /* Defined in H5Ztrans.c */

H5_DLL H5Z_data_xform_t *H5Z_xform_create(const char *expr);
H5_DLL herr_t            H5Z_xform_copy(H5Z_data_xform_t **data_xform_prop);
H5_DLL herr_t            H5Z_xform_destroy(H5Z_data_xform_t *data_xform_prop);
H5_DLL herr_t            H5Z_xform_eval(H5Z_data_xform_t *data_xform_prop, void *array, size_t array_size,
                                        const H5T_t *buf_type);
H5_DLL bool              H5Z_xform_noop(const H5Z_data_xform_t *data_xform_prop);
H5_DLL const char       *H5Z_xform_extract_xform_str(const H5Z_data_xform_t *data_xform_prop);

#endif

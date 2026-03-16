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
 * This file contains private information about the H5D module
 */
#ifndef H5Aprivate_H
#define H5Aprivate_H

/* Include package's public header */
#include "H5Apublic.h"

/* Private headers needed by this file */
#include "H5Gprivate.h" /* Groups				*/
#include "H5Oprivate.h" /* Object headers                       */
#include "H5Sprivate.h" /* Dataspace                            */
#include "H5Tprivate.h" /* Datatypes                            */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Forward references of package typedefs */
typedef struct H5A_t H5A_t;

/* Attribute iteration operator for internal library callbacks */
typedef herr_t (*H5A_lib_iterate_t)(const H5A_t *attr, void *op_data);

/* Describe kind of callback to make for each attribute */
typedef enum H5A_attr_iter_op_type_t {
#ifndef H5_NO_DEPRECATED_SYMBOLS
    H5A_ATTR_OP_APP,  /* Application callback */
#endif                /* H5_NO_DEPRECATED_SYMBOLS */
    H5A_ATTR_OP_APP2, /* Revised application callback */
    H5A_ATTR_OP_LIB   /* Library internal callback */
} H5A_attr_iter_op_type_t;

typedef struct H5A_attr_iter_op_t {
    H5A_attr_iter_op_type_t op_type;
    union {
#ifndef H5_NO_DEPRECATED_SYMBOLS
        H5A_operator1_t app_op;    /* Application callback for each attribute */
#endif                             /* H5_NO_DEPRECATED_SYMBOLS */
        H5A_operator2_t   app_op2; /* Revised application callback for each attribute */
        H5A_lib_iterate_t lib_op;  /* Library internal callback for each attribute */
    } u;
} H5A_attr_iter_op_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* General attribute routines */
H5_DLL herr_t            H5A_init(void);
H5_DLL struct H5O_loc_t *H5A_oloc(H5A_t *attr);
H5_DLL H5G_name_t       *H5A_nameof(H5A_t *attr);
H5_DLL H5T_t            *H5A_type(const H5A_t *attr);
H5_DLL hid_t             H5A_get_space(H5A_t *attr);
H5_DLL herr_t            H5O_attr_iterate_real(hid_t loc_id, const H5O_loc_t *loc, H5_index_t idx_type,
                                               H5_iter_order_t order, hsize_t skip, hsize_t *last_attr,
                                               const H5A_attr_iter_op_t *attr_op, void *op_data);

#endif /* H5Aprivate_H */

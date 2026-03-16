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
 * This file contains private information about the H5PL module
 */

#ifndef H5PLprivate_H
#define H5PLprivate_H

/* Include package's public header */
#include "H5PLpublic.h"

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions                    */
#include "H5FDprivate.h" /* File Drivers                         */
#include "H5VLprivate.h" /* Virtual Object Layer                 */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Key used to find VOL connector plugins */
typedef struct H5PL_vol_key_t {
    H5VL_get_connector_kind_t kind; /* Kind of VOL lookup to do */
    union {
        H5VL_class_value_t value; /* VOL connector value */
        const char        *name;  /* VOL connector name */
    } u;
} H5PL_vol_key_t;

/* Key used to find VFD plugins */
typedef struct H5PL_vfd_key_t {
    H5FD_get_driver_kind_t kind; /* Kind of VFD lookup to do */
    union {
        H5FD_class_value_t value; /* VFD value */
        const char        *name;  /* VFD name */
    } u;
} H5PL_vfd_key_t;

/* The key that will be used to find the plugin */
typedef union H5PL_key_t {
    int            id; /* I/O filters */
    H5PL_vol_key_t vol;
    H5PL_vfd_key_t vfd;
} H5PL_key_t;

/* Enum dictating the type of plugins to process
 * when iterating through available plugins
 */
typedef enum {
    H5PL_ITER_TYPE_FILTER,
    H5PL_ITER_TYPE_VOL,
    H5PL_ITER_TYPE_VFD,
    H5PL_ITER_TYPE_ALL,
} H5PL_iterate_type_t;

/* Callback function for iterating through the available plugins */
typedef herr_t (*H5PL_iterate_t)(H5PL_type_t plugin_type, const void *plugin_info, void *op_data);

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* Internal API routines */
H5_DLL const void *H5PL_load(H5PL_type_t plugin_type, const H5PL_key_t *key);
H5_DLL herr_t      H5PL_iterate(H5PL_iterate_type_t iter_type, H5PL_iterate_t iter_op, void *op_data);
H5_DLL herr_t      H5PL_init(void);

#endif /* H5PLprivate_H */

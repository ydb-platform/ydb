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
 * This file contains private information about the H5M module
 */
#ifndef H5Mprivate_H
#define H5Mprivate_H

/* Include package's public header */
#include "H5Mpublic.h"

/* Private headers needed by this file */
#include "H5Oprivate.h" /* Object headers              */
#include "H5Sprivate.h" /* Dataspaces                  */
#include "H5Zprivate.h" /* Data filters                */

/**************************/
/* Library Private Macros */
/**************************/

/*
 * Feature: Define H5M_DEBUG on the compiler command line if you want to
 *        debug maps. NDEBUG must not be defined in order for this
 *        to have any effect.
 */
#ifdef NDEBUG
#undef H5M_DEBUG
#endif

/* ========  Map creation property names ======== */

/* ========  Map access property names ======== */
#define H5M_ACS_KEY_PREFETCH_SIZE_NAME                                                                       \
    "key_prefetch_size" /* Number of keys to prefetch during map iteration */
#define H5M_ACS_KEY_ALLOC_SIZE_NAME                                                                          \
    "key_alloc_size" /* Initial allocation size for keys prefetched during map iteration */

/* Default temporary buffer size */
#define H5D_TEMP_BUF_SIZE (1024 * 1024)

/* Default I/O vector size */
#define H5D_IO_VECTOR_SIZE 1024

/* Default VL allocation & free info */
#define H5D_VLEN_ALLOC      NULL
#define H5D_VLEN_ALLOC_INFO NULL
#define H5D_VLEN_FREE       NULL
#define H5D_VLEN_FREE_INFO  NULL

/* Default virtual dataset list size */
#define H5D_VIRTUAL_DEF_LIST_SIZE 8

/****************************/
/* Library Private Typedefs */
/****************************/
H5_DLL herr_t H5M_init(void);

/*****************************/
/* Library Private Variables */
/*****************************/

/******************************/
/* Library Private Prototypes */
/******************************/

#endif /* H5Mprivate_H */

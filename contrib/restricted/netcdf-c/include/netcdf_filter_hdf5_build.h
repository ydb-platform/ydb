/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/hdf5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* This include file is used if one wished to build a filter plugin
   independent of HDF5. See examples in the plugins directory
*/

#ifndef NETCDF_FILTER_HDF5_BUILD_H
#define NETCDF_FILTER_HDF5_BUILD_H 1

/**************************************************/
/* Build To the HDF5 C-API for Filters */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* Support headers */
#include <netcdf.h>
#include <netcdf_filter.h>

#ifdef USE_HDF5
#include <hdf5.h>
/* Older versions of the hdf library may define H5PL_type_t here */
#include <H5PLextern.h>

#else /*!USE_HDF5*/ /* Provide replacement definitions */

/* WARNING: In order make NCZARR independent of HDF5,
   while still using HDF5-style filters, some HDF5
   declarations need to be duplicated here with
   different names. Watch out for changes in
   the underlying HDF5 declarations.

   See the file H5Zpublic.h for more detailed descriptions.

   Note that these declarations are always enabled because
   HDF5-style filters may have been created with these definitions
   but for use by HDF5.

   Note also that certain filters in the plugins directory will not build if HDF5 is not installed:
   notably blosc.
*/

/* H5Z_FILTER_RESERVED => H5Z_FILTER_RESERVED */
#define H5Z_FILTER_RESERVED 256 /*filter ids below this value are reserved for library use */

/* H5Z_FILTER_MAX => H5Z_FILTER_MAX */
#define H5Z_FILTER_MAX 65535 /*maximum filter id */

/* Only a limited set of definition and invocation flags are allowed */
#define H5Z_FLAG_MANDATORY      0x0000  /*filter is mandatory		*/
#define H5Z_FLAG_OPTIONAL	0x0001	/*filter is optional		*/
#define H5Z_FLAG_REVERSE	0x0100	/*reverse direction; read	*/
#define H5Z_FLAG_SKIP_EDC	0x0200	/*skip EDC filters for read	*/

typedef int htri_t;
typedef int herr_t;
typedef int hbool_t;
typedef size_t hsize_t;
typedef long long hid_t;

/* htri_t (*H5Z_can_apply_func_t)(hid_t dcpl_id, hid_t type_id, hid_t space_id) => currently not supported; must be NULL. */
typedef htri_t (*H5Z_can_apply_func_t)(long long, long long, long long);

/* herr_t (*H5Z_set_local_func_t)(hid_t dcpl_id, hid_t type_id, hid_t space_id); => currently not supported; must be NULL. */
typedef herr_t (*H5Z_set_local_func_t)(long long, long long, long long);

/* H5Z_funct_t => H5Z_filter_func_t */
typedef size_t (*H5Z_func_t)(unsigned int flags, size_t cd_nelmts,
			     const unsigned int cd_values[], size_t nbytes,
			     size_t *buf_size, void **buf);

typedef int H5Z_filter_t;

#define H5Z_CLASS_T_VERS 1

/*
 * The filter table maps filter identification numbers to structs that
 * contain a pointers to the filter function and timing statistics.
 */
typedef struct H5Z_class2_t {
    int version;                    /* Version number of the struct; should be H5Z_FILTER_CLASS_VER */
    H5Z_filter_t id;		            /* Filter ID number                             */
    unsigned encoder_present;       /* Does this filter have an encoder?            */
    unsigned decoder_present;       /* Does this filter have a decoder?             */
    const char *name;               /* Comment for debugging                        */
    H5Z_can_apply_func_t can_apply; /* The "can apply" callback for a filter        */
    H5Z_set_local_func_t set_local; /* The "set local" callback for a filter        */
    H5Z_func_t filter;              /* The actual filter function                   */
} H5Z_class2_t;

/* The HDF5/H5Zarr dynamic loader looks for the following:*/

/* Plugin type used by the plugin library */
typedef enum H5PL_type_t {
    H5PL_TYPE_ERROR         = -1,   /* Error                */
    H5PL_TYPE_FILTER        =  0,   /* Filter               */
    H5PL_TYPE_NONE          =  1    /* This must be last!   */
} H5PL_type_t;

#endif /*USE_HDF5*/

/* Following External Discovery Functions should be present for the dynamic loading of filters */

/* returns specific constant H5ZP_TYPE_FILTER */
typedef H5PL_type_t (*H5PL_get_plugin_type_proto)(void);

/* return <pointer to instance of H5Z_filter_class> */
typedef const void* (*H5PL_get_plugin_info_proto)(void);

/*************************/
/* Following are always defined */

/* Misc Macros */

#ifndef HGOTO_ERROR
#define HGOTO_ERROR(pline, err, action, msg) {fprintf(stderr,"%s\n",msg); ret_value = -1; goto done;}
#endif
#ifndef H5_ATTR_FALLTHROUGH
#define H5_ATTR_FALLTHROUGH /*FALLTHROUGH*/
#endif
#ifndef H5MM_memcpy
#define H5MM_memcpy memcpy
#endif
#ifndef H5MM_malloc
#define H5MM_malloc malloc
#endif
#ifndef H5MM_realloc
#define H5MM_realloc realloc
#endif
#ifndef H5MM_xfree
#define H5MM_xfree(x) do{if((x)!=NULL) free(x);}while(0)
#endif
#ifndef H5_ATTR_UNUSED
#define H5_ATTR_UNUSED 
#endif
#ifndef FUNC_ENTER_STATIC
#define FUNC_ENTER_STATIC
#endif
#ifndef FUNC_LEAVE_NOAPI
#define FUNC_LEAVE_NOAPI(ret_value) return ret_value;
#endif
#ifndef FALSE
#define FALSE 0
#define TRUE 1
#endif
#ifndef SUCCEED
#define SUCCEED 0 
#define FAIL -1 
#endif
#ifndef FUNC_ENTER_NOAPI_NOINIT_NOERR
#define FUNC_ENTER_NOAPI_NOINIT_NOERR
#endif
#ifndef FUNC_ENTER_STATIC_NOERR
#define FUNC_ENTER_STATIC_NOERR
#endif
#ifndef FUNC_LEAVE_NOAPI
#define FUNC_LEAVE_NOAPI(x) return x;
#endif
#ifndef FUNC_LEAVE_NOAPI_VOID
#define FUNC_LEAVE_NOAPI_VOID return;
#endif
#ifndef H5_CHECKED_ASSIGN
#define H5_CHECKED_ASSIGN(dst,dtype,src,stype) (dst) = (dtype)(src)
#endif
#ifndef H5_CHECK_OVERFLOW
#define H5_CHECK_OVERFLOW(dst,dtype,stype)
#endif
#ifndef HDceil
#define HDceil(x) ceil(x)
#endif
#ifndef HDassert
#define HDassert(x)
#endif
#ifndef HDmemset
#define HDmemset memset
#endif

#ifndef UINT32ENCODE
#  define UINT32ENCODE(p, i) {                              \
   *(p) = (uint8_t)( (i)        & 0xff); (p)++;                      \
   *(p) = (uint8_t)(((i) >>  8) & 0xff); (p)++;                      \
   *(p) = (uint8_t)(((i) >> 16) & 0xff); (p)++;                      \
   *(p) = (uint8_t)(((i) >> 24) & 0xff); (p)++;                      \
}
#  define UINT32DECODE(p, i) {                              \
   (i)    =  (uint32_t)(*(p) & 0xff);       (p)++;                  \
   (i) |= ((uint32_t)(*(p) & 0xff) <<  8); (p)++;                  \
   (i) |= ((uint32_t)(*(p) & 0xff) << 16); (p)++;                  \
   (i) |= ((uint32_t)(*(p) & 0xff) << 24); (p)++;                  \
}
#endif

/* Protect old HDF5 code (pre 1.8.12) */
#ifdef USE_HDF5
# if H5_VERSION_LE(1,8,11)
# define H5allocate_memory(size,clear) ((clear)?calloc(1,(size)):malloc(size))
# define H5free_memory(buf) free(buf)
# define H5resize_memory(buf,size) realloc(buf,size)
# endif
#else
# define H5allocate_memory(size,clear) ((clear)?calloc(1,(size)):malloc(size))
# define H5free_memory(buf) free(buf)
# define H5resize_memory(buf,size) realloc(buf,size)
#endif

#endif /*NETCDF_FILTER_HDF5_BUILD_H*/

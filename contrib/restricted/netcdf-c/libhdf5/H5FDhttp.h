/*********************************************************************
*    Copyright 2018, UCAR/Unidata
*    See netcdf/COPYRIGHT file for copying and redistribution conditions.
* ********************************************************************/

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Dennis Heimbigner (dmh@ucar.edu)
 *              Dec. 26 2018
 *
 * Purpose:	The public header file for the s3 driver.
 * 
 * Derived from the HDF5 Source file H5FDstdio.c
 */

#ifndef H5FDHTTP_H
#define H5FDHTTP_H

#include "H5Ipublic.h"

/**
The big issue to be addressed: H5FD_CLASS_VERSION defined?
Apparently this first occurs in HDF5 version 1.13.2.
This affects the H5FD_class_t structure.
*/
#if H5_VERSION_GE(1, 13, 2)
#  ifndef H5FD_CLASS_VERSION
/* If not defined then fake it */
#  define H5FD_CLASS_VERSION 0x00
#  endif
#endif

/* Class Version field changes. */
#if H5FD_CLASS_VERSION > 0
/* see https://support.hdfgroup.org/documentation/hdf5-docs/registered_virtual_file_drivers_vfds.html */
#define H5_VFD_HTTP     ((H5FD_class_value_t)(514))
#define H5FD_HTTP	(H5FD_http_init())
#else
#define H5_VFD_HTTP     ((H5FD_class_value_t)(514))
//#define H5FD_HTTP	(H5FDperform_init(H5FD_http_init))
#define H5FD_HTTP	(H5FD_http_init())
#endif

#ifdef __cplusplus
extern "C" {
#endif

EXTERNL hid_t H5FD_http_init(void);
EXTERNL hid_t H5FD_http_finalize(void);
EXTERNL herr_t H5Pset_fapl_http(hid_t fapl_id);

#ifdef __cplusplus
}
#endif

#endif /*H5FDHTTP_H*/

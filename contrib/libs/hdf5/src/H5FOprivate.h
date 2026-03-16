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
 * This file contains library private information about the H5FO module
 */
#ifndef H5FOprivate_H
#define H5FOprivate_H

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions			*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5SLprivate.h" /* Skip lists				*/

/* Typedefs */

/* Typedef for open object cache */
typedef H5SL_t H5FO_t; /* Currently, all open objects are stored in skip list */

/* Macros */

/* Private routines */
H5_DLL herr_t  H5FO_create(const H5F_t *f);
H5_DLL void   *H5FO_opened(const H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5FO_insert(const H5F_t *f, haddr_t addr, void *obj, bool delete_flag);
H5_DLL herr_t  H5FO_delete(H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5FO_mark(const H5F_t *f, haddr_t addr, bool deleted);
H5_DLL bool    H5FO_marked(const H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5FO_dest(const H5F_t *f);
H5_DLL herr_t  H5FO_top_create(H5F_t *f);
H5_DLL herr_t  H5FO_top_incr(const H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5FO_top_decr(const H5F_t *f, haddr_t addr);
H5_DLL hsize_t H5FO_top_count(const H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5FO_top_dest(H5F_t *f);

#endif /* H5FOprivate_H */

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

/* Purpose: This file contains declarations which define macros for the
 *          H5R package.  Including this header means that the source file
 *          is part of the H5R package.
 */
#ifndef H5Rmodule_H
#define H5Rmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5R_MODULE
#define H5_MY_PKG     H5R
#define H5_MY_PKG_ERR H5E_REFERENCE

/** \page H5R_UG The HDF5 References
 * @todo Under Construction
 */

/**
 * \defgroup H5R References (H5R)
 *
 * Use the functions in this module to manage HDF5 references. Referents can
 * be HDF5 objects, attributes, and selections on datasets a.k.a. dataset
 * regions.
 *
 */

#endif /* H5Rmodule_H */

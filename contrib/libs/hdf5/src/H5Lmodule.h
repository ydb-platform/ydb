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
 * Purpose: This file contains declarations which define macros for the
 *          H5L package.  Including this header means that the source file
 *          is part of the H5L package.
 */
#ifndef H5Lmodule_H
#define H5Lmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5L_MODULE
#define H5_MY_PKG     H5L
#define H5_MY_PKG_ERR H5E_LINK

/** \page H5L_UG The HDF5 Links
 * @todo Under Construction
 */

/**
 * \defgroup H5L Links (H5L)
 *
 * Use the functions in this module to manage HDF5 links and link types.
 * @see \ref TRAV for #H5Literate, #H5Literate_by_name and #H5Lvisit, #H5Lvisit_by_name
 * @see \ref H5LA for #H5Lregister, #H5Lunregister and #H5Lis_registered
 *
 * \defgroup TRAV Link Traversal
 * \ingroup H5L
 * Traverse through links
 *
 * \defgroup H5LA Advanced Link Functions
 * \ingroup H5L
 * Registration of User-defined links
 */

#endif /* H5Lmodule_H */

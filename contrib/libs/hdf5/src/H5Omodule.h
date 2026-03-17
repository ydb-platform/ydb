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
 *          H5O package.  Including this header means that the source file
 *          is part of the H5O package.
 */
#ifndef H5Omodule_H
#define H5Omodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5O_MODULE
#define H5_MY_PKG     H5O
#define H5_MY_PKG_ERR H5E_OHDR

/** \page H5O_UG The HDF5 Objects
 * @todo Under Construction
 */

/**
 * \defgroup H5O Objects (H5O)
 *
 * Use the functions in this module to manage HDF5 objects.
 *
 * HDF5 objects (groups, datasets, datatype objects) are usually created
 * using functions in the object-specific modules (\ref H5G, \ref H5D,
 * \ref H5T). However, new objects can also be created by copying existing
 * objects.
 *
 * Many functions in this module are variations on object introspection,
 * that is, the retrieval of detailed information about HDF5 objects in a file.
 * Objects in an HDF5 file can be "visited" in an iterative fashion.
 *
 * HDF5 objects are usually updated using functions in the object-specific
 * modules. However, there are certain generic object properties, such as
 * reference counts, that can be manipulated using functions in this module.
 *
 * HDF5 objects are deleted as a side effect of rendering them unreachable
 * from the root group. The net effect is the diminution of the object's
 * reference count to zero, which can (but should not usually) be affected
 * by a function in this module.
 *
 * <table>
 * <tr><th>Create</th><th>Read</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5O_examples.c create
 *   </td>
 *   <td>
 *   \snippet{lineno} H5O_examples.c read
 *   </td>
 * <tr><th>Update</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet{lineno} H5O_examples.c update
 *   </td>
 *   <td>
 *   \snippet{lineno} H5O_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 */
#endif /* H5Omodule_H */

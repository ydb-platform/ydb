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
 *          H5PL package.  Including this header means that the source file
 *          is part of the H5PL package.
 */

#ifndef H5PLmodule_H
#define H5PLmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5PL_MODULE
#define H5_MY_PKG     H5PL
#define H5_MY_PKG_ERR H5E_PLUGIN

/** \page H5PL_UG The HDF5 Plugins
 * @todo Under Construction
 */

/**
 * \defgroup H5PL Dynamically-loaded Plugins (H5PL)
 *
 * Use the functions in this module to manage the loading behavior of HDF5
 * plugins.
 *
 * <table>
 * <tr><th>Create</th><th>Read</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet H5PL_examples.c create
 *   </td>
 *   <td>
 *   \snippet H5PL_examples.c read
 *   </td>
 * <tr><th>Update</th><th>Delete</th></tr>
 * <tr valign="top">
 *   <td>
 *   \snippet H5PL_examples.c update
 *   </td>
 *   <td>
 *   \snippet H5PL_examples.c delete
 *   </td>
 * </tr>
 * </table>
 *
 * \attention The loading behavior of HDF5 plugins can be controlled via the
 *            functions described below and certain environment variables, such
 *            as \c HDF5_PLUGIN_PRELOAD  and \c HDF5_PLUGIN_PATH.
 *
 */

#endif /* H5PLmodule_H */

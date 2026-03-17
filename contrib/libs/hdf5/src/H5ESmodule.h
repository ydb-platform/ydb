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
 * Purpose:     This file contains declarations which define macros for the
 *              H5ES package.  Including this header means that the source file
 *              is part of the H5ES package.
 */
#ifndef H5ESmodule_H
#define H5ESmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5ES_MODULE
#define H5_MY_PKG     H5ES
#define H5_MY_PKG_ERR H5E_EVENTSET

/** \page H5ES_UG The HDF5 Event Set
 * @todo Under Construction
 *
 * \section sec_async The HDF5 Event Set Interface
 *
 * \section subsec_async_intro Introduction
 * HDF5 provides asynchronous APIs for the HDF5 VOL connectors that support asynchronous HDF5
 * operations using the HDF5 Event Set (H5ES) API. This allows I/O to proceed in the background
 * while the application is performing other tasks.
 *
 * To support AIO capabilities for the HDF5 VOL connectors, the AIO versions for the functions
 * listed in the table below were added to HDF5 library version 1.14.0 and later. The async version
 * of the function has “_async” suffix added to the function name. For example, the async version
 * for H5Fcreate is H5Fcreate_async.
 *
 * <table>
 * <tr>
 * <th>Interface</th>
 * <th>Functions</th>
 * </tr>
 * <tr>
 * <th>H5F</th>
 * <td>#H5Fcreate, #H5Fflush, #H5Fis_accessible, #H5Fopen, #H5Fclose
 * </td>
 * </tr>
 * <tr>
 * <th>H5G</th>
 * <td>#H5Gcreate, #H5Gget_info, #H5Gget_info_by_idx, #H5Gget_info_by_name, #H5Gclose
 * </td>
 * </tr>
 * <tr>
 * <th>H5D</th>
 * <td>#H5Dcreate, #H5Dopen, #H5Dset_extent, #H5Dwrite, #H5Dread, #H5Dget_space, #H5Dclose
 * </td>
 * </tr>
 * <tr>
 * <th>H5A</th>
 * <td>#H5Acreate, #H5Acreate_by_name, #H5Aopen, #H5Aopen_by_name, #H5Aexists, #H5Awrite, #H5Aread,
#H5Aclose, #H5Aopen_by_idx, #H5Arename, #H5Arename_by_name
 * </td>
 * </tr>
 * <tr>
 * <th>H5L</th>
 * <td>#H5Lcreate_hard, #H5Lcreate_soft, #H5Ldelete, #H5Ldelete_by_idx, #H5Lexists
 * </td>
 * </tr>
 * <tr>
 * <th>H5O</th>
 * <td>#H5Ocopy, #H5Orefresh, #H5Oflush, #H5Oclose, #H5Oopen, #H5Oopen_by_idx
 * </td>
 * </tr>
 * <tr>
 * <th>H5R</th>
 * <td>#H5Ropen_attr, #H5Ropen_object #H5Ropen_region, #H5Rdereference
 * </td>
 * </tr>
 * <tr>
 * <th>H5M</th>
 * <td>#H5Mcreate, #H5Mopen, #H5Mput, #H5Mget, #H5Mclose
 * </td>
 * </tr>
 * <tr>
 * <th>H5T</th>
 * <td>#H5Tcommit, #H5Topen, #H5Tcopy, #H5Tclose
 * </td>
 * </tr>
 * </table>
 *
 * Async versions of the functions have an extra parameter called the event set parameter or es_id.
 * For example, compare the signatures of #H5Dclose and #H5Dclose_async:
 * \code
 * herr_t H5Dclose(hid_t dset_id);
 * herr_t H5Dclose_async(hid_t dset_id, hid_t es_id);
 * \endcode
 *
 * An event set is an in-memory object that is created by an application and used to track many
 * asynchronous operations with a single object. They function like a "bag" -- holding request
 * tokens from one or more asynchronous operations and provide a simple interface for inspecting
 * the status of the entire set of operations.
 *
 * See the \ref H5ES APIs that were added to the HDF5 library to manage event sets.
 *
 * Previous Chapter \ref sec_vol - Next Chapter \ref sec_map
 *
 */

/**\defgroup H5ES Event Set Interface (H5ES)
 *
 * \todo Add the event set life cycle.
 *
 * \brief Event Set Interface
 *
 * \details \Bold{This interface can only be used with the HDF5 VOL connectors that
 *          enable the asynchronous feature in HDF5.} The native HDF5 library has
 *          only synchronous operations.
 *
 *          HDF5 VOL connectors with support for asynchronous operations:
 *          - ASYNC
 *          - DAOS
 *
 * \par Example:
 * \code
 * fid = H5Fopen(..);
 * gid = H5Gopen(fid, ..);  //Starts when H5Fopen completes
 * did = H5Dopen(gid, ..);  //Starts when H5Gopen completes
 *
 * es_id = H5EScreate();  // Create event set for tracking async operations
 * status = H5Dwrite_async(did, .., es_id);  //Asynchronous, starts when H5Dopen completes,
 *                                           // may run concurrently with other H5Dwrite_async
 *                                           // in event set.
 * status = H5Dwrite_async(did, .., es_id);  //Asynchronous, starts when H5Dopen completes,
 *                                           // may run concurrently with other H5Dwrite_async
 *                                           // in event set....
 * <other user code>
 * ...
 * H5ESwait(es_id); // Wait for operations in event set to complete, buffers
 *                  // used for H5Dwrite_async must only be changed after wait
 *                  // returns.
 * \endcode
 */

#endif /* H5ESmodule_H */

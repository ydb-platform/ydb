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
 *              H5M package.  Including this header means that the source file
 *              is part of the H5M package.
 */
#ifndef H5Mmodule_H
#define H5Mmodule_H

/* Define the proper control macros for the generic FUNC_ENTER/LEAVE and error
 *      reporting macros.
 */
#define H5M_MODULE
#define H5_MY_PKG     H5M
#define H5_MY_PKG_ERR H5E_MAP

/**
 * \page H5M_UG The HDF5 VOL Data Mapping
 * \Bold{The HDF5 Data Mapping can only be used with the HDF5 VOL connectors that
 * implement map objects.} The native HDF5 library does not support this feature.
 *
 * \section sec_map The HDF5 Map Object
 *
 * \todo Describe the map life cycle.
 *
 * \todo How does MAPL fit into \ref subsubsec_plist_class.
 *
 * Previous Chapter \ref sec_async - Next Chapter \ref sec_addition
 *
 */

/**
 * \defgroup H5M VOL Mapping (H5M)
 *
 * \details \Bold{The interface can only be used with the HDF5 VOL connectors that
 *          implement map objects.} The native HDF5 library does not support this
 *          feature.
 *
 *          While the HDF5 data model is a flexible way to store data, some
 *          applications require a more general way to index information. HDF5
 *          effectively uses key-value stores internally for a variety of
 *          purposes, but it does not expose a generic key-value store to the
 *          API. The Map APIs provide this capability to the HDF5 applications
 *          in the form of HDF5 map objects. These Map objects contain
 *          application-defined key-value stores, to which key-value pairs can
 *          be added, and from which values can be retrieved by key.
 *
 *          HDF5 VOL connectors with support for map objects:
 *          - DAOS
 *
 * \par Example:
 * \code
 * hid_t file_id, fapl_id, map_id, vls_type_id;
 * const char *names[2] = ["Alice", "Bob"];
 * uint64_t IDs[2] = [25385486, 34873275];
 * uint64_t val_out;
 *
 * <HDF5 VOL setup code ....>
 *
 * vls_type_id = H5Tcopy(H5T_C_S1);
 * H5Tset_size(vls_type_id, H5T_VARIABLE);
 * file_id = H5Fcreate("file.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
 * map_id = H5Mcreate(file_id, "map", vls_type_id, H5T_NATIVE_UINT64, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
 * H5Mput(map_id, vls_type_id, &names[0], H5T_NATIVE_UINT64, &IDs[0], H5P_DEFAULT);
 * H5Mput(map_id, vls_type_id, &names[1], H5T_NATIVE_UINT64, &IDs[1], H5P_DEFAULT);
 * H5Mget(map_id, vls_type_id, &names[0], H5T_NATIVE_UINT64, &val_out, H5P_DEFAULT);
 * if(val_out != IDs[0])
 *   ERROR;
 * H5Mclose(map_id);
 * H5Tclose(vls_type_id);
 * H5Fclose(file_id);
 * \endcode
 *
 */

#endif /* H5Dmodule_H */

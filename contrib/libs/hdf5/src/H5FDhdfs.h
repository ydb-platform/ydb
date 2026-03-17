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
 * Purpose:    The public header file for the hdfs driver.
 */

#ifndef H5FDhdfs_H
#define H5FDhdfs_H

#ifdef H5_HAVE_LIBHDFS
#define H5FD_HDFS       (H5FDperform_init(H5FD_hdfs_init))
#define H5FD_HDFS_VALUE H5_VFD_HDFS
#else /* H5_HAVE_LIBHDFS */
#define H5FD_HDFS       (H5I_INVALID_HID)
#define H5FD_HDFS_VALUE H5_VFD_INVALID
#endif /* H5_HAVE_LIBHDFS */

#ifdef H5_HAVE_LIBHDFS
#ifdef __cplusplus
extern "C" {
#endif

/****************************************************************************
 *
 * Structure: H5FD_hdfs_fapl_t
 *
 * Purpose:
 *
 *     H5FD_hdfs_fapl_t is a public structure that is used to pass
 *     configuration information to the appropriate HDFS VFD via the FAPL.
 *     A pointer to an instance of this structure is a parameter to
 *     H5Pset_fapl_hdfs() and H5Pget_fapl_hdfs().
 *
 *
 *
 * `version` (int32_t)
 *
 *     Version number of the `H5FD_hdfs_fapl_t` structure.  Any instance passed
 *     to the above calls must have a recognized version number, or an error
 *     will be flagged.
 *
 *     This field should be set to `H5FD__CURR_HDFS_FAPL_T_VERSION`.
 *
 * `namenode_name` (const char[])
 *
 *     Name of "Name Node" to access as the HDFS server.
 *
 *     Must not be longer than `H5FD__HDFS_NODE_NAME_SPACE`.
 *
 *     TBD: Can be NULL.
 *
 * `namenode_port` (int32_t) TBD
 *
 *     Port number to use to connect with Name Node.
 *
 *     TBD: If 0, uses a default port.
 *
 * `kerberos_ticket_cache` (const char[])
 *
 *     Path to the location of the Kerberos authentication cache.
 *
 *     Must not be longer than `H5FD__HDFS_KERB_CACHE_PATH_SPACE`.
 *
 *     TBD: Can be NULL.
 *
 * `user_name` (const char[])
 *
 *     Username to use when accessing file.
 *
 *     Must not be longer than `H5FD__HDFS_USER_NAME_SPACE`.
 *
 *     TBD: Can be NULL.
 *
 * `stream_buffer_size` (int32_t)
 *
 *     Size (in bytes) of the file read stream buffer.
 *
 *     TBD: If -1, relies on a default value.
 *
 ****************************************************************************/

#define H5FD__CURR_HDFS_FAPL_T_VERSION 1

#define H5FD__HDFS_NODE_NAME_SPACE       128
#define H5FD__HDFS_USER_NAME_SPACE       128
#define H5FD__HDFS_KERB_CACHE_PATH_SPACE 128

typedef struct H5FD_hdfs_fapl_t {
    int32_t version;
    char    namenode_name[H5FD__HDFS_NODE_NAME_SPACE + 1];
    int32_t namenode_port;
    char    user_name[H5FD__HDFS_USER_NAME_SPACE + 1];
    char    kerberos_ticket_cache[H5FD__HDFS_KERB_CACHE_PATH_SPACE + 1];
    int32_t stream_buffer_size;
} H5FD_hdfs_fapl_t;

H5_DLL hid_t H5FD_hdfs_init(void);

/**
 * \ingroup FAPL
 *
 * \todo Add missing documentation
 */
H5_DLL herr_t H5Pget_fapl_hdfs(hid_t fapl_id, H5FD_hdfs_fapl_t *fa_out);

/**
 * \ingroup FAPL
 *
 * \todo Add missing documentation
 */
H5_DLL herr_t H5Pset_fapl_hdfs(hid_t fapl_id, H5FD_hdfs_fapl_t *fa);

#ifdef __cplusplus
}
#endif
#endif /* H5_HAVE_LIBHDFS */

#endif /* ifndef H5FDhdfs_H */

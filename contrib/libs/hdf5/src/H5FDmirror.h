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
 * Purpose: Public, shared definitions for Mirror VFD & remote Writer.
 */

#ifndef H5FDmirror_H
#define H5FDmirror_H

#ifdef H5_HAVE_MIRROR_VFD

#define H5FD_MIRROR       (H5FDperform_init(H5FD_mirror_init))
#define H5FD_MIRROR_VALUE H5_VFD_MIRROR

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Mirror VFD use and operation.
 * ============================================================================
 */

/* ---------------------------------------------------------------------------
 * Structure:   H5FD_mirror_fapl_t
 *
 * Used to pass configuration information to the Mirror VFD.
 * Populate components as appropriate and pass structure pointer to
 * `H5Pset_fapl_mirror()`.
 *
 * `magic` (uint32_t)
 *      Semi-unique number to sanity-check pointers to this structure type.
 *      MUST equal H5FD_MIRROR_FAPL_MAGIC to be considered valid.
 *
 * `version` (uint32_t)
 *      Indicates expected components of the structure.
 *
 * `handshake_port (int)
 *      Port number to expect to reach the "Mirror Server" on the remote host.
 *
 * `remote_ip` (char[])
 *      IP address string of "Mirror Server" remote host.
 * ---------------------------------------------------------------------------
 */
#define H5FD_MIRROR_FAPL_MAGIC          0xF8DD514C
#define H5FD_MIRROR_CURR_FAPL_T_VERSION 1
#define H5FD_MIRROR_MAX_IP_LEN          32
typedef struct H5FD_mirror_fapl_t {
    uint32_t magic;
    uint32_t version;
    int      handshake_port;
    char     remote_ip[H5FD_MIRROR_MAX_IP_LEN + 1];
} H5FD_mirror_fapl_t;

H5_DLL hid_t H5FD_mirror_init(void);

/**
 * \ingroup FAPL
 *
 * \todo Add missing documentation
 */
H5_DLL herr_t H5Pget_fapl_mirror(hid_t fapl_id, H5FD_mirror_fapl_t *fa_out);

/**
 * \ingroup FAPL
 *
 * \todo Add missing documentation
 */
H5_DLL herr_t H5Pset_fapl_mirror(hid_t fapl_id, H5FD_mirror_fapl_t *fa);

#ifdef __cplusplus
}
#endif

#else /* H5_HAVE_MIRROR_VFD */

#define H5FD_MIRROR (H5I_INAVLID_HID)

#endif /* H5_HAVE_MIRROR_VFD */

#endif /* H5FDmirror_H */

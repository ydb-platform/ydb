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
 * Purpose:	The public header file for the "splitter" driver.
 */

#ifndef H5FDsplitter_H
#define H5FDsplitter_H

#define H5FD_SPLITTER       (H5FDperform_init(H5FD_splitter_init))
#define H5FD_SPLITTER_VALUE H5_VFD_SPLITTER

/* The version of the H5FD_splitter_vfd_config_t structure used */
#define H5FD_CURR_SPLITTER_VFD_CONFIG_VERSION 1

/* Maximum length of a filename/path string in the Write-Only channel,
 * including the NULL-terminator.
 */
#define H5FD_SPLITTER_PATH_MAX 4096

/* Semi-unique constant used to help identify structure pointers */
#define H5FD_SPLITTER_MAGIC 0x2B916880

/* ----------------------------------------------------------------------------
 * Structure:   H5FD_spliiter_vfd_config_t
 *
 * One-stop shopping for configuring a Splitter VFD (rather than many
 * parameters passed into H5Pset/get functions).
 *
 * magic (int32_t)
 *      Semi-unique number, used to sanity-check that a given pointer is
 *      likely (or not) to be this structure type. MUST be first.
 *      If magic is not H5FD_SPLITTER_MAGIC, the structure (and/or pointer to)
 *      must be considered invalid.
 *
 * version (unsigned int)
 *      Version number of this structure -- informs component membership.
 *      If not H5FD_CURR_SPLITTER_VFD_CONFIG_VERSION, the structure (and/or
 *      pointer to) must be considered invalid.
 *
 * rw_fapl_id (hid_t)
 *      Library-given identification number of the Read/Write channel driver
 *      File Access Property List.
 *      The driver must support read/write access.
 *      Must be set to H5P_DEFAULT or a valid FAPL ID.
 *
 * wo_fapl_id (hid_t)
 *      Library-given identification number of the Read/Write channel driver
 *      File Access Property List.
 *      The driver feature flags must include H5FD_FEAT_DEFAULT_VFD_COMPAITBLE.
 *      Must be set to H5P_DEFAULT or a valid FAPL ID.
 *
 * wo_file_path (char[H5FD_SPLITTER_PATH_MAX + 1])
 *      String buffer for the Write-Only channel target file.
 *      Must be null-terminated, cannot be empty.
 *
 * log_file_path (char[H5FD_SPLITTER_PATH_MAX + 1])
 *      String buffer for the Splitter VFD logging output.
 *      Must be null-terminated.
 *      If null, no logfile is created.
 *
 * ignore_wo_errors (hbool_t)
 *      Toggle flag for how judiciously to respond to errors on the Write-Only
 *      channel.
 *
 * ----------------------------------------------------------------------------
 */
typedef struct H5FD_splitter_vfd_config_t {
    int32_t      magic;
    unsigned int version;
    hid_t        rw_fapl_id;
    hid_t        wo_fapl_id;
    char         wo_path[H5FD_SPLITTER_PATH_MAX + 1];
    char         log_file_path[H5FD_SPLITTER_PATH_MAX + 1];
    hbool_t      ignore_wo_errs;
} H5FD_splitter_vfd_config_t;

#ifdef __cplusplus
extern "C" {
#endif
H5_DLL hid_t H5FD_splitter_init(void);

/**
 * \ingroup FAPL
 *
 * \todo Add missing documentation
 */
H5_DLL herr_t H5Pset_fapl_splitter(hid_t fapl_id, H5FD_splitter_vfd_config_t *config_ptr);

/**
 * \ingroup FAPL
 *
 * \todo Add missing documentation
 */
H5_DLL herr_t H5Pget_fapl_splitter(hid_t fapl_id, H5FD_splitter_vfd_config_t *config_ptr);

#ifdef __cplusplus
}
#endif

#endif

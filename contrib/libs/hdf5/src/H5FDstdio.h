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
 * Purpose:	The public header file for the C stdio driver
 */
#ifndef H5FDstdio_H
#define H5FDstdio_H

#include "H5Ipublic.h"

#define H5FD_STDIO (H5FDperform_init(H5FD_stdio_init))

#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t H5FD_stdio_init(void);
/**
 * \ingroup FAPL
 *
 * \brief Sets the standard I/O driver
 *
 * \fapl_id
 * \returns \herr_t
 *
 * \details H5Pset_fapl_stdio() modifies the file access property list to use
 *          the standard I/O driver, H5FDstdio().
 *
 * \since 1.4.0
 *
 */
H5_DLL herr_t H5Pset_fapl_stdio(hid_t fapl_id);

#ifdef __cplusplus
}
#endif

#endif

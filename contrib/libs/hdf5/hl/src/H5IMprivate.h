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

#ifndef H5IMprivate_H
#define H5IMprivate_H

/* High-level library internal header file */
#include "H5HLprivate2.h"

/* public IM prototypes			*/
#include "H5IMpublic.h"

#define IMAGE_CLASS   "IMAGE"
#define PALETTE_CLASS "PALETTE"
#define IMAGE_VERSION "1.2"
#define IMAGE8_RANK   2
#define IMAGE24_RANK  3

/*-------------------------------------------------------------------------
 * Private functions
 *-------------------------------------------------------------------------
 */
H5_HLDLL herr_t H5IM_find_palette(hid_t loc_id);

#endif

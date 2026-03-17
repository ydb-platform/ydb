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

#ifndef H5DSprivate_H
#define H5DSprivate_H

/* High-level library internal header file */
#include "H5HLprivate2.h"

/* public LT prototypes			*/
#include "H5DSpublic.h"

/* attribute type of a DS dataset when old references are used*/
typedef struct ds_list_t {
    hobj_ref_t   ref;     /* object reference  */
    unsigned int dim_idx; /* dimension index of the dataset */
} ds_list_t;

/* attribute type of a DS dataset when new references are used*/
typedef struct nds_list_t {
    H5R_ref_t    ref;
    unsigned int dim_idx; /* dimension index of the dataset */
} nds_list_t;

/*-------------------------------------------------------------------------
 * private functions
 *-------------------------------------------------------------------------
 */

#endif

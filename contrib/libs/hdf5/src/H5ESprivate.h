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

/*-------------------------------------------------------------------------
 *
 * Created:     H5ESprivate.h
 *
 * Purpose:     Private header for library accessible event set routines.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5ESprivate_H
#define H5ESprivate_H

/* Include package's public headers */
#include "H5ESpublic.h"
#include "H5ESdevelop.h"

/* Private headers needed by this file */
#include "H5VLprivate.h" /* Virtual Object Layer        */

/**************************/
/* Library Private Macros */
/**************************/

/****************************/
/* Library Private Typedefs */
/****************************/

/* Typedef for event set objects */
typedef struct H5ES_t H5ES_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
herr_t H5ES_insert(hid_t es_id, H5VL_t *connector, void *token, const char *caller, const char *caller_args,
                   ...);
H5_DLL herr_t H5ES_init(void);

#endif /* H5ESprivate_H */

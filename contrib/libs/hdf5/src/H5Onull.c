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
 * Created:             H5Onull.c
 *
 * Purpose:             The null message.
 *
 *-------------------------------------------------------------------------
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h" /* Generic Functions			*/
#include "H5Opkg.h"    /* Object headers			*/

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_NULL[1] = {{
    H5O_NULL_ID, /*message id number             */
    "null",      /*message name for debugging    */
    0,           /*native message size           */
    0,           /* messages are shareable?       */
    NULL,        /*no decode method              */
    NULL,        /*no encode method              */
    NULL,        /*no copy method                */
    NULL,        /*no size method                */
    NULL,        /*no reset method               */
    NULL,        /*no free method                */
    NULL,        /*no file delete method         */
    NULL,        /*no link method		*/
    NULL,        /*no set share method	        */
    NULL,        /*no can share method		*/
    NULL,        /*no pre copy native value to file */
    NULL,        /*no copy native value to file  */
    NULL,        /*no post copy native value to file */
    NULL,        /*no get creation index		*/
    NULL,        /*no set creation index		*/
    NULL         /*no debug method               */
}};

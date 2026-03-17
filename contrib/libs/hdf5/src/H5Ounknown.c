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
 * Created:             H5Ounknown.c
 *
 * Purpose:             Handle unknown message classes in a minimal way.
 *
 *-------------------------------------------------------------------------
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Opkg.h"      /* Object headers			*/

/* PRIVATE PROTOTYPES */
static herr_t H5O__unknown_free(void *_mesg);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_UNKNOWN[1] = {{
    H5O_UNKNOWN_ID,    /*message id number             */
    "unknown",         /*message name for debugging    */
    0,                 /*native message size           */
    0,                 /* messages are shareable?       */
    NULL,              /*decode message                */
    NULL,              /*encode message                */
    NULL,              /*copy the native value         */
    NULL,              /*size of symbol table entry    */
    NULL,              /*default reset method          */
    H5O__unknown_free, /* free method			*/
    NULL,              /* file delete method		*/
    NULL,              /* link method			*/
    NULL,              /*set share method		*/
    NULL,              /*can share method		*/
    NULL,              /* pre copy native value to file */
    NULL,              /* copy native value to file    */
    NULL,              /* post copy native value to file */
    NULL,              /* get creation index		*/
    NULL,              /* set creation index		*/
    NULL               /*debug the message             */
}};

/* Declare a free list to manage the H5O_unknown_t struct */
H5FL_DEFINE(H5O_unknown_t);

/*-------------------------------------------------------------------------
 * Function:    H5O__unknown_free
 *
 * Purpose:     Frees the message
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__unknown_free(void *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(mesg);

    mesg = H5FL_FREE(H5O_unknown_t, mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__unknown_free() */

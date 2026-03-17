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
 * Created:             H5Obogus.c
 *
 * Purpose:             "bogus" message.  This message is guaranteed to never
 *                      be found in a valid HDF5 file and is only used to
 *                      generate a test file which verifies the library's
 *                      correct operation when parsing unknown object header
 *                      messages.
 *
 *-------------------------------------------------------------------------
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/

#ifdef H5O_ENABLE_BOGUS

/* PRIVATE PROTOTYPES */
static void  *H5O__bogus_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                size_t p_size, const uint8_t *p);
static herr_t H5O__bogus_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static size_t H5O__bogus_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__bogus_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_BOGUS_VALID[1] = {{
    H5O_BOGUS_VALID_ID,    /*message id number             */
    "bogus valid",         /*message name for debugging    */
    0,                     /*native message size           */
    H5O_SHARE_IS_SHARABLE, /* messages are shareable?       */
    H5O__bogus_decode,     /*decode message                */
    H5O__bogus_encode,     /*encode message                */
    NULL,                  /*copy the native value         */
    H5O__bogus_size,       /*raw message size              */
    NULL,                  /*free internal memory          */
    NULL,                  /*free method			*/
    NULL,                  /* file delete method		*/
    NULL,                  /* link method			*/
    NULL,                  /*set share method		*/
    NULL,                  /*can share method		*/
    NULL,                  /* pre copy native value to file */
    NULL,                  /* copy native value to file    */
    NULL,                  /* post copy native value to file    */
    NULL,                  /* get creation index		*/
    NULL,                  /* set creation index		*/
    H5O__bogus_debug       /*debug the message             */
}};

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_BOGUS_INVALID[1] = {{
    H5O_BOGUS_INVALID_ID,  /*message id number             */
    "bogus invalid",       /*message name for debugging    */
    0,                     /*native message size           */
    H5O_SHARE_IS_SHARABLE, /* messages are shareable?       */
    H5O__bogus_decode,     /*decode message                */
    H5O__bogus_encode,     /*encode message                */
    NULL,                  /*copy the native value         */
    H5O__bogus_size,       /*raw message size              */
    NULL,                  /*free internal memory          */
    NULL,                  /*free method                   */
    NULL,                  /* file delete method           */
    NULL,                  /* link method                  */
    NULL,                  /*set share method              */
    NULL,                  /*can share method              */
    NULL,                  /* pre copy native value to file */
    NULL,                  /* copy native value to file    */
    NULL,                  /* post copy native value to file    */
    NULL,                  /* get creation index           */
    NULL,                  /* set creation index           */
    H5O__bogus_debug       /*debug the message             */
}};

/*-------------------------------------------------------------------------
 * Function:    H5O__bogus_decode
 *
 * Purpose:     Decode a "bogus" message and return a pointer to a new
 *              native message struct.
 *
 * Return:      Success:        Pointer to new message in native struct
 *              Failure:        NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__bogus_decode(H5F_t *f, H5O_t H5_ATTR_NDEBUG_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                  unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    const uint8_t *p_end = p + p_size - 1;
    H5O_bogus_t   *mesg  = NULL;
    void          *ret_value;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    /* Allocate the bogus message */
    if (NULL == (mesg = (H5O_bogus_t *)H5MM_calloc(sizeof(H5O_bogus_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    UINT32DECODE(p, mesg->u);

    /* Validate the bogus info */
    if (mesg->u != H5O_BOGUS_VALUE)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "invalid bogus value :-)");

    /* Set return value */
    ret_value = mesg;

done:
    if (ret_value == NULL && mesg != NULL)
        H5MM_xfree(mesg);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__bogus_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__bogus_encode
 *
 * Purpose:     Encodes a "bogus" message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__bogus_encode(H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p,
                  const void H5_ATTR_UNUSED *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(mesg);

    /* encode */
    UINT32ENCODE(p, H5O_BOGUS_VALUE);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__bogus_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__bogus_size
 *
 * Purpose:     Returns the size of the raw message in bytes not
 *              counting the message typ or size fields, but only the data
 *              fields.  This function doesn't take into account
 *              alignment.
 *
 * Return:      Success:        Message data size in bytes w/o alignment.
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__bogus_size(const H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared,
                const void H5_ATTR_UNUSED *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(4)
} /* end H5O__bogus_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__bogus_debug
 *
 * Purpose:     Prints debugging info for the message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__bogus_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_bogus_t *mesg = (const H5O_bogus_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s `%u'\n", indent, "", fwidth, "Bogus Value:", mesg->u);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__bogus_debug() */
#endif /* H5O_ENABLE_BOGUS */

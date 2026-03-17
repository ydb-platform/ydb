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
 * Created:             H5Oname.c
 *
 * Purpose:             Object name (comment) message
 *
 *-------------------------------------------------------------------------
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/

/* PRIVATE PROTOTYPES */
static void *H5O__name_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags, size_t p_size,
                              const uint8_t *p);
static herr_t H5O__name_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__name_copy(const void *_mesg, void *_dest);
static size_t H5O__name_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__name_reset(void *_mesg);
static herr_t H5O__name_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_NAME[1] = {{
    H5O_NAME_ID,        /*message id number             */
    "name",             /*message name for debugging    */
    sizeof(H5O_name_t), /*native message size           */
    0,                  /* messages are shareable?       */
    H5O__name_decode,   /*decode message                */
    H5O__name_encode,   /*encode message                */
    H5O__name_copy,     /*copy the native value         */
    H5O__name_size,     /*raw message size              */
    H5O__name_reset,    /*free internal memory          */
    NULL,               /* free method			*/
    NULL,               /* file delete method		*/
    NULL,               /* link method			*/
    NULL,               /*set share method		*/
    NULL,               /*can share method		*/
    NULL,               /* pre copy native value to file */
    NULL,               /* copy native value to file    */
    NULL,               /* post copy native value to file    */
    NULL,               /* get creation index		*/
    NULL,               /* set creation index		*/
    H5O__name_debug     /*debug the message             */
}};

/*-------------------------------------------------------------------------
 * Function:    H5O__name_decode
 *
 * Purpose:     Decode a name message and return a pointer to a new
 *              native message struct.
 *
 * Return:      Success:    Ptr to new message in native struct.
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__name_decode(H5F_t H5_ATTR_NDEBUG_UNUSED *f, H5O_t H5_ATTR_UNUSED *open_oh,
                 unsigned H5_ATTR_UNUSED mesg_flags, unsigned H5_ATTR_UNUSED *ioflags, size_t p_size,
                 const uint8_t *p)
{
    H5O_name_t *mesg      = NULL;
    void       *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (NULL == (mesg = (H5O_name_t *)H5MM_calloc(sizeof(H5O_name_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    if (NULL == (mesg->s = (char *)H5MM_strndup((const char *)p, p_size - 1)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    ret_value = mesg;

done:
    if (NULL == ret_value)
        if (mesg) {
            H5MM_xfree(mesg->s);
            H5MM_xfree(mesg);
        }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__name_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__name_encode
 *
 * Purpose:     Encodes a name message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__name_encode(H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_name_t *mesg = (const H5O_name_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(mesg && mesg->s);

    /* encode */
    strcpy((char *)p, mesg->s);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__name_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__name_copy
 *
 * Purpose:     Copies a message from _MESG to _DEST, allocating _DEST if
 *              necessary.
 *
 * Return:      Success:        Ptr to _DEST
 *
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__name_copy(const void *_mesg, void *_dest)
{
    const H5O_name_t *mesg      = (const H5O_name_t *)_mesg;
    H5O_name_t       *dest      = (H5O_name_t *)_dest;
    void             *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(mesg);

    if (!dest && NULL == (dest = (H5O_name_t *)H5MM_calloc(sizeof(H5O_name_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* copy */
    *dest = *mesg;
    if (NULL == (dest->s = H5MM_xstrdup(mesg->s)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set return value */
    ret_value = dest;

done:
    if (NULL == ret_value)
        if (dest && NULL == _dest)
            dest = (H5O_name_t *)H5MM_xfree(dest);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__name_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__name_size
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
H5O__name_size(const H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, const void *_mesg)
{
    const H5O_name_t *mesg      = (const H5O_name_t *)_mesg;
    size_t            ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);

    ret_value = mesg->s ? strlen(mesg->s) + 1 : 0;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__name_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__name_reset
 *
 * Purpose:     Frees internal pointers and resets the message to an
 *              initial state.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__name_reset(void *_mesg)
{
    H5O_name_t *mesg = (H5O_name_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(mesg);

    /* reset */
    mesg->s = (char *)H5MM_xfree(mesg->s);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__name_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5O__name_debug
 *
 * Purpose:     Prints debugging info for the message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__name_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_name_t *mesg = (const H5O_name_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s `%s'\n", indent, "", fwidth, "Name:", mesg->s);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__name_debug() */

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
 * Purpose: A message holding driver info settings in the superblock extension
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Opkg.h"      /* Object headers			*/
#include "H5MMprivate.h" /* Memory management			*/

static void  *H5O__drvinfo_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                  size_t p_size, const uint8_t *p);
static herr_t H5O__drvinfo_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__drvinfo_copy(const void *_mesg, void *_dest);
static size_t H5O__drvinfo_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__drvinfo_reset(void *_mesg);
static herr_t H5O__drvinfo_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_DRVINFO[1] = {{
    H5O_DRVINFO_ID,        /*message id number                     */
    "driver info",         /*message name for debugging            */
    sizeof(H5O_drvinfo_t), /*native message size                   */
    0,                     /* messages are shareable?               */
    H5O__drvinfo_decode,   /*decode message                        */
    H5O__drvinfo_encode,   /*encode message                        */
    H5O__drvinfo_copy,     /*copy the native value                 */
    H5O__drvinfo_size,     /*raw message size			*/
    H5O__drvinfo_reset,    /*free internal memory			*/
    NULL,                  /* free method				*/
    NULL,                  /* file delete method			*/
    NULL,                  /* link method				*/
    NULL,                  /*set share method		        */
    NULL,                  /*can share method		        */
    NULL,                  /* pre copy native value to file	*/
    NULL,                  /* copy native value to file		*/
    NULL,                  /* post copy native value to file	*/
    NULL,                  /* get creation index		        */
    NULL,                  /* set creation index		        */
    H5O__drvinfo_debug     /*debug the message			*/
}};

/* Current version of driver info information */
#define H5O_DRVINFO_VERSION 0

/*-------------------------------------------------------------------------
 * Function:    H5O__drvinfo_decode
 *
 * Purpose:     Decode a shared message table message and return a pointer
 *              to a newly allocated H5O_drvinfo_t struct.
 *
 * Return:      Success:	Pointer to new message in native struct
 *              Failure:	NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__drvinfo_decode(H5F_t H5_ATTR_UNUSED *f, H5O_t H5_ATTR_UNUSED *open_oh,
                    unsigned H5_ATTR_UNUSED mesg_flags, unsigned H5_ATTR_UNUSED *ioflags, size_t p_size,
                    const uint8_t *p)
{
    H5O_drvinfo_t *mesg      = NULL;           /* Native message */
    const uint8_t *p_end     = p + p_size - 1; /* End of the p buffer */
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    /* Version of message */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (*p++ != H5O_DRVINFO_VERSION)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for message");

    /* Allocate space for message */
    if (NULL == (mesg = (H5O_drvinfo_t *)H5MM_calloc(sizeof(H5O_drvinfo_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for driver info message");

    /* Retrieve driver name */
    if (H5_IS_BUFFER_OVERFLOW(p, 8, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5MM_memcpy(mesg->name, p, 8);
    mesg->name[8] = '\0';
    p += 8;

    /* Decode buffer size */
    if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    UINT16DECODE(p, mesg->len);
    if (0 == mesg->len)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "message length can't be zero");

    /* Allocate space for buffer */
    if (NULL == (mesg->buf = (uint8_t *)H5MM_malloc(mesg->len)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for driver info buffer");

    /* Copy encoded driver info into buffer */
    if (H5_IS_BUFFER_OVERFLOW(p, mesg->len, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5MM_memcpy(mesg->buf, p, mesg->len);

    /* Set return value */
    ret_value = (void *)mesg;

done:
    if (!ret_value && mesg) {
        H5MM_xfree(mesg->buf);
        H5MM_xfree(mesg);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__drvinfo_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__drvinfo_encode
 *
 * Purpose:	Encode a v1 B-tree 'K' value message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__drvinfo_encode(H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p,
                    const void *_mesg)
{
    const H5O_drvinfo_t *mesg = (const H5O_drvinfo_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(p);
    assert(mesg);

    /* Store version, driver name, buffer length, & encoded buffer */
    *p++ = H5O_DRVINFO_VERSION;
    H5MM_memcpy(p, mesg->name, 8);
    p += 8;
    assert(mesg->len <= 65535);
    UINT16ENCODE(p, mesg->len);
    H5MM_memcpy(p, mesg->buf, mesg->len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__drvinfo_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__drvinfo_copy
 *
 * Purpose:	Copies a message from _MESG to _DEST, allocating _DEST if
 *		necessary.
 *
 * Return:	Success:	Ptr to _DEST
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__drvinfo_copy(const void *_mesg, void *_dest)
{
    const H5O_drvinfo_t *mesg      = (const H5O_drvinfo_t *)_mesg;
    H5O_drvinfo_t       *dest      = (H5O_drvinfo_t *)_dest;
    void                *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(mesg);

    if (!dest && NULL == (dest = (H5O_drvinfo_t *)H5MM_malloc(sizeof(H5O_drvinfo_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for shared message table message");

    /* Shallow copy the fields */
    *dest = *mesg;

    /* Copy the buffer */
    if (NULL == (dest->buf = (uint8_t *)H5MM_malloc(mesg->len))) {
        if (dest != _dest)
            dest = (H5O_drvinfo_t *)H5MM_xfree(dest);
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    } /* end if */
    H5MM_memcpy(dest->buf, mesg->buf, mesg->len);

    /* Set return value */
    ret_value = dest;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__drvinfo_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5O__drvinfo_size
 *
 * Purpose:	Returns the size of the raw message in bytes not counting the
 *		message type or size fields, but only the data fields.
 *
 * Return:	Success:	Message data size in bytes w/o alignment.
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__drvinfo_size(const H5F_t H5_ATTR_UNUSED *f, bool H5_ATTR_UNUSED disable_shared, const void *_mesg)
{
    const H5O_drvinfo_t *mesg      = (const H5O_drvinfo_t *)_mesg;
    size_t               ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(mesg);

    ret_value = 1 +        /* Version number */
                8 +        /* Driver name */
                2 +        /* Buffer length */
                mesg->len; /* Buffer */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__drvinfo_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__drvinfo_reset
 *
 * Purpose:     Frees internal pointers and resets the message to an
 *              initial state.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__drvinfo_reset(void *_mesg)
{
    H5O_drvinfo_t *mesg = (H5O_drvinfo_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(mesg);

    /* reset */
    mesg->buf = (uint8_t *)H5MM_xfree(mesg->buf);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__drvinfo_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5O__drvinfo_debug
 *
 * Purpose:	Prints debugging info for the message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__drvinfo_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_drvinfo_t *mesg = (const H5O_drvinfo_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Driver name:", mesg->name);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Buffer size:", mesg->len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__drvinfo_debug() */

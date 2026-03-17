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
 * Purpose:	A message holding "implicitly shared object header message"
 *              information in the superblock extension.
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Opkg.h"      /* Object headers			*/
#include "H5MMprivate.h" /* Memory management			*/

static void  *H5O__shmesg_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                 size_t p_size, const uint8_t *p);
static herr_t H5O__shmesg_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__shmesg_copy(const void *_mesg, void *_dest);
static size_t H5O__shmesg_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__shmesg_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_SHMESG[1] = {{
    H5O_SHMESG_ID,              /*message id number                     */
    "shared message table",     /*message name for debugging            */
    sizeof(H5O_shmesg_table_t), /*native message size                   */
    0,                          /* messages are shareable?       */
    H5O__shmesg_decode,         /*decode message                        */
    H5O__shmesg_encode,         /*encode message                        */
    H5O__shmesg_copy,           /*copy the native value                 */
    H5O__shmesg_size,           /*raw message size			*/
    NULL,                       /*free internal memory			*/
    NULL,                       /* free method				*/
    NULL,                       /* file delete method			*/
    NULL,                       /* link method				*/
    NULL,                       /* set share method			*/
    NULL,                       /*can share method		        */
    NULL,                       /* pre copy native value to file	*/
    NULL,                       /* copy native value to file		*/
    NULL,                       /* post copy native value to file	*/
    NULL,                       /* get creation index		        */
    NULL,                       /* set creation index		        */
    H5O__shmesg_debug           /*debug the message			*/
}};

/*-------------------------------------------------------------------------
 * Function:    H5O__shmesg_decode
 *
 * Purpose:     Decode a shared message table message and return a pointer
 *              to a newly allocated H5O_shmesg_table_t struct.
 *
 * Return:      Success:    Ptr to new message in native struct.
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__shmesg_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                   unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    H5O_shmesg_table_t *mesg;                       /* New shared message table */
    const uint8_t      *p_end     = p + p_size - 1; /* End of the p buffer */
    void               *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (NULL == (mesg = (H5O_shmesg_table_t *)H5MM_calloc(sizeof(H5O_shmesg_table_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for shared message table message");

    /* Retrieve version, table address, and number of indexes */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    mesg->version = *p++;

    if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_addr(f), p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5F_addr_decode(f, &p, &(mesg->addr));

    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    mesg->nindexes = *p++;

    /* Set return value */
    ret_value = (void *)mesg;

done:
    if (!ret_value && mesg)
        H5MM_xfree(mesg);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shmesg_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__shmesg_encode
 *
 * Purpose:	Encode a shared message table message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__shmesg_encode(H5F_t *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_shmesg_table_t *mesg = (const H5O_shmesg_table_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(p);
    assert(mesg);

    /* Store version, table address, and number of indexes */
    *p++ = (uint8_t)mesg->version;
    H5F_addr_encode(f, &p, mesg->addr);
    *p++ = (uint8_t)mesg->nindexes;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__shmesg_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__shmesg_copy
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
H5O__shmesg_copy(const void *_mesg, void *_dest)
{
    const H5O_shmesg_table_t *mesg      = (const H5O_shmesg_table_t *)_mesg;
    H5O_shmesg_table_t       *dest      = (H5O_shmesg_table_t *)_dest;
    void                     *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(mesg);

    if (!dest && NULL == (dest = (H5O_shmesg_table_t *)H5MM_malloc(sizeof(H5O_shmesg_table_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for shared message table message");

    /* All this message requires is a shallow copy */
    *dest = *mesg;

    /* Set return value */
    ret_value = dest;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shmesg_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5O__shmesg_size
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
H5O__shmesg_size(const H5F_t *f, bool H5_ATTR_UNUSED disable_shared, const void H5_ATTR_UNUSED *_mesg)
{
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);

    ret_value = (size_t)(1 +                  /* Version number        */
                         H5F_SIZEOF_ADDR(f) + /* Table address */
                         1);                  /* Number of indexes */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__shmesg_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__shmesg_debug
 *
 * Purpose:	Prints debugging info for the message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__shmesg_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_shmesg_table_t *mesg = (const H5O_shmesg_table_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Version:", mesg->version);
    fprintf(stream, "%*s%-*s %" PRIuHADDR " (rel)\n", indent, "", fwidth,
            "Shared message table address:", mesg->addr);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Number of indexes:", mesg->nindexes);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__shmesg_debug() */

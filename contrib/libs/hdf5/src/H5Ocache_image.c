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
 * Created:     H5Ocache_image.c
 *
 * Purpose:     A message indicating that a metadata cache image block
 *              of the indicated length exists at the specified offset
 *              in the HDF5 file.
 *
 *              The mdci_msg only appears in the superblock extension
 *
 *-------------------------------------------------------------------------
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */
#define H5F_FRIEND     /*suppress error about including H5Fpkg   */

#include "H5private.h"   /* Generic Functions                     */
#include "H5Eprivate.h"  /* Error handling                        */
#include "H5Fpkg.h"      /* Files                                 */
#include "H5FLprivate.h" /* Free Lists                            */
#include "H5Opkg.h"      /* Object headers                        */
#include "H5MFprivate.h" /* File space management                 */

/* Callbacks for message class */
static void *H5O__mdci_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags, size_t p_size,
                              const uint8_t *p);
static herr_t H5O__mdci_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__mdci_copy(const void *_mesg, void *_dest);
static size_t H5O__mdci_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__mdci_free(void *mesg);
static herr_t H5O__mdci_delete(H5F_t *f, H5O_t *open_oh, void *_mesg);
static herr_t H5O__mdci_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_MDCI[1] = {{
    H5O_MDCI_MSG_ID,    /* message id number              */
    "mdci",             /* message name for debugging     */
    sizeof(H5O_mdci_t), /* native message size            */
    0,                  /* messages are shareable?         */
    H5O__mdci_decode,   /* decode message                 */
    H5O__mdci_encode,   /* encode message                 */
    H5O__mdci_copy,     /* copy method                    */
    H5O__mdci_size,     /* size of mdc image message      */
    NULL,               /* reset method                   */
    H5O__mdci_free,     /* free method                    */
    H5O__mdci_delete,   /* file delete method             */
    NULL,               /* link method                    */
    NULL,               /* set share method               */
    NULL,               /* can share method               */
    NULL,               /* pre copy native value to file  */
    NULL,               /* copy native value to file      */
    NULL,               /* post copy native value to file */
    NULL,               /* get creation index             */
    NULL,               /* set creation index             */
    H5O__mdci_debug     /* debugging                      */
}};

/* Only one version of the metadata cache image message at present */
#define H5O_MDCI_VERSION_0 0

/* Declare the free list for H5O_mdci_t's */
H5FL_DEFINE_STATIC(H5O_mdci_t);

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_decode
 *
 * Purpose:     Decode a metadata cache image  message and return a
 *              pointer to a newly allocated H5O_mdci_t struct.
 *
 * Return:      Success:    Pointer to new message in native struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__mdci_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                 unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    H5O_mdci_t    *mesg      = NULL;           /* New cache image message */
    const uint8_t *p_end     = p + p_size - 1; /* End of the p buffer */
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    /* Version of message */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (*p++ != H5O_MDCI_VERSION_0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for message");

    /* Allocate space for message */
    if (NULL == (mesg = (H5O_mdci_t *)H5FL_MALLOC(H5O_mdci_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for metadata cache image message");

    if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_addr(f), p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5F_addr_decode(f, &p, &(mesg->addr));

    if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_size(f), p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5F_DECODE_LENGTH(f, p, mesg->size);

    /* Set return value */
    ret_value = (void *)mesg;

done:
    if (!ret_value && mesg)
        H5FL_FREE(H5O_mdci_t, mesg);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__mdci_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_encode
 *
 * Purpose:     Encode metadata cache image message
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mdci_encode(H5F_t *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_mdci_t *mesg = (const H5O_mdci_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(f);
    assert(p);
    assert(mesg);

    /* encode */
    *p++ = H5O_MDCI_VERSION_0;
    H5F_addr_encode(f, &p, mesg->addr);
    H5F_ENCODE_LENGTH(f, p, mesg->size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mdci_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_copy
 *
 * Purpose:     Copies a message from _MESG to _DEST, allocating _DEST if
 *              necessary.
 *
 * Return:      Success:        Ptr to _DEST
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__mdci_copy(const void *_mesg, void *_dest)
{
    const H5O_mdci_t *mesg      = (const H5O_mdci_t *)_mesg;
    H5O_mdci_t       *dest      = (H5O_mdci_t *)_dest;
    void             *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(mesg);
    if (!dest && NULL == (dest = H5FL_MALLOC(H5O_mdci_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* copy */
    *dest = *mesg;

    /* Set return value */
    ret_value = dest;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_mdci__copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_size
 *
 * Purpose:     Returns the size of the raw message in bytes not counting
 *              the message type or size fields, but only the data fields.
 *              This function doesn't take into account alignment.
 *
 * Return:      Success:        Message data size in bytes without alignment.
 *
 *              Failure:        zero
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__mdci_size(const H5F_t *f, bool H5_ATTR_UNUSED disable_shared, const void H5_ATTR_UNUSED *_mesg)
{
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value = (size_t)(1 +                  /* Version number           */
                         H5F_SIZEOF_ADDR(f) + /* addr of metadata cache   */
                                              /* image block              */
                         H5F_SIZEOF_SIZE(f)); /* length of metadata cache */
                                              /* image block              */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__mdci_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_free
 *
 * Purpose:     Free the message
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mdci_free(void *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(mesg);

    mesg = H5FL_FREE(H5O_mdci_t, mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mdci_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_delete
 *
 * Purpose:     Free file space referenced by message
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mdci_delete(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, void *_mesg)
{
    H5O_mdci_t *mesg = (H5O_mdci_t *)_mesg;
    haddr_t     final_eoa;           /* For sanity check */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(mesg);

    /* Free file space for cache image */
    if (H5_addr_defined(mesg->addr)) {
        /* The space for the cache image block was allocated directly
         * from the VFD layer at the end of file.  As this was the
         * last file space allocation before shutdown, the cache image
         * should still be the last item in the file.
         */
        if (f->shared->closing) {
            /* Get the eoa, and verify that it has the expected value */
            if (HADDR_UNDEF == (final_eoa = H5FD_get_eoa(f->shared->lf, H5FD_MEM_DEFAULT)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "unable to get file size");

            assert(H5_addr_eq(final_eoa, mesg->addr + mesg->size));

            if (H5FD_free(f->shared->lf, H5FD_MEM_SUPER, f, mesg->addr, mesg->size) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTFREE, FAIL, "can't free MDC image");
        }
        else if (H5MF_xfree(f, H5FD_MEM_SUPER, mesg->addr, mesg->size) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "unable to free file space for cache image block");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__mdci_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__mdci_debug
 *
 * Purpose:     Prints debugging info.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__mdci_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_mdci_t *mdci = (const H5O_mdci_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mdci);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "Metadata Cache Image Block address:", mdci->addr);

    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Metadata Cache Image Block size in bytes:", mdci->size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__mdci_debug() */

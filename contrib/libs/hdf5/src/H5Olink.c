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
 * Created:             H5Olink.c
 *
 * Purpose:             Link messages
 *
 *-------------------------------------------------------------------------
 */

#define H5G_FRIEND     /*suppress error about including H5Gpkg   */
#define H5L_FRIEND     /*suppress error about including H5Lpkg	  */
#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5Lpkg.h"      /* Links                                */
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/

/* PRIVATE PROTOTYPES */
static void *H5O__link_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags, size_t p_size,
                              const uint8_t *p);
static herr_t H5O__link_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__link_copy(const void *_mesg, void *_dest);
static size_t H5O__link_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__link_reset(void *_mesg);
static herr_t H5O__link_free(void *_mesg);
static herr_t H5O__link_pre_copy_file(H5F_t *file_src, const void *mesg_src, bool *deleted,
                                      const H5O_copy_t *cpy_info, void *udata);
static void  *H5O__link_copy_file(H5F_t *file_src, void *native_src, H5F_t *file_dst, bool *recompute_size,
                                  unsigned *mesg_flags, H5O_copy_t *cpy_info, void *udata);
static herr_t H5O__link_post_copy_file(const H5O_loc_t *src_oloc, const void *mesg_src, H5O_loc_t *dst_oloc,
                                       void *mesg_dst, unsigned *mesg_flags, H5O_copy_t *cpy_info);
static herr_t H5O__link_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_LINK[1] = {{
    H5O_LINK_ID,              /*message id number             */
    "link",                   /*message name for debugging    */
    sizeof(H5O_link_t),       /*native message size           */
    0,                        /* messages are shareable?       */
    H5O__link_decode,         /*decode message                */
    H5O__link_encode,         /*encode message                */
    H5O__link_copy,           /*copy the native value         */
    H5O__link_size,           /*size of symbol table entry    */
    H5O__link_reset,          /* reset method			*/
    H5O__link_free,           /* free method			*/
    H5O_link_delete,          /* file delete method		*/
    NULL,                     /* link method			*/
    NULL,                     /*set share method		*/
    NULL,                     /*can share method		*/
    H5O__link_pre_copy_file,  /* pre copy native value to file */
    H5O__link_copy_file,      /* copy native value to file    */
    H5O__link_post_copy_file, /* post copy native value to file    */
    NULL,                     /* get creation index		*/
    NULL,                     /* set creation index		*/
    H5O__link_debug           /*debug the message             */
}};

/* Current version of link information */
#define H5O_LINK_VERSION 1

/* Flags for link flag encoding */
#define H5O_LINK_NAME_SIZE       0x03 /* 2-bit field for size of name length */
#define H5O_LINK_STORE_CORDER    0x04 /* Whether to store creation index */
#define H5O_LINK_STORE_LINK_TYPE 0x08 /* Whether to store non-default link type */
#define H5O_LINK_STORE_NAME_CSET 0x10 /* Whether to store non-default name character set */
#define H5O_LINK_ALL_FLAGS                                                                                   \
    (H5O_LINK_NAME_SIZE | H5O_LINK_STORE_CORDER | H5O_LINK_STORE_LINK_TYPE | H5O_LINK_STORE_NAME_CSET)

/* Individual definitions of name size values */
#define H5O_LINK_NAME_1 0x00 /* Use 1-byte value for name length */
#define H5O_LINK_NAME_2 0x01 /* Use 2-byte value for name length */
#define H5O_LINK_NAME_4 0x02 /* Use 4-byte value for name length */
#define H5O_LINK_NAME_8 0x03 /* Use 8-byte value for name length */

/* Declare a free list to manage the H5O_link_t struct */
H5FL_DEFINE_STATIC(H5O_link_t);

/*-------------------------------------------------------------------------
 * Function:    H5O__link_decode
 *
 * Purpose:     Decode a message and return a pointer to
 *              a newly allocated one.
 *
 * Return:      Success:    Pointer to new message in native order
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__link_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                 unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    H5O_link_t    *lnk = NULL;                 /* Pointer to link message */
    size_t         len = 0;                    /* Length of a string in the message */
    unsigned char  link_flags;                 /* Flags for encoding link info */
    const uint8_t *p_end     = p + p_size - 1; /* End of the p buffer */
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (*p++ != H5O_LINK_VERSION)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for message");

    /* Allocate space for message */
    if (NULL == (lnk = H5FL_CALLOC(H5O_link_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Get the encoding flags for the link */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    link_flags = *p++;
    if (link_flags & ~H5O_LINK_ALL_FLAGS)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad flag value for message");

    /* Check for non-default link type */
    if (link_flags & H5O_LINK_STORE_LINK_TYPE) {
        /* Get the type of the link */
        if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        lnk->type = (H5L_type_t)*p++;
        if (lnk->type < H5L_TYPE_HARD || lnk->type > H5L_TYPE_MAX)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad link type");
    }
    else
        lnk->type = H5L_TYPE_HARD;

    /* Get the link creation time from the file */
    if (link_flags & H5O_LINK_STORE_CORDER) {
        if (H5_IS_BUFFER_OVERFLOW(p, 8, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        INT64DECODE(p, lnk->corder);
        lnk->corder_valid = true;
    }
    else {
        lnk->corder       = 0;
        lnk->corder_valid = false;
    }

    /* Check for non-default name character set */
    if (link_flags & H5O_LINK_STORE_NAME_CSET) {
        /* Get the link name's character set */
        if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        lnk->cset = (H5T_cset_t)*p++;
        if (lnk->cset < H5T_CSET_ASCII || lnk->cset > H5T_CSET_UTF8)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad cset type");
    }
    else
        lnk->cset = H5T_CSET_ASCII;

    /* Get the length of the link's name */
    switch (link_flags & H5O_LINK_NAME_SIZE) {
        case 0: /* 1 byte size */
            if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            len = *p++;
            break;

        case 1: /* 2 byte size */
            if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT16DECODE(p, len);
            break;

        case 2: /* 4 byte size */
            if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT32DECODE(p, len);
            break;

        case 3: /* 8 byte size */
            if (H5_IS_BUFFER_OVERFLOW(p, 8, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT64DECODE(p, len);
            break;

        default:
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "no appropriate size for name length");
    }
    if (len == 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "invalid name length");

    /* Get the link's name */
    if (H5_IS_BUFFER_OVERFLOW(p, len, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (NULL == (lnk->name = (char *)H5MM_malloc(len + 1)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    H5MM_memcpy(lnk->name, p, len);
    lnk->name[len] = '\0';
    p += len;

    /* Get the appropriate information for each type of link */
    switch (lnk->type) {
        case H5L_TYPE_HARD:
            /* Get the address of the object the link points to */
            if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_addr(f), p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            H5F_addr_decode(f, &p, &(lnk->u.hard.addr));
            break;

        case H5L_TYPE_SOFT:
            /* Get the link value */
            if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT16DECODE(p, len);
            if (len == 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "invalid link length");

            if (H5_IS_BUFFER_OVERFLOW(p, len, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            if (NULL == (lnk->u.soft.name = (char *)H5MM_malloc((size_t)len + 1)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
            H5MM_memcpy(lnk->u.soft.name, p, len);
            lnk->u.soft.name[len] = '\0';
            p += len;
            break;

        /* User-defined links */
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_ERROR:
        case H5L_TYPE_MAX:
        default:
            if (lnk->type < H5L_TYPE_UD_MIN || lnk->type > H5L_TYPE_MAX)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "unknown link type");

            /* A UD link.  Get the user-supplied data */
            if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT16DECODE(p, len);
            if (lnk->type == H5L_TYPE_EXTERNAL && len < 3)
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "external link information length < 3");
            lnk->u.ud.size = len;
            if (len > 0) {
                if (H5_IS_BUFFER_OVERFLOW(p, len, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
                if (NULL == (lnk->u.ud.udata = H5MM_malloc((size_t)len)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
                H5MM_memcpy(lnk->u.ud.udata, p, len);
                p += len;
            }
            else
                lnk->u.ud.udata = NULL;
    }

    /* Set return value */
    ret_value = lnk;

done:
    if (!ret_value && lnk) {
        H5MM_xfree(lnk->name);
        if (lnk->type == H5L_TYPE_SOFT && lnk->u.soft.name != NULL)
            H5MM_xfree(lnk->u.soft.name);
        if (lnk->type >= H5L_TYPE_UD_MIN && lnk->u.ud.size > 0 && lnk->u.ud.udata != NULL)
            H5MM_xfree(lnk->u.ud.udata);
        H5FL_FREE(H5O_link_t, lnk);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__link_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_encode
 *
 * Purpose:     Encodes a link message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__link_encode(H5F_t *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_link_t *lnk = (const H5O_link_t *)_mesg;
    uint64_t          len;        /* Length of a string in the message */
    unsigned char     link_flags; /* Flags for encoding link info */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(lnk);

    /* Get length of link's name */
    len = (uint64_t)strlen(lnk->name);
    assert(len > 0);

    /* encode */
    *p++ = H5O_LINK_VERSION;

    /* The encoding flags for the link */
    if (len > 4294967295)
        link_flags = H5O_LINK_NAME_8;
    else if (len > 65535)
        link_flags = H5O_LINK_NAME_4;
    else if (len > 255)
        link_flags = H5O_LINK_NAME_2;
    else
        link_flags = H5O_LINK_NAME_1;
    link_flags = (unsigned char)(link_flags | (lnk->corder_valid ? H5O_LINK_STORE_CORDER : 0));
    link_flags = (unsigned char)(link_flags | ((lnk->type != H5L_TYPE_HARD) ? H5O_LINK_STORE_LINK_TYPE : 0));
    link_flags = (unsigned char)(link_flags | ((lnk->cset != H5T_CSET_ASCII) ? H5O_LINK_STORE_NAME_CSET : 0));
    *p++       = link_flags;

    /* Store the type of a non-default link */
    if (link_flags & H5O_LINK_STORE_LINK_TYPE)
        *p++ = (uint8_t)lnk->type;

    /* Store the link creation order in the file, if its valid */
    if (lnk->corder_valid)
        INT64ENCODE(p, lnk->corder);

    /* Store a non-default link name character set */
    if (link_flags & H5O_LINK_STORE_NAME_CSET)
        *p++ = (uint8_t)lnk->cset;

    /* Store the link name's length */
    switch (link_flags & H5O_LINK_NAME_SIZE) {
        case 0: /* 1 byte size */
            *p++ = (uint8_t)len;
            break;

        case 1: /* 2 byte size */
            UINT16ENCODE(p, len);
            break;

        case 2: /* 4 byte size */
            UINT32ENCODE(p, len);
            break;

        case 3: /* 8 byte size */
            UINT64ENCODE(p, len);
            break;

        default:
            assert(0 && "bad size for name");
    } /* end switch */

    /* Store the link's name */
    H5MM_memcpy(p, lnk->name, (size_t)len);
    p += len;

    /* Store the appropriate information for each type of link */
    switch (lnk->type) {
        case H5L_TYPE_HARD:
            /* Store the address of the object the link points to */
            H5F_addr_encode(f, &p, lnk->u.hard.addr);
            break;

        case H5L_TYPE_SOFT:
            /* Store the link value */
            len = (uint16_t)strlen(lnk->u.soft.name);
            assert(len > 0);
            UINT16ENCODE(p, len);
            H5MM_memcpy(p, lnk->u.soft.name, (size_t)len);
            p += len;
            break;

        /* User-defined links */
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_ERROR:
        case H5L_TYPE_MAX:
        default:
            assert(lnk->type >= H5L_TYPE_UD_MIN && lnk->type <= H5L_TYPE_MAX);

            /* Store the user-supplied data, however long it is */
            len = (uint16_t)lnk->u.ud.size;
            UINT16ENCODE(p, len);
            if (len > 0) {
                H5MM_memcpy(p, lnk->u.ud.udata, (size_t)len);
                p += len;
            }
            break;
    } /* end switch */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__link_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_copy
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
H5O__link_copy(const void *_mesg, void *_dest)
{
    const H5O_link_t *lnk       = (const H5O_link_t *)_mesg;
    H5O_link_t       *dest      = (H5O_link_t *)_dest;
    void             *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(lnk);
    if (!dest && NULL == (dest = H5FL_MALLOC(H5O_link_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy static information */
    *dest = *lnk;

    /* Duplicate the link's name */
    assert(lnk->name);
    if (NULL == (dest->name = H5MM_xstrdup(lnk->name)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "can't duplicate link name");

    /* Copy other information needed for different link types */
    if (lnk->type == H5L_TYPE_SOFT) {
        if (NULL == (dest->u.soft.name = H5MM_xstrdup(lnk->u.soft.name)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "can't duplicate soft link value");
    } /* end if */
    else if (lnk->type >= H5L_TYPE_UD_MIN) {
        if (lnk->u.ud.size > 0) {
            if (NULL == (dest->u.ud.udata = H5MM_malloc(lnk->u.ud.size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
            H5MM_memcpy(dest->u.ud.udata, lnk->u.ud.udata, lnk->u.ud.size);
        } /* end if */
    }     /* end if */

    /* Set return value */
    ret_value = dest;

done:
    if (NULL == ret_value)
        if (dest) {
            if (dest->name && dest->name != lnk->name)
                dest->name = (char *)H5MM_xfree(dest->name);
            if (NULL == _dest)
                dest = H5FL_FREE(H5O_link_t, dest);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__link_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_size
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
H5O__link_size(const H5F_t *f, bool H5_ATTR_UNUSED disable_shared, const void *_mesg)
{
    const H5O_link_t *lnk = (const H5O_link_t *)_mesg;
    uint64_t          name_len;      /* Length of name */
    size_t            name_size;     /* Size of encoded name length */
    size_t            ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    HDcompile_assert(sizeof(uint64_t) >= sizeof(size_t));

    /* Get name's length */
    name_len = (uint64_t)strlen(lnk->name);

    /* Determine correct value for name size bits */
    if (name_len > 4294967295)
        name_size = 8;
    else if (name_len > 65535)
        name_size = 4;
    else if (name_len > 255)
        name_size = 2;
    else
        name_size = 1;

    /* Set return value */
    ret_value = 1 +                                            /* Version */
                1 +                                            /* Link encoding flags */
                (lnk->type != H5L_TYPE_HARD ? (size_t)1 : 0) + /* Link type */
                (lnk->corder_valid ? 8 : 0) +                  /* Creation order */
                (lnk->cset != H5T_CSET_ASCII ? 1 : 0) +        /* Character set */
                name_size +                                    /* Name length */
                name_len;                                      /* Name */

    /* Add the appropriate length for each type of link */
    switch (lnk->type) {
        case H5L_TYPE_HARD:
            ret_value += H5F_SIZEOF_ADDR(f);
            break;

        case H5L_TYPE_SOFT:
            ret_value += 2 +                       /* Link value length */
                         strlen(lnk->u.soft.name); /* Link value */
            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default: /* Default is user-defined link type */
            assert(lnk->type >= H5L_TYPE_UD_MIN);
            ret_value += 2 +             /* User-defined data size */
                         lnk->u.ud.size; /* User-defined data */
            break;
    } /* end switch */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__link_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__link_reset
 *
 * Purpose:	Frees resources within a message, but doesn't free
 *		the message itself.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__link_reset(void *_mesg)
{
    H5O_link_t *lnk = (H5O_link_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    if (lnk) {
        /* Free information for link (but don't free link pointer) */
        if (lnk->type == H5L_TYPE_SOFT)
            lnk->u.soft.name = (char *)H5MM_xfree(lnk->u.soft.name);
        else if (lnk->type >= H5L_TYPE_UD_MIN) {
            if (lnk->u.ud.size > 0)
                lnk->u.ud.udata = H5MM_xfree(lnk->u.ud.udata);
        } /* end if */
        lnk->name = (char *)H5MM_xfree(lnk->name);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__link_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5O__link_free
 *
 * Purpose:	Frees the message contents and the message itself
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__link_free(void *_mesg)
{
    H5O_link_t *lnk = (H5O_link_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    assert(lnk);

    lnk = H5FL_FREE(H5O_link_t, lnk);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__link_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O_link_delete
 *
 * Purpose:     Free file space referenced by message
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_link_delete(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, void *_mesg)
{
    H5O_link_t *lnk       = (H5O_link_t *)_mesg;
    hid_t       file_id   = -1;      /* ID for the file the link is located in (passed to user callback) */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(f);
    assert(lnk);

    /* Check for adjusting the link count when the link is removed */
    /* Adjust the reference count of the object when a hard link is removed */
    if (lnk->type == H5L_TYPE_HARD) {
        H5O_loc_t oloc;

        /* Construct object location for object, in order to decrement it's ref count */
        H5O_loc_reset(&oloc);
        oloc.file = f;
        assert(H5_addr_defined(lnk->u.hard.addr));
        oloc.addr = lnk->u.hard.addr;

        /* Decrement the ref count for the object */
        if (H5O_link(&oloc, -1) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "unable to decrement object link count");
    } /* end if */
    /* Perform the "delete" callback when a user-defined link is removed */
    else if (lnk->type >= H5L_TYPE_UD_MIN) {
        const H5L_class_t *link_class; /* User-defined link class */

        /* Get the link class for this type of link. */
        if (NULL == (link_class = H5L_find_class(lnk->type)))
            HGOTO_ERROR(H5E_OHDR, H5E_NOTREGISTERED, FAIL, "link class not registered");

        /* Check for delete callback */
        if (link_class->del_func) {
            /* Get a file ID for the file the link is in */
            if ((file_id = H5F_get_id(f)) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to get file ID");

            /* Call user-defined link's 'delete' callback */
            if ((link_class->del_func)(lnk->name, file_id, lnk->u.ud.udata, lnk->u.ud.size) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CALLBACK, FAIL, "link deletion callback returned failure");
        } /* end if */
    }     /* end if */

done:
    /* Release the file ID */
    if (file_id > 0 && H5I_dec_ref(file_id) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTCLOSEFILE, FAIL, "can't close file");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_link_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_pre_copy_file
 *
 * Purpose:     Perform any necessary actions before copying message between
 *              files for link messages.
 *
 * Return:      Success:        Non-negative
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__link_pre_copy_file(H5F_t H5_ATTR_UNUSED *file_src, const void H5_ATTR_UNUSED *native_src, bool *deleted,
                        const H5O_copy_t *cpy_info, void H5_ATTR_UNUSED *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(deleted);
    assert(cpy_info);

    /* If we are performing a 'shallow hierarchy' copy, and this link won't
     *  be included in the final group, indicate that it should be deleted
     *  in the destination object header before performing any other actions
     *  on it.
     */
    if (cpy_info->max_depth >= 0 && cpy_info->curr_depth >= cpy_info->max_depth)
        *deleted = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__link_pre_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_copy_file
 *
 * Purpose:     Copies a message from _MESG to _DEST in file
 *
 * Return:      Success:        Ptr to _DEST
 *
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__link_copy_file(H5F_t H5_ATTR_UNUSED *file_src, void *native_src, H5F_t H5_ATTR_UNUSED *file_dst,
                    bool H5_ATTR_UNUSED *recompute_size, unsigned H5_ATTR_UNUSED *mesg_flags,
                    H5O_copy_t H5_ATTR_UNUSED *cpy_info, void H5_ATTR_UNUSED *udata)
{
    H5O_link_t *link_src  = (H5O_link_t *)native_src;
    void       *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(link_src);
    assert(cpy_info);
    assert(cpy_info->max_depth < 0 || cpy_info->curr_depth < cpy_info->max_depth);

    /* Sanity check source link type */
    if (link_src->type > H5L_TYPE_SOFT && link_src->type < H5L_TYPE_UD_MIN)
        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, NULL, "unrecognized built-in link type");

    /* Allocate "blank" link for destination */
    /* (values will be filled in during 'post copy' operation) */
    if (NULL == (ret_value = H5FL_CALLOC(H5O_link_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__link_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_post_copy_file
 *
 * Purpose:     Finish copying a message from between files
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__link_post_copy_file(const H5O_loc_t *src_oloc, const void *mesg_src, H5O_loc_t *dst_oloc, void *mesg_dst,
                         unsigned H5_ATTR_UNUSED *mesg_flags, H5O_copy_t *cpy_info)
{
    const H5O_link_t *link_src  = (const H5O_link_t *)mesg_src;
    H5O_link_t       *link_dst  = (H5O_link_t *)mesg_dst;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(link_src);
    assert(dst_oloc);
    assert(H5_addr_defined(dst_oloc->addr));
    assert(dst_oloc->file);
    assert(link_dst);
    assert(cpy_info);
    assert(cpy_info->max_depth < 0 || cpy_info->curr_depth < cpy_info->max_depth);

    /* Copy the link (and the object it points to) */
    if (H5L__link_copy_file(dst_oloc->file, link_src, src_oloc, link_dst, cpy_info) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__link_post_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_debug
 *
 * Purpose:     Prints debugging info for a message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__link_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_link_t *lnk       = (const H5O_link_t *)_mesg;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(lnk);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Link Type:",
            (lnk->type == H5L_TYPE_HARD
                 ? "Hard"
                 : (lnk->type == H5L_TYPE_SOFT
                        ? "Soft"
                        : (lnk->type == H5L_TYPE_EXTERNAL
                               ? "External"
                               : (lnk->type >= H5L_TYPE_UD_MIN ? "User-defined" : "Unknown")))));

    if (lnk->corder_valid)
        fprintf(stream, "%*s%-*s %" PRId64 "\n", indent, "", fwidth, "Creation Order:", lnk->corder);

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Link Name Character Set:",
            (lnk->cset == H5T_CSET_ASCII ? "ASCII" : (lnk->cset == H5T_CSET_UTF8 ? "UTF-8" : "Unknown")));
    fprintf(stream, "%*s%-*s '%s'\n", indent, "", fwidth, "Link Name:", lnk->name);

    /* Display link-specific information */
    switch (lnk->type) {
        case H5L_TYPE_HARD:
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
                    "Object address:", lnk->u.hard.addr);
            break;

        case H5L_TYPE_SOFT:
            fprintf(stream, "%*s%-*s '%s'\n", indent, "", fwidth, "Link Value:", lnk->u.soft.name);
            break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            if (lnk->type >= H5L_TYPE_UD_MIN) {
                if (lnk->type == H5L_TYPE_EXTERNAL) {
                    const char *objname =
                        (const char *)lnk->u.ud.udata + (strlen((const char *)lnk->u.ud.udata) + 1);

                    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
                            "External File Name:", (const char *)lnk->u.ud.udata);
                    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "External Object Name:", objname);
                } /* end if */
                else {
                    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth,
                            "User-Defined Link Size:", lnk->u.ud.size);
                } /* end else */
            }     /* end if */
            else
                HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unrecognized link type");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__link_debug() */

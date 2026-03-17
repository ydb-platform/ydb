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
 * Created:             H5Oainfo.c
 *
 * Purpose:             Attribute Information messages.
 *
 *-------------------------------------------------------------------------
 */

#define H5A_FRIEND     /*suppress error about including H5Apkg	  */
#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Apkg.h"      /* Attributes				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Opkg.h"      /* Object headers			*/

/* PRIVATE PROTOTYPES */
static void  *H5O__ainfo_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                size_t p_size, const uint8_t *p);
static herr_t H5O__ainfo_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__ainfo_copy(const void *_mesg, void *_dest);
static size_t H5O__ainfo_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__ainfo_free(void *_mesg);
static herr_t H5O__ainfo_delete(H5F_t *f, H5O_t *open_oh, void *_mesg);
static herr_t H5O__ainfo_pre_copy_file(H5F_t *file_src, const void *mesg_src, bool *deleted,
                                       const H5O_copy_t *cpy_info, void *udata);
static void  *H5O__ainfo_copy_file(H5F_t *file_src, void *mesg_src, H5F_t *file_dst, bool *recompute_size,
                                   unsigned *mesg_flags, H5O_copy_t *cpy_info, void *udata);
static herr_t H5O__ainfo_post_copy_file(const H5O_loc_t *src_oloc, const void *mesg_src, H5O_loc_t *dst_oloc,
                                        void *mesg_dst, unsigned *mesg_flags, H5O_copy_t *cpy_info);
static herr_t H5O__ainfo_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_AINFO[1] = {{
    H5O_AINFO_ID,              /*message id number             */
    "ainfo",                   /*message name for debugging    */
    sizeof(H5O_ainfo_t),       /*native message size           */
    0,                         /* messages are shareable?       */
    H5O__ainfo_decode,         /*decode message                */
    H5O__ainfo_encode,         /*encode message                */
    H5O__ainfo_copy,           /*copy the native value         */
    H5O__ainfo_size,           /*size of symbol table entry    */
    NULL,                      /*default reset method          */
    H5O__ainfo_free,           /* free method			*/
    H5O__ainfo_delete,         /* file delete method		*/
    NULL,                      /* link method			*/
    NULL,                      /*set share method		*/
    NULL,                      /*can share method		*/
    H5O__ainfo_pre_copy_file,  /* pre copy native value to file */
    H5O__ainfo_copy_file,      /* copy native value to file    */
    H5O__ainfo_post_copy_file, /* post copy native value to file */
    NULL,                      /* get creation index		*/
    NULL,                      /* set creation index		*/
    H5O__ainfo_debug           /*debug the message             */
}};

/* Current version of attribute info information */
#define H5O_AINFO_VERSION 0

/* Flags for attribute info flag encoding */
#define H5O_AINFO_TRACK_CORDER 0x01
#define H5O_AINFO_INDEX_CORDER 0x02
#define H5O_AINFO_ALL_FLAGS    (H5O_AINFO_TRACK_CORDER | H5O_AINFO_INDEX_CORDER)

/* Declare a free list to manage the H5O_ainfo_t struct */
H5FL_DEFINE_STATIC(H5O_ainfo_t);

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_decode
 *
 * Purpose:     Decode a message and return a pointer to a newly allocated one.
 *
 * Return:      Success:        Ptr to new message in native form.
 *              Failure:        NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__ainfo_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                  unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    const uint8_t *p_end = p + p_size - 1; /* End of input buffer */
    H5O_ainfo_t   *ainfo = NULL;           /* Attribute info */
    unsigned char  flags;                  /* Flags for encoding attribute info */
    uint8_t        sizeof_addr;            /* Size of addresses in this file */
    void          *ret_value = NULL;       /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    sizeof_addr = H5F_sizeof_addr(f);

    /* Version of message */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (*p++ != H5O_AINFO_VERSION)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for message");

    /* Allocate space for message */
    if (NULL == (ainfo = H5FL_MALLOC(H5O_ainfo_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Get the flags for the message */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    flags = *p++;
    if (flags & ~H5O_AINFO_ALL_FLAGS)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad flag value for message");
    ainfo->track_corder = (flags & H5O_AINFO_TRACK_CORDER) ? true : false;
    ainfo->index_corder = (flags & H5O_AINFO_INDEX_CORDER) ? true : false;

    /* Set the number of attributes on the object to an invalid value, so we query it later */
    ainfo->nattrs = HSIZET_MAX;

    /* Max. creation order value for the object */
    if (ainfo->track_corder) {
        if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        UINT16DECODE(p, ainfo->max_crt_idx);
    }
    else
        ainfo->max_crt_idx = H5O_MAX_CRT_ORDER_IDX;

    /* Address of fractal heap to store "dense" attributes */
    if (H5_IS_BUFFER_OVERFLOW(p, sizeof_addr, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5F_addr_decode(f, &p, &(ainfo->fheap_addr));

    /* Address of v2 B-tree to index names of attributes (names are always indexed) */
    if (H5_IS_BUFFER_OVERFLOW(p, sizeof_addr, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5F_addr_decode(f, &p, &(ainfo->name_bt2_addr));

    /* Address of v2 B-tree to index creation order of links, if there is one */
    if (ainfo->index_corder) {
        if (H5_IS_BUFFER_OVERFLOW(p, sizeof_addr, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        H5F_addr_decode(f, &p, &(ainfo->corder_bt2_addr));
    }
    else
        ainfo->corder_bt2_addr = HADDR_UNDEF;

    /* Set return value */
    ret_value = ainfo;

done:
    if (ret_value == NULL && ainfo != NULL)
        ainfo = H5FL_FREE(H5O_ainfo_t, ainfo);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__ainfo_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_encode
 *
 * Purpose:     Encodes a message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__ainfo_encode(H5F_t *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_ainfo_t *ainfo = (const H5O_ainfo_t *)_mesg;
    unsigned char      flags; /* Flags for encoding attribute info */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(ainfo);

    /* Message version */
    *p++ = H5O_AINFO_VERSION;

    /* The flags for the attribute indices */
    flags = (unsigned char)(ainfo->track_corder ? H5O_AINFO_TRACK_CORDER : 0);
    flags = (unsigned char)(flags | (ainfo->index_corder ? H5O_AINFO_INDEX_CORDER : 0));
    *p++  = flags;

    /* Max. creation order value for the object */
    if (ainfo->track_corder)
        UINT16ENCODE(p, ainfo->max_crt_idx);

    /* Address of fractal heap to store "dense" attributes */
    H5F_addr_encode(f, &p, ainfo->fheap_addr);

    /* Address of v2 B-tree to index names of attributes */
    H5F_addr_encode(f, &p, ainfo->name_bt2_addr);

    /* Address of v2 B-tree to index creation order of attributes, if they are indexed */
    if (ainfo->index_corder)
        H5F_addr_encode(f, &p, ainfo->corder_bt2_addr);
    else
        assert(!H5_addr_defined(ainfo->corder_bt2_addr));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__ainfo_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_copy
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
H5O__ainfo_copy(const void *_mesg, void *_dest)
{
    const H5O_ainfo_t *ainfo     = (const H5O_ainfo_t *)_mesg;
    H5O_ainfo_t       *dest      = (H5O_ainfo_t *)_dest;
    void              *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(ainfo);
    if (!dest && NULL == (dest = H5FL_MALLOC(H5O_ainfo_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* copy */
    *dest = *ainfo;

    /* Set return value */
    ret_value = dest;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__ainfo_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_size
 *
 * Purpose:     Returns the size of the raw message in bytes not counting
 *              the message type or size fields, but only the data fields.
 *              This function doesn't take into account alignment.
 *
 * Return:      Success:        Message data size in bytes without alignment.
 *              Failure:        zero
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__ainfo_size(const H5F_t *f, bool H5_ATTR_UNUSED disable_shared, const void *_mesg)
{
    const H5O_ainfo_t *ainfo     = (const H5O_ainfo_t *)_mesg;
    size_t             ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value =
        (size_t)(1                               /* Version */
                 + 1                             /* Index flags */
                 + (ainfo->track_corder ? 2 : 0) /* Curr. max. creation order value */
                 + H5F_SIZEOF_ADDR(f)            /* Address of fractal heap to store "dense" attributes */
                 + H5F_SIZEOF_ADDR(f)            /* Address of v2 B-tree for indexing names of attributes */
                 + (ainfo->index_corder
                        ? H5F_SIZEOF_ADDR(f)
                        : 0)); /* Address of v2 B-tree for indexing creation order values of attributes */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__ainfo_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__ainfo_free
 *
 * Purpose:	Frees the message
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__ainfo_free(void *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(mesg);

    mesg = H5FL_FREE(H5O_ainfo_t, mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__ainfo_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_delete
 *
 * Purpose:     Free file space referenced by message.  Note that open_oh
 *              *must* be non-NULL - this means that calls to
 *              H5O_msg_delete must include an oh if the type is ainfo.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__ainfo_delete(H5F_t *f, H5O_t H5_ATTR_NDEBUG_UNUSED *open_oh, void *_mesg)
{
    H5O_ainfo_t *ainfo     = (H5O_ainfo_t *)_mesg;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(ainfo);
    assert(open_oh);

    /* If the object is using "dense" attribute storage, delete it */
    if (H5_addr_defined(ainfo->fheap_addr))
        /* Delete the attribute */
        if (H5A__dense_delete(f, ainfo) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "unable to free dense attribute storage");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__ainfo_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_pre_copy_file
 *
 * Purpose:     Perform any necessary actions before copying message between
 *              files.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__ainfo_pre_copy_file(H5F_t H5_ATTR_UNUSED *file_src, const void H5_ATTR_UNUSED *native_src, bool *deleted,
                         const H5O_copy_t *cpy_info, void H5_ATTR_UNUSED *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(deleted);
    assert(cpy_info);

    /* If we are not copying attributes into the destination file, indicate
     *  that this message should be deleted.
     */
    if (cpy_info->copy_without_attr)
        *deleted = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__ainfo_pre_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_copy_file
 *
 * Purpose:     Copies a message from _MESG to _DEST in file
 *
 * Return:      Success:        Ptr to _DEST
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__ainfo_copy_file(H5F_t H5_ATTR_NDEBUG_UNUSED *file_src, void *mesg_src, H5F_t *file_dst,
                     bool H5_ATTR_UNUSED *recompute_size, unsigned H5_ATTR_UNUSED *mesg_flags,
                     H5O_copy_t H5_ATTR_NDEBUG_UNUSED *cpy_info, void H5_ATTR_UNUSED *udata)
{
    H5O_ainfo_t *ainfo_src = (H5O_ainfo_t *)mesg_src;
    H5O_ainfo_t *ainfo_dst = NULL;
    void        *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(file_src);
    assert(ainfo_src);
    assert(file_dst);
    assert(cpy_info);
    assert(!cpy_info->copy_without_attr);

    /* Allocate space for the destination message */
    if (NULL == (ainfo_dst = H5FL_MALLOC(H5O_ainfo_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the top level of the information */
    *ainfo_dst = *ainfo_src;

    if (H5_addr_defined(ainfo_src->fheap_addr)) {
        /* Prepare to copy dense attributes - actual copy in post_copy */

        /* Set copied metadata tag */
        H5_BEGIN_TAG(H5AC__COPIED_TAG)

        if (H5A__dense_create(file_dst, ainfo_dst) < 0)
            HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTINIT, NULL, "unable to create dense storage for attributes");

        /* Reset metadata tag */
        H5_END_TAG
    } /* end if */

    /* Set return value */
    ret_value = ainfo_dst;

done:
    /* Release destination attribute information on failure */
    if (!ret_value && ainfo_dst)
        ainfo_dst = H5FL_FREE(H5O_ainfo_t, ainfo_dst);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__ainfo_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_post_copy_file
 *
 * Purpose:     Finish copying a message from between files.
 *              We have to copy the values of a reference attribute in the
 *              post copy because H5O_post_copy_file() fails in the case that
 *              an object may have a reference attribute that points to the
 *              object itself.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__ainfo_post_copy_file(const H5O_loc_t *src_oloc, const void *mesg_src, H5O_loc_t *dst_oloc,
                          void *mesg_dst, unsigned H5_ATTR_UNUSED *mesg_flags, H5O_copy_t *cpy_info)
{
    const H5O_ainfo_t *ainfo_src = (const H5O_ainfo_t *)mesg_src;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(ainfo_src);

    if (H5_addr_defined(ainfo_src->fheap_addr))
        if (H5A__dense_post_copy_file_all(src_oloc, ainfo_src, dst_oloc, (H5O_ainfo_t *)mesg_dst, cpy_info) <
            0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTCOPY, FAIL, "can't copy attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__ainfo_post_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__ainfo_debug
 *
 * Purpose:     Prints debugging info for a message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__ainfo_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_ainfo_t *ainfo = (const H5O_ainfo_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(ainfo);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth, "Number of attributes:", ainfo->nattrs);
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Track creation order of attributes:", ainfo->track_corder ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Index creation order of attributes:", ainfo->index_corder ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Max. creation index value:", (unsigned)ainfo->max_crt_idx);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "'Dense' attribute storage fractal heap address:", ainfo->fheap_addr);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "'Dense' attribute storage name index v2 B-tree address:", ainfo->name_bt2_addr);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "'Dense' attribute storage creation order index v2 B-tree address:", ainfo->corder_bt2_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__ainfo_debug() */

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
 * Created:             H5Olinfo.c
 *
 * Purpose:             Link information messages
 *
 *-------------------------------------------------------------------------
 */

#define H5G_FRIEND     /* Suppress error about including H5Gpkg */
#define H5L_FRIEND     /* Suppress error about including H5Lpkg */
#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Lpkg.h"      /* Links                                */
#include "H5Opkg.h"      /* Object headers			*/

/* PRIVATE PROTOTYPES */
static void  *H5O__linfo_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                size_t p_size, const uint8_t *p);
static herr_t H5O__linfo_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__linfo_copy(const void *_mesg, void *_dest);
static size_t H5O__linfo_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__linfo_free(void *_mesg);
static herr_t H5O__linfo_delete(H5F_t *f, H5O_t *open_oh, void *_mesg);
static void  *H5O__linfo_copy_file(H5F_t *file_src, void *native_src, H5F_t *file_dst, bool *recompute_size,
                                   unsigned *mesg_flags, H5O_copy_t *cpy_info, void *udata);
static herr_t H5O__linfo_post_copy_file(const H5O_loc_t *parent_src_oloc, const void *mesg_src,
                                        H5O_loc_t *dst_oloc, void *mesg_dst, unsigned *mesg_flags,
                                        H5O_copy_t *cpy_info);
static herr_t H5O__linfo_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_LINFO[1] = {{
    H5O_LINFO_ID,              /*message id number             */
    "linfo",                   /*message name for debugging    */
    sizeof(H5O_linfo_t),       /*native message size           */
    0,                         /* messages are shareable?       */
    H5O__linfo_decode,         /*decode message                */
    H5O__linfo_encode,         /*encode message                */
    H5O__linfo_copy,           /*copy the native value         */
    H5O__linfo_size,           /*size of symbol table entry    */
    NULL,                      /*default reset method          */
    H5O__linfo_free,           /* free method			*/
    H5O__linfo_delete,         /* file delete method		*/
    NULL,                      /* link method			*/
    NULL,                      /*set share method		*/
    NULL,                      /*can share method		*/
    NULL,                      /* pre copy native value to file */
    H5O__linfo_copy_file,      /* copy native value to file    */
    H5O__linfo_post_copy_file, /* post copy native value to file */
    NULL,                      /* get creation index		*/
    NULL,                      /* set creation index		*/
    H5O__linfo_debug           /*debug the message             */
}};

/* Current version of link info information */
#define H5O_LINFO_VERSION 0

/* Flags for link info index flag encoding */
#define H5O_LINFO_TRACK_CORDER 0x01
#define H5O_LINFO_INDEX_CORDER 0x02
#define H5O_LINFO_ALL_FLAGS    (H5O_LINFO_TRACK_CORDER | H5O_LINFO_INDEX_CORDER)

/* Data exchange structure to use when copying links from src to dst */
typedef struct {
    const H5O_loc_t *src_oloc;  /* Source object location */
    H5O_loc_t       *dst_oloc;  /* Destination object location */
    H5O_linfo_t     *dst_linfo; /* Destination object's link info message */
    H5O_copy_t      *cpy_info;  /* Information for copy operation */
} H5O_linfo_postcopy_ud_t;

/* Declare a free list to manage the H5O_linfo_t struct */
H5FL_DEFINE_STATIC(H5O_linfo_t);

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_decode
 *
 * Purpose:     Decode a message and return a pointer to a newly allocated one.
 *
 * Return:      Success:        Pointer to new message in native form
 *              Failure:        NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__linfo_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                  unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    const uint8_t *p_end = p + p_size - 1;         /* End of the p buffer */
    H5O_linfo_t   *linfo = NULL;                   /* Link info */
    unsigned char  index_flags;                    /* Flags for encoding link index info */
    uint8_t        addr_size = H5F_SIZEOF_ADDR(f); /* Temp var */
    void          *ret_value = NULL;               /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    /* Check input buffer before decoding version and index flags */
    if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

    /* Version of message */
    if (*p++ != H5O_LINFO_VERSION)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for message");

    /* Allocate space for message */
    if (NULL == (linfo = H5FL_MALLOC(H5O_linfo_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Get the index flags for the group */
    index_flags = *p++;
    if (index_flags & ~H5O_LINFO_ALL_FLAGS)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad flag value for message");
    linfo->track_corder = (index_flags & H5O_LINFO_TRACK_CORDER) ? true : false;
    linfo->index_corder = (index_flags & H5O_LINFO_INDEX_CORDER) ? true : false;

    /* Set the number of links in the group to an invalid value, so we query it later */
    linfo->nlinks = HSIZET_MAX;

    /* Max. link creation order value for the group, if tracked */
    if (linfo->track_corder) {
        if (H5_IS_BUFFER_OVERFLOW(p, 8, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        INT64DECODE(p, linfo->max_corder);
    }
    else
        linfo->max_corder = 0;

    /* Check input buffer before decoding the next two addresses */
    if (H5_IS_BUFFER_OVERFLOW(p, addr_size + addr_size, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

    /* Address of fractal heap to store "dense" links */
    H5F_addr_decode(f, &p, &(linfo->fheap_addr));

    /* Address of v2 B-tree to index names of links (names are always indexed) */
    H5F_addr_decode(f, &p, &(linfo->name_bt2_addr));

    /* Address of v2 B-tree to index creation order of links, if there is one */
    if (linfo->index_corder) {
        if (H5_IS_BUFFER_OVERFLOW(p, addr_size, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        H5F_addr_decode(f, &p, &(linfo->corder_bt2_addr));
    }
    else
        linfo->corder_bt2_addr = HADDR_UNDEF;

    /* Set return value */
    ret_value = linfo;

done:
    if (ret_value == NULL)
        if (linfo != NULL)
            linfo = H5FL_FREE(H5O_linfo_t, linfo);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__linfo_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_encode
 *
 * Purpose:     Encodes a message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__linfo_encode(H5F_t *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_linfo_t *linfo = (const H5O_linfo_t *)_mesg;
    unsigned char      index_flags; /* Flags for encoding link index info */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(linfo);

    /* Message version */
    *p++ = H5O_LINFO_VERSION;

    /* The flags for the link indices */
    index_flags = (uint8_t)(linfo->track_corder ? H5O_LINFO_TRACK_CORDER : 0);
    index_flags = (uint8_t)(index_flags | (linfo->index_corder ? H5O_LINFO_INDEX_CORDER : 0));
    *p++        = index_flags;

    /* Max. link creation order value for the group, if tracked */
    if (linfo->track_corder)
        INT64ENCODE(p, linfo->max_corder);

    /* Address of fractal heap to store "dense" links */
    H5F_addr_encode(f, &p, linfo->fheap_addr);

    /* Address of v2 B-tree to index names of links */
    H5F_addr_encode(f, &p, linfo->name_bt2_addr);

    /* Address of v2 B-tree to index creation order of links, if they are indexed */
    if (linfo->index_corder)
        H5F_addr_encode(f, &p, linfo->corder_bt2_addr);
    else
        assert(!H5_addr_defined(linfo->corder_bt2_addr));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__linfo_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_copy
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
H5O__linfo_copy(const void *_mesg, void *_dest)
{
    const H5O_linfo_t *linfo     = (const H5O_linfo_t *)_mesg;
    H5O_linfo_t       *dest      = (H5O_linfo_t *)_dest;
    void              *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(linfo);
    if (!dest && NULL == (dest = H5FL_MALLOC(H5O_linfo_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* copy */
    *dest = *linfo;

    /* Set return value */
    ret_value = dest;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__linfo_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_size
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
H5O__linfo_size(const H5F_t *f, bool H5_ATTR_UNUSED disable_shared, const void *_mesg)
{
    const H5O_linfo_t *linfo     = (const H5O_linfo_t *)_mesg;
    size_t             ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value =
        1                                       /* Version */
        + 1                                     /* Index flags */
        + (linfo->track_corder ? (size_t)8 : 0) /* Curr. max. creation order value */
        + (size_t)H5F_SIZEOF_ADDR(f)            /* Address of fractal heap to store "dense" links */
        + (size_t)H5F_SIZEOF_ADDR(f)            /* Address of v2 B-tree for indexing names of links */
        + (linfo->index_corder ? (size_t)H5F_SIZEOF_ADDR(f)
                               : 0); /* Address of v2 B-tree for indexing creation order values of links */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__linfo_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__linfo_free
 *
 * Purpose:	Frees the message
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__linfo_free(void *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(mesg);

    mesg = H5FL_FREE(H5O_linfo_t, mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__linfo_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_delete
 *
 * Purpose:     Free file space referenced by message
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__linfo_delete(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, void *_mesg)
{
    H5O_linfo_t *linfo     = (H5O_linfo_t *)_mesg;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(linfo);

    /* If the group is using "dense" link storage, delete it */
    if (H5_addr_defined(linfo->fheap_addr))
        if (H5G__dense_delete(f, linfo, true) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "unable to free dense link storage");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__linfo_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_copy_file
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
H5O__linfo_copy_file(H5F_t H5_ATTR_UNUSED *file_src, void *native_src, H5F_t *file_dst,
                     bool H5_ATTR_UNUSED *recompute_size, unsigned H5_ATTR_UNUSED *mesg_flags,
                     H5O_copy_t *cpy_info, void *_udata)
{
    H5O_linfo_t        *linfo_src = (H5O_linfo_t *)native_src;
    H5O_linfo_t        *linfo_dst = NULL;
    H5G_copy_file_ud_t *udata     = (H5G_copy_file_ud_t *)_udata;
    void               *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(H5AC__COPIED_TAG)

    /* check args */
    assert(linfo_src);
    assert(cpy_info);

    /* Copy the source message */
    if (NULL == (linfo_dst = (H5O_linfo_t *)H5O__linfo_copy(linfo_src, NULL)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, NULL, "memory allocation failed");

    /* If we are performing a 'shallow hierarchy' copy, and the links in this
     *  group won't be included in the destination, reset the link info for
     *  this group.
     */
    if (cpy_info->max_depth >= 0 && cpy_info->curr_depth >= cpy_info->max_depth) {
        linfo_dst->nlinks          = 0;
        linfo_dst->max_corder      = 0;
        linfo_dst->fheap_addr      = HADDR_UNDEF;
        linfo_dst->name_bt2_addr   = HADDR_UNDEF;
        linfo_dst->corder_bt2_addr = HADDR_UNDEF;
    } /* end if */
    else {
        /* Create the components of the dense link storage for the destination group */
        /* (XXX: should probably get the "creation" parameters for the source group's
         *      dense link storage components and use those - QAK)
         */
        if (H5_addr_defined(linfo_src->fheap_addr)) {
            /* Create the dense link storage */
            if (H5G__dense_create(file_dst, linfo_dst, udata->common.src_pline) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "unable to create 'dense' form of new format group");
        } /* end if */
    }     /* end else */

    /* Set return value */
    ret_value = linfo_dst;

done:
    if (!ret_value)
        if (linfo_dst)
            linfo_dst = H5FL_FREE(H5O_linfo_t, linfo_dst);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* H5O__linfo_copy_file() */

/*-------------------------------------------------------------------------
 * Function:	H5O__linfo_post_copy_file_cb
 *
 * Purpose:	Callback routine for copying links from src to dst file
 *              during "post copy" routine
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__linfo_post_copy_file_cb(const H5O_link_t *src_lnk, void *_udata)
{
    H5O_linfo_postcopy_ud_t *udata = (H5O_linfo_postcopy_ud_t *)_udata; /* 'User data' passed in */
    H5O_link_t               dst_lnk;                                   /* Destination link to insert */
    bool                     dst_lnk_init = false;        /* Whether the destination link is initialized */
    herr_t                   ret_value    = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(src_lnk);
    assert(udata);

    /* Copy the link (and the object it points to) */
    if (H5L__link_copy_file(udata->dst_oloc->file, src_lnk, udata->src_oloc, &dst_lnk, udata->cpy_info) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, H5_ITER_ERROR, "unable to copy link");
    dst_lnk_init = true;

    /* Set metadata tag in API context */
    H5_BEGIN_TAG(H5AC__COPIED_TAG)

    /* Insert the new object in the destination file's group */
    /* (Doesn't increment the link count - that's already been taken care of for hard links) */
    if (H5G__dense_insert(udata->dst_oloc->file, udata->dst_linfo, &dst_lnk) < 0)
        HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTINSERT, H5_ITER_ERROR, "unable to insert destination link");

    /* Reset metadata tag in API context */
    H5_END_TAG

done:
    /* Check if the destination link has been initialized */
    if (dst_lnk_init)
        H5O_msg_reset(H5O_LINK_ID, &dst_lnk);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__linfo_post_copy_file_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_post_copy_file
 *
 * Purpose:     Finish copying a message from between files
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__linfo_post_copy_file(const H5O_loc_t *src_oloc, const void *mesg_src, H5O_loc_t *dst_oloc,
                          void *mesg_dst, unsigned H5_ATTR_UNUSED *mesg_flags, H5O_copy_t *cpy_info)
{
    const H5O_linfo_t *linfo_src = (const H5O_linfo_t *)mesg_src;
    H5O_linfo_t       *linfo_dst = (H5O_linfo_t *)mesg_dst;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(src_oloc && src_oloc->file);
    assert(linfo_src);
    assert(dst_oloc && dst_oloc->file);
    assert(H5_addr_defined(dst_oloc->addr));
    assert(linfo_dst);
    assert(cpy_info);

    /* If we are performing a 'shallow hierarchy' copy, get out now */
    if (cpy_info->max_depth >= 0 && cpy_info->curr_depth >= cpy_info->max_depth)
        HGOTO_DONE(SUCCEED);

    /* Check for copying dense link storage */
    if (H5_addr_defined(linfo_src->fheap_addr)) {
        H5O_linfo_postcopy_ud_t udata; /* User data for iteration callback */

        /* Set up dense link iteration user data */
        udata.src_oloc  = src_oloc;
        udata.dst_oloc  = dst_oloc;
        udata.dst_linfo = linfo_dst;
        udata.cpy_info  = cpy_info;

        /* Iterate over the links in the group, building a table of the link messages */
        if (H5G__dense_iterate(src_oloc->file, linfo_src, H5_INDEX_NAME, H5_ITER_NATIVE, (hsize_t)0, NULL,
                               H5O__linfo_post_copy_file_cb, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTNEXT, FAIL, "error iterating over links");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__linfo_post_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__linfo_debug
 *
 * Purpose:     Prints debugging info for a message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__linfo_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_linfo_t *linfo = (const H5O_linfo_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(linfo);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Track creation order of links:", linfo->track_corder ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Index creation order of links:", linfo->index_corder ? "TRUE" : "FALSE");
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth, "Number of links:", linfo->nlinks);
    fprintf(stream, "%*s%-*s %" PRId64 "\n", indent, "", fwidth,
            "Max. creation order value:", linfo->max_corder);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "'Dense' link storage fractal heap address:", linfo->fheap_addr);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "'Dense' link storage name index v2 B-tree address:", linfo->name_bt2_addr);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth,
            "'Dense' link storage creation order index v2 B-tree address:", linfo->corder_bt2_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__linfo_debug() */

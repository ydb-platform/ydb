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

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5HLprivate.h" /* Local Heaps				*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers			*/

/* PRIVATE PROTOTYPES */
static void  *H5O__efl_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags, size_t p_size,
                              const uint8_t *p);
static herr_t H5O__efl_encode(H5F_t *f, bool disable_shared, uint8_t *p, const void *_mesg);
static void  *H5O__efl_copy(const void *_mesg, void *_dest);
static size_t H5O__efl_size(const H5F_t *f, bool disable_shared, const void *_mesg);
static herr_t H5O__efl_reset(void *_mesg);
static void  *H5O__efl_copy_file(H5F_t *file_src, void *mesg_src, H5F_t *file_dst, bool *recompute_size,
                                 unsigned *mesg_flags, H5O_copy_t *cpy_info, void *udata);
static herr_t H5O__efl_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_EFL[1] = {{
    H5O_EFL_ID,           /*message id number		*/
    "external file list", /*message name for debugging    */
    sizeof(H5O_efl_t),    /*native message size	    	*/
    0,                    /* messages are shareable?       */
    H5O__efl_decode,      /*decode message		*/
    H5O__efl_encode,      /*encode message		*/
    H5O__efl_copy,        /*copy native value		*/
    H5O__efl_size,        /*size of message on disk	*/
    H5O__efl_reset,       /*reset method		    	*/
    NULL,                 /* free method			*/
    NULL,                 /* file delete method		*/
    NULL,                 /* link method			*/
    NULL,                 /*set share method		*/
    NULL,                 /*can share method		*/
    NULL,                 /* pre copy native value to file */
    H5O__efl_copy_file,   /* copy native value to file    */
    NULL,                 /* post copy native value to file    */
    NULL,                 /* get creation index		*/
    NULL,                 /* set creation index		*/
    H5O__efl_debug        /*debug the message		*/
}};

#define H5O_EFL_VERSION 1

/*-------------------------------------------------------------------------
 * Function:    H5O__efl_decode
 *
 * Purpose:	    Decode an external file list message and return a pointer to
 *              the message (and some other data).
 *
 *              We allow zero dimension size starting from the 1.8.7 release.
 *              The dataset size of external storage can be zero.
 *
 * Return:      Success:    Pointer to a new message struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__efl_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    H5O_efl_t     *mesg = NULL;
    int            version;
    const uint8_t *p_end     = p + p_size - 1; /* pointer to last byte in p */
    const char    *s         = NULL;
    H5HL_t        *heap      = NULL;
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(p);

    if (NULL == (mesg = (H5O_efl_t *)H5MM_calloc(sizeof(H5O_efl_t))))
        HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Version (1 byte) */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    version = *p++;
    if (version != H5O_EFL_VERSION)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for external file list message");

    /* Reserved (3 bytes) */
    if (H5_IS_BUFFER_OVERFLOW(p, 3, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    p += 3;

    /* Number of slots (2x 2 bytes) */
    if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    UINT16DECODE(p, mesg->nalloc);
    if (mesg->nalloc <= 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad number of allocated slots when parsing efl msg");

    if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    UINT16DECODE(p, mesg->nused);
    if (mesg->nused > mesg->nalloc)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad number of in-use slots when parsing efl msg");

    /* Heap address */
    if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_addr(f), p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    H5F_addr_decode(f, &p, &(mesg->heap_addr));
    if (H5_addr_defined(mesg->heap_addr) == false)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad local heap address when parsing efl msg");

    /* Decode the file list */
    mesg->slot = (H5O_efl_entry_t *)H5MM_calloc(mesg->nalloc * sizeof(H5O_efl_entry_t));
    if (NULL == mesg->slot)
        HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, NULL, "memory allocation failed");

    if (NULL == (heap = H5HL_protect(f, mesg->heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to protect local heap");

#ifdef H5O_DEBUG
    /* Verify that the name at offset 0 in the local heap is the empty string */
    s = (const char *)H5HL_offset_into(heap, 0);
    if (s == NULL)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "could not obtain pointer into local heap");
    if (*s != '\0')
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "entry at offset 0 in local heap not an empty string");
#endif

    for (size_t u = 0; u < mesg->nused; u++) {
        /* Name */
        if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_size(f), p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        H5F_DECODE_LENGTH(f, p, mesg->slot[u].name_offset);

        if ((s = (const char *)H5HL_offset_into(heap, mesg->slot[u].name_offset)) == NULL)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to get external file name");
        if (*s == '\0')
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "invalid external file name");
        mesg->slot[u].name = H5MM_xstrdup(s);
        if (mesg->slot[u].name == NULL)
            HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, NULL, "string duplication failed");

        /* File offset */
        if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_size(f), p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        H5F_DECODE_LENGTH(f, p, mesg->slot[u].offset);

        /* Size */
        if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_size(f), p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        H5F_DECODE_LENGTH(f, p, mesg->slot[u].size);
    }

    if (H5HL_unprotect(heap) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to unprotect local heap");

    /* Set return value */
    ret_value = mesg;

done:
    if (ret_value == NULL) {
        if (mesg != NULL) {
            if (mesg->slot != NULL) {
                for (size_t u = 0; u < mesg->nused; u++)
                    H5MM_xfree(mesg->slot[u].name);
                H5MM_xfree(mesg->slot);
            }
            H5MM_xfree(mesg);
        }
        if (heap != NULL)
            if (H5HL_unprotect(heap) < 0)
                HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to unprotect local heap");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__efl_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__efl_encode
 *
 * Purpose:	Encodes a message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__efl_encode(H5F_t *f, bool H5_ATTR_UNUSED disable_shared, uint8_t *p, const void *_mesg)
{
    const H5O_efl_t *mesg = (const H5O_efl_t *)_mesg;
    size_t           u; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);
    assert(p);

    /* Version */
    *p++ = H5O_EFL_VERSION;

    /* Reserved */
    *p++ = 0;
    *p++ = 0;
    *p++ = 0;

    /* Number of slots */
    assert(mesg->nalloc > 0);
    UINT16ENCODE(p, mesg->nused); /*yes, twice*/
    assert(mesg->nused > 0 && mesg->nused <= mesg->nalloc);
    UINT16ENCODE(p, mesg->nused);

    /* Heap address */
    assert(H5_addr_defined(mesg->heap_addr));
    H5F_addr_encode(f, &p, mesg->heap_addr);

    /* Encode file list */
    for (u = 0; u < mesg->nused; u++) {
        /*
         * The name should have been added to the heap when the dataset was
         * created.
         */
        assert(mesg->slot[u].name_offset);
        H5F_ENCODE_LENGTH(f, p, mesg->slot[u].name_offset);
        H5F_ENCODE_LENGTH(f, p, (hsize_t)mesg->slot[u].offset);
        H5F_ENCODE_LENGTH(f, p, mesg->slot[u].size);
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__efl_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5O__efl_copy
 *
 * Purpose:	Copies a message from _MESG to _DEST, allocating _DEST if
 *		necessary.
 *
 * Return:	Success:	Ptr to _DEST
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__efl_copy(const void *_mesg, void *_dest)
{
    const H5O_efl_t *mesg = (const H5O_efl_t *)_mesg;
    H5O_efl_t       *dest = (H5O_efl_t *)_dest;
    size_t           u;                      /* Local index variable */
    bool             slot_allocated = false; /* Flag to indicate that dynamic allocation has begun */
    void            *ret_value      = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(mesg);

    /* Allocate destination message, if necessary */
    if (!dest && NULL == (dest = (H5O_efl_t *)H5MM_calloc(sizeof(H5O_efl_t))))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, NULL, "can't allocate efl message");

    /* copy */
    *dest = *mesg;

    /* Deep copy allocated information */
    if (dest->nalloc > 0) {
        if (NULL == (dest->slot = (H5O_efl_entry_t *)H5MM_calloc(dest->nalloc * sizeof(H5O_efl_entry_t))))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, NULL, "can't allocate efl message slots");
        slot_allocated = true;
        for (u = 0; u < mesg->nused; u++) {
            dest->slot[u] = mesg->slot[u];
            if (NULL == (dest->slot[u].name = H5MM_xstrdup(mesg->slot[u].name)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, NULL, "can't allocate efl message slot name");
        } /* end for */
    }     /* end if */

    /* Set return value */
    ret_value = dest;

done:
    if (NULL == ret_value) {
        if (slot_allocated) {
            for (u = 0; u < dest->nused; u++)
                if (dest->slot[u].name != NULL && dest->slot[u].name != mesg->slot[u].name)
                    dest->slot[u].name = (char *)H5MM_xfree(dest->slot[u].name);
            dest->slot = (H5O_efl_entry_t *)H5MM_xfree(dest->slot);
        } /* end if */
        if (NULL == _dest)
            dest = (H5O_efl_t *)H5MM_xfree(dest);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__efl_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5O__efl_size
 *
 * Purpose:	Returns the size of the raw message in bytes not counting the
 *		message type or size fields, but only the data fields.	This
 *		function doesn't take into account message alignment. This
 *		function doesn't count unused slots.
 *
 * Return:	Success:	Message data size in bytes.
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__efl_size(const H5F_t *f, bool H5_ATTR_UNUSED disable_shared, const void *_mesg)
{
    const H5O_efl_t *mesg      = (const H5O_efl_t *)_mesg;
    size_t           ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);

    ret_value = (size_t)H5F_SIZEOF_ADDR(f) +                /*heap address	*/
                2 +                                         /*slots allocated*/
                2 +                                         /*num slots used*/
                4 +                                         /*reserved	*/
                mesg->nused * ((size_t)H5F_SIZEOF_SIZE(f) + /*name offset	*/
                               (size_t)H5F_SIZEOF_SIZE(f) + /*file offset	*/
                               (size_t)H5F_SIZEOF_SIZE(f)); /*file size	*/

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__efl_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__efl_reset
 *
 * Purpose:	Frees internal pointers and resets the message to an
 *		initial state.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__efl_reset(void *_mesg)
{
    H5O_efl_t *mesg = (H5O_efl_t *)_mesg;
    size_t     u; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(mesg);

    /* reset */
    if (mesg->slot) {
        for (u = 0; u < mesg->nused; u++) {
            mesg->slot[u].name        = (char *)H5MM_xfree(mesg->slot[u].name);
            mesg->slot[u].name_offset = 0;
        } /* end for */
        mesg->slot = (H5O_efl_entry_t *)H5MM_xfree(mesg->slot);
    } /* end if */
    mesg->heap_addr = HADDR_UNDEF;
    mesg->nused = mesg->nalloc = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__efl_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5O_efl_total_size
 *
 * Purpose:	Return the total size of the external file list by summing
 *		the sizes of all of the files.
 *
 * Return:	Success:	Total reserved size for external data.
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5O_efl_total_size(H5O_efl_t *efl)
{
    hsize_t ret_value = 0, tmp;

    FUNC_ENTER_NOAPI(0)

    if (efl->nused > 0 && H5O_EFL_UNLIMITED == efl->slot[efl->nused - 1].size)
        ret_value = H5O_EFL_UNLIMITED;
    else {
        size_t u; /* Local index variable */

        for (u = 0; u < efl->nused; u++, ret_value = tmp) {
            tmp = ret_value + efl->slot[u].size;
            if (tmp <= ret_value)
                HGOTO_ERROR(H5E_EFL, H5E_OVERFLOW, 0, "total external storage size overflowed");
        } /* end for */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_efl_total_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__efl_copy_file
 *
 * Purpose:     Copies an efl message from _MESG to _DEST in file
 *
 * Return:      Success:        Ptr to _DEST
 *
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__efl_copy_file(H5F_t H5_ATTR_UNUSED *file_src, void *mesg_src, H5F_t *file_dst,
                   bool H5_ATTR_UNUSED *recompute_size, unsigned H5_ATTR_UNUSED *mesg_flags,
                   H5O_copy_t H5_ATTR_UNUSED *cpy_info, void H5_ATTR_UNUSED *_udata)
{
    H5O_efl_t *efl_src = (H5O_efl_t *)mesg_src;
    H5O_efl_t *efl_dst = NULL;
    H5HL_t    *heap    = NULL; /* Pointer to local heap for EFL file names */
    size_t     idx, size, name_offset, heap_size;
    void      *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(H5AC__COPIED_TAG)

    /* check args */
    assert(efl_src);
    assert(file_dst);

    /* Allocate space for the destination efl */
    if (NULL == (efl_dst = (H5O_efl_t *)H5MM_calloc(sizeof(H5O_efl_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the "top level" information */
    H5MM_memcpy(efl_dst, efl_src, sizeof(H5O_efl_t));

    /* Determine size needed for destination heap */
    heap_size = H5HL_ALIGN(1); /* "empty" name */
    for (idx = 0; idx < efl_src->nused; idx++)
        heap_size += H5HL_ALIGN(strlen(efl_src->slot[idx].name) + 1);

    /* Create name heap */
    if (H5HL_create(file_dst, heap_size, &efl_dst->heap_addr /*out*/) < 0)
        HGOTO_ERROR(H5E_EFL, H5E_CANTINIT, NULL, "can't create heap");

    /* Pin the heap down in memory */
    if (NULL == (heap = H5HL_protect(file_dst, efl_dst->heap_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_EFL, H5E_PROTECT, NULL, "unable to protect EFL file name heap");

    /* Insert "empty" name first */
    if (H5HL_insert(file_dst, heap, (size_t)1, "", &name_offset) < 0)
        HGOTO_ERROR(H5E_EFL, H5E_CANTINSERT, NULL, "can't insert file name into heap");
    assert(0 == name_offset);

    /* allocate array of external file entries */
    if (efl_src->nalloc > 0) {
        size = efl_src->nalloc * sizeof(H5O_efl_entry_t);
        if ((efl_dst->slot = (H5O_efl_entry_t *)H5MM_calloc(size)) == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* copy content from the source. Need to update later */
        H5MM_memcpy(efl_dst->slot, efl_src->slot, size);
    } /* end if */

    /* copy the name from the source */
    for (idx = 0; idx < efl_src->nused; idx++) {
        efl_dst->slot[idx].name = H5MM_xstrdup(efl_src->slot[idx].name);
        if (H5HL_insert(file_dst, heap, strlen(efl_dst->slot[idx].name) + 1, efl_dst->slot[idx].name,
                        &(efl_dst->slot[idx].name_offset)) < 0)
            HGOTO_ERROR(H5E_EFL, H5E_CANTINSERT, NULL, "can't insert file name into heap");
    }

    /* Set return value */
    ret_value = efl_dst;

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_EFL, H5E_PROTECT, NULL, "unable to unprotect EFL file name heap");
    if (!ret_value)
        if (efl_dst)
            H5MM_xfree(efl_dst);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O__efl_copy_file() */

/*-------------------------------------------------------------------------
 * Function:	H5O__efl_debug
 *
 * Purpose:	Prints debugging info for a message.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__efl_debug(H5F_t H5_ATTR_UNUSED *f, const void *_mesg, FILE *stream, int indent, int fwidth)
{
    const H5O_efl_t *mesg = (const H5O_efl_t *)_mesg;
    size_t           u;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(mesg);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Heap address:", mesg->heap_addr);

    fprintf(stream, "%*s%-*s %zu/%zu\n", indent, "", fwidth, "Slots used/allocated:", mesg->nused,
            mesg->nalloc);

    for (u = 0; u < mesg->nused; u++) {
        char buf[64];

        snprintf(buf, sizeof(buf), "File %zu", u);
        fprintf(stream, "%*s%s:\n", indent, "", buf);

        fprintf(stream, "%*s%-*s \"%s\"\n", indent + 3, "", MAX(fwidth - 3, 0), "Name:", mesg->slot[u].name);

        fprintf(stream, "%*s%-*s %zu\n", indent + 3, "", MAX(fwidth - 3, 0),
                "Name offset:", mesg->slot[u].name_offset);

        fprintf(stream, "%*s%-*s %" PRIdMAX "\n", indent + 3, "", MAX(fwidth - 3, 0),
                "Offset of data in file:", (intmax_t)(mesg->slot[u].offset));

        fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent + 3, "", MAX(fwidth - 3, 0),
                "Bytes reserved for data:", (mesg->slot[u].size));
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__efl_debug() */

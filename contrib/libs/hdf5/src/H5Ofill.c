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
 * Purpose:    The fill message indicates a bit pattern to use for
 *             uninitialized data points of a dataset.
 */

#include "H5Omodule.h" /* This source code file is part of the H5O module */

#include "H5private.h"   /* Generic Functions    */
#include "H5Dprivate.h"  /* Datasets                */
#include "H5Eprivate.h"  /* Error handling       */
#include "H5FLprivate.h" /* Free Lists           */
#include "H5Iprivate.h"  /* IDs                  */
#include "H5MMprivate.h" /* Memory management    */
#include "H5Opkg.h"      /* Object headers       */
#include "H5Pprivate.h"  /* Property lists       */
#include "H5Sprivate.h"  /* Dataspaces           */

static void  *H5O__fill_old_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                   size_t p_size, const uint8_t *p);
static herr_t H5O__fill_old_encode(H5F_t *f, uint8_t *p, const void *_mesg);
static size_t H5O__fill_old_size(const H5F_t *f, const void *_mesg);
static void  *H5O__fill_new_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                   size_t p_size, const uint8_t *p);
static herr_t H5O__fill_new_encode(H5F_t *f, uint8_t *p, const void *_mesg);
static size_t H5O__fill_new_size(const H5F_t *f, const void *_mesg);
static void  *H5O__fill_copy(const void *_mesg, void *_dest);
static herr_t H5O__fill_reset(void *_mesg);
static herr_t H5O__fill_free(void *_mesg);
static herr_t H5O__fill_pre_copy_file(H5F_t *file_src, const void *mesg_src, bool *deleted,
                                      const H5O_copy_t *cpy_info, void *udata);
static herr_t H5O__fill_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* Set up & include shared message "interface" info */
#define H5O_SHARED_TYPE        H5O_MSG_FILL
#define H5O_SHARED_DECODE      H5O__fill_shared_decode
#define H5O_SHARED_DECODE_REAL H5O__fill_old_decode
#define H5O_SHARED_ENCODE      H5O__fill_shared_encode
#define H5O_SHARED_ENCODE_REAL H5O__fill_old_encode
#define H5O_SHARED_SIZE        H5O__fill_shared_size
#define H5O_SHARED_SIZE_REAL   H5O__fill_old_size
#define H5O_SHARED_DELETE      H5O__fill_shared_delete
#undef H5O_SHARED_DELETE_REAL
#define H5O_SHARED_LINK H5O__fill_shared_link
#undef H5O_SHARED_LINK_REAL
#define H5O_SHARED_COPY_FILE H5O__fill_shared_copy_file
#undef H5O_SHARED_COPY_FILE_REAL
#define H5O_SHARED_POST_COPY_FILE H5O__fill_shared_post_copy_file
#undef H5O_SHARED_POST_COPY_FILE_REAL
#undef H5O_SHARED_POST_COPY_FILE_UPD
#define H5O_SHARED_DEBUG      H5O__fill_shared_debug
#define H5O_SHARED_DEBUG_REAL H5O__fill_debug
#include "H5Oshared.h" /* Shared Object Header Message Callbacks */

/* Set up & include shared message "interface" info */
/* (Kludgy 'undef's in order to re-include the H5Oshared.h header) */
#undef H5O_SHARED_TYPE
#define H5O_SHARED_TYPE H5O_MSG_FILL_NEW
#undef H5O_SHARED_DECODE
#define H5O_SHARED_DECODE H5O__fill_new_shared_decode
#undef H5O_SHARED_DECODE_REAL
#define H5O_SHARED_DECODE_REAL H5O__fill_new_decode
#undef H5O_SHARED_ENCODE
#define H5O_SHARED_ENCODE H5O__fill_new_shared_encode
#undef H5O_SHARED_ENCODE_REAL
#define H5O_SHARED_ENCODE_REAL H5O__fill_new_encode
#undef H5O_SHARED_SIZE
#define H5O_SHARED_SIZE H5O__fill_new_shared_size
#undef H5O_SHARED_SIZE_REAL
#define H5O_SHARED_SIZE_REAL H5O__fill_new_size
#undef H5O_SHARED_DELETE
#define H5O_SHARED_DELETE H5O__fill_new_shared_delete
#undef H5O_SHARED_DELETE_REAL
#undef H5O_SHARED_LINK
#define H5O_SHARED_LINK H5O__fill_new_shared_link
#undef H5O_SHARED_LINK_REAL
#undef H5O_SHARED_COPY_FILE
#define H5O_SHARED_COPY_FILE H5O__fill_new_shared_copy_file
#undef H5O_SHARED_COPY_FILE_REAL
#undef H5O_SHARED_POST_COPY_FILE
#define H5O_SHARED_POST_COPY_FILE H5O__fill_new_shared_post_copy_file
#undef H5O_SHARED_POST_COPY_FILE_REAL
#undef H5O_SHARED_POST_COPY_FILE_UPD
#undef H5O_SHARED_DEBUG
#define H5O_SHARED_DEBUG H5O__fill_new_shared_debug
#undef H5O_SHARED_DEBUG_REAL
#define H5O_SHARED_DEBUG_REAL H5O__fill_debug
#undef H5Oshared_H
#include "H5Oshared.h" /* Shared Object Header Message Callbacks */

/* This message derives from H5O message class, for old fill value before version 1.5 */
const H5O_msg_class_t H5O_MSG_FILL[1] = {{
    H5O_FILL_ID,                               /*message id number                         */
    "fill",                                    /*message name for debugging                */
    sizeof(H5O_fill_t),                        /*native message size                       */
    H5O_SHARE_IS_SHARABLE | H5O_SHARE_IN_OHDR, /* messages are shareable?   */
    H5O__fill_shared_decode,                   /*decode message                            */
    H5O__fill_shared_encode,                   /*encode message                            */
    H5O__fill_copy,                            /*copy the native value                     */
    H5O__fill_shared_size,                     /*raw message size                          */
    H5O__fill_reset,                           /*free internal memory                      */
    H5O__fill_free,                            /* free method                              */
    H5O__fill_shared_delete,                   /* file delete method                       */
    H5O__fill_shared_link,                     /* link method                              */
    NULL,                                      /* set share method                         */
    NULL,                                      /*can share method                          */
    H5O__fill_pre_copy_file,                   /* pre copy native value to file            */
    H5O__fill_shared_copy_file,                /* copy native value to file                */
    H5O__fill_shared_post_copy_file,           /* post copy native value to file      */
    NULL,                                      /* get creation index                       */
    NULL,                                      /* set creation index                       */
    H5O__fill_shared_debug                     /*debug the message                         */
}};

/* This message derives from H5O message class, for new fill value after version 1.4  */
const H5O_msg_class_t H5O_MSG_FILL_NEW[1] = {{
    H5O_FILL_NEW_ID,                           /*message id number                 */
    "fill_new",                                /*message name for debugging        */
    sizeof(H5O_fill_t),                        /*native message size               */
    H5O_SHARE_IS_SHARABLE | H5O_SHARE_IN_OHDR, /* messages are shareable?   */
    H5O__fill_new_shared_decode,               /*decode message                    */
    H5O__fill_new_shared_encode,               /*encode message                    */
    H5O__fill_copy,                            /*copy the native value             */
    H5O__fill_new_shared_size,                 /*raw message size                  */
    H5O__fill_reset,                           /*free internal memory              */
    H5O__fill_free,                            /* free method                      */
    H5O__fill_new_shared_delete,               /* file delete method               */
    H5O__fill_new_shared_link,                 /* link method                      */
    NULL,                                      /* set share method                 */
    NULL,                                      /*can share method                  */
    H5O__fill_pre_copy_file,                   /* pre copy native value to file    */
    H5O__fill_new_shared_copy_file,            /* copy native value to file        */
    H5O__fill_new_shared_post_copy_file,       /* post copy native value to file  */
    NULL,                                      /* get creation index               */
    NULL,                                      /* set creation index               */
    H5O__fill_new_shared_debug                 /*debug the message                 */
}};

/* Format version bounds for fill value */
const unsigned H5O_fill_ver_bounds[] = {
    H5O_FILL_VERSION_1,     /* H5F_LIBVER_EARLIEST */
    H5O_FILL_VERSION_3,     /* H5F_LIBVER_V18 */
    H5O_FILL_VERSION_3,     /* H5F_LIBVER_V110 */
    H5O_FILL_VERSION_3,     /* H5F_LIBVER_V112 */
    H5O_FILL_VERSION_LATEST /* H5F_LIBVER_LATEST */
};

/* Masks, shift values & flags for fill value message */
#define H5O_FILL_MASK_ALLOC_TIME      0x03
#define H5O_FILL_SHIFT_ALLOC_TIME     0
#define H5O_FILL_MASK_FILL_TIME       0x03
#define H5O_FILL_SHIFT_FILL_TIME      2
#define H5O_FILL_FLAG_UNDEFINED_VALUE 0x10
#define H5O_FILL_FLAG_HAVE_VALUE      0x20
#define H5O_FILL_FLAGS_ALL                                                                                   \
    (H5O_FILL_MASK_ALLOC_TIME | (H5O_FILL_MASK_FILL_TIME << H5O_FILL_SHIFT_FILL_TIME) |                      \
     H5O_FILL_FLAG_UNDEFINED_VALUE | H5O_FILL_FLAG_HAVE_VALUE)

/* Declare a free list to manage the H5O_fill_t struct */
H5FL_DEFINE(H5O_fill_t);

/* Declare extern the free list to manage blocks of type conversion data */
H5FL_BLK_EXTERN(type_conv);

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_new_decode
 *
 * Purpose:     Decode a new fill value message.  The new fill value
 *              message is fill value plus space allocation time and
 *              fill value writing time and whether fill value is defined.
 *
 * Return:      Success:    Pointer to new message in native struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__fill_new_decode(H5F_t H5_ATTR_UNUSED *f, H5O_t H5_ATTR_UNUSED *open_oh,
                     unsigned H5_ATTR_UNUSED mesg_flags, unsigned H5_ATTR_UNUSED *ioflags, size_t p_size,
                     const uint8_t *p)
{
    H5O_fill_t    *fill      = NULL;
    const uint8_t *p_end     = p + p_size - 1; /* End of the p buffer */
    void          *ret_value = NULL;           /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (NULL == (fill = H5FL_CALLOC(H5O_fill_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill value message");

    /* Version */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    fill->version = *p++;
    if (fill->version < H5O_FILL_VERSION_1 || fill->version > H5O_FILL_VERSION_LATEST)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "bad version number for fill value message");

    /* Decode each version */
    if (fill->version < H5O_FILL_VERSION_3) {

        /* Versions 1 & 2 */

        /* Buffer size check for the next three bytes */
        if (H5_IS_BUFFER_OVERFLOW(p, 3, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

        /* Space allocation time */
        fill->alloc_time = (H5D_alloc_time_t)*p++;

        /* Fill value write time */
        fill->fill_time = (H5D_fill_time_t)*p++;

        /* Whether fill value is defined */
        fill->fill_defined = *p++;

        /* Only decode fill value information if one is defined */
        if (fill->fill_defined) {

            if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            INT32DECODE(p, fill->size);

            if (fill->size > 0) {
                H5_CHECK_OVERFLOW(fill->size, ssize_t, size_t);

                if (H5_IS_BUFFER_OVERFLOW(p, fill->size, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

                if (NULL == (fill->buf = H5MM_malloc((size_t)fill->size)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill value");
                H5MM_memcpy(fill->buf, p, (size_t)fill->size);
            }
        }
        else
            fill->size = -1;
    }
    else {

        /* Version 3 */

        unsigned flags; /* Status flags */

        /* Flags */
        if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        flags = *p++;

        /* Check for unknown flags */
        if (flags & (unsigned)~H5O_FILL_FLAGS_ALL)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "unknown flag for fill value message");

        /* Space allocation time */
        fill->alloc_time =
            (H5D_alloc_time_t)((flags >> H5O_FILL_SHIFT_ALLOC_TIME) & H5O_FILL_MASK_ALLOC_TIME);

        /* Fill value write time */
        fill->fill_time = (H5D_fill_time_t)((flags >> H5O_FILL_SHIFT_FILL_TIME) & H5O_FILL_MASK_FILL_TIME);

        /* Check for undefined fill value */
        if (flags & H5O_FILL_FLAG_UNDEFINED_VALUE) {

            if (flags & H5O_FILL_FLAG_HAVE_VALUE)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "have value and undefined value flags both set");

            /* Set value for "undefined" fill value */
            fill->size = -1;
        }
        else if (flags & H5O_FILL_FLAG_HAVE_VALUE) {
            /* Fill value size */
            if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT32DECODE(p, fill->size);

            /* Fill value */
            H5_CHECK_OVERFLOW(fill->size, ssize_t, size_t);

            if (H5_IS_BUFFER_OVERFLOW(p, fill->size, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

            if (NULL == (fill->buf = H5MM_malloc((size_t)fill->size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill value");
            H5MM_memcpy(fill->buf, p, (size_t)fill->size);

            /* Set the "defined" flag */
            fill->fill_defined = true;
        }
        else
            /* Set the "defined" flag */
            fill->fill_defined = true;
    }

    /* Set return value */
    ret_value = (void *)fill;

done:
    if (!ret_value && fill) {
        H5MM_xfree(fill->buf);
        fill = H5FL_FREE(H5O_fill_t, fill);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__fill_new_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_old_decode
 *
 * Purpose:     Decode an old fill value message
 *
 * Return:      Success:    Pointer to new message in native struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__fill_old_decode(H5F_t *f, H5O_t *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                     unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    H5O_fill_t    *fill      = NULL; /* Decoded fill value message */
    htri_t         exists    = false;
    H5T_t         *dt        = NULL;
    const uint8_t *p_end     = p + p_size - 1; /* End of the p buffer */
    void          *ret_value = NULL;           /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (NULL == (fill = H5FL_CALLOC(H5O_fill_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill value message");

    /* Set non-zero default fields */
    fill->version    = H5O_FILL_VERSION_2;
    fill->alloc_time = H5D_ALLOC_TIME_LATE;
    fill->fill_time  = H5D_FILL_TIME_IFSET;

    /* Fill value size */
    if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    UINT32DECODE(p, fill->size);

    /* Only decode the fill value itself if there is one */
    if (fill->size > 0) {
        H5_CHECK_OVERFLOW(fill->size, ssize_t, size_t);

        /* Ensure that fill size doesn't exceed buffer size, due to possible data corruption */
        if (H5_IS_BUFFER_OVERFLOW(p, fill->size, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

        /* Get the datatype message  */
        if ((exists = H5O_msg_exists_oh(open_oh, H5O_DTYPE_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, NULL, "unable to read object header");
        if (exists) {
            if (NULL == (dt = (H5T_t *)H5O_msg_read_oh(f, open_oh, H5O_DTYPE_ID, NULL)))
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "can't read DTYPE message");
            /* Verify size */
            if (fill->size != (ssize_t)H5T_GET_SIZE(dt))
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, NULL, "inconsistent fill value size");
        }

        if (NULL == (fill->buf = H5MM_malloc((size_t)fill->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill value");
        H5MM_memcpy(fill->buf, p, (size_t)fill->size);
        fill->fill_defined = true;
    }
    else
        fill->size = -1;

    /* Set return value */
    ret_value = (void *)fill;

done:
    if (dt)
        H5O_msg_free(H5O_DTYPE_ID, dt);

    if (!ret_value && fill) {
        H5MM_xfree(fill->buf);
        H5FL_FREE(H5O_fill_t, fill);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__fill_old_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_new_encode
 *
 * Purpose:    Encode a new fill value message.  The new fill value
 *          message is fill value plus space allocation time and
 *          fill value writing time and whether fill value is defined.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__fill_new_encode(H5F_t H5_ATTR_UNUSED *f, uint8_t *p, const void *_fill)
{
    const H5O_fill_t *fill = (const H5O_fill_t *)_fill;

    FUNC_ENTER_PACKAGE_NOERR

    assert(f);
    assert(p);
    assert(fill && NULL == fill->type);

    /* Version */
    *p++ = (uint8_t)fill->version;

    if (fill->version < H5O_FILL_VERSION_3) {
        /* Space allocation time */
        *p++ = (uint8_t)fill->alloc_time;

        /* Fill value writing time */
        *p++ = (uint8_t)fill->fill_time;

        /* Whether fill value is defined */
        *p++ = (uint8_t)fill->fill_defined;

        /* Only write out the size and fill value if it is defined */
        if (fill->fill_defined) {
            UINT32ENCODE(p, fill->size);
            if (fill->size > 0)
                if (fill->buf) {
                    H5_CHECK_OVERFLOW(fill->size, ssize_t, size_t);
                    H5MM_memcpy(p, fill->buf, (size_t)fill->size);
                } /* end if */
        }         /* end if */
    }             /* end if */
    else {
        uint8_t flags = 0; /* Fill value setting flags */

        /* Encode space allocation time */
        assert(fill->alloc_time == (H5O_FILL_MASK_ALLOC_TIME & fill->alloc_time));
        flags =
            (uint8_t)(flags | ((H5O_FILL_MASK_ALLOC_TIME & fill->alloc_time) << H5O_FILL_SHIFT_ALLOC_TIME));

        /* Encode fill value writing time */
        assert(fill->fill_time == (H5O_FILL_MASK_FILL_TIME & fill->fill_time));
        flags = (uint8_t)(flags | ((H5O_FILL_MASK_FILL_TIME & fill->fill_time) << H5O_FILL_SHIFT_FILL_TIME));

        /* Check if we need to encode a fill value size */
        if (fill->size < 0) {
            /* Indicate that the fill value has been "undefined" by the user */
            flags |= H5O_FILL_FLAG_UNDEFINED_VALUE;

            /* Flags */
            *p++ = (uint8_t)flags;

            /* Sanity check */
            assert(!fill->buf);
        } /* end if */
        else if (fill->size > 0) {
            /* Indicate that a fill value size is present */
            flags |= H5O_FILL_FLAG_HAVE_VALUE;

            /* Flags */
            *p++ = (uint8_t)flags;

            /* Encode the size of fill value */
            INT32ENCODE(p, fill->size);

            /* Encode the fill value */
            assert(fill->buf);
            H5_CHECK_OVERFLOW(fill->size, ssize_t, size_t);
            H5MM_memcpy(p, fill->buf, (size_t)fill->size);
        } /* end if */
        else {
            /* Flags */
            *p++ = (uint8_t)flags;

            /* Sanity check */
            assert(!fill->buf);
        } /* end else */
    }     /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__fill_new_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_old_encode
 *
 * Purpose:     Encode an old fill value message.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__fill_old_encode(H5F_t H5_ATTR_UNUSED *f, uint8_t *p, const void *_fill)
{
    const H5O_fill_t *fill = (const H5O_fill_t *)_fill;

    FUNC_ENTER_PACKAGE_NOERR

    assert(f);
    assert(p);
    assert(fill && NULL == fill->type);

    UINT32ENCODE(p, fill->size);
    if (fill->buf)
        H5MM_memcpy(p, fill->buf, (size_t)fill->size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__fill_old_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_copy
 *
 * Purpose:    Copies a message from _MESG to _DEST, allocating _DEST if
 *        necessary.  The new fill value message is fill value plus
 *        space allocation time and fill value writing time and
 *        whether fill value is defined.
 *
 * Return:    Success:    Ptr to _DEST
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5O__fill_copy(const void *_src, void *_dst)
{
    const H5O_fill_t *src       = (const H5O_fill_t *)_src;
    H5O_fill_t       *dst       = (H5O_fill_t *)_dst;
    void             *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(src);

    if (!dst && NULL == (dst = H5FL_MALLOC(H5O_fill_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill message");

    /* Shallow copy basic fields */
    *dst = *src;

    /* Copy data type of fill value */
    if (src->type) {
        if (NULL == (dst->type = H5T_copy(src->type, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, NULL, "can't copy datatype");
    } /* end if */
    else
        dst->type = NULL;

    /* Copy fill value and its size */
    if (src->buf) {
        H5_CHECK_OVERFLOW(src->size, ssize_t, size_t);
        if (NULL == (dst->buf = H5MM_malloc((size_t)src->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for fill value");
        H5MM_memcpy(dst->buf, src->buf, (size_t)src->size);

        /* Check for needing to convert/copy fill value */
        if (src->type) {
            H5T_path_t *tpath; /* Conversion information */

            /* Set up type conversion function */
            if (NULL == (tpath = H5T_path_find(src->type, dst->type)))
                HGOTO_ERROR(H5E_OHDR, H5E_UNSUPPORTED, NULL,
                            "unable to convert between src and dst data types");

            /* If necessary, convert fill value datatypes (which copies VL components, etc.) */
            if (!H5T_path_noop(tpath)) {
                hid_t    dst_id, src_id; /* Source & destination datatypes for type conversion */
                uint8_t *bkg_buf = NULL; /* Background conversion buffer */
                size_t   bkg_size;       /* Size of background buffer */

                /* Wrap copies of types to convert */
                dst_id = H5I_register(H5I_DATATYPE, H5T_copy(dst->type, H5T_COPY_TRANSIENT), false);
                if (dst_id < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "unable to copy/register datatype");
                src_id = H5I_register(H5I_DATATYPE, H5T_copy(src->type, H5T_COPY_ALL), false);
                if (src_id < 0) {
                    H5I_dec_ref(dst_id);
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "unable to copy/register datatype");
                } /* end if */

                /* Allocate a background buffer */
                bkg_size = MAX(H5T_get_size(dst->type), H5T_get_size(src->type));
                if (H5T_path_bkg(tpath) && NULL == (bkg_buf = H5FL_BLK_CALLOC(type_conv, bkg_size))) {
                    H5I_dec_ref(src_id);
                    H5I_dec_ref(dst_id);
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
                } /* end if */

                /* Convert fill value */
                if (H5T_convert(tpath, src_id, dst_id, (size_t)1, (size_t)0, (size_t)0, dst->buf, bkg_buf) <
                    0) {
                    H5I_dec_ref(src_id);
                    H5I_dec_ref(dst_id);
                    if (bkg_buf)
                        bkg_buf = H5FL_BLK_FREE(type_conv, bkg_buf);
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTCONVERT, NULL, "datatype conversion failed");
                } /* end if */

                /* Release the background buffer */
                H5I_dec_ref(src_id);
                H5I_dec_ref(dst_id);
                if (bkg_buf)
                    bkg_buf = H5FL_BLK_FREE(type_conv, bkg_buf);
            } /* end if */
        }     /* end if */
    }         /* end if */
    else
        dst->buf = NULL;

    /* Set return value */
    ret_value = dst;

done:
    if (!ret_value && dst) {
        if (dst->buf)
            H5MM_xfree(dst->buf);
        if (dst->type)
            (void)H5T_close_real(dst->type);
        if (!_dst)
            dst = H5FL_FREE(H5O_fill_t, dst);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__fill_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_new_size
 *
 * Purpose:    Returns the size of the raw message in bytes not counting the
 *          message type or size fields, but only the data fields.  This
 *          function doesn't take into account alignment.  The new fill
 *          value message is fill value plus space allocation time and
 *          fill value writing time and whether fill value is defined.
 *
 * Return:    Success:    Message data size in bytes w/o alignment.
 *          Failure:    0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__fill_new_size(const H5F_t H5_ATTR_UNUSED *f, const void *_fill)
{
    const H5O_fill_t *fill      = (const H5O_fill_t *)_fill;
    size_t            ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(f);
    assert(fill);

    /* Determine size for different versions */
    if (fill->version < H5O_FILL_VERSION_3) {
        ret_value = 1 + /* Version number        */
                    1 + /* Space allocation time */
                    1 + /* Fill value write time */
                    1;  /* Fill value defined    */
        if (fill->fill_defined)
            ret_value += 4 +                                        /* Fill value size       */
                         (fill->size > 0 ? (size_t)fill->size : 0); /* Size of fill value     */
    }                                                               /* end if */
    else {
        ret_value = 1 + /* Version number        */
                    1;  /* Status flags          */
        if (fill->size > 0)
            ret_value += 4 +                 /* Fill value size       */
                         (size_t)fill->size; /* Size of fill value    */
    }                                        /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__fill_new_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_old_size
 *
 * Purpose:     Returns the size of the raw message in bytes not counting the
 *              message type or size fields, but only the data fields.  This
 *              function doesn't take into account alignment.
 *
 * Return:      Success:        Message data size in bytes w/o alignment.
 *              Failure:        0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5O__fill_old_size(const H5F_t H5_ATTR_UNUSED *f, const void *_fill)
{
    const H5O_fill_t *fill = (const H5O_fill_t *)_fill;

    FUNC_ENTER_PACKAGE_NOERR

    assert(fill);

    FUNC_LEAVE_NOAPI(4 + (size_t)fill->size)
} /* end H5O__fill_old_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O_fill_reset_dyn
 *
 * Purpose:    Resets dynamic fill value fields
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_fill_reset_dyn(H5O_fill_t *fill)
{
    hid_t  fill_type_id = -1;      /* Datatype ID for fill value datatype when reclaiming VL fill values */
    herr_t ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(fill);

    if (fill->buf) {
        if (fill->type && H5T_detect_class(fill->type, H5T_VLEN, false) > 0) {
            H5T_t *fill_type;  /* Copy of fill value datatype */
            H5S_t *fill_space; /* Scalar dataspace for fill value element */

            /* Copy the fill value datatype and get an ID for it */
            if (NULL == (fill_type = H5T_copy(fill->type, H5T_COPY_TRANSIENT)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to copy fill value datatype");
            if ((fill_type_id = H5I_register(H5I_DATATYPE, fill_type, false)) < 0) {
                (void)H5T_close_real(fill_type);
                HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, FAIL, "unable to register fill value datatype");
            } /* end if */

            /* Create a scalar dataspace for the fill value element */
            if (NULL == (fill_space = H5S_create(H5S_SCALAR)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCREATE, FAIL, "can't create scalar dataspace");

            /* Reclaim any variable length components of the fill value */
            if (H5T_reclaim(fill_type_id, fill_space, fill->buf) < 0) {
                H5S_close(fill_space);
                HGOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "unable to reclaim variable-length fill value data");
            } /* end if */

            /* Release the scalar fill value dataspace */
            H5S_close(fill_space);
        } /* end if */

        /* Release the fill value buffer now */
        fill->buf = H5MM_xfree(fill->buf);
    } /* end if */
    fill->size = 0;
    if (fill->type) {
        (void)H5T_close_real(fill->type);
        fill->type = NULL;
    } /* end if */

done:
    if (fill_type_id > 0 && H5I_dec_ref(fill_type_id) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to decrement ref count for temp ID");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_fill_reset_dyn() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_reset
 *
 * Purpose:    Resets a message to an initial state.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__fill_reset(void *_fill)
{
    H5O_fill_t *fill = (H5O_fill_t *)_fill;

    FUNC_ENTER_PACKAGE_NOERR

    assert(fill);

    /* Reset dynamic fields */
    H5O_fill_reset_dyn(fill);

    /* Reset value fields */
    fill->alloc_time   = H5D_ALLOC_TIME_LATE;
    fill->fill_time    = H5D_FILL_TIME_IFSET;
    fill->fill_defined = false;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__fill_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_free
 *
 * Purpose:    Frees the message
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__fill_free(void *fill)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(fill);

    fill = H5FL_FREE(H5O_fill_t, fill);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__fill_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_pre_copy_file
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
H5O__fill_pre_copy_file(H5F_t H5_ATTR_UNUSED *file_src, const void *mesg_src, bool H5_ATTR_UNUSED *deleted,
                        const H5O_copy_t *cpy_info, void H5_ATTR_UNUSED *udata)
{
    const H5O_fill_t *fill_src  = (const H5O_fill_t *)mesg_src; /* Source fill value */
    herr_t            ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(cpy_info);
    assert(cpy_info->file_dst);

    /* Check to ensure that the version of the message to be copied does not exceed
       the message version allowed by the destination file's high bound */
    if (fill_src->version > H5O_fill_ver_bounds[H5F_HIGH_BOUND(cpy_info->file_dst)])
        HGOTO_ERROR(H5E_OHDR, H5E_BADRANGE, FAIL, "fill value message version out of bounds");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__fill_pre_copy_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O__fill_debug
 *
 * Purpose:    Prints debugging info for the message.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__fill_debug(H5F_t H5_ATTR_UNUSED *f, const void *_fill, FILE *stream, int indent, int fwidth)
{
    const H5O_fill_t *fill = (const H5O_fill_t *)_fill;
    H5D_fill_value_t  fill_status; /* Whether the fill value is defined */

    FUNC_ENTER_PACKAGE_NOERR

    assert(f);
    assert(fill);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Space Allocation Time:");
    switch (fill->alloc_time) {
        case H5D_ALLOC_TIME_EARLY:
            fprintf(stream, "Early\n");
            break;

        case H5D_ALLOC_TIME_LATE:
            fprintf(stream, "Late\n");
            break;

        case H5D_ALLOC_TIME_INCR:
            fprintf(stream, "Incremental\n");
            break;

        case H5D_ALLOC_TIME_DEFAULT:
        case H5D_ALLOC_TIME_ERROR:
        default:
            fprintf(stream, "Unknown!\n");
            break;
    } /* end switch */
    fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Fill Time:");
    switch (fill->fill_time) {
        case H5D_FILL_TIME_ALLOC:
            fprintf(stream, "On Allocation\n");
            break;

        case H5D_FILL_TIME_NEVER:
            fprintf(stream, "Never\n");
            break;

        case H5D_FILL_TIME_IFSET:
            fprintf(stream, "If Set\n");
            break;

        case H5D_FILL_TIME_ERROR:
        default:
            fprintf(stream, "Unknown!\n");
            break;

    } /* end switch */
    fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Fill Value Defined:");
    H5P_is_fill_value_defined((const H5O_fill_t *)fill, &fill_status);
    switch (fill_status) {
        case H5D_FILL_VALUE_UNDEFINED:
            fprintf(stream, "Undefined\n");
            break;

        case H5D_FILL_VALUE_DEFAULT:
            fprintf(stream, "Default\n");
            break;

        case H5D_FILL_VALUE_USER_DEFINED:
            fprintf(stream, "User Defined\n");
            break;

        case H5D_FILL_VALUE_ERROR:
        default:
            fprintf(stream, "Unknown!\n");
            break;

    } /* end switch */
    fprintf(stream, "%*s%-*s %zd\n", indent, "", fwidth, "Size:", fill->size);
    fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Data type:");
    if (fill->type) {
        H5T_debug(fill->type, stream);
        fprintf(stream, "\n");
    } /* end if */
    else
        fprintf(stream, "<dataset type>\n");

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__fill_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5O_fill_convert
 *
 * Purpose:    Convert a fill value from whatever data type it currently has
 *          to the specified dataset type.  The `type' field of the fill
 *          value struct will be set to NULL to indicate that it has the
 *          same type as the dataset.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_fill_convert(H5O_fill_t *fill, H5T_t *dset_type, bool *fill_changed)
{
    H5T_path_t *tpath;                    /* Type conversion info    */
    void       *buf = NULL, *bkg = NULL;  /* Conversion buffers    */
    hid_t       src_id = -1, dst_id = -1; /* Datatype identifiers    */
    herr_t      ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(fill);
    assert(dset_type);
    assert(fill_changed);

    /* No-op cases */
    if (!fill->buf || !fill->type || 0 == H5T_cmp(fill->type, dset_type, false)) {
        /* Don't need datatype for fill value */
        if (fill->type)
            (void)H5T_close_real(fill->type);
        fill->type = NULL;

        /* Note that the fill value info has changed */
        *fill_changed = true;

        HGOTO_DONE(SUCCEED);
    } /* end if */

    /*
     * Can we convert between source and destination data types?
     */
    if (NULL == (tpath = H5T_path_find(fill->type, dset_type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to convert between src and dst datatypes");

    /* Don't bother doing anything if there will be no actual conversion */
    if (!H5T_path_noop(tpath)) {
        size_t fill_type_size;

        if ((src_id = H5I_register(H5I_DATATYPE, H5T_copy(fill->type, H5T_COPY_ALL), false)) < 0 ||
            (dst_id = H5I_register(H5I_DATATYPE, H5T_copy(dset_type, H5T_COPY_ALL), false)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to copy/register data type");

        /*
         * Datatype conversions are always done in place, so we need a buffer
         * that is large enough for both source and destination.
         */
        fill_type_size = H5T_get_size(fill->type);

        if (NULL == (buf = H5MM_malloc(MAX(fill_type_size, H5T_get_size(dset_type)))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for type conversion");
        H5MM_memcpy(buf, fill->buf, fill_type_size);

        /* Use CALLOC here to clear the buffer in case later the library thinks there's
         * data in the background. */
        if (H5T_path_bkg(tpath) && NULL == (bkg = H5MM_calloc(H5T_get_size(dset_type))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for type conversion");

        /* Do the conversion */
        if (H5T_convert(tpath, src_id, dst_id, (size_t)1, (size_t)0, (size_t)0, buf, bkg) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "datatype conversion failed");

        /* Update the fill message */
        H5T_vlen_reclaim_elmt(fill->buf, fill->type);
        H5MM_xfree(fill->buf);
        fill->buf = buf;

        (void)H5T_close_real(fill->type);
        fill->type = NULL;
        H5_CHECKED_ASSIGN(fill->size, ssize_t, H5T_get_size(dset_type), size_t);

        /* Note that the fill value info has changed */
        *fill_changed = true;
    } /* end if */

done:
    if (src_id >= 0 && H5I_dec_ref(src_id) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to decrement ref count for temp ID");
    if (dst_id >= 0 && H5I_dec_ref(dst_id) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to decrement ref count for temp ID");
    if (bkg)
        H5MM_xfree(bkg);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_fill_convert() */

/*-------------------------------------------------------------------------
 * Function:    H5O_fill_set_version
 *
 * Purpose:     Set the version to encode a fill value with.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_fill_set_version(H5F_t *f, H5O_fill_t *fill)
{
    unsigned version;             /* Message version */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(fill);

    /* Upgrade to the version indicated by the file's low bound if higher */
    version = MAX(fill->version, H5O_fill_ver_bounds[H5F_LOW_BOUND(f)]);

    /* Version bounds check */
    if (version > H5O_fill_ver_bounds[H5F_HIGH_BOUND(f)])
        HGOTO_ERROR(H5E_OHDR, H5E_BADRANGE, FAIL, "Filter pipeline version out of bounds");

    /* Set the message version */
    fill->version = version;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_fill_set_version() */

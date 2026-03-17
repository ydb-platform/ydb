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
#define H5S_FRIEND     /*prevent warning from including H5Spkg.h */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Dprivate.h"  /* Datasets				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Gprivate.h"  /* Groups			  	*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Opkg.h"      /* Object headers		  	*/
#include "H5Spkg.h"      /* Dataspaces 				*/

/* PRIVATE PROTOTYPES */
static void  *H5O__sdspace_decode(H5F_t *f, H5O_t *open_oh, unsigned mesg_flags, unsigned *ioflags,
                                  size_t p_size, const uint8_t *p);
static herr_t H5O__sdspace_encode(H5F_t *f, uint8_t *p, const void *_mesg);
static void  *H5O__sdspace_copy(const void *_mesg, void *_dest);
static size_t H5O__sdspace_size(const H5F_t *f, const void *_mesg);
static herr_t H5O__sdspace_reset(void *_mesg);
static herr_t H5O__sdspace_free(void *_mesg);
static herr_t H5O__sdspace_pre_copy_file(H5F_t *file_src, const void *mesg_src, bool *deleted,
                                         const H5O_copy_t *cpy_info, void *_udata);
static herr_t H5O__sdspace_debug(H5F_t *f, const void *_mesg, FILE *stream, int indent, int fwidth);

/* Set up & include shared message "interface" info */
#define H5O_SHARED_TYPE        H5O_MSG_SDSPACE
#define H5O_SHARED_DECODE      H5O__sdspace_shared_decode
#define H5O_SHARED_DECODE_REAL H5O__sdspace_decode
#define H5O_SHARED_ENCODE      H5O__sdspace_shared_encode
#define H5O_SHARED_ENCODE_REAL H5O__sdspace_encode
#define H5O_SHARED_SIZE        H5O__sdspace_shared_size
#define H5O_SHARED_SIZE_REAL   H5O__sdspace_size
#define H5O_SHARED_DELETE      H5O__sdspace_shared_delete
#undef H5O_SHARED_DELETE_REAL
#define H5O_SHARED_LINK H5O__sdspace_shared_link
#undef H5O_SHARED_LINK_REAL
#define H5O_SHARED_COPY_FILE H5O__sdspace_shared_copy_file
#undef H5O_SHARED_COPY_FILE_REAL
#define H5O_SHARED_POST_COPY_FILE H5O__sdspace_shared_post_copy_file
#undef H5O_SHARED_POST_COPY_FILE_REAL
#undef H5O_SHARED_POST_COPY_FILE_UPD
#define H5O_SHARED_DEBUG      H5O__sdspace_shared_debug
#define H5O_SHARED_DEBUG_REAL H5O__sdspace_debug
#include "H5Oshared.h" /* Shared Object Header Message Callbacks */

/* This message derives from H5O message class */
const H5O_msg_class_t H5O_MSG_SDSPACE[1] = {{
    H5O_SDSPACE_ID,                            /* message id number		    	*/
    "dataspace",                               /* message name for debugging	   	*/
    sizeof(H5S_extent_t),                      /* native message size		    	*/
    H5O_SHARE_IS_SHARABLE | H5O_SHARE_IN_OHDR, /* messages are shareable? */
    H5O__sdspace_shared_decode,                /* decode message			*/
    H5O__sdspace_shared_encode,                /* encode message			*/
    H5O__sdspace_copy,                         /* copy the native value		*/
    H5O__sdspace_shared_size,                  /* size of symbol table entry	    	*/
    H5O__sdspace_reset,                        /* default reset method		    	*/
    H5O__sdspace_free,                         /* free method				*/
    H5O__sdspace_shared_delete,                /* file delete method		        */
    H5O__sdspace_shared_link,                  /* link method			        */
    NULL,                                      /* set share method		        */
    NULL,                                      /*can share method		        */
    H5O__sdspace_pre_copy_file,                /* pre copy native value to file        */
    H5O__sdspace_shared_copy_file,             /* copy native value to file          */
    H5O__sdspace_shared_post_copy_file,        /* post copy native value to file*/
    NULL,                                      /* get creation index		        */
    NULL,                                      /* set creation index		        */
    H5O__sdspace_shared_debug                  /* debug the message		    	*/
}};

/* Declare external the free list for H5S_extent_t's */
H5FL_EXTERN(H5S_extent_t);

/* Declare external the free list for hsize_t arrays */
H5FL_ARR_EXTERN(hsize_t);

/*--------------------------------------------------------------------------
 NAME
    H5O__sdspace_decode
 PURPOSE
    Decode a simple dimensionality message and return a pointer to a memory
        struct with the decoded information
 USAGE
    void *H5O__sdspace_decode(f, mesg_flags, p)
        H5F_t *f;	        IN: pointer to the HDF5 file struct
        unsigned mesg_flags;    IN: Message flags to influence decoding
        const uint8 *p;		IN: the raw information buffer
 RETURNS
    Pointer to the new message in native order on success, NULL on failure
 DESCRIPTION
        This function decodes the "raw" disk form of a simple dimensionality
    message into a struct in memory native format.  The struct is allocated
    within this function using malloc() and is returned to the caller.
--------------------------------------------------------------------------*/
static void *
H5O__sdspace_decode(H5F_t *f, H5O_t H5_ATTR_UNUSED *open_oh, unsigned H5_ATTR_UNUSED mesg_flags,
                    unsigned H5_ATTR_UNUSED *ioflags, size_t p_size, const uint8_t *p)
{
    const uint8_t *p_end = p + p_size - 1; /* End of the p buffer */
    H5S_extent_t  *sdim  = NULL;           /* New extent dimensionality structure */
    unsigned       flags, version;
    unsigned       i;
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(p);

    if (NULL == (sdim = H5FL_CALLOC(H5S_extent_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "dataspace structure allocation failed");
    sdim->type = H5S_NO_CLASS;

    /* Check version */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    version = *p++;

    if (version < H5O_SDSPACE_VERSION_1 || version > H5O_SDSPACE_VERSION_2)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "wrong version number in dataspace message");
    sdim->version = version;

    /* Get rank */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    sdim->rank = *p++;

    if (sdim->rank > H5S_MAX_RANK)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "simple dataspace dimensionality is too large");

    /* Get dataspace flags for later */
    if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    flags = *p++;

    /* Get or determine the type of the extent */
    if (version >= H5O_SDSPACE_VERSION_2) {
        if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        sdim->type = (H5S_class_t)*p++;

        if (sdim->type != H5S_SIMPLE && sdim->rank > 0)
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "invalid rank for scalar or NULL dataspace");
    }
    else {
        /* Set the dataspace type to be simple or scalar as appropriate
         * (version 1 does not allow H5S_NULL)
         */
        if (sdim->rank > 0)
            sdim->type = H5S_SIMPLE;
        else
            sdim->type = H5S_SCALAR;

        /* Increment past reserved byte */
        if (H5_IS_BUFFER_OVERFLOW(p, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        p++;
    }

    /* Version 1 has 4 reserved bytes */
    if (version == H5O_SDSPACE_VERSION_1) {
        if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
        p += 4;
    }

    /* Decode dimension sizes */
    if (sdim->rank > 0) {

        /* Sizes */

        /* Check that we have space to decode sdim->rank values */
        if (H5_IS_BUFFER_OVERFLOW(p, (H5F_sizeof_size(f) * sdim->rank), p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");

        if (NULL == (sdim->size = (hsize_t *)H5FL_ARR_MALLOC(hsize_t, (size_t)sdim->rank)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "memory allocation failed");
        for (i = 0; i < sdim->rank; i++)
            H5F_DECODE_LENGTH(f, p, sdim->size[i]);

        /* Max sizes */

        if (flags & H5S_VALID_MAX) {
            if (NULL == (sdim->max = (hsize_t *)H5FL_ARR_MALLOC(hsize_t, (size_t)sdim->rank)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "memory allocation failed");

            /* Check that we have space to decode sdim->rank values */
            if (H5_IS_BUFFER_OVERFLOW(p, (H5F_sizeof_size(f) * sdim->rank), p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            for (i = 0; i < sdim->rank; i++)
                H5F_DECODE_LENGTH(f, p, sdim->max[i]);
        }

        /* NOTE: The version 1 permutation indexes were never implemented so
         *       there is nothing to decode.
         */
    }

    /* Compute the number of elements in the extent */
    if (sdim->type == H5S_NULL)
        sdim->nelem = 0;
    else {
        for (i = 0, sdim->nelem = 1; i < sdim->rank; i++)
            sdim->nelem *= sdim->size[i];
    }

    /* Set return value */
    ret_value = (void *)sdim;

done:
    if (!ret_value && sdim) {
        H5S__extent_release(sdim);
        H5FL_FREE(H5S_extent_t, sdim);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__sdspace_decode() */

/*--------------------------------------------------------------------------
 NAME
    H5O__sdspace_encode
 PURPOSE
    Encode a simple dimensionality message
 USAGE
    herr_t H5O__sdspace_encode(f, raw_size, p, mesg)
        H5F_t *f;	        IN: pointer to the HDF5 file struct
        size_t raw_size;	IN: size of the raw information buffer
        const uint8 *p;		IN: the raw information buffer
        const void *mesg;	IN: Pointer to the extent dimensionality struct
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
        This function encodes the native memory form of the simple
    dimensionality message in the "raw" disk form.

--------------------------------------------------------------------------*/
static herr_t
H5O__sdspace_encode(H5F_t *f, uint8_t *p, const void *_mesg)
{
    const H5S_extent_t *sdim  = (const H5S_extent_t *)_mesg;
    unsigned            flags = 0;
    unsigned            u; /* Local counting variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(p);
    assert(sdim);

    /* Version */
    assert(sdim->version > 0);
    assert(sdim->type != H5S_NULL || sdim->version >= H5O_SDSPACE_VERSION_2);
    *p++ = (uint8_t)sdim->version;

    /* Rank */
    *p++ = (uint8_t)sdim->rank;

    /* Flags */
    if (sdim->max)
        flags |= H5S_VALID_MAX;
    *p++ = (uint8_t)flags;

    /* Dataspace type */
    if (sdim->version > H5O_SDSPACE_VERSION_1)
        *p++ = (uint8_t)sdim->type;
    else {
        *p++ = 0; /*reserved*/
        *p++ = 0; /*reserved*/
        *p++ = 0; /*reserved*/
        *p++ = 0; /*reserved*/
        *p++ = 0; /*reserved*/
    }             /* end else */

    /* Encode dataspace dimensions for simple dataspaces */
    if (H5S_SIMPLE == sdim->type) {
        /* Encode current & maximum dimensions */
        if (sdim->rank > 0) {
            for (u = 0; u < sdim->rank; u++)
                H5F_ENCODE_LENGTH(f, p, sdim->size[u]);
            if (flags & H5S_VALID_MAX)
                for (u = 0; u < sdim->rank; u++)
                    H5F_ENCODE_LENGTH(f, p, sdim->max[u]);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__sdspace_encode() */

/*--------------------------------------------------------------------------
 NAME
    H5O__sdspace_copy
 PURPOSE
    Copies a message from MESG to DEST, allocating DEST if necessary.
 USAGE
    void *H5O__sdspace_copy(_mesg, _dest)
        const void *_mesg;	IN: Pointer to the source extent dimensionality struct
        const void *_dest;	IN: Pointer to the destination extent dimensionality struct
 RETURNS
    Pointer to DEST on success, NULL on failure
 DESCRIPTION
        This function copies a native (memory) simple dimensionality message,
    allocating the destination structure if necessary.
--------------------------------------------------------------------------*/
static void *
H5O__sdspace_copy(const void *_mesg, void *_dest)
{
    const H5S_extent_t *mesg      = (const H5S_extent_t *)_mesg;
    H5S_extent_t       *dest      = (H5S_extent_t *)_dest;
    void               *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(mesg);
    if (!dest && NULL == (dest = H5FL_CALLOC(H5S_extent_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy extent information */
    if (H5S__extent_copy_real(dest, mesg, true) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, NULL, "can't copy extent");

    /* Set return value */
    ret_value = dest;

done:
    if (NULL == ret_value)
        if (dest && NULL == _dest)
            dest = H5FL_FREE(H5S_extent_t, dest);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__sdspace_copy() */

/*--------------------------------------------------------------------------
 NAME
    H5O__sdspace_size
 PURPOSE
    Return the raw message size in bytes
 USAGE
    void *H5O__sdspace_size(f, mesg)
        H5F_t *f;	  IN: pointer to the HDF5 file struct
        const void *mesg;	IN: Pointer to the source extent dimensionality struct
 RETURNS
    Size of message on success, zero on failure
 DESCRIPTION
        This function returns the size of the raw simple dimensionality message on
    success.  (Not counting the message type or size fields, only the data
    portion of the message).  It doesn't take into account alignment.

--------------------------------------------------------------------------*/
static size_t
H5O__sdspace_size(const H5F_t *f, const void *_mesg)
{
    const H5S_extent_t *space     = (const H5S_extent_t *)_mesg;
    size_t              ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Basic information for all dataspace messages */
    ret_value = (size_t)(1 +                                                  /* Version */
                         1 +                                                  /* Rank */
                         1 +                                                  /* Flags */
                         1 +                                                  /* Dataspace type/reserved */
                         ((space->version > H5O_SDSPACE_VERSION_1) ? 0 : 4)); /* Eliminated/reserved */

    /* Add in the dimension sizes */
    ret_value += space->rank * H5F_SIZEOF_SIZE(f);

    /* Add in the space for the maximum dimensions, if they are present */
    ret_value += space->max ? (space->rank * H5F_SIZEOF_SIZE(f)) : 0;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__sdspace_size() */

/*-------------------------------------------------------------------------
 * Function:	H5O__sdspace_reset
 *
 * Purpose:	Frees the inside of a dataspace message and resets it to some
 *		initial value.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__sdspace_reset(void *_mesg)
{
    H5S_extent_t *mesg = (H5S_extent_t *)_mesg;

    FUNC_ENTER_PACKAGE_NOERR

    H5S__extent_release(mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__sdspace_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5O__sdsdpace_free
 *
 * Purpose:	Frees the message
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__sdspace_free(void *mesg)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(mesg);

    mesg = H5FL_FREE(H5S_extent_t, mesg);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__sdspace_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O__sdspace_pre_copy_file
 *
 * Purpose:     Perform any necessary actions before copying message between
 *              files
 *
 * Return:      Success:        Non-negative
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__sdspace_pre_copy_file(H5F_t H5_ATTR_UNUSED *file_src, const void *mesg_src, bool H5_ATTR_UNUSED *deleted,
                           const H5O_copy_t *cpy_info, void *_udata)
{
    const H5S_extent_t *src_space_extent = (const H5S_extent_t *)mesg_src; /* Source dataspace extent */
    H5D_copy_file_ud_t *udata            = (H5D_copy_file_ud_t *)_udata;   /* Dataset copying user data */
    herr_t              ret_value        = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(file_src);
    assert(src_space_extent);
    assert(cpy_info);
    assert(cpy_info->file_dst);

    /* Check to ensure that the version of the message to be copied does not exceed
       the message version allowed by the destination file's high bound */
    if (src_space_extent->version > H5O_sdspace_ver_bounds[H5F_HIGH_BOUND(cpy_info->file_dst)])
        HGOTO_ERROR(H5E_OHDR, H5E_BADRANGE, FAIL, "dataspace message version out of bounds");

    /* If the user data is non-NULL, assume we are copying a dataset
     * and make a copy of the dataspace extent for later in the object copying
     * process.  (We currently only need to make a copy of the dataspace extent
     * if the layout is an early version, but that information isn't
     * available here, so we just make a copy of it in all cases)
     */
    if (udata) {
        /* Allocate copy of dataspace extent */
        if (NULL == (udata->src_space_extent = H5FL_CALLOC(H5S_extent_t)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_NOSPACE, FAIL, "dataspace extent allocation failed");

        /* Create a copy of the dataspace extent */
        if (H5S__extent_copy_real(udata->src_space_extent, src_space_extent, true) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy extent");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_dspace_pre_copy_file() */

/*--------------------------------------------------------------------------
 NAME
    H5O__sdspace_debug
 PURPOSE
    Prints debugging information for a simple dimensionality message
 USAGE
    void *H5O__sdspace_debug(f, mesg, stream, indent, fwidth)
        H5F_t *f;	        IN: pointer to the HDF5 file struct
        const void *mesg;	IN: Pointer to the source extent dimensionality struct
        FILE *stream;		IN: Pointer to the stream for output data
        int indent;		IN: Amount to indent information by
        int fwidth;		IN: Field width (?)
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
        This function prints debugging output to the stream passed as a
    parameter.
--------------------------------------------------------------------------*/
static herr_t
H5O__sdspace_debug(H5F_t H5_ATTR_UNUSED *f, const void *mesg, FILE *stream, int indent, int fwidth)
{
    const H5S_extent_t *sdim = (const H5S_extent_t *)mesg;

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(sdim);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    fprintf(stream, "%*s%-*s %lu\n", indent, "", fwidth, "Rank:", (unsigned long)(sdim->rank));

    if (sdim->rank > 0) {
        unsigned u; /* local counting variable */

        fprintf(stream, "%*s%-*s {", indent, "", fwidth, "Dim Size:");
        for (u = 0; u < sdim->rank; u++)
            fprintf(stream, "%s%" PRIuHSIZE, u ? ", " : "", sdim->size[u]);
        fprintf(stream, "}\n");

        fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Dim Max:");
        if (sdim->max) {
            fprintf(stream, "{");
            for (u = 0; u < sdim->rank; u++) {
                if (H5S_UNLIMITED == sdim->max[u])
                    fprintf(stream, "%sUNLIM", u ? ", " : "");
                else
                    fprintf(stream, "%s%" PRIuHSIZE, u ? ", " : "", sdim->max[u]);
            } /* end for */
            fprintf(stream, "}\n");
        } /* end if */
        else
            fprintf(stream, "CONSTANT\n");
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__sdspace_debug() */

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

#include "H5Zmodule.h" /* This source code file is part of the H5Z module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access                          */
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Zpkg.h"      /* Data filters				*/

/* Local function prototypes */
static size_t H5Z__filter_fletcher32(unsigned flags, size_t cd_nelmts, const unsigned cd_values[],
                                     size_t nbytes, size_t *buf_size, void **buf);

/* This message derives from H5Z */
const H5Z_class2_t H5Z_FLETCHER32[1] = {{
    H5Z_CLASS_T_VERS,       /* H5Z_class_t version */
    H5Z_FILTER_FLETCHER32,  /* Filter id number		*/
    1,                      /* encoder_present flag (set to true) */
    1,                      /* decoder_present flag (set to true) */
    "fletcher32",           /* Filter name for debugging	*/
    NULL,                   /* The "can apply" callback     */
    NULL,                   /* The "set local" callback     */
    H5Z__filter_fletcher32, /* The actual filter function	*/
}};

#define FLETCHER_LEN 4

/*-------------------------------------------------------------------------
 * Function:	H5Z__filter_fletcher32
 *
 * Purpose:	Implement an I/O filter of Fletcher32 Checksum
 *
 * Return:	Success: Size of buffer filtered
 *		Failure: 0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5Z__filter_fletcher32(unsigned flags, size_t H5_ATTR_UNUSED cd_nelmts,
                       const unsigned H5_ATTR_UNUSED cd_values[], size_t nbytes, size_t *buf_size, void **buf)
{
    void          *outbuf = NULL; /* Pointer to new buffer */
    unsigned char *src    = (unsigned char *)(*buf);
    uint32_t       fletcher;          /* Checksum value */
    uint32_t       reversed_fletcher; /* Possible wrong checksum value */
    uint8_t        c[4];
    uint8_t        tmp;
    size_t         ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(sizeof(uint32_t) >= 4);

    if (flags & H5Z_FLAG_REVERSE) { /* Read */
        /* Do checksum if it's enabled for read; otherwise skip it
         * to save performance. */
        if (!(flags & H5Z_FLAG_SKIP_EDC)) {
            unsigned char *tmp_src;             /* Pointer to checksum in buffer */
            size_t         src_nbytes = nbytes; /* Original number of bytes */
            uint32_t       stored_fletcher;     /* Stored checksum value */

            /* Get the stored checksum */
            src_nbytes -= FLETCHER_LEN;
            tmp_src = src + src_nbytes;
            UINT32DECODE(tmp_src, stored_fletcher);

            /* Compute checksum (can't fail) */
            fletcher = H5_checksum_fletcher32(src, src_nbytes);

            /* The reversed checksum.  There was a bug in the calculating code of
             * the Fletcher32 checksum in the library before v1.6.3.  The checksum
             * value wasn't consistent between big-endian and little-endian systems.
             * This bug was fixed in Release 1.6.3.  However, after fixing the bug,
             * the checksum value is no longer the same as before on little-endian
             * system.  We'll check both the correct checksum and the wrong
             * checksum to be consistent with Release 1.6.2 and before.
             */
            H5MM_memcpy(c, &fletcher, (size_t)4);

            tmp  = c[1];
            c[1] = c[0];
            c[0] = tmp;

            tmp  = c[3];
            c[3] = c[2];
            c[2] = tmp;

            H5MM_memcpy(&reversed_fletcher, c, (size_t)4);

            /* Verify computed checksum matches stored checksum */
            if (stored_fletcher != fletcher && stored_fletcher != reversed_fletcher)
                HGOTO_ERROR(H5E_STORAGE, H5E_READERROR, 0, "data error detected by Fletcher32 checksum");
        }

        /* Set return values */
        /* (Reuse the input buffer, just note that the size is smaller by the size of the checksum) */
        ret_value = nbytes - FLETCHER_LEN;
    }
    else {                  /* Write */
        unsigned char *dst; /* Temporary pointer to destination buffer */

        /* Compute checksum (can't fail) */
        fletcher = H5_checksum_fletcher32(src, nbytes);

        if (NULL == (outbuf = H5MM_malloc(nbytes + FLETCHER_LEN)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0,
                        "unable to allocate Fletcher32 checksum destination buffer");

        dst = (unsigned char *)outbuf;

        /* Copy raw data */
        H5MM_memcpy((void *)dst, (void *)(*buf), nbytes);

        /* Append checksum to raw data for storage */
        dst += nbytes;
        UINT32ENCODE(dst, fletcher);

        /* Free input buffer */
        H5MM_xfree(*buf);

        /* Set return values */
        *buf_size = nbytes + FLETCHER_LEN;
        *buf      = outbuf;
        outbuf    = NULL;
        ret_value = *buf_size;
    }

done:
    if (outbuf)
        H5MM_xfree(outbuf);
    FUNC_LEAVE_NOAPI(ret_value)
}

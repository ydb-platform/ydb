/*
 * Copyright (c) 2016-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <orte_config.h>


#include <stdlib.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_ZLIB_H
#include <zlib.h>
#endif

#include "opal/util/output.h"
#include "compress.h"

#if OPAL_HAVE_ZLIB
bool orte_util_compress_block(uint8_t *inbytes,
                              size_t inlen,
                              uint8_t **outbytes,
                              size_t *olen)
{
    z_stream strm;
    size_t len;
    uint8_t *tmp;

    if (inlen < ORTE_COMPRESS_LIMIT) {
        return false;
    }

    /* set default output */
    *outbytes = NULL;
    *olen = 0;

    /* setup the stream */
    memset (&strm, 0, sizeof (strm));
    deflateInit (&strm, 9);

    /* get an upper bound on the required output storage */
    len = deflateBound(&strm, inlen);
    if (NULL == (tmp = (uint8_t*)malloc(len))) {
        return false;
    }
    strm.next_in = inbytes;
    strm.avail_in = inlen;

    /* allocating the upper bound guarantees zlib will
     * always successfully compress into the available space */
    strm.avail_out = len;
    strm.next_out = tmp;

    deflate (&strm, Z_FINISH);
    deflateEnd (&strm);

    *outbytes = tmp;
    *olen = len - strm.avail_out;
    return true;  // we did the compression
}
#else
bool orte_util_compress_block(uint8_t *inbytes,
                              size_t inlen,
                              uint8_t **outbytes,
                              size_t *olen)
{
    return false;  // we did not compress
}
#endif

#if OPAL_HAVE_ZLIB
bool orte_util_uncompress_block(uint8_t **outbytes, size_t olen,
                                uint8_t *inbytes, size_t len)
{
    uint8_t *dest;
    z_stream strm;

    /* set the default error answer */
    *outbytes = NULL;

    /* setting destination to the fully decompressed size */
    dest = (uint8_t*)malloc(olen);
    if (NULL == dest) {
        return false;
    }

    memset (&strm, 0, sizeof (strm));
    if (Z_OK != inflateInit(&strm)) {
        free(dest);
        return false;
    }
    strm.avail_in = len;
    strm.next_in = inbytes;
    strm.avail_out = olen;
    strm.next_out = dest;

    if (Z_STREAM_END != inflate (&strm, Z_FINISH)) {
        opal_output(0, "\tDECOMPRESS FAILED: %s", strm.msg);
    }
    inflateEnd (&strm);
    *outbytes = dest;
    return true;
}
#else
bool orte_util_uncompress_block(uint8_t **outbytes, size_t olen,
                                uint8_t *inbytes, size_t len)
{
    return false;
}
#endif

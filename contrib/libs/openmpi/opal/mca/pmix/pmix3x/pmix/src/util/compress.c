/*
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>


#include <stdlib.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_ZLIB_H
#include <zlib.h>
#endif

#include "src/include/pmix_globals.h"
#include "src/util/compress.h"

#if PMIX_HAVE_ZLIB
bool pmix_util_compress_string(char *instring,
                               uint8_t **outbytes,
                               size_t *nbytes)
{
    z_stream strm;
    size_t len, outlen;
    uint8_t *tmp, *ptr;
    uint32_t inlen;

    /* set default output */
    *outbytes = NULL;

    /* setup the stream */
    inlen = strlen(instring);
    memset (&strm, 0, sizeof (strm));
    deflateInit (&strm, 9);

    /* get an upper bound on the required output storage */
    len = deflateBound(&strm, inlen);
    if (NULL == (tmp = (uint8_t*)malloc(len))) {
        *outbytes = NULL;
        return false;
    }
    strm.next_in = (uint8_t*)instring;
    strm.avail_in = strlen(instring);

    /* allocating the upper bound guarantees zlib will
     * always successfully compress into the available space */
    strm.avail_out = len;
    strm.next_out = tmp;

    deflate (&strm, Z_FINISH);
    deflateEnd (&strm);

    /* allocate 4 bytes beyond the size reqd by zlib so we
     * can pass the size of the uncompressed string to the
     * decompress side */
    outlen = len - strm.avail_out + sizeof(uint32_t);
    ptr = (uint8_t*)malloc(outlen);
    if (NULL == ptr) {
        free(tmp);
        return false;
    }
    *outbytes = ptr;
    *nbytes = outlen;

    /* fold the uncompressed length into the buffer */
    memcpy(ptr, &inlen, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    /* bring over the compressed data */
    memcpy(ptr, tmp, outlen-sizeof(uint32_t));
    free(tmp);
    pmix_output_verbose(10, pmix_globals.debug_output,
                        "JOBDATA COMPRESS INPUT STRING OF LEN %d OUTPUT SIZE %lu",
                        inlen, outlen-sizeof(uint32_t));
    return true;  // we did the compression
}
#else
bool pmix_util_compress_string(char *instring,
                               uint8_t **outbytes,
                               size_t *nbytes)
{
    return false;  // we did not compress
}
#endif

#if PMIX_HAVE_ZLIB
void pmix_util_uncompress_string(char **outstring,
                                 uint8_t *inbytes, size_t len)
{
    uint8_t *dest;
    int32_t len2;
    z_stream strm;
    int rc;

    /* set the default error answer */
    *outstring = NULL;

    /* the first 4 bytes contains the uncompressed size */
    memcpy(&len2, inbytes, sizeof(uint32_t));

    pmix_output_verbose(10, pmix_globals.debug_output,
                        "DECOMPRESSING INPUT OF LEN %lu OUTPUT %d", len, len2);

    /* setting destination to the fully decompressed size, +1 to
     * hold the NULL terminator */
    dest = (uint8_t*)malloc(len2+1);
    if (NULL == dest) {
        return;
    }
    memset(dest, 0, len2+1);

    memset (&strm, 0, sizeof (strm));
    if (Z_OK != inflateInit(&strm)) {
        free(dest);
        return;
    }
    strm.avail_in = len;
    strm.next_in = (uint8_t*)(inbytes + sizeof(uint32_t));
    strm.avail_out = len2;
    strm.next_out = (uint8_t*)dest;

    rc = inflate (&strm, Z_FINISH);
    inflateEnd (&strm);
    /* ensure this is NULL terminated! */
    dest[len2] = '\0';
    *outstring = (char*)dest;
    pmix_output_verbose(10, pmix_globals.debug_output,
                        "\tFINAL LEN: %lu CODE: %d", strlen(*outstring), rc);
    return;
}
#else
/* this can never actually be used - there is no way we should
 * receive a PMIX_COMPRESSED_STRING unless we compressed it,
 * which means PMIX_HAVE_ZLIB must have been true. Still, we
 * include the stub just to avoid requiring #if's in the rest
 * of the code */
void pmix_util_uncompress_string(char **outstring,
                                 uint8_t *inbytes, size_t len)
{
    *outstring = NULL;
}
#endif

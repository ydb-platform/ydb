/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      IBM Corporation.  All rights reserved.
 * Copyright (c) 2009      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _OPAL_CRC_H_
#define _OPAL_CRC_H_

#include "opal_config.h"

#include <stddef.h>

BEGIN_C_DECLS

#define CRC_POLYNOMIAL ((unsigned int)0x04c11db7)
#define CRC_INITIAL_REGISTER ((unsigned int)0xffffffff)


#define OPAL_CSUM( SRC, LEN )  opal_uicsum( SRC, LEN )
#define OPAL_CSUM_PARTIAL( SRC, LEN, UI1, UI2 ) \
    opal_uicsum_partial( SRC, LEN, UI1, UI2 )
#define OPAL_CSUM_BCOPY_PARTIAL( SRC, DST, LEN1, LEN2, UI1, UI2 ) \
    opal_bcopy_uicsum_partial( SRC, DST, LEN1, LEN2, UI1, UI2 )
#define OPAL_CSUM_ZERO  0


OPAL_DECLSPEC unsigned long
opal_bcopy_csum_partial(
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t csumlen,
    unsigned long*  lastPartialLong,
    size_t*  lastPartialLength
    );

static inline unsigned long
opal_bcopy_csum (
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t csumlen
    )
{
    unsigned long plong = 0;
    size_t plength = 0;
    return opal_bcopy_csum_partial(source, destination, copylen, csumlen, &plong, &plength);
}

OPAL_DECLSPEC unsigned int
opal_bcopy_uicsum_partial (
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t csumlen,
    unsigned int*  lastPartialInt,
    size_t*  lastPartialLength
    );

static inline unsigned int
opal_bcopy_uicsum (
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t csumlen
    )
{
    unsigned int pint = 0;
    size_t plength = 0;
    return opal_bcopy_uicsum_partial(source, destination, copylen, csumlen, &pint, &plength);
}

OPAL_DECLSPEC unsigned long
opal_csum_partial (
    const void *  source,
    size_t csumlen,
    unsigned long*  lastPartialLong,
    size_t*  lastPartialLength
    );


static inline unsigned long
opal_csum(const void *  source, size_t csumlen)
{
    unsigned long lastPartialLong = 0;
    size_t lastPartialLength = 0;
    return opal_csum_partial(source, csumlen, &lastPartialLong, &lastPartialLength);
}
/*
 * The buffer passed to this function is assumed to be 16-bit aligned
 */
static inline uint16_t
opal_csum16 (const void *  source, size_t csumlen)
{
    uint16_t *src = (uint16_t *) source;
    register uint32_t csum = 0;

    while (csumlen > 1) {
	    csum += *src++;
        csumlen -= 2;
    }
    /* Add leftover byte, if any */
    if(csumlen > 0)
        csum += *((unsigned char*)src);
    /* Fold 32-bit checksum to 16 bits */
    while(csum >> 16) {
        csum = (csum & 0xFFFF) + (csum >> 16);
    }
    return csum;
}

OPAL_DECLSPEC unsigned int
opal_uicsum_partial (
    const void *  source,
    size_t csumlen,
    unsigned int *  lastPartialInt,
    size_t*  lastPartialLength
    );

static inline unsigned int
opal_uicsum(const void *  source, size_t csumlen)
{
    unsigned int lastPartialInt = 0;
    size_t lastPartialLength = 0;
    return opal_uicsum_partial(source, csumlen, &lastPartialInt, &lastPartialLength);
}

/*
 * CRC Support
 */

void opal_initialize_crc_table(void);

OPAL_DECLSPEC unsigned int
opal_bcopy_uicrc_partial(
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t crclen,
    unsigned int partial_crc);

static inline unsigned int
opal_bcopy_uicrc(
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t crclen)
{
    return opal_bcopy_uicrc_partial(source, destination, copylen, crclen, CRC_INITIAL_REGISTER);
}

OPAL_DECLSPEC unsigned int
opal_uicrc_partial(
    const void *  source,
    size_t crclen,
    unsigned int partial_crc);


static inline unsigned int
opal_uicrc(const void *  source, size_t crclen)
{
    return opal_uicrc_partial(source, crclen, CRC_INITIAL_REGISTER);
}

END_C_DECLS

#endif


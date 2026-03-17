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
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"
#ifdef HAVE_STDIO_H
#include <stdio.h>
#endif /* HAVE_STDIO_H */
#include <stdlib.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif  /* HAVE_STRINGS_H */
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include "opal/util/crc.h"


#if (OPAL_ALIGNMENT_LONG == 8)
#define OPAL_CRC_WORD_MASK_ 0x7
#elif (OPAL_ALIGNMENT_LONG == 4)
#define OPAL_CRC_WORD_MASK_ 0x3
#else
#define OPAL_CRC_WORD_MASK_ 0xFFFF
#endif


#define WORDALIGNED(v) \
    (((intptr_t)v & OPAL_CRC_WORD_MASK_) ? false : true)


#define INTALIGNED(v) \
    (((intptr_t)v & 3) ? false : true)

/*
 * this version of bcopy_csum() looks a little too long, but it
 * handles cumulative checksumming for arbitrary lengths and address
 * alignments as best as it can; the contents of lastPartialLong and
 * lastPartialLength are updated to reflected the last partial word's
 * value and length (in bytes) -- this should allow proper handling of
 * checksumming contiguous or noncontiguous buffers via multiple calls
 * of bcopy_csum() - Mitch
 */

unsigned long
opal_bcopy_csum_partial (
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t csumlen,
    unsigned long *  lastPartialLong,
    size_t*  lastPartialLength
    )
{
    unsigned long *  src = (unsigned long *) source;
    unsigned long *  dest = (unsigned long *) destination;
    unsigned long csum = 0;
    size_t csumlenresidue;
    unsigned long i, temp;

    csumlenresidue = (csumlen > copylen) ? (csumlen - copylen) : 0;
    temp = *lastPartialLong;

    if (WORDALIGNED(source) && WORDALIGNED(dest)) {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned long) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned long) - *lastPartialLength));
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		dest = (unsigned long *)((char *)dest + sizeof(unsigned long) - *lastPartialLength);
		csum += (temp - *lastPartialLong);
		copylen -= sizeof(unsigned long) - *lastPartialLength;
		/* now we have an unaligned source and an unaligned destination */
		for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		    memcpy(&temp, src, sizeof(temp));
		    src++;
		    csum += temp;
		    memcpy(dest, &temp, sizeof(temp));
		    dest++;
		}
		*lastPartialLength = 0;
		*lastPartialLong = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned long *)((char *)src + copylen);
		dest = (unsigned long *)((char *)dest + copylen);
		csum += (temp - *lastPartialLong);
		*lastPartialLong = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else { /* fast path... */
	    size_t numLongs = copylen/sizeof(unsigned long);
	    for(i = 0; i < numLongs; i++) {
		    csum += *src;
		    *dest++ = *src++;
	    }
	    *lastPartialLong = 0;
	    *lastPartialLength = 0;
	    if (WORDALIGNED(copylen) && (csumlenresidue == 0)) {
		    return(csum);
	    }
	    else {
		    copylen -= i * sizeof(unsigned long);
	    }
	}
    } else if (WORDALIGNED(source)) {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned long) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned long) - *lastPartialLength));
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		dest = (unsigned long *)((char *)dest + sizeof(unsigned long) - *lastPartialLength);
		csum += (temp - *lastPartialLong);
		copylen -= sizeof(unsigned long) - *lastPartialLength;
		/* now we have an unaligned source and an unknown alignment for our destination */
		if (WORDALIGNED(dest)) {
		    size_t numLongs = copylen/sizeof(unsigned long);
		    for(i = 0; i < numLongs; i++) {
			    memcpy(&temp, src, sizeof(temp));
			    src++;
			    csum += temp;
			    *dest++ = temp;
		    }
		    copylen -= i * sizeof(unsigned long);
		}
		else {
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			    memcpy(&temp, src, sizeof(temp));
			    src++;
			    csum += temp;
			    memcpy(dest, &temp, sizeof(temp));
			    dest++;
		    }
		}
		*lastPartialLong = 0;
		*lastPartialLength = 0;
	    }
	    else { /* NO, we don't... */
		    memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		    memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		    src = (unsigned long *)((char *)src + copylen);
		    dest = (unsigned long *)((char *)dest + copylen);
		    csum += (temp - *lastPartialLong);
		    *lastPartialLong = temp;
		    *lastPartialLength += copylen;
		    copylen = 0;
	    }
	}
	else {
	    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		temp = *src++;
		csum += temp;
		memcpy(dest, &temp, sizeof(temp));
		dest++;
	    }
	    *lastPartialLong = 0;
	    *lastPartialLength = 0;
	}
    } else if (WORDALIGNED(dest)) {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned long) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned long) - *lastPartialLength));
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		dest = (unsigned long *)((char *)dest + sizeof(unsigned long) - *lastPartialLength);
		csum += (temp - *lastPartialLong);
		copylen -= sizeof(unsigned long) - *lastPartialLength;
		/* now we have a source of unknown alignment and a unaligned destination */
		if (WORDALIGNED(src)) {
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			temp = *src++;
			csum += temp;
			memcpy(dest, &temp, sizeof(temp));
			dest++;
		    }
		    *lastPartialLong = 0;
		    *lastPartialLength = 0;
		}
		else {
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			memcpy(&temp, src, sizeof(temp));
			src++;
			csum += temp;
			memcpy(dest, &temp, sizeof(temp));
			dest++;
		    }
		    *lastPartialLength = 0;
		    *lastPartialLong = 0;
		}
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned long *)((char *)src + copylen);
		dest = (unsigned long *)((char *)dest + copylen);
		csum += (temp - *lastPartialLong);
		*lastPartialLong = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else {
	    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		memcpy(&temp, src, sizeof(temp));
		src++;
		csum += temp;
		*dest++ = temp;
	    }
	    *lastPartialLength = 0;
	    *lastPartialLong = 0;
	}
    } else {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned long) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned long) - *lastPartialLength));
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		dest = (unsigned long *)((char *)dest + sizeof(unsigned long) - *lastPartialLength);
		csum += (temp - *lastPartialLong);
		copylen -= sizeof(unsigned long) - *lastPartialLength;
		/* now we have an unknown alignment for our source and destination */
		if (WORDALIGNED(src) && WORDALIGNED(dest)) {
		    size_t numLongs = copylen/sizeof(unsigned long);
		    for(i = 0; i < numLongs; i++) {
			    csum += *src;
			    *dest++ = *src++;
		    }
		    copylen -= i * sizeof(unsigned long);
		}
		else { /* safe but slower for all other alignments */
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			    memcpy(&temp, src, sizeof(temp));
			    src++;
			    csum += temp;
			    memcpy(dest, &temp, sizeof(temp));
			    dest++;
		    }
		}
		*lastPartialLong = 0;
		*lastPartialLength = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned long *)((char *)src + copylen);
		dest = (unsigned long *)((char *)dest + copylen);
		csum += (temp - *lastPartialLong);
		*lastPartialLong = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else {
	    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		memcpy(&temp, src, sizeof(temp));
		src++;
		csum += temp;
		memcpy(dest, &temp, sizeof(temp));
		dest++;
	    }
	    *lastPartialLength = 0;
	    *lastPartialLong = 0;
	}
    }

    /* if copylen is non-zero there was a bit left, less than an unsigned long's worth */
    if ((copylen != 0) && (csumlenresidue == 0)) {
	temp = *lastPartialLong;
	if (*lastPartialLength) {
	    if (copylen >= (sizeof(unsigned long) - *lastPartialLength)) {
		/* copy all remaining bytes from src to dest */
		unsigned long copytemp = 0;
		memcpy(&copytemp, src, copylen);
		memcpy(dest, &copytemp, copylen);
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		/* avoid unsigned arithmetic overflow by subtracting the old partial
		 * word from the new one before adding to the checksum...
        */
		csum += (temp - *lastPartialLong);
		copylen -= sizeof(unsigned long) - *lastPartialLength;
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		*lastPartialLength = copylen;
		/* reset temp, and calculate next partial word */
		temp = 0;
		if (copylen) {
		    memcpy(&temp, src, copylen);
		}
		/* add it to the the checksum */
		csum += temp;
		*lastPartialLong = temp;
	    }
	    else {
		/* copy all remaining bytes from src to dest */
		unsigned long copytemp = 0;
		memcpy(&copytemp, src, copylen);
		memcpy(dest, &copytemp, copylen);
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       copylen);
		/* avoid unsigned arithmetic overflow by subtracting the old partial
		 * word from the new one before adding to the checksum...
        */
		csum += temp - *lastPartialLong;
		*lastPartialLong = temp;
		*lastPartialLength += copylen;
	    }
	}
	else { /* fast path... */
	    /* temp and *lastPartialLong are 0 if *lastPartialLength is 0... */
	    memcpy(&temp, src, copylen);
	    csum += temp;
	    memcpy(dest, &temp, copylen);
	    *lastPartialLong = temp;
	    *lastPartialLength = copylen;
	    /* done...return the checksum */
	}
    }
    else if (csumlenresidue != 0) {
	if (copylen != 0) {
	    temp = 0;
	    memcpy(&temp, src, copylen);
	    memcpy(dest, &temp, copylen);
	}
	if (csumlenresidue < (sizeof(unsigned long) - copylen - *lastPartialLength)) {
	    temp = *lastPartialLong;
	    memcpy(((char *)&temp + *lastPartialLength), src, (copylen + csumlenresidue));
	    /* avoid unsigned arithmetic overflow by subtracting the old partial */
	    /* word from the new one before adding to the checksum... */
	    csum += temp - *lastPartialLong;
	    src++;
	    *lastPartialLong = temp;
	    *lastPartialLength += copylen + csumlenresidue;
	    csumlenresidue = 0;
	}
	else {
	    /* we have enough chksum data to fill out our last partial */
	    /* word */
	    temp = *lastPartialLong;
	    memcpy(((char *)&temp + *lastPartialLength), src,
		   (sizeof(unsigned long) - *lastPartialLength));
	    /* avoid unsigned arithmetic overflow by subtracting the old partial */
	    /* word from the new one before adding to the checksum... */
	    csum += temp - *lastPartialLong;
	    src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
	    csumlenresidue -= sizeof(unsigned long) - *lastPartialLength - copylen;
	    *lastPartialLength = 0;
	    *lastPartialLong = 0;
	}
	if (WORDALIGNED(src)) {
	    for (i = 0; i < csumlenresidue/sizeof(unsigned long); i++) {
		csum += *src++;
	    }
	}
	else {
	    for (i = 0; i < csumlenresidue/sizeof(unsigned long); i++) {
		memcpy(&temp, src, sizeof(temp));
		csum += temp;
		src++;
	    }
	}
	csumlenresidue -= i * sizeof(unsigned long);
	if (csumlenresidue) {
	    temp = 0;
	    memcpy(&temp, src, csumlenresidue);
	    csum += temp;
	    *lastPartialLong = temp;
	    *lastPartialLength = csumlenresidue;
	}
    } /* end else if (csumlenresidue != 0) */

    return csum;
}

unsigned int
opal_bcopy_uicsum_partial (
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t csumlen,
    unsigned int*  lastPartialInt,
    size_t*  lastPartialLength
    )
{
    unsigned int *  src = (unsigned int *) source;
    unsigned int *  dest = (unsigned int *) destination;
    unsigned int csum = 0;
    size_t csumlenresidue;
    unsigned long i;
    unsigned int temp;

    csumlenresidue = (csumlen > copylen) ? (csumlen - copylen) : 0;
    temp = *lastPartialInt;

    if (INTALIGNED(source) && INTALIGNED(dest)) {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned int) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned int) - *lastPartialLength));
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		dest = (unsigned int *)((char *)dest + sizeof(unsigned int) - *lastPartialLength);
		csum += (temp - *lastPartialInt);
		copylen -= sizeof(unsigned int) - *lastPartialLength;
		/* now we have an unaligned source and an unaligned destination */
		for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		    memcpy(&temp, src, sizeof(temp));
		    src++;
		    csum += temp;
		    memcpy(dest, &temp, sizeof(temp));
		    dest++;
		}
		*lastPartialLength = 0;
		*lastPartialInt = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned int *)((char *)src + copylen);
		dest = (unsigned int *)((char *)dest + copylen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else { /* fast path... */
	    size_t numLongs = copylen/sizeof(unsigned int);
	    for(i = 0; i < numLongs; i++) {
		    csum += *src;
		    *dest++ = *src++;
	    }
	    *lastPartialInt = 0;
	    *lastPartialLength = 0;
	    if (INTALIGNED(copylen) && (csumlenresidue == 0)) {
		    return(csum);
	    }
	    else {
		    copylen -= i * sizeof(unsigned int);
	    }
	}
    } else if (INTALIGNED(source)) {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned int) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned int) - *lastPartialLength));
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		dest = (unsigned int *)((char *)dest + sizeof(unsigned int) - *lastPartialLength);
		csum += (temp - *lastPartialInt);
		copylen -= sizeof(unsigned int) - *lastPartialLength;
		/* now we have an unaligned source and an unknown alignment for our destination */
		if (INTALIGNED(dest)) {
		    size_t numLongs = copylen/sizeof(unsigned int);
		    for(i = 0; i < numLongs; i++) {
			memcpy(&temp, src, sizeof(temp));
			src++;
			csum += temp;
			*dest++ = temp;
		    }
		    copylen -= i * sizeof(unsigned int);
		}
		else {
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			memcpy(&temp, src, sizeof(temp));
			src++;
			csum += temp;
			memcpy(dest, &temp, sizeof(temp));
			dest++;
		    }
		}
		*lastPartialInt = 0;
		*lastPartialLength = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned int *)((char *)src + copylen);
		dest = (unsigned int *)((char *)dest + copylen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else {
	    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		temp = *src++;
		csum += temp;
		memcpy(dest, &temp, sizeof(temp));
		dest++;
	    }
	    *lastPartialInt = 0;
	    *lastPartialLength = 0;
	}
    } else if (INTALIGNED(dest)) {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned int) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned int) - *lastPartialLength));
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		dest = (unsigned int *)((char *)dest + sizeof(unsigned int) - *lastPartialLength);
		csum += (temp - *lastPartialInt);
		copylen -= sizeof(unsigned int) - *lastPartialLength;
		/* now we have a source of unknown alignment and a unaligned destination */
		if (INTALIGNED(src)) {
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			temp = *src++;
			csum += temp;
			memcpy(dest, &temp, sizeof(temp));
			dest++;
		    }
		    *lastPartialInt = 0;
		    *lastPartialLength = 0;
		}
		else {
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			memcpy(&temp, src, sizeof(temp));
			src++;
			csum += temp;
			memcpy(dest, &temp, sizeof(temp));
			dest++;
		    }
		    *lastPartialLength = 0;
		    *lastPartialInt = 0;
		}
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned int *)((char *)src + copylen);
		dest = (unsigned int *)((char *)dest + copylen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else {
	    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		memcpy(&temp, src, sizeof(temp));
		src++;
		csum += temp;
		*dest++ = temp;
	    }
	    *lastPartialLength = 0;
	    *lastPartialInt = 0;
	}
    } else {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (copylen >= (sizeof(unsigned int) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		memcpy(dest, ((char *)&temp + *lastPartialLength),
		       (sizeof(unsigned int) - *lastPartialLength));
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		dest = (unsigned int *)((char *)dest + sizeof(unsigned int) - *lastPartialLength);
		csum += (temp - *lastPartialInt);
		copylen -= sizeof(unsigned int) - *lastPartialLength;
		/* now we have an unknown alignment for our source and destination */
		if (INTALIGNED(src) && INTALIGNED(dest)) {
		    size_t numLongs = copylen/sizeof(unsigned int);
		    for(i = 0; i < numLongs; i++) {
			csum += *src;
			*dest++ = *src++;
		    }
		    copylen -= i * sizeof(unsigned int);
		}
		else { /* safe but slower for all other alignments */
		    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
			memcpy(&temp, src, sizeof(temp));
			src++;
			csum += temp;
			memcpy(dest, &temp, sizeof(temp));
			dest++;
		    }
		}
		*lastPartialInt = 0;
		*lastPartialLength = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, copylen);
		memcpy(dest, ((char *)&temp + *lastPartialLength), copylen);
		src = (unsigned int *)((char *)src + copylen);
		dest = (unsigned int *)((char *)dest + copylen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += copylen;
		copylen = 0;
	    }
	}
	else {
	    for( ;copylen >= sizeof(*src); copylen -= sizeof(*src)) {
		memcpy(&temp, src, sizeof(temp));
		src++;
		csum += temp;
		memcpy(dest, &temp, sizeof(temp));
		dest++;
	    }
	    *lastPartialLength = 0;
	    *lastPartialInt = 0;
	}
    }

    /* if copylen is non-zero there was a bit left, less than an unsigned int's worth */
    if ((copylen != 0) && (csumlenresidue == 0)) {
	temp = *lastPartialInt;
	if (*lastPartialLength) {
	    if (copylen >= (sizeof(unsigned int) - *lastPartialLength)) {
		/* copy all remaining bytes from src to dest */
		unsigned int copytemp = 0;
		memcpy(&copytemp, src, copylen);
		memcpy(dest, &copytemp, copylen);
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		/* avoid unsigned arithmetic overflow by subtracting the old partial
		 * word from the new one before adding to the checksum...
        */
		csum += (temp - *lastPartialInt);
		copylen -= sizeof(unsigned int) - *lastPartialLength;
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		*lastPartialLength = copylen;
		/* reset temp, and calculate next partial word */
		temp = 0;
		if (copylen) {
		    memcpy(&temp, src, copylen);
		}
		/* add it to the the checksum */
		csum += temp;
		*lastPartialInt = temp;
	    }
	    else {
		/* copy all remaining bytes from src to dest */
		unsigned int copytemp = 0;
		memcpy(&copytemp, src, copylen);
		memcpy(dest, &copytemp, copylen);
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       copylen);
		/* avoid unsigned arithmetic overflow by subtracting the old partial
		 * word from the new one before adding to the checksum...
        */
		csum += temp - *lastPartialInt;
		*lastPartialInt = temp;
		*lastPartialLength += copylen;
	    }
	}
	else { /* fast path... */
	    /* temp and *lastPartialInt are 0 if *lastPartialLength is 0... */
	    memcpy(&temp, src, copylen);
	    csum += temp;
	    memcpy(dest, &temp, copylen);
	    *lastPartialInt = temp;
	    *lastPartialLength = copylen;
	    /* done...return the checksum */
	}
    }
    else if (csumlenresidue != 0) {
	if (copylen != 0) {
	    temp = 0;
	    memcpy(&temp, src, copylen);
	    memcpy(dest, &temp, copylen);
	}
	if (csumlenresidue < (sizeof(unsigned int) - copylen - *lastPartialLength)) {
	    temp = *lastPartialInt;
	    memcpy(((char *)&temp + *lastPartialLength), src, (copylen + csumlenresidue));
	    /* avoid unsigned arithmetic overflow by subtracting the old partial
	     * word from the new one before adding to the checksum...
        */
	    csum += temp - *lastPartialInt;
	    src++;
	    *lastPartialInt = temp;
	    *lastPartialLength += copylen + csumlenresidue;
	    csumlenresidue = 0;
	}
	else {
	    /* we have enough chksum data to fill out our last partial
	     * word
        */
	    temp = *lastPartialInt;
	    memcpy(((char *)&temp + *lastPartialLength), src,
		   (sizeof(unsigned int) - *lastPartialLength));
	    /* avoid unsigned arithmetic overflow by subtracting the old partial
	     * word from the new one before adding to the checksum...
        */
	    csum += temp - *lastPartialInt;
	    src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
	    csumlenresidue -= sizeof(unsigned int) - *lastPartialLength - copylen;
	    *lastPartialLength = 0;
	    *lastPartialInt = 0;
	}
	if (INTALIGNED(src)) {
	    for (i = 0; i < csumlenresidue/sizeof(unsigned int); i++) {
		csum += *src++;
	    }
	}
	else {
	    for (i = 0; i < csumlenresidue/sizeof(unsigned int); i++) {
		memcpy(&temp, src, sizeof(temp));
		csum += temp;
		src++;
	    }
	}
	csumlenresidue -= i * sizeof(unsigned int);
	if (csumlenresidue) {
	    temp = 0;
	    memcpy(&temp, src, csumlenresidue);
	    csum += temp;
	    *lastPartialInt = temp;
	    *lastPartialLength = csumlenresidue;
	}
    } /* end else if (csumlenresidue != 0) */

    return csum;
}


/*
 * csum() generates a bcopy_csum() - compatible checksum that can be
 * called multiple times
 */

unsigned long
opal_csum_partial (
    const void *  source,
    size_t csumlen,
    unsigned long*  lastPartialLong,
    size_t* lastPartialLength
    )
{
    unsigned long *  src = (unsigned long *) source;
    unsigned long csum = 0;
    unsigned long i, temp;



    temp = *lastPartialLong;

    if (WORDALIGNED(source))  {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (csumlen >= (sizeof(unsigned long) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		csum += (temp - *lastPartialLong);
		csumlen -= sizeof(unsigned long) - *lastPartialLength;
		/* now we have an unaligned source */
		for(i = 0; i < csumlen/sizeof(unsigned long); i++) {
		    memcpy(&temp, src, sizeof(temp));
		    csum += temp;
		    src++;
		}
		csumlen -= i * sizeof(unsigned long);
		*lastPartialLong = 0;
		*lastPartialLength = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, csumlen);
		src = (unsigned long *)((char *)src + csumlen);
		csum += (temp - *lastPartialLong);
		*lastPartialLong = temp;
		*lastPartialLength += csumlen;
		csumlen = 0;
	    }
	}
	else { /* fast path... */
	    size_t numLongs = csumlen/sizeof(unsigned long);
	    for(i = 0; i < numLongs; i++) {
		    csum += *src++;
	    }
	    *lastPartialLong = 0;
	    *lastPartialLength = 0;
	    if (WORDALIGNED(csumlen)) {
		    return(csum);
	    }
	    else {
		    csumlen -= i * sizeof(unsigned long);
	    }
	}
    } else {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (csumlen >= (sizeof(unsigned long) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		csum += (temp - *lastPartialLong);
		csumlen -= sizeof(unsigned long) - *lastPartialLength;
		/* now we have a source of unknown alignment */
		if (WORDALIGNED(src)) {
		    for(i = 0; i < csumlen/sizeof(unsigned long); i++) {
			csum += *src++;
		    }
		    csumlen -= i * sizeof(unsigned long);
		    *lastPartialLong = 0;
		    *lastPartialLength = 0;
		}
		else {
		    for(i = 0; i < csumlen/sizeof(unsigned long); i++) {
			memcpy(&temp, src, sizeof(temp));
			csum += temp;
			src++;
		    }
		    csumlen -= i * sizeof(unsigned long);
		    *lastPartialLong = 0;
		    *lastPartialLength = 0;
		}
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, csumlen);
		src = (unsigned long *)((char *)src + csumlen);
		csum += (temp - *lastPartialLong);
		*lastPartialLong = temp;
		*lastPartialLength += csumlen;
		csumlen = 0;
	    }
	}
	else {
	    for( ;csumlen >= sizeof(*src); csumlen -= sizeof(*src)) {
		memcpy(&temp, src, sizeof(temp));
		src++;
		csum += temp;
	    }
	    *lastPartialLength = 0;
	    *lastPartialLong = 0;
	}
    }

    /* if csumlen is non-zero there was a bit left, less than an unsigned long's worth */
    if (csumlen != 0) {
	temp = *lastPartialLong;
	if (*lastPartialLength) {
	    if (csumlen >= (sizeof(unsigned long) - *lastPartialLength)) {
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned long) - *lastPartialLength));
		csum += (temp - *lastPartialLong);
		csumlen -= sizeof(unsigned long) - *lastPartialLength;
		src = (unsigned long *)((char *)src + sizeof(unsigned long) - *lastPartialLength);
		*lastPartialLength = csumlen;
		/* reset temp, and calculate next partial word */
		temp = 0;
		if (csumlen) {
		    memcpy(&temp, src, csumlen);
		}
		/* add it to the the checksum */
		csum += temp;
		*lastPartialLong = temp;
	    }
	    else {
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       csumlen);
		csum += (temp - *lastPartialLong);
		*lastPartialLong = temp;
		*lastPartialLength += csumlen;
	    }
	}
	else { /* fast path... */
	    /* temp and *lastPartialLong are 0 if *lastPartialLength is 0... */
	    memcpy(&temp, src, csumlen);
	    csum += temp;
	    *lastPartialLong = temp;
	    *lastPartialLength = csumlen;
	    /* done...return the checksum */
	}
    }

    return csum;
}

unsigned int
opal_uicsum_partial (
    const void *  source,
    size_t csumlen,
    unsigned int*  lastPartialInt,
    size_t*  lastPartialLength
    )
{
    unsigned int *  src = (unsigned int *) source;
    unsigned int csum = 0;
    unsigned int temp;
    unsigned long i;


    temp = *lastPartialInt;

    if (INTALIGNED(source))  {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (csumlen >= (sizeof(unsigned int) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		csum += (temp - *lastPartialInt);
		csumlen -= sizeof(unsigned int) - *lastPartialLength;
		/* now we have an unaligned source */
		for(i = 0; i < csumlen/sizeof(unsigned int); i++) {
		    memcpy(&temp, src, sizeof(temp));
		    csum += temp;
		    src++;
		}
		csumlen -= i * sizeof(unsigned int);
		*lastPartialInt = 0;
		*lastPartialLength = 0;
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, csumlen);
		src = (unsigned int *)((char *)src + csumlen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += csumlen;
		csumlen = 0;
	    }
	}
	else { /* fast path... */
	    size_t numLongs = csumlen/sizeof(unsigned int);
	    for(i = 0; i < numLongs; i++) {
		    csum += *src++;
	    }
	    *lastPartialInt = 0;
	    *lastPartialLength = 0;
	    if (INTALIGNED(csumlen)) {
		    return(csum);
	    }
	    else {
		    csumlen -= i * sizeof(unsigned int);
	    }
	}
    } else {
	if (*lastPartialLength) {
	    /* do we have enough data to fill out the partial word? */
	    if (csumlen >= (sizeof(unsigned int) - *lastPartialLength)) { /* YES, we do... */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		csum += (temp - *lastPartialInt);
		csumlen -= sizeof(unsigned int) - *lastPartialLength;
		/* now we have a source of unknown alignment */
		if (INTALIGNED(src)) {
		    for(i = 0; i < csumlen/sizeof(unsigned int); i++) {
			csum += *src++;
		    }
		    csumlen -= i * sizeof(unsigned int);
		    *lastPartialInt = 0;
		    *lastPartialLength = 0;
		}
		else {
		    for(i = 0; i < csumlen/sizeof(unsigned int); i++) {
			memcpy(&temp, src, sizeof(temp));
			csum += temp;
			src++;
		    }
		    csumlen -= i * sizeof(unsigned int);
		    *lastPartialInt = 0;
		    *lastPartialLength = 0;
		}
	    }
	    else { /* NO, we don't... */
		memcpy(((char *)&temp + *lastPartialLength), src, csumlen);
		src = (unsigned int *)((char *)src + csumlen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += csumlen;
		csumlen = 0;
	    }
	}
	else {
	    for( ;csumlen >= sizeof(*src); csumlen -= sizeof(*src)) {
		memcpy(&temp, src, sizeof(temp));
		src++;
		csum += temp;
	    }
	    *lastPartialLength = 0;
	    *lastPartialInt = 0;
	}
    }

    /* if csumlen is non-zero there was a bit left, less than an unsigned int's worth */
    if (csumlen != 0) {
	temp = *lastPartialInt;
	if (*lastPartialLength) {
	    if (csumlen >= (sizeof(unsigned int) - *lastPartialLength)) {
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       (sizeof(unsigned int) - *lastPartialLength));
		csum += (temp - *lastPartialInt);
		csumlen -= sizeof(unsigned int) - *lastPartialLength;
		src = (unsigned int *)((char *)src + sizeof(unsigned int) - *lastPartialLength);
		*lastPartialLength = csumlen;
		/* reset temp, and calculate next partial word */
		temp = 0;
		if (csumlen) {
		    memcpy(&temp, src, csumlen);
		}
		/* add it to the the checksum */
		csum += temp;
		*lastPartialInt = temp;
	    }
	    else {
		/* fill out rest of partial word and add to checksum */
		memcpy(((char *)&temp + *lastPartialLength), src,
		       csumlen);
		csum += (temp - *lastPartialInt);
		*lastPartialInt = temp;
		*lastPartialLength += csumlen;
	    }
	}
	else { /* fast path... */
	    /* temp and *lastPartialInt are 0 if *lastPartialLength is 0... */
	    memcpy(&temp, src, csumlen);
	    csum += temp;
	    *lastPartialInt = temp;
	    *lastPartialLength = csumlen;
	    /* done...return the checksum */
	}
    }

    return csum;
}

/* globals for CRC32 bcopy and calculation routines */

static bool _opal_crc_table_initialized = false;
static unsigned int _opal_crc_table[256];

/* CRC32 table generation routine - thanks to Charles Michael Heard for his
 * optimized CRC32 code...
 */

void opal_initialize_crc_table(void)
{
    register int i,j;
    register unsigned int crc_accum;

    for (i = 0; i < 256; i++) {
        crc_accum = (i << 24);
        for (j = 0; j < 8; j++) {
            if (crc_accum & 0x80000000)
                crc_accum = (crc_accum << 1) ^ CRC_POLYNOMIAL;
            else
                crc_accum = (crc_accum << 1);
        }
        _opal_crc_table[i] = crc_accum;
    }

    /* set global bool to true to do this work once! */
    _opal_crc_table_initialized = true;
    return;
}

unsigned int opal_bcopy_uicrc_partial(
    const void *  source,
    void *  destination,
    size_t copylen,
    size_t crclen,
    unsigned int partial_crc)
{
    size_t crclenresidue = (crclen > copylen) ? (crclen - copylen) : 0;
    register int i, j;
    register unsigned char t;
    unsigned int tmp;

    if (!_opal_crc_table_initialized) {
        opal_initialize_crc_table();
    }

    if (INTALIGNED(source) && INTALIGNED(destination)) {
        register unsigned int *  src = (unsigned int *)source;
        register unsigned int *  dst = (unsigned int *)destination;
        register unsigned char *ts, *td;
        /* copy whole integers */
        while (copylen >= sizeof(unsigned int)) {
            tmp = *src++;
            *dst++ = tmp;
            ts = (unsigned char *)&tmp;
            for (j = 0; j < (int)sizeof(unsigned int); j++) {
                i = ((partial_crc >> 24) ^ *ts++) & 0xff;
                partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
            }
            copylen -= sizeof(unsigned int);
        }
        ts = (unsigned char *)src;
        td = (unsigned char *)dst;
        /* copy partial integer */
        while (copylen--) {
            t = *ts++;
            *td++ = t;
            i = ((partial_crc >> 24) ^ t) & 0xff;
            partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
        }
        /* calculate CRC over remaining bytes... */
        while (crclenresidue--) {
            i = ((partial_crc >> 24) ^ *ts++) & 0xff;
            partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
        }
    }
    else {
        register unsigned char *  src = (unsigned char *)source;
        register unsigned char *  dst = (unsigned char *)destination;
        while (copylen--) {
            t = *src++;
            *dst++ = t;
            i = ((partial_crc >> 24) ^ t) & 0xff;
            partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
        }
        while (crclenresidue--) {
            i = ((partial_crc >> 24) ^ *src++) & 0xff;
            partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
        }
    }

    return partial_crc;
}


unsigned int opal_uicrc_partial(
    const void *  source, size_t crclen, unsigned int partial_crc)
{
    register int i, j;
    register unsigned char * t;
    unsigned int tmp;

    if (!_opal_crc_table_initialized) {
        opal_initialize_crc_table();
    }

    if (INTALIGNED(source)) {
        register unsigned int *  src = (unsigned int *)source;
        while (crclen >= sizeof(unsigned int)) {
            tmp = *src++;
            t = (unsigned char *)&tmp;
            for (j = 0; j < (int)sizeof(unsigned int); j++) {
                i = ((partial_crc >> 24) ^ *t++) & 0xff;
                partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
            }
            crclen -= sizeof(unsigned int);
        }
        t = (unsigned char *)src;
        while (crclen--) {
            i = ((partial_crc >> 24) ^ *t++) & 0xff;
            partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
        }
    }
    else {
        register unsigned char *  src = (unsigned char *)source;
        while (crclen--) {
            i = ((partial_crc >> 24) ^ *src++) & 0xff;
            partial_crc = (partial_crc << 8) ^ _opal_crc_table[i];
        }
    }

    return partial_crc;
}


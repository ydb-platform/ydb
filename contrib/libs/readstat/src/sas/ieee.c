#include <stdint.h>
#include <string.h>

#include "ieee.h"
#include "../readstat_bits.h"

/* These routines are modified versions of those found in SAS publication TS-140,
 * "RECORD LAYOUT OF A SAS VERSION 5 OR 6 DATA SET IN SAS TRANSPORT (XPORT) FORMAT"
 * https://support.sas.com/techsup/technote/ts140.pdf
 *
 * Modifications include using stdint.h and supporting infinite IEEE values.
 */

static void xpt2ieee(unsigned char *xport, unsigned char *ieee);
static void ieee2xpt(unsigned char *ieee, unsigned char *xport);

#ifndef FLOATREP
#define FLOATREP get_native()
int get_native(void);
#endif

void memreverse(void *intp_void, int l) {
    if (!machine_is_little_endian())
        return;

    int i,j;
    char save;
    char *intp = (char *)intp_void;

    j = l/2;
    for (i=0;i<j;i++) {
        save = intp[i];
        intp[i] = intp[l-i-1];
        intp[l-i-1] = save;
    }
}

int cnxptiee(const void *from_bytes, int fromtype, void *to_bytes, int totype)
{
    unsigned char *from = (unsigned char *)from_bytes;
    unsigned char *to = (unsigned char *)to_bytes;
    unsigned char temp[8];
    int i;

    if (fromtype == CN_TYPE_NATIVE) {
        fromtype = FLOATREP;
    }
    switch(fromtype) {
        case CN_TYPE_IEEEL :
            if (totype == CN_TYPE_IEEEL)
                break;
            for (i=7;i>=0;i--) {
                temp[7-i] = from[i];
            }
            from = temp;
            fromtype = CN_TYPE_IEEEB;
            /* Break intentionally omitted. */
        case CN_TYPE_IEEEB :
            /* Break intentionally omitted. */
        case CN_TYPE_XPORT :
            break;
        default:
            return(-1);
    }
    if (totype == CN_TYPE_NATIVE) {
        totype = FLOATREP;
    }
    switch(totype) {
        case CN_TYPE_XPORT :
        case CN_TYPE_IEEEB :
        case CN_TYPE_IEEEL :
            break;
        default:
            return(-2);
    }
    if (fromtype == totype) {
        memcpy(to,from,8);
        return(0);
    }
    switch(fromtype) {
        case CN_TYPE_IEEEB :
            if (totype == CN_TYPE_XPORT)
                ieee2xpt(from,to);
            else memcpy(to,from,8);
            break;
        case CN_TYPE_XPORT :
            xpt2ieee(from,to);
            break;
    }
    if (totype == CN_TYPE_IEEEL) {
        memcpy(temp,to,8);
        for (i=7;i>=0;i--) {
            to[7-i] = temp[i];
        }
    }
    return(0);
}

int get_native(void) {
    static unsigned char float_reps[][8] = {
        {0x41,0x10,0x00,0x00,0x00,0x00,0x00,0x00},
        {0x3f,0xf0,0x00,0x00,0x00,0x00,0x00,0x00},
        {0x00,0x00,0x00,0x00,0x00,0x00,0xf0,0x3f}
    };

    static double one = 1.00;

    int i,j;
    j = sizeof(float_reps)/8;
    for (i=0;i<j;i++)  {
        if (memcmp(&one,float_reps+i,8) == 0)
            return(i+1);
    }
    return(-1);
}

void xpt2ieee(unsigned char *xport, unsigned char *ieee) {
    char temp[8];
    register int shift;
    register int nib;
    uint32_t ieee1,ieee2;
    uint32_t xport1 = 0;
    uint32_t xport2 = 0;

    memcpy(temp,xport,8);
    memset(ieee,0,8);

    if (*temp && memcmp(temp+1,ieee,7) == 0) {
        ieee[0] = ieee[1] = 0xff;
        ieee[2] = ~(*temp);
        return;
    }

    memcpy(&xport1,temp,sizeof(uint32_t));
    memreverse(&xport1,sizeof(uint32_t));
    memcpy(&xport2,temp+4,sizeof(uint32_t));
    memreverse(&xport2,sizeof(uint32_t));

    /***************************************************************/
    /* Translate IBM format floating point numbers into IEEE */
    /* format floating point numbers. */
    /* */
    /* IEEE format: */
    /* */
    /* 6 5 0 */
    /* 3 1 0 */
    /* */
    /* SEEEEEEEEEEEMMMM ............ MMMM */
    /* */
    /* Sign bit, 11 bits exponent, 52 bit fraction. Exponent is */
    /* excess 1023. The fraction is multiplied by a power of 2 of */

    /* the actual exponent. Normalized floating point numbers are */
    /* represented with the binary point immediately to the left */
    /* of the fraction with an implied "1" to the left of the */
    /* binary point. */
    /* */
    /* IBM format: */
    /* */
    /* 6 5 0 */
    /* 3 1 0 */
    /* */
    /* SEEEEEEEMMMM ......... MMMM */
    /* */
    /* Sign bit, 7 bit exponent, 56 bit fraction. Exponent is */
    /* excess 64. The fraction is multiplied bya power of 16 of */
    /* the actual exponent. Normalized floating point numbers are */
    /* represented with the radix point immediately to the left of*/
    /* the high order hex fraction digit. */
    /* */
    /* How do you translate from IBM format to IEEE? */
    /* */
    /* Translating back to ieee format from ibm is easier than */
    /* going the other way. You lose at most, 3 bits of fraction, */
    /* but nothing can be done about that. The only tricky parts */
    /* are setting up the correct binary exponent from the ibm */
    /* hex exponent, and removing the implicit "1" bit of the ieee*/
    /* fraction (see vzctdbl). We must shift down the high order */
    /* nibble of the ibm fraction until it is 1. This is the */
    /* implicit 1. The bit is then cleared and the exponent */
    /* adjusted by the number of positions shifted. A more */
    /* thorough discussion is in vzctdbl.c. */

    if ((xport1 & 0x7fffffff) == 0x7fffffff && xport2 == 0xffffffff) {
        ieee1 = (xport1 & 0x80000000) | 0x7ff00000;
        ieee2 = 0;
        goto doret;
    }

    /* Get the first half of the ibm number without the exponent */
    /* into the ieee number */
    ieee1 = xport1 & 0x00ffffff;

    /* get the second half of the ibm number into the second half */
    /* of the ieee number . If both halves were 0. then just */
    /* return since the ieee number is zero. */
    if ((!(ieee2 = xport2)) && !xport1)
        return;

    /* The fraction bit to the left of the binary point in the */
    /* ieee format was set and the number was shifted 0, 1, 2, or */
    /* 3 places. This will tell us how to adjust the ibm exponent */
    /* to be a power of 2 ieee exponent and how to shift the */
    /* fraction bits to restore the correct magnitude. */

    if ((nib = (int)xport1) & 0x00800000) {
        shift = 3;
    } else if (nib & 0x00400000) {
        shift = 2;
    } else if (nib & 0x00200000) {
        shift = 1;
    } else {
        shift = 0;
    }

    if (shift) {
        /* shift the ieee number down the correct number of places */
        /* then set the second half of the ieee number to be the */
        /* second half of the ibm number shifted appropriately, */
        /* ored with the bits from the first half that would have */
        /* been shifted in if we could shift a double. All we are */
        /* worried about are the low order 3 bits of the first */
        /* half since we're only shifting by 1, 2, or 3. */
        ieee1 >>= shift;
        ieee2 = (xport2 >> shift) |
            ((xport1 & 0x00000007) << (29 + (3 - shift)));
    }

    /* clear the 1 bit to the left of the binary point */
    ieee1 &= 0xffefffff;

    /* set the exponent of the ieee number to be the actual */
    /* exponent plus the shift count + 1023. Or this into the */
    /* first half of the ieee number. The ibm exponent is excess */
    /* 64 but is adjusted by 65 since during conversion to ibm */
    /* format the exponent is incremented by 1 and the fraction */
    /* bits left 4 positions to the right of the radix point. */
    ieee1 |=
        (((((int32_t)(*temp & 0x7f) - 65) * 4) + shift + 1023) << 20) |
        (xport1 & 0x80000000);

doret:
    memreverse(&ieee1,sizeof(uint32_t));
    memcpy(ieee,&ieee1,sizeof(uint32_t));
    memreverse(&ieee2,sizeof(uint32_t));
    memcpy(ieee+4,&ieee2,sizeof(uint32_t));
    return;
}

/*-------------------------------------------------------------*/
/* Name: ieee2xpt */
/* Purpose: converts IEEE to transport */
/* Usage: rc = ieee2xpt(to_ieee,p_data); */
/* Notes: this routine is an adaptation of the wzctdbl routine */
/* from the Apollo. */
/*-------------------------------------------------------------*/

void ieee2xpt(unsigned char *ieee, unsigned char *xport) {
    register int shift;
    unsigned char misschar;
    int ieee_exp;
    uint32_t xport1,xport2;
    uint32_t ieee1 = 0;
    uint32_t ieee2 = 0;

    char ieee8[8];

    memcpy(ieee8,ieee,8);

    /*------get 2 longs for shifting------------------------------*/
    memcpy(&ieee1,ieee8,sizeof(uint32_t));
    memreverse(&ieee1,sizeof(uint32_t));
    memcpy(&ieee2,ieee8+4,sizeof(uint32_t));
    memreverse(&ieee2,sizeof(uint32_t));

    memset(xport,0,8);

    /*-----if IEEE value is missing (1st 2 bytes are FFFF)-----*/
    if (*ieee8 == (char)0xff && ieee8[1] == (char)0xff) {
        misschar = ~ieee8[2];
        *xport = (misschar == 0xD2) ? 0x6D : misschar;
        return;
    }

    /**************************************************************/
    /* Translate IEEE floating point number into IBM format float */
    /* */
    /* IEEE format: */
    /* */
    /* 6 5 0 */
    /* 3 1 0 */
    /* */
    /* SEEEEEEEEEEEMMMM ........ MMMM */
    /* */
    /* Sign bit, 11 bit exponent, 52 fraction. Exponent is excess */
    /* 1023. The fraction is multiplied by a power of 2 of the */
    /* actual exponent. Normalized floating point numbers are */
    /* represented with the binary point immediately to the left */
    /* of the fraction with an implied "1" to the left of the */
    /* binary point. */
    /* */
    /* IBM format: */
    /* */
    /* 6 5 0 */
    /* 3 5 0 */
    /* */
    /* SEEEEEEEMMMM ......... MMMM */
    /* */
    /* Sign bit, 7 bit exponent, 56 bit fraction. Exponent is */
    /* excess 64. The fraction is multiplied by a power of 16 of */
    /* of the actual exponent. Normalized floating point numbers */
    /* are presented with the radix point immediately to the left */
    /* of the high order hex fraction digit. */
    /* */
    /* How do you translate from local to IBM format? */
    /* */
    /* The ieee format gives you a number that has a power of 2 */
    /* exponent and a fraction of the form "1.<fraction bits>". */
    /* The first step is to get that "1" bit back into the */
    /* fraction. Right shift it down 1 position, set the high */
    /* order bit and reduce the binary exponent by 1. Now we have */

    /* a fraction that looks like ".1<fraction bits>" and it's */
    /* ready to be shoved into ibm format. The ibm fraction has 4 */
    /* more bits than the ieee, the ieee fraction must therefore */
    /* be shifted left 4 positions before moving it in. We must */
    /* also correct the fraction bits to account for the loss of 2*/
    /* bits when converting from a binary exponent to a hex one */
    /* (>> 2). We must shift the fraction left for 0, 1, 2, or 3 */
    /* positions to maintain the proper magnitude. Doing */
    /* conversion this way would tend to lose bits in the fraction*/
    /* which is not desirable or necessary if we cheat a bit. */
    /* First of all, we know that we are going to have to shift */
    /* the ieee fraction left 4 places to put it in the right */
    /* position; we won't do that, we'll just leave it where it is*/
    /* and increment the ibm exponent by one, this will have the */
    /* same effect and we won't have to do any shifting. Now, */
    /* since we have 4 bits in front of the fraction to work with,*/
    /* we won't lose any bits. We set the bit to the left of the */
    /* fraction which is the implicit "1" in the ieee fraction. We*/
    /* then adjust the fraction to account for the loss of bits */
    /* when going to a hex exponent. This adjustment will never */
    /* involve shifting by more than 3 positions so no bits are */
    /* lost. */

    /* Get ieee number less the exponent into the first half of */
    /* the ibm number */

    xport1 = ieee1 & 0x000fffff;

    /* get the second half of the number into the second half of */
    /* the ibm number and see if both halves are 0. If so, ibm is */
    /* also 0 and we just return */

    if ((!(xport2 = ieee2)) && !ieee1) {
        ieee_exp = 0;
        goto doret;
    }

    /* get the actual exponent value out of the ieee number. The */
    /* ibm fraction is a power of 16 and the ieee fraction a power*/
    /* of 2 (16 ** n == 2 ** 4n). Save the low order 2 bits since */
    /* they will get lost when we divide the exponent by 4 (right */
    /* shift by 2) and we will have to shift the fraction by the */
    /* appropriate number of bits to keep the proper magnitude. */
    shift = (int)
        (ieee_exp = (int)(((ieee1 >> 16) & 0x7ff0) >> 4) - 1023)
        & 3;
    /* the ieee format has an implied "1" immediately to the left */
    /* of the binary point. Show it in here. */
    xport1 |= 0x00100000;
    if (shift)
    {
        /* set the first half of the ibm number by shifting it left */

        /* the appropriate number of bits and oring in the bits */
        /* from the lower half that would have been shifted in (if */
        /* we could shift a double). The shift count can never */
        /* exceed 3, so all we care about are the high order 3 */
        /* bits. We don't want sign extention so make sure it's an */
        /* unsigned char. We'll shift either5, 6, or 7 places to */
        /* keep 3, 2, or 1 bits. After that, shift the second half */
        /* of the number the right number of places. We always get */
        /* zero fill on left shifts. */
        xport1 = (xport1 << shift) |
            ((unsigned char) (((ieee2 >> 24) & 0xE0) >>
                (5 + (3 - shift))));

        xport2 <<= shift;
    }

    /* Now set the ibm exponent and the sign of the fraction. The */
    /* power of 2 ieee exponent must be divided by 4 and made */
    /* excess 64 (we add 65 here because of the poisition of the */
    /* fraction bits, essentially 4 positions lower than they */
    /* should be so we incrment the ibm exponent). */

    xport1 |=

        (((ieee_exp >>2) + 65) | ((ieee1 >> 24) & 0x80)) << 24;
    /* If the ieee exponent is greater than 248 or less than -260, */
    /* then it cannot fit in the ibm exponent field. Send back the */
    /* appropriate flag. */

doret:
    if (ieee_exp < -260) {
        memset(xport,0x00,8);
    } else if (ieee_exp > 248) {
        memset(xport+1,0xFF,7);

        *xport = 0x7F | ((ieee1 >> 24) & 0x80);
    } else {
        memreverse(&xport1,sizeof(uint32_t));
        memcpy(xport,&xport1,sizeof(uint32_t));
        memreverse(&xport2,sizeof(uint32_t));
        memcpy(xport+4,&xport2,sizeof(uint32_t));
    }
    return;
}


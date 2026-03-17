/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
/* $Id: string.c,v 1.76 2010/05/26 21:43:33 dmh Exp $ */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "ncdispatch.h"
#include "rnd.h"
#include "ncutf8.h"

/* There are 3 levels of UTF8 checking: 1=> (exact)validating 2=>relaxed
   and 3=>very relaxed
*/
/* Use semi-relaxed check */
#define UTF8_CHECK 2

/*
 * Free string, and, if needed, its values.
 * Formerly
NC_free_string()
 */
void
free_NC_string(NC_string *ncstrp)
{
	if(ncstrp==NULL)
		return;
	free(ncstrp);
}


static int
nextUTF8(const char* cp)
{
    /*  The goal here is to recognize the length of each
	multibyte utf8 character sequence and skip it.
        Again, we assume that every non-ascii character is legal.
        We can define three possible tests of decreasing correctness
        (in the sense that the least correct will allow some sequences that
        are technically illegal UTF8).
        As Regular expressions they are as follows:
        1. most correct:
            UTF8   ([\xC2-\xDF][\x80-\xBF])                       \
                 | (\xE0[\xA0-\xBF][\x80-\xBF])                   \
                 | ([\xE1-\xEC][\x80-\xBF][\x80-\xBF])            \
                 | (\xED[\x80-\x9F][\x80-\xBF])                   \
                 | ([\xEE-\xEF][\x80-\xBF][\x80-\xBF])            \
                 | (\xF0[\x90-\xBF][\x80-\xBF][\x80-\xBF])        \
                 | ([\xF1-\xF3][\x80-\xBF][\x80-\xBF][\x80-\xBF]) \
                 | (\xF4[\x80-\x8F][\x80-\xBF][\x80-\xBF])        \

        2. partially relaxed:
            UTF8 ([\xC0-\xDF][\x80-\xBF])
                 |([\xE0-\xEF][\x80-\xBF][\x80-\xBF])
                 |([\xF0-\xF7][\x80-\xBF][\x80-\xBF][\x80-\xBF])

        3. The most relaxed version of UTF8:
            UTF8 ([\xC0-\xD6].)|([\xE0-\xEF]..)|([\xF0-\xF7]...)

        We use #2 here.

	The tests are derived from the table at
	    http://www.w3.org/2005/03/23-lex-U
    */

/* Define a test macro to test against a range */
#define RANGE(c,lo,hi) (((uchar)c) >= lo && ((uchar)c) <= hi)
/* Define a common RANGE */
#define RANGE0(c) RANGE(c,0x80,0xBF)

    int ch0;

    int skip = -1; /* assume failed */

    ch0 = (uchar)*cp;
    if(ch0 <= 0x7f) skip = 1; /* remove ascii case */
    else

#if UTF8_CHECK == 2
    /* Do relaxed validation check */
    if(RANGE(ch0,0xC0,0XDF)) {/* 2-bytes, but check */
        if(cp[1] != 0 && RANGE0(cp[1]))
		skip = 2; /* two bytes */
    } else if(RANGE(ch0,0xE0,0XEF)) {/* 3-bytes, but check */
        if(cp[1] != 0 && RANGE0(cp[1]) && cp[2] != 0 && RANGE0(cp[1]))
		skip = 3; /* three bytes */
    } else if(RANGE(ch0,0xF0,0XF7)) {/* 3-bytes, but check */
        if(cp[1] != 0 && RANGE0(cp[1]) && cp[2] != 0
           && RANGE0(cp[1]) && cp[3] != 0 && RANGE0(cp[1]))
		skip = 4; /* four bytes*/
    }
#elif UTF8_CHECK == 1
    /* Do exact validation check */
    if(RANGE(ch0,0xC2,0xDF)) {/* non-overlong 2-bytes */
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE0(ch1)) skip = 2;
    } else if((ch0 == 0xE0)) {/* 3-bytes, not overlong */
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE(ch1,0xA0,0xBF)) {
	    int ch2 = (uchar)cp[2];
	    if(ch2 != 0 && RANGE0(ch2)) skip = 3;
    } else if((ch0 == 0xED)) {/* 3-bytes minus surrogates */
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE(ch1,0x80,0x9f)) {
	    int ch2 = (uchar)cp[2];
	    if(ch2 != 0 && RANGE0(ch2)) skip = 3;
    } else if(RANGE(ch0,0xE1,0xEC) || ch0 == 0xEE || ch0 == 0xEF)
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE0(ch1)) {
	    int ch2 = (uchar)cp[2];
	    if(ch2 != 0 && RANGE0(ch2)) skip = 3;
	}
    } else if((ch0 == 0xF0)) {/* planes 1-3 */
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE(ch1,0x90,0xBF) {
	    int ch2 = (uchar)cp[2];
	    if(ch2 != 0 && RANGE0(ch2)) {
	        int ch3 = (uchar)cp[3];
	        if(ch3 != 0 && RANGE0(ch3)) skip = 4;
	    }
	}
    } else if((ch0 == 0xF4)) {/* plane 16 */
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE0(ch1)) {
	    int ch2 = (uchar)cp[2];
	    if(ch2 != 0 && RANGE0(ch2)) {
	        int ch3 = (uchar)cp[3];
	        if(ch3 != 0 && RANGE0(ch3)) skip = 4;
	    }
	}
    } else if(RANGE(ch0,0xF1,0xF3) { /* planes 4-15 */
	int ch1 = (uchar)cp[1];
	if(ch1 != 0 && RANGE0(ch1)) {
	    int ch2 = (uchar)cp[2];
	    if(ch2 != 0 && RANGE0(ch2)) {
	        int ch3 = (uchar)cp[3];
	        if(ch3 != 0 && RANGE0(ch3)) skip = 4;
	    }
	}
    }
#else
#error "Must Define UTF8_CHECK as 1 or 2"
#endif
    return skip;
}


/*
 * Verify that a name string is valid syntax.  The allowed name
 * syntax (in RE form) is:
 *
 * ([a-zA-Z0-9_]|{UTF8})([^\x00-\x1F\x7F/]|{UTF8})*
 *
 * where UTF8 represents a multibyte UTF-8 encoding.  Also, no
 * trailing spaces are permitted in names.  This definition
 * must be consistent with the one in ncgen.l.  We do not allow '/'
 * because HDF5 does not permit slashes in names as slash is used as a
 * group separator.  If UTF-8 is supported, then a multi-byte UTF-8
 * character can occur anywhere within an identifier.  We later
 * normalize UTF-8 strings to NFC to facilitate matching and queries.
 */
int
NC_check_name(const char *name)
{
	int skip;
	int ch;
	const char *cp = name;
	int stat;

	assert(name != NULL);

	if(*name == 0		/* empty names disallowed */
	   || strchr(cp, '/'))	/* '/' can't be in a name */
		goto fail;

	/* check validity of any UTF-8 */
	stat = nc_utf8_validate((const unsigned char *)name);
	if (stat != NC_NOERR)
	    goto fail;

	/* First char must be [a-z][A-Z][0-9]_ | UTF8 */
	ch = (uchar)*cp;
	if(ch <= 0x7f) {
	    if(   !('A' <= ch && ch <= 'Z')
	       && !('a' <= ch && ch <= 'z')
	       && !('0' <= ch && ch <= '9')
	       && ch != '_' )
		goto fail;
	    cp++;
	} else {
	    if((skip = nextUTF8(cp)) < 0)
		goto fail;
	    cp += skip;
	}

	while(*cp != 0) {
	    ch = (uchar)*cp;
	    /* handle simple 0x00-0x7f characters here */
	    if(ch <= 0x7f) {
                if( ch < ' ' || ch > 0x7E) /* control char or DEL */
		  goto fail;
		cp++;
	    } else {
		if((skip = nextUTF8(cp)) < 0) goto fail;
		cp += skip;
	    }
	    if(cp - name > NC_MAX_NAME)
		return NC_EMAXNAME;
	}
	if(ch <= 0x7f && isspace(ch)) /* trailing spaces disallowed */
	    goto fail;
	return NC_NOERR;
fail:
        return NC_EBADNAME;
}


/*
 * Allocate a NC_string structure large enough
 * to hold slen characters.
 * Formerly
NC_new_string(count, str)
 */

NC_string *
new_NC_string(size_t slen, const char *str)
{
	NC_string *ncstrp;
	size_t sz;

	if (slen > SIZE_MAX - M_RNDUP(sizeof(NC_string)) - 1)
		return NULL;
	
	sz = M_RNDUP(sizeof(NC_string)) + slen + 1;

#if 0
	sz = _RNDUP(sz, X_ALIGN);
#endif

	ncstrp = (NC_string *)malloc(sz);
	if( ncstrp == NULL )
		return NULL;
	(void) memset(ncstrp, 0, sz);

	ncstrp->nchars = sz - M_RNDUP(sizeof(NC_string)) - 1;
	assert(ncstrp->nchars + 1 > slen);
	ncstrp->cp = (char *)ncstrp + M_RNDUP(sizeof(NC_string));

	if(str != NULL && *str != 0)
	{
		(void) strncpy(ncstrp->cp, str, ncstrp->nchars +1);
		ncstrp->cp[ncstrp->nchars] = 0;
	}

	return(ncstrp);
}


/*
 * If possible, change the value of an NC_string to 'str'.
 *
 * Formerly
NC_re_string()
 */

int
   set_NC_string(NC_string *ncstrp, const char *str)
 {
	size_t slen;

	assert(str != NULL && *str != 0);

	slen = strlen(str);

	if(ncstrp->nchars < slen)
		return NC_ENOTINDEFINE;

	strncpy(ncstrp->cp, str, ncstrp->nchars);
	/* Don't adjust ncstrp->nchars, it includes extra space in the
	 * header for potential later expansion of string. */

	return NC_NOERR;
}

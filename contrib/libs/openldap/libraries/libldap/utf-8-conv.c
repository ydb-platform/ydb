/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* Portions Copyright (C) 1999, 2000 Novell, Inc. All Rights Reserved.
 * 
 * THIS WORK IS SUBJECT TO U.S. AND INTERNATIONAL COPYRIGHT LAWS AND
 * TREATIES. USE, MODIFICATION, AND REDISTRIBUTION OF THIS WORK IS SUBJECT
 * TO VERSION 2.0.1 OF THE OPENLDAP PUBLIC LICENSE, A COPY OF WHICH IS
 * AVAILABLE AT HTTP://WWW.OPENLDAP.ORG/LICENSE.HTML OR IN THE FILE "LICENSE"
 * IN THE TOP-LEVEL DIRECTORY OF THE DISTRIBUTION. ANY USE OR EXPLOITATION
 * OF THIS WORK OTHER THAN AS AUTHORIZED IN VERSION 2.0.1 OF THE OPENLDAP
 * PUBLIC LICENSE, OR OTHER PRIOR WRITTEN CONSENT FROM NOVELL, COULD SUBJECT
 * THE PERPETRATOR TO CRIMINAL AND CIVIL LIABILITY. 
 *---
 * Note: A verbatim copy of version 2.0.1 of the OpenLDAP Public License 
 * can be found in the file "build/LICENSE-2.0.1" in this distribution
 * of OpenLDAP Software.
 */

/*
 * UTF-8 Conversion Routines
 *
 * These routines convert between Wide Character and UTF-8,
 * or between MultiByte and UTF-8 encodings.
 *
 * Both single character and string versions of the functions are provided.
 * All functions return -1 if the character or string cannot be converted.
 */

#include "portable.h"

#if SIZEOF_WCHAR_T >= 4
/* These routines assume ( sizeof(wchar_t) >= 4 ) */

#include <stdio.h>
#include <ac/stdlib.h>		/* For wctomb, wcstombs, mbtowc, mbstowcs */
#include <ac/string.h>
#include <ac/time.h>		/* for time_t */

#include "ldap-int.h"

#include <ldap_utf8.h>

static unsigned char mask[] = { 0, 0x7f, 0x1f, 0x0f, 0x07, 0x03, 0x01 };


/*-----------------------------------------------------------------------------
					UTF-8 Format Summary

ASCII chars 						7 bits
    0xxxxxxx
    
2-character UTF-8 sequence:        11 bits
    110xxxxx  10xxxxxx

3-character UTF-8                  16 bits
    1110xxxx  10xxxxxx  10xxxxxx   
    
4-char UTF-8                       21 bits 
    11110xxx  10xxxxxx  10xxxxxx  10xxxxxx
    
5-char UTF-8                       26 bits
    111110xx  10xxxxxx  10xxxxxx  10xxxxxx  10xxxxxx
    
6-char UTF-8                       31 bits
    1111110x  10xxxxxx  10xxxxxx  10xxxxxx  10xxxxxx  10xxxxxx
    
Unicode address space   (0 - 0x10FFFF)    21 bits
ISO-10646 address space (0 - 0x7FFFFFFF)  31 bits

Note: This code does not prevent UTF-8 sequences which are longer than
      necessary from being decoded.
*/

/*----------------------------------------------------------------------------- 
   Convert a UTF-8 character to a wide char. 
   Return the length of the UTF-8 input character in bytes.
*/
int
ldap_x_utf8_to_wc ( wchar_t *wchar, const char *utf8char )
{
	int utflen, i;
	wchar_t ch;

	if (utf8char == NULL) return -1;

	/* Get UTF-8 sequence length from 1st byte */
	utflen = LDAP_UTF8_CHARLEN2(utf8char, utflen);
	
	if( utflen==0 || utflen > (int)LDAP_MAX_UTF8_LEN ) return -1;

	/* First byte minus length tag */
	ch = (wchar_t)(utf8char[0] & mask[utflen]);
	
	for(i=1; i < utflen; i++) {
		/* Subsequent bytes must start with 10 */
		if ((utf8char[i] & 0xc0) != 0x80) return -1;
	
		ch <<= 6;			/* 6 bits of data in each subsequent byte */
		ch |= (wchar_t)(utf8char[i] & 0x3f);
	}
	
	if (wchar) *wchar = ch;

	return utflen;
}

/*-----------------------------------------------------------------------------
   Convert a UTF-8 string to a wide char string.
   No more than 'count' wide chars will be written to the output buffer.
   Return the size of the converted string in wide chars, excl null terminator.
*/
int
ldap_x_utf8s_to_wcs ( wchar_t *wcstr, const char *utf8str, size_t count )
{
	size_t wclen = 0;
	int utflen, i;
	wchar_t ch;


	/* If input ptr is NULL or empty... */
	if (utf8str == NULL || !*utf8str) {
		if ( wcstr )
			*wcstr = 0;
		return 0;
	}

	/* Examine next UTF-8 character.  If output buffer is NULL, ignore count */
	while ( *utf8str && (wcstr==NULL || wclen<count) ) {
		/* Get UTF-8 sequence length from 1st byte */
		utflen = LDAP_UTF8_CHARLEN2(utf8str, utflen);
		
		if( utflen==0 || utflen > (int)LDAP_MAX_UTF8_LEN ) return -1;

		/* First byte minus length tag */
		ch = (wchar_t)(utf8str[0] & mask[utflen]);
		
		for(i=1; i < utflen; i++) {
			/* Subsequent bytes must start with 10 */
			if ((utf8str[i] & 0xc0) != 0x80) return -1;
		
			ch <<= 6;			/* 6 bits of data in each subsequent byte */
			ch |= (wchar_t)(utf8str[i] & 0x3f);
		}
		
		if (wcstr) wcstr[wclen] = ch;
		
		utf8str += utflen;	/* Move to next UTF-8 character */
		wclen++;			/* Count number of wide chars stored/required */
	}

	/* Add null terminator if there's room in the buffer. */
	if (wcstr && wclen < count) wcstr[wclen] = 0;

	return wclen;
}


/*----------------------------------------------------------------------------- 
   Convert one wide char to a UTF-8 character.
   Return the length of the converted UTF-8 character in bytes.
   No more than 'count' bytes will be written to the output buffer.
*/
int
ldap_x_wc_to_utf8 ( char *utf8char, wchar_t wchar, size_t count )
{
	int len=0;

	if (utf8char == NULL)   /* Just determine the required UTF-8 char length. */
	{						/* Ignore count */
		if( wchar < 0 )
			return -1;
		if( wchar < 0x80 )
			return 1;
		if( wchar < 0x800 )
			return 2; 
		if( wchar < 0x10000 )
			return 3;
		if( wchar < 0x200000 ) 
			return 4;
		if( wchar < 0x4000000 ) 
			return 5;
#if SIZEOF_WCHAR_T > 4
		/* UL is not strictly needed by ANSI C */
		if( wchar < (wchar_t)0x80000000UL )
#endif /* SIZEOF_WCHAR_T > 4 */
			return 6;
		return -1;
	}

	
	if ( wchar < 0 ) {				/* Invalid wide character */
		len = -1;

	} else if( wchar < 0x80 ) {
		if (count >= 1) {
			utf8char[len++] = (char)wchar;
		}

	} else if( wchar < 0x800 ) {
		if (count >=2) {
			utf8char[len++] = 0xc0 | ( wchar >> 6 );
			utf8char[len++] = 0x80 | ( wchar & 0x3f );
		}

	} else if( wchar < 0x10000 ) {
		if (count >= 3) {	
			utf8char[len++] = 0xe0 | ( wchar >> 12 );
			utf8char[len++] = 0x80 | ( (wchar >> 6) & 0x3f );
			utf8char[len++] = 0x80 | ( wchar & 0x3f );
		}
	
	} else if( wchar < 0x200000 ) {
		if (count >= 4) {
			utf8char[len++] = 0xf0 | ( wchar >> 18 );
			utf8char[len++] = 0x80 | ( (wchar >> 12) & 0x3f );
			utf8char[len++] = 0x80 | ( (wchar >> 6) & 0x3f );
			utf8char[len++] = 0x80 | ( wchar & 0x3f );
		}

	} else if( wchar < 0x4000000 ) {
		if (count >= 5) {
			utf8char[len++] = 0xf8 | ( wchar >> 24 );
			utf8char[len++] = 0x80 | ( (wchar >> 18) & 0x3f );
			utf8char[len++] = 0x80 | ( (wchar >> 12) & 0x3f );
			utf8char[len++] = 0x80 | ( (wchar >> 6) & 0x3f );
			utf8char[len++] = 0x80 | ( wchar & 0x3f );
		}

	} else
#if SIZEOF_WCHAR_T > 4
		/* UL is not strictly needed by ANSI C */
		if( wchar < (wchar_t)0x80000000UL )
#endif /* SIZEOF_WCHAR_T > 4 */
	{
		if (count >= 6) {
			utf8char[len++] = 0xfc | ( wchar >> 30 );
			utf8char[len++] = 0x80 | ( (wchar >> 24) & 0x3f );
			utf8char[len++] = 0x80 | ( (wchar >> 18) & 0x3f );
			utf8char[len++] = 0x80 | ( (wchar >> 12) & 0x3f );
			utf8char[len++] = 0x80 | ( (wchar >> 6) & 0x3f );
			utf8char[len++] = 0x80 | ( wchar & 0x3f );
		}

#if SIZEOF_WCHAR_T > 4
	} else {
		len = -1;
#endif /* SIZEOF_WCHAR_T > 4 */
	}
	
	return len;

}


/*-----------------------------------------------------------------------------
   Convert a wide char string to a UTF-8 string.
   No more than 'count' bytes will be written to the output buffer.
   Return the # of bytes written to the output buffer, excl null terminator.
*/
int
ldap_x_wcs_to_utf8s ( char *utf8str, const wchar_t *wcstr, size_t count )
{
	int len = 0;
	int n;
	char *p = utf8str;
	wchar_t empty = 0;		/* To avoid use of L"" construct */

	if (wcstr == NULL)		/* Treat input ptr NULL as an empty string */
		wcstr = &empty;

	if (utf8str == NULL)	/* Just compute size of output, excl null */
	{
		while (*wcstr)
		{
			/* Get UTF-8 size of next wide char */
			n = ldap_x_wc_to_utf8( NULL, *wcstr++, LDAP_MAX_UTF8_LEN);
			if (n == -1)
				return -1;
			len += n;
		}

		return len;
	}

	
	/* Do the actual conversion. */

	n = 1;					/* In case of empty wcstr */
	while (*wcstr)
	{
		n = ldap_x_wc_to_utf8( p, *wcstr++, count);
		
		if (n <= 0)  		/* If encoding error (-1) or won't fit (0), quit */
			break;
		
		p += n;
		count -= n;			/* Space left in output buffer */
	}

	/* If not enough room for last character, pad remainder with null
	   so that return value = original count, indicating buffer full. */
	if (n == 0)
	{
		while (count--)
			*p++ = 0;
	}

	/* Add a null terminator if there's room. */
	else if (count)
		*p = 0;

	if (n == -1)			/* Conversion encountered invalid wide char. */
		return -1;

	/* Return the number of bytes written to output buffer, excl null. */ 
	return (p - utf8str);
}

#ifdef ANDROID
int wctomb(char *s, wchar_t wc) { return wcrtomb(s,wc,NULL); }
int mbtowc(wchar_t *pwc, const char *s, size_t n) { return mbrtowc(pwc, s, n, NULL); }
#endif

/*-----------------------------------------------------------------------------
   Convert a UTF-8 character to a MultiByte character.
   Return the size of the converted character in bytes.
*/
int
ldap_x_utf8_to_mb ( char *mbchar, const char *utf8char,
		int (*f_wctomb)(char *mbchar, wchar_t wchar) )
{
	wchar_t wchar;
	int n;
	char tmp[6];				/* Large enough for biggest multibyte char */

	if (f_wctomb == NULL)		/* If no conversion function was given... */
		f_wctomb = wctomb;		/*    use the local ANSI C function */
 
	/* First convert UTF-8 char to a wide char */
	n = ldap_x_utf8_to_wc( &wchar, utf8char);

	if (n == -1)
		return -1;		/* Invalid UTF-8 character */

	if (mbchar == NULL)
		n = f_wctomb( tmp, wchar );
	else
		n = f_wctomb( mbchar, wchar);

	return n;
}

/*-----------------------------------------------------------------------------
   Convert a UTF-8 string to a MultiByte string.
   No more than 'count' bytes will be written to the output buffer.
   Return the size of the converted string in bytes, excl null terminator.
*/
int
ldap_x_utf8s_to_mbs ( char *mbstr, const char *utf8str, size_t count,
		size_t (*f_wcstombs)(char *mbstr, const wchar_t *wcstr, size_t count) )
{
	wchar_t *wcs;
	size_t wcsize;
    int n;

	if (f_wcstombs == NULL)		/* If no conversion function was given... */
		f_wcstombs = wcstombs;	/*    use the local ANSI C function */
 
	if (utf8str == NULL || *utf8str == 0)	/* NULL or empty input string */
	{
		if (mbstr)
			*mbstr = 0;
		return 0;
	}

/* Allocate memory for the maximum size wchar string that we could get. */
	wcsize = strlen(utf8str) + 1;
	wcs = (wchar_t *)LDAP_MALLOC(wcsize * sizeof(wchar_t));
	if (wcs == NULL)
		return -1;				/* Memory allocation failure. */

	/* First convert the UTF-8 string to a wide char string */
	n = ldap_x_utf8s_to_wcs( wcs, utf8str, wcsize);

	/* Then convert wide char string to multi-byte string */
	if (n != -1)
	{
		n = f_wcstombs(mbstr, wcs, count);
	}

	LDAP_FREE(wcs);

	return n;
}

/*-----------------------------------------------------------------------------
   Convert a MultiByte character to a UTF-8 character.
   'mbsize' indicates the number of bytes of 'mbchar' to check.
   Returns the number of bytes written to the output character.
*/
int
ldap_x_mb_to_utf8 ( char *utf8char, const char *mbchar, size_t mbsize,
		int (*f_mbtowc)(wchar_t *wchar, const char *mbchar, size_t count) )
{
    wchar_t wchar;
    int n;

	if (f_mbtowc == NULL)		/* If no conversion function was given... */
		f_mbtowc = mbtowc;		/*    use the local ANSI C function */
 
    if (mbsize == 0)				/* 0 is not valid. */
        return -1;

    if (mbchar == NULL || *mbchar == 0)
    {
        if (utf8char)
            *utf8char = 0;
        return 1;
    }

	/* First convert the MB char to a Wide Char */
	n = f_mbtowc( &wchar, mbchar, mbsize);

	if (n == -1)
		return -1;

	/* Convert the Wide Char to a UTF-8 character. */
	n = ldap_x_wc_to_utf8( utf8char, wchar, LDAP_MAX_UTF8_LEN);

	return n;
}


/*-----------------------------------------------------------------------------
   Convert a MultiByte string to a UTF-8 string.
   No more than 'count' bytes will be written to the output buffer.
   Return the size of the converted string in bytes, excl null terminator.
*/   
int
ldap_x_mbs_to_utf8s ( char *utf8str, const char *mbstr, size_t count,
		size_t (*f_mbstowcs)(wchar_t *wcstr, const char *mbstr, size_t count) )
{
	wchar_t *wcs;
	int n;
	size_t wcsize;

	if (mbstr == NULL)		   /* Treat NULL input string as an empty string */
		mbstr = "";

	if (f_mbstowcs == NULL)		/* If no conversion function was given... */
		f_mbstowcs = mbstowcs;	/*    use the local ANSI C function */
 
	/* Allocate memory for the maximum size wchar string that we could get. */
	wcsize = strlen(mbstr) + 1;
	wcs = (wchar_t *)LDAP_MALLOC( wcsize * sizeof(wchar_t) );
	if (wcs == NULL)
		return -1;

	/* First convert multi-byte string to a wide char string */
	n = f_mbstowcs(wcs, mbstr, wcsize);

	/* Convert wide char string to UTF-8 string */
	if (n != -1)
	{
		n = ldap_x_wcs_to_utf8s( utf8str, wcs, count);
	}

	LDAP_FREE(wcs);

	return n;	
}

#endif /* SIZEOF_WCHAR_T >= 4 */

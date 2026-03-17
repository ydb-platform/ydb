/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#include "config.h"
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include "netcdf.h"
#include "ncutf8.h"
#include "utf8proc.h"

/* Provide a wrapper around whatever utf8 library we use. */

/*
 * Check validity of a UTF8 encoded null-terminated byte string.
 * Return codes:
 * NC_NOERR -- string is valid utf8
 * NC_ENOMEM -- out of memory
 * NC_EINVAL -- invalid argument or internal error
 * NC_EBADNAME-- not valid utf8
 */

int nc_utf8_validate(const unsigned char* name)
{
    int ncstat = NC_NOERR;
    const nc_utf8proc_uint8_t *str;
    nc_utf8proc_ssize_t nchars = -1;
    nc_utf8proc_int32_t codepoint;
    nc_utf8proc_ssize_t count;

    str = (const nc_utf8proc_uint8_t*)name;
    while(*str) {
        count = nc_utf8proc_iterate(str,nchars,&codepoint);
	if(count < 0) {
	    switch (count) {
	    case UTF8PROC_ERROR_NOMEM:
	    case UTF8PROC_ERROR_OVERFLOW:
		ncstat = NC_ENOMEM;
		break;
	    case UTF8PROC_ERROR_INVALIDOPTS:
		ncstat = NC_EINVAL;
		break;
	    case UTF8PROC_ERROR_INVALIDUTF8:
	    case UTF8PROC_ERROR_NOTASSIGNED:
	    default:
		ncstat = NC_EBADNAME;
		break;
	    }
	    goto done;
	} else { /* move to next char */
	    str += count;
	}
    }
done:
    return ncstat;
}

/*
 * Returns a pointer to newly allocated memory of a
 * normalized version of the null-terminated string 'str'.
 * Normalized string is returned in normalp argument;
 * caller must free.
 * Return codes:
 * NC_NOERR -- success
 * NC_ENOMEM -- out of memory
 * NC_EINVAL -- illegal argument or internal error
 * NC_EBADNAME -- other failure
 */
int
nc_utf8_normalize(const unsigned char* utf8, unsigned char** normalp)
{
    int ncstat = NC_NOERR;
    const nc_utf8proc_uint8_t* str = (const nc_utf8proc_uint8_t*)utf8;
    nc_utf8proc_uint8_t* retval = NULL;
    nc_utf8proc_ssize_t count;
    count = nc_utf8proc_map(str, 0, &retval, UTF8PROC_NULLTERM | UTF8PROC_STABLE | UTF8PROC_COMPOSE);
    if(count < 0) {/* error */
	switch (count) {
	case UTF8PROC_ERROR_NOMEM:
	case UTF8PROC_ERROR_OVERFLOW:
	ncstat = NC_ENOMEM;
	    break;
	case UTF8PROC_ERROR_INVALIDOPTS:
	    ncstat = NC_EINVAL;
	    break;
	case UTF8PROC_ERROR_INVALIDUTF8:
	case UTF8PROC_ERROR_NOTASSIGNED:
	default:
	    ncstat = NC_EBADNAME;
	    break;
	}
	goto done;
    } else
	if(normalp) *normalp = (unsigned char*)retval;
done:
    return ncstat;
}

/*
 * Convert a normalized utf8 string to utf16. This is approximate
 * because it just does the truncation version of conversion for
 * each 32-bit codepoint to get the corresponding utf16.
 * Return codes:
 * NC_NOERR -- success
 * NC_ENOMEM -- out of memory
 * NC_EINVAL -- invalid argument or internal error
 * NC_EBADNAME-- not valid utf16
 */

int nc_utf8_to_utf16(const unsigned char* s8, unsigned short** utf16p, size_t* len16p)
{
    int ncstat = NC_NOERR;
    const nc_utf8proc_uint8_t *str;
    nc_utf8proc_ssize_t nchars = -1;
    nc_utf8proc_int32_t codepoint;
    nc_utf8proc_ssize_t count;
    size_t len8, len16;
    unsigned short* utf16;
    unsigned short* p16;

    len8 = strlen((char*)s8);
    utf16 = (unsigned short*)malloc(sizeof(unsigned short)*(len8+1));
    if(utf16 == NULL) {
      ncstat = NC_ENOMEM;
      goto done;
    }
    str = (const nc_utf8proc_uint8_t*)s8;
    /* Walk the string and convert each codepoint */
    p16 = utf16;
    len16 = 0;
    while(*str) {
      count = nc_utf8proc_iterate(str,nchars,&codepoint);
      if(count < 0) {
	    switch (count) {
	    case UTF8PROC_ERROR_NOMEM:
	    case UTF8PROC_ERROR_OVERFLOW:
          ncstat = NC_ENOMEM;
          break;
	    case UTF8PROC_ERROR_INVALIDOPTS:
          ncstat = NC_EINVAL;
          break;
	    case UTF8PROC_ERROR_INVALIDUTF8:
	    case UTF8PROC_ERROR_NOTASSIGNED:
	    default:
          ncstat = NC_EBADNAME;
          break;
	    }
	    goto done;
      } else { /* move to next char */
	    /* Complain if top 16 bits not zero */
	    if((codepoint & (nc_utf8proc_int32_t)0xFFFF0000) != 0) {
	          ncstat = NC_EBADNAME;
	          goto done;
	    }
	    /* Truncate codepoint to 16 bits and store */
	    *p16++ = (unsigned short)(codepoint & 0x0000FFFF);
	    str += count;
	    len16++;
      }
    }
    *p16++ = (unsigned short)0;
    if(utf16p)
      *utf16p = utf16;
    else
      free(utf16);

    if(len16p) *len16p = len16;
 done:
    if(ncstat) free(utf16);
    return ncstat;
}

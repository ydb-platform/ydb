/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef NCUTF8_H
#define NCUTF8_H 1

#include "ncexternl.h"

/* Provide a wrapper around whatever utf8 library we use. */

/*
 * Check validity of a UTF8 encoded null-terminated byte string.
 * Return codes:
 * NC_NOERR -- string is valid utf8
 * NC_ENOMEM -- out of memory
 * NC_EBADNAME-- not valid utf8
 */
EXTERNL int nc_utf8_validate(const unsigned char * name);

/*
 * Apply NFC normalization to a string.
 * Returns a pointer to newly allocated memory of an NFC
 * normalized version of the null-terminated string 'str'.
 * Pointer to normalized string is returned in normalp argument;
 * caller must free.
 * Return codes:
 * NC_NOERR -- success
 * NC_ENOMEM -- out of memory
 * NC_EBADNAME -- other failure
 */
EXTERNL int nc_utf8_normalize(const unsigned char* str, unsigned char** normalp);

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

EXTERNL int nc_utf8_to_utf16(const unsigned char* s8, unsigned short** utf16p, size_t* lenp);

#endif /*NCUTF8_H*/

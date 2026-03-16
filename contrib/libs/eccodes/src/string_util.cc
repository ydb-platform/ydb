/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

// Compare two strings ignoring case.
// strcasecmp is not in the C standard. However, it's defined by
// 4.4BSD, POSIX.1-2001. So we use our own
int strcmp_nocase(const char* s1, const char* s2)
{
    const unsigned char *us1 = (const unsigned char*)s1,
                        *us2 = (const unsigned char*)s2;

    while (tolower(*us1) == tolower(*us2++)) {
        if (*us1++ == '\0')
            return (0);
    }
    return (tolower(*us1) - tolower(*--us2));
}

// Strip whitespace from the end of a string
void string_rtrim(char* s)
{
    size_t len = 0;
    if (!s)
        return;
    len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1]))
        len--;
    s[len] = '\0';
}

void string_lrtrim(char** x, int do_left, int do_right)
{
    DEBUG_ASSERT(do_left || do_right);
    if (do_left) {
        while (isspace(**x) && **x != '\0')
            (*x)++;
    }
    if (**x == '\0')
        return;
    if (do_right) {
        char* p = (*x) + strlen(*x) - 1;
        while (isspace(*p)) {
            *p = '\0';
            p--;
        }
        if (isspace(*p))
            *p = '\0';
    }
}

// Return the component after final slash
//  "/tmp/x"  -> "x"
//  "/tmp/"   -> ""
const char* extract_filename(const char* filepath)
{
    // Note: Windows users could pass in fwd slashes!
    // so have to check both separators
    const char* s = strrchr(filepath, '/');
    if (!s)
        s = strrchr(filepath, '\\');
    if (!s)
        return filepath;
    else
        return s + 1;
}

// Returns an array of strings the last of which is NULL.
// Note: The delimiter here is a 'string' but must be ONE character!
//       Splitting with several delimiters is not supported.
char** string_split(char* inputString, const char* delimiter)
{
    char** result       = NULL;
    char* p             = inputString;
    char* lastDelimiter = NULL;
    char* aToken        = NULL;
    char* lasts         = NULL;
    size_t numTokens    = 0;
    size_t strLength    = 0;
    size_t index        = 0;
    char delimiterChar  = 0;

    DEBUG_ASSERT(inputString);
    DEBUG_ASSERT(delimiter && (strlen(delimiter) == 1));
    delimiterChar = delimiter[0];
    while (*p) {
        const char ctmp = *p;
        if (ctmp == delimiterChar) {
            ++numTokens;
            lastDelimiter = p;
        }
        p++;
    }
    strLength = strlen(inputString);
    if (lastDelimiter < (inputString + strLength - 1)) {
        ++numTokens; // there is a trailing token
    }
    ++numTokens; // terminating NULL string to mark the end

    result = (char**)malloc(numTokens * sizeof(char*));
    ECCODES_ASSERT(result);

    // Start tokenizing
    aToken = strtok_r(inputString, delimiter, &lasts);
    while (aToken) {
        ECCODES_ASSERT(index < numTokens);
        *(result + index++) = strdup(aToken);
        aToken              = strtok_r(NULL, delimiter, &lasts);
    }
    ECCODES_ASSERT(index == numTokens - 1);
    *(result + index) = NULL;

    return result;
}

// Return GRIB_SUCCESS if we can convert 'input' to an integer, GRIB_INVALID_ARGUMENT otherwise.
// If 'strict' is 1 then disallow characters at the end which are not valid digits.
// E.g., in strict mode, "4i" will be rejected. Otherwise it will convert it to 4
int string_to_long(const char* input, long* output, int strict)
{
    const int base = 10;
    char* endptr;
    long val = 0;

    if (!input)
        return GRIB_INVALID_ARGUMENT;

    errno = 0;
    val   = strtol(input, &endptr, base);
    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
        (errno != 0 && val == 0)) {
        // perror("strtol");
        return GRIB_INVALID_ARGUMENT;
    }
    if (endptr == input) {
        // fprintf(stderr, "No digits were found\n");
        return GRIB_INVALID_ARGUMENT;
    }
    if (strict && *endptr != 0) {
        // fprintf(stderr, "Left over characters at the end; not a pure number\n");
        return GRIB_INVALID_ARGUMENT;
    }
    *output = val;
    return GRIB_SUCCESS;
}

// Return 1 if 's' ends with 'suffix', 0 otherwise
int string_ends_with(const char* s, const char* suffix)
{
    const size_t len1 = strlen(s);
    const size_t len2 = strlen(suffix);
    if (len2 > len1)
        return 0;

    if (strcmp(&s[len1 - len2], suffix) == 0)
        return 1;
    return 0;
}

int string_count_char(const char* str, char c)
{
    int i = 0, count = 0;
    DEBUG_ASSERT(str);
    for (i=0; str[i]; i++) {
        if (str[i] == c) count++;
    }
    return count;
}

const char* codes_get_product_name(ProductKind product)
{
    switch (product) {
        case PRODUCT_GRIB:
            return "GRIB";
        case PRODUCT_BUFR:
            return "BUFR";
        case PRODUCT_METAR:
            return "METAR";
        case PRODUCT_GTS:
            return "GTS";
        case PRODUCT_TAF:
            return "TAF";
        case PRODUCT_ANY:
            return "ANY";
    }
    return "unknown";
}

const char* grib_get_type_name(int type)
{
    switch (type) {
        case GRIB_TYPE_LONG:
            return "long";
        case GRIB_TYPE_STRING:
            return "string";
        case GRIB_TYPE_BYTES:
            return "bytes";
        case GRIB_TYPE_DOUBLE:
            return "double";
        case GRIB_TYPE_LABEL:
            return "label";
        case GRIB_TYPE_SECTION:
            return "section";
    }
    return "unknown";
}

// Replace all occurrences of character in string.
// Returns pointer to the NUL byte at the end of 's'
char* string_replace_char(char *s, char oldc, char newc)
{
    for (; *s; ++s)
        if (*s == oldc)
            *s = newc;
    return s;
}

// Remove all instances of character 'c' from 'str'
void string_remove_char(char* str, char c)
{
    size_t i, j;
    DEBUG_ASSERT(str);
    size_t len = strlen(str);
    for(i=0; i<len; i++) {
        if(str[i] == c) {
            for(j=i; j<len; j++) {
                str[j] = str[j+1];
            }
            len--;
            i--;
        }
    }
}

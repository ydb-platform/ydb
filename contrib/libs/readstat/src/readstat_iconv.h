#include <iconv.h>

/* ICONV_CONST defined by autotools during configure according
 * to the current platform. Some people copy-paste the source code, so
 * provide some fallback logic */
#ifndef ICONV_CONST
#define ICONV_CONST
#endif

typedef ICONV_CONST char ** readstat_iconv_inbuf_t;

typedef struct readstat_charset_entry_s {
    int     code;
    char    name[32];
} readstat_charset_entry_t;

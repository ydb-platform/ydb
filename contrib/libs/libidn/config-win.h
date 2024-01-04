#pragma once

#include "config-linux.h"

/* Define to 1 if translation of program messages to the user's native
   language is requested. */
#undef ENABLE_NLS

#undef HAVE_STRUCT_STAT_ST_ATIM_TV_NSEC

/* Define if you have <langinfo.h> and nl_langinfo(CODESET). */
#undef HAVE_LANGINFO_CODESET

/* Define if you have the iconv() function and it works. */
#undef HAVE_ICONV

#define strcasecmp stricmp

#undef HAVE_DECL_PROGRAM_INVOCATION_NAME
#undef HAVE_DECL_PROGRAM_INVOCATION_SHORT_NAME
#define HAVE_DECL___ARGV 1

typedef int ssize_t;

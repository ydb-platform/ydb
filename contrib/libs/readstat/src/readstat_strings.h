#if defined(_MSC_VER)
#   define strncasecmp _strnicmp
#   define strcasecmp _stricmp
#else
#   include <strings.h>
#endif

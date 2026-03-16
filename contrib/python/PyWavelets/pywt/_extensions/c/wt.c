#include "wt.h"

#ifdef TYPE
#error TYPE should not be defined here.
#else

#ifdef REAL_TYPE
#error REAL_TYPE should not be defined here.
#else

#define TYPE float
#define REAL_TYPE float
#include "wt.template.c"
#undef REAL_TYPE
#undef TYPE

#define TYPE double
#define REAL_TYPE double
#include "wt.template.c"
#undef REAL_TYPE
#undef TYPE

#ifdef HAVE_C99_COMPLEX
    #define TYPE float_complex
    #define REAL_TYPE float
    #include "wt.template.c"
    #undef REAL_TYPE
    #undef TYPE

    #define TYPE double_complex
    #define REAL_TYPE double
    #include "wt.template.c"
    #undef REAL_TYPE
    #undef TYPE
#endif

#endif /* REAL_TYPE */
#endif /* TYPE */

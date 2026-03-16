#include "cwt.h"

#ifdef TYPE
#error TYPE should not be defined here.
#else


#define TYPE float
#include "cwt.template.c"
#undef TYPE

#define TYPE double
#include "cwt.template.c"
#undef TYPE

#endif /* TYPE */

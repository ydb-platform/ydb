#pragma once

#ifdef TYPE
#error TYPE should not be defined here.
#else

/* ignore warning about initializing floats from double values */
#if defined _MSC_VER
#pragma warning (push)
#pragma warning (disable:4305)
#elif defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

#define TYPE float
#include "wavelets_coeffs.template.h"
#undef TYPE

#if defined _MSC_VER
#pragma warning (pop)
#elif defined __GNUC__
#pragma GCC diagnostic pop
#endif

#define TYPE double
#include "wavelets_coeffs.template.h"
#undef TYPE

#endif /* TYPE */

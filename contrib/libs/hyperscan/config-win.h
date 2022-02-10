#include "config-linux.h"

/* C++ compiler has x86intrin.h */
#undef HAVE_CXX_X86INTRIN_H 

/* C compiler has x86intrin.h */
#undef HAVE_C_X86INTRIN_H 

/* C++ compiler has intrin.h */
#define HAVE_CXX_INTRIN_H

/* C compiler has intrin.h */
#define HAVE_C_INTRIN_H

/* Define if compiler has __builtin_constant_p */
#undef HAVE__BUILTIN_CONSTANT_P

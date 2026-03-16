#if __clang__ && (__GNUC__ == 4) && (__GNUC_MINOR__ == 2)
// sys/cdefs.h defines __extern_always_inline expected by fast math.h only for C
// or GNU C++ >= 4.3, but Clang 7 pretends to be 4.2.
#undef __GNUC_MINOR__
#define __GNUC_MINOR__ 3
#endif

#define __CRFSUITE_BUILD
#include "include/crfsuite.hpp"

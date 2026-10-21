#include "config-linux.h"

#define SOLIB_EXT ".dylib"
#undef WITH_GNULD
#define WITH_OSXLD 1
#undef HAVE_PTHREAD_SETNAME_GNU
#define HAVE_PTHREAD_SETNAME_DARWIN 1

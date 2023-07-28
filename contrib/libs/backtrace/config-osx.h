#pragma once
#include "config-linux.h"

/* Set the defines to use dyld instead of ld on macOS */
#undef HAVE_DL_ITERATE_PHDR
#undef HAVE_LINK_H
#define HAVE_MACH_O_DYLD_H 1

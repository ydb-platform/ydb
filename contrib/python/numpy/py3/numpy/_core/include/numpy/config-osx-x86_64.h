#pragma once

#include "config-osx-arm64.h"

#define HAVE_FTELLO 1
#define HAVE_FSEEKO 1

#undef HAVE_LDOUBLE_IEEE_DOUBLE_LE
#define HAVE_LDOUBLE_INTEL_EXTENDED_16_BYTES_LE 1

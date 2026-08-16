#pragma once

#include "_numpyconfig-osx-arm64.h"

#undef NPY_SIZEOF_LONGDOUBLE
#undef NPY_SIZEOF_COMPLEX_LONGDOUBLE

#define NPY_SIZEOF_LONGDOUBLE 16
#define NPY_SIZEOF_COMPLEX_LONGDOUBLE 32

#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyatomic_std.h>
#else
#error "No <cpython/pyatomic_std.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/tracemalloc.h>
#else
#error "No <cpython/tracemalloc.h> in Python2"
#endif

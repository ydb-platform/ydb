#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_tracemalloc.h>
#else
#error "No <internal/pycore_tracemalloc.h> in Python2"
#endif

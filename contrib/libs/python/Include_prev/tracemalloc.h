#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/tracemalloc.h>
#else
#error "No <tracemalloc.h> in Python2"
#endif

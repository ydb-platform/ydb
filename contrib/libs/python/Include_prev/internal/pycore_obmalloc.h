#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_obmalloc.h>
#else
#error "No <internal/pycore_obmalloc.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_function.h>
#else
#error "No <internal/pycore_function.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_interp.h>
#else
#error "No <internal/pycore_interp.h> in Python2"
#endif

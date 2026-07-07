#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_dtoa.h>
#else
#error "No <internal/pycore_dtoa.h> in Python2"
#endif

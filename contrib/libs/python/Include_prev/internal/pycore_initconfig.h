#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_initconfig.h>
#else
#error "No <internal/pycore_initconfig.h> in Python2"
#endif

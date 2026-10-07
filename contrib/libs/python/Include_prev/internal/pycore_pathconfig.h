#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_pathconfig.h>
#else
#error "No <internal/pycore_pathconfig.h> in Python2"
#endif

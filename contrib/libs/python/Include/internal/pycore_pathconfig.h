#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_pathconfig.h>
#else
#error "No <internal/pycore_pathconfig.h> in Python2"
#endif

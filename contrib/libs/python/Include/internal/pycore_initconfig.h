#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_initconfig.h>
#else
#error "No <internal/pycore_initconfig.h> in Python2"
#endif

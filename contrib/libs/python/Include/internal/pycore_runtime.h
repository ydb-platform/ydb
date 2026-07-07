#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_runtime.h>
#else
#error "No <internal/pycore_runtime.h> in Python2"
#endif

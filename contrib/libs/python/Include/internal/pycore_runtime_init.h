#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_runtime_init.h>
#else
#error "No <internal/pycore_runtime_init.h> in Python2"
#endif

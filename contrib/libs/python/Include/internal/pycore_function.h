#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_function.h>
#else
#error "No <internal/pycore_function.h> in Python2"
#endif

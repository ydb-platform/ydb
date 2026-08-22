#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_gil.h>
#else
#error "No <internal/pycore_gil.h> in Python2"
#endif

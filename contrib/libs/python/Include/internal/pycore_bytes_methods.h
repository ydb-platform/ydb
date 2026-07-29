#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_bytes_methods.h>
#else
#error "No <internal/pycore_bytes_methods.h> in Python2"
#endif

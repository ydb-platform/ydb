#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_mimalloc.h>
#else
#error "No <internal/pycore_mimalloc.h> in Python2"
#endif

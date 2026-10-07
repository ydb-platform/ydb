#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_hamt.h>
#else
#error "No <internal/pycore_hamt.h> in Python2"
#endif

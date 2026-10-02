#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_pyhash.h>
#else
#error "No <internal/pycore_pyhash.h> in Python2"
#endif

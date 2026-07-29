#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_pymem_init.h>
#else
#error "No <internal/pycore_pymem_init.h> in Python2"
#endif

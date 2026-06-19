#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_crossinterp.h>
#else
#error "No <internal/pycore_crossinterp.h> in Python2"
#endif

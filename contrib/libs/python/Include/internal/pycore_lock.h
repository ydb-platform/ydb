#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_lock.h>
#else
#error "No <internal/pycore_lock.h> in Python2"
#endif

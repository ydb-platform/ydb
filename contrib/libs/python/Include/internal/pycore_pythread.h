#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_pythread.h>
#else
#error "No <internal/pycore_pythread.h> in Python2"
#endif

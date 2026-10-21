#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_pythonrun.h>
#else
#error "No <internal/pycore_pythonrun.h> in Python2"
#endif

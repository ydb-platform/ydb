#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_pyerrors.h>
#else
#error "No <internal/pycore_pyerrors.h> in Python2"
#endif

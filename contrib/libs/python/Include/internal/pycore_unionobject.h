#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_unionobject.h>
#else
#error "No <internal/pycore_unionobject.h> in Python2"
#endif

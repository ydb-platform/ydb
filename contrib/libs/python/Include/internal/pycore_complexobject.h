#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_complexobject.h>
#else
#error "No <internal/pycore_complexobject.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_moduleobject.h>
#else
#error "No <internal/pycore_moduleobject.h> in Python2"
#endif

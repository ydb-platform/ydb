#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_global_objects.h>
#else
#error "No <internal/pycore_global_objects.h> in Python2"
#endif

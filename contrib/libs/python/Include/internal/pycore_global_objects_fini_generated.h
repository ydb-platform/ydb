#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_global_objects_fini_generated.h>
#else
#error "No <internal/pycore_global_objects_fini_generated.h> in Python2"
#endif

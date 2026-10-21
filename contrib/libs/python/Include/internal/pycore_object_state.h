#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_object_state.h>
#else
#error "No <internal/pycore_object_state.h> in Python2"
#endif

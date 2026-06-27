#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_object_stack.h>
#else
#error "No <internal/pycore_object_stack.h> in Python2"
#endif

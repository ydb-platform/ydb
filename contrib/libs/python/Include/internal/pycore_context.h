#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/internal/pycore_context.h>
#else
#error "No <internal/pycore_context.h> in Python2"
#endif

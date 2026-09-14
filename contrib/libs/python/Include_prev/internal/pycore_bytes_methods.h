#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_bytes_methods.h>
#else
#error "No <internal/pycore_bytes_methods.h> in Python2"
#endif

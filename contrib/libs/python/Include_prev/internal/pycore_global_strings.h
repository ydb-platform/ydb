#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_global_strings.h>
#else
#error "No <internal/pycore_global_strings.h> in Python2"
#endif

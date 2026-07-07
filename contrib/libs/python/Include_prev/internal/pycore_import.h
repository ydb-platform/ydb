#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_import.h>
#else
#error "No <internal/pycore_import.h> in Python2"
#endif

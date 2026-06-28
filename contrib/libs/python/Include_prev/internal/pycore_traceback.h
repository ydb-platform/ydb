#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_traceback.h>
#else
#error "No <internal/pycore_traceback.h> in Python2"
#endif

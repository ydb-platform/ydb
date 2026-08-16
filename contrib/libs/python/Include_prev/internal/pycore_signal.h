#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_signal.h>
#else
#error "No <internal/pycore_signal.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/internal/pycore_condvar.h>
#else
#error "No <internal/pycore_condvar.h> in Python2"
#endif

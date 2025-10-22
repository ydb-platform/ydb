#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pythonrun.h>
#else
#error "No <cpython/pythonrun.h> in Python2"
#endif

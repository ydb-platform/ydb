#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pythonrun.h>
#else
#error "No <cpython/pythonrun.h> in Python2"
#endif

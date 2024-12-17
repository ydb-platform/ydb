#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyframe.h>
#else
#error "No <cpython/pyframe.h> in Python2"
#endif

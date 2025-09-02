#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyctype.h>
#else
#error "No <cpython/pyctype.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pyctype.h>
#else
#error "No <cpython/pyctype.h> in Python2"
#endif

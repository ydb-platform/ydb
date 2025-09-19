#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pytime.h>
#else
#error "No <cpython/pytime.h> in Python2"
#endif

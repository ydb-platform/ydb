#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/code.h>
#else
#error "No <cpython/code.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/compile.h>
#else
#error "No <cpython/compile.h> in Python2"
#endif

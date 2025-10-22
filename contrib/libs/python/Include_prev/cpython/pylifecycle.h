#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pylifecycle.h>
#else
#error "No <cpython/pylifecycle.h> in Python2"
#endif

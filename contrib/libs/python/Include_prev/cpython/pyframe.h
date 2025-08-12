#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pyframe.h>
#else
#error "No <cpython/pyframe.h> in Python2"
#endif

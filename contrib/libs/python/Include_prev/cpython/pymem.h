#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pymem.h>
#else
#error "No <cpython/pymem.h> in Python2"
#endif

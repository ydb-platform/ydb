#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/classobject.h>
#else
#error "No <cpython/classobject.h> in Python2"
#endif

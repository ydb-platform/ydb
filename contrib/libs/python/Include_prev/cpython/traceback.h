#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/traceback.h>
#else
#error "No <cpython/traceback.h> in Python2"
#endif

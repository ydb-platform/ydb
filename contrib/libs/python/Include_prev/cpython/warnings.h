#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/warnings.h>
#else
#error "No <cpython/warnings.h> in Python2"
#endif

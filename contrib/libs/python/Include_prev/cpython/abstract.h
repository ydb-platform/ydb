#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/abstract.h>
#else
#error "No <cpython/abstract.h> in Python2"
#endif

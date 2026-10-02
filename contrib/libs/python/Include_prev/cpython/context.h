#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/context.h>
#else
#error "No <cpython/context.h> in Python2"
#endif

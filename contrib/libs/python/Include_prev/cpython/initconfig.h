#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/initconfig.h>
#else
#error "No <cpython/initconfig.h> in Python2"
#endif

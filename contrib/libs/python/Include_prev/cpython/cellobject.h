#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/cellobject.h>
#else
#error "No <cpython/cellobject.h> in Python2"
#endif

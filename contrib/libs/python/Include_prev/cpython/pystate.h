#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pystate.h>
#else
#error "No <cpython/pystate.h> in Python2"
#endif

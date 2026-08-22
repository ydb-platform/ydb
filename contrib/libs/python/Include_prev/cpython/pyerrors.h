#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pyerrors.h>
#else
#error "No <cpython/pyerrors.h> in Python2"
#endif

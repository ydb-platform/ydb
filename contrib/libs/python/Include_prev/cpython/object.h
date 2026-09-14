#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/object.h>
#else
#error "No <cpython/object.h> in Python2"
#endif

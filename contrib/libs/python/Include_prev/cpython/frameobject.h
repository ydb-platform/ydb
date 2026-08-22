#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/frameobject.h>
#else
#error "No <cpython/frameobject.h> in Python2"
#endif

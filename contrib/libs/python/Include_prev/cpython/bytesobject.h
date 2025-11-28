#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/bytesobject.h>
#else
#error "No <cpython/bytesobject.h> in Python2"
#endif

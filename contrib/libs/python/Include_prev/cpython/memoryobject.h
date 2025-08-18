#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/memoryobject.h>
#else
#error "No <cpython/memoryobject.h> in Python2"
#endif

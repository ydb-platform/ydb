#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/memoryobject.h>
#else
#error "No <cpython/memoryobject.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/tupleobject.h>
#else
#error "No <cpython/tupleobject.h> in Python2"
#endif

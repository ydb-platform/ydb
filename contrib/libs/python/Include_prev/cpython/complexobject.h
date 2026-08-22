#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/complexobject.h>
#else
#error "No <cpython/complexobject.h> in Python2"
#endif

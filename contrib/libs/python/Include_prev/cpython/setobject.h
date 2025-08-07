#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/setobject.h>
#else
#error "No <cpython/setobject.h> in Python2"
#endif

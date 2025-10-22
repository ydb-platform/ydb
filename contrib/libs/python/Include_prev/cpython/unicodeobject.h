#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/unicodeobject.h>
#else
#error "No <cpython/unicodeobject.h> in Python2"
#endif

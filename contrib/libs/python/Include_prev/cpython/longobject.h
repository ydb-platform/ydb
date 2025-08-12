#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/longobject.h>
#else
#error "No <cpython/longobject.h> in Python2"
#endif

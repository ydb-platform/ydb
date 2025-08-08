#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/interpreteridobject.h>
#else
#error "No <cpython/interpreteridobject.h> in Python2"
#endif

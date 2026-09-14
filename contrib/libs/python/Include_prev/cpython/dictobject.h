#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/dictobject.h>
#else
#error "No <cpython/dictobject.h> in Python2"
#endif

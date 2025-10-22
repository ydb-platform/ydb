#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/weakrefobject.h>
#else
#error "No <cpython/weakrefobject.h> in Python2"
#endif

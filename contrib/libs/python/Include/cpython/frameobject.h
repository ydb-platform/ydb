#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/frameobject.h>
#else
#error "No <cpython/frameobject.h> in Python2"
#endif

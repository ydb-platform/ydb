#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/weakrefobject.h>
#else
#error "No <cpython/weakrefobject.h> in Python2"
#endif

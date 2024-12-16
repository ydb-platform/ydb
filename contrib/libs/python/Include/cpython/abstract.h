#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/abstract.h>
#else
#error "No <cpython/abstract.h> in Python2"
#endif

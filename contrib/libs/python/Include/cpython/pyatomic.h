#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyatomic.h>
#else
#error "No <cpython/pyatomic.h> in Python2"
#endif

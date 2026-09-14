#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pyatomic_gcc.h>
#else
#error "No <cpython/pyatomic_gcc.h> in Python2"
#endif

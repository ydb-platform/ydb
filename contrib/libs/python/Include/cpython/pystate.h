#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/pystate.h>
#else
#error "No <cpython/pystate.h> in Python2"
#endif

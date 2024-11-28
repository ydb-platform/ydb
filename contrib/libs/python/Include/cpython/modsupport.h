#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/modsupport.h>
#else
#error "No <cpython/modsupport.h> in Python2"
#endif

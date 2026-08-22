#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/modsupport.h>
#else
#error "No <cpython/modsupport.h> in Python2"
#endif

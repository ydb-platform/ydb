#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/import.h>
#else
#error "No <cpython/import.h> in Python2"
#endif

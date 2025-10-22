#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pydebug.h>
#else
#error "No <cpython/pydebug.h> in Python2"
#endif

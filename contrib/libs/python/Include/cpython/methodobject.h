#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/methodobject.h>
#else
#error "No <cpython/methodobject.h> in Python2"
#endif

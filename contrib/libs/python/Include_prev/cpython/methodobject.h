#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/methodobject.h>
#else
#error "No <cpython/methodobject.h> in Python2"
#endif

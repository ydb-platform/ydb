#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/funcobject.h>
#else
#error "No <cpython/funcobject.h> in Python2"
#endif

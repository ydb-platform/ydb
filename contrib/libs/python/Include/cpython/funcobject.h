#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/funcobject.h>
#else
#error "No <cpython/funcobject.h> in Python2"
#endif

#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/longobject.h>
#else
#error "No <cpython/longobject.h> in Python2"
#endif

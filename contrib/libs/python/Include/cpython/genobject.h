#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/genobject.h>
#else
#error "No <cpython/genobject.h> in Python2"
#endif

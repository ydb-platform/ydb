#pragma once

#ifdef USE_PYTHON3
#include <contrib/tools/python3/Include/cpython/picklebufobject.h>
#else
#error "No <cpython/picklebufobject.h> in Python2"
#endif

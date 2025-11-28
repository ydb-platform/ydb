#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/picklebufobject.h>
#else
#error "No <cpython/picklebufobject.h> in Python2"
#endif

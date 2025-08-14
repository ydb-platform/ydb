#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/bytearrayobject.h>
#else
#error "No <cpython/bytearrayobject.h> in Python2"
#endif

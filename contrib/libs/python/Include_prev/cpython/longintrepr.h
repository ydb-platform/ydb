#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/longintrepr.h>
#else
#error "No <cpython/longintrepr.h> in Python2"
#endif

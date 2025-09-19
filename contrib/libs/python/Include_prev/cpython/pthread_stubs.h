#pragma once

#ifdef USE_PYTHON3_PREV
#include <contrib/tools/python3_prev/Include/cpython/pthread_stubs.h>
#else
#error "No <cpython/pthread_stubs.h> in Python2"
#endif
